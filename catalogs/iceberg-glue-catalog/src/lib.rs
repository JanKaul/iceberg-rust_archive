use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_sdk_glue::{
    types::{DatabaseInput, StorageDescriptor, TableInput},
    Client,
};
use iceberg_rust::{
    catalog::{
        commit::{
            apply_table_updates, apply_view_updates, check_table_requirements,
            check_view_requirements, CommitTable, CommitView, TableRequirement,
        },
        create::{CreateMaterializedView, CreateTable, CreateView},
        identifier::Identifier,
        namespace::Namespace,
        tabular::Tabular,
        Catalog,
    },
    error::Error as IcebergError,
    materialized_view::MaterializedView,
    object_store::{store::IcebergStore, Bucket, ObjectStoreBuilder},
    spec::{
        self,
        identifier::FullIdentifier,
        materialized_view_metadata::MaterializedViewMetadata,
        table_metadata::{new_metadata_location, TableMetadata},
        tabular::TabularMetadata,
        util::strip_prefix,
        view_metadata::ViewMetadata,
    },
    table::Table,
    view::View,
};
use object_store::ObjectStore;
use schema::schema_to_glue;
use crate::utils::{get_parameters, METADATA_LOCATION};

use crate::error::Error;

#[derive(Debug)]
pub struct GlueCatalog {
    name: String,
    client: Client,
    object_store: ObjectStoreBuilder,
    cache: Arc<RwLock<HashMap<Identifier, (String, TabularMetadata)>>>,
}

pub mod error;
pub mod schema;
mod utils;

impl GlueCatalog {
    pub fn new(
        config: &SdkConfig,
        name: &str,
        object_store: ObjectStoreBuilder,
    ) -> Result<Self, Error> {
        Ok(GlueCatalog {
            name: name.to_owned(),
            client: Client::new(config),
            object_store,
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }
}

#[async_trait]
impl Catalog for GlueCatalog {
    /// Catalog name
    fn name(&self) -> &str {
        &self.name
    }
    /// Create a namespace in the catalog
    async fn create_namespace(
        &self,
        namespace: &Namespace,
        _properties: Option<HashMap<String, String>>,
    ) -> Result<HashMap<String, String>, IcebergError> {
        self.client
            .create_database()
            .database_input(
                DatabaseInput::builder()
                    .name(namespace[0].as_str())
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .map_err(Error::from)?;
        Ok(HashMap::new())
    }
    /// Drop a namespace in the catalog
    async fn drop_namespace(&self, _namespace: &Namespace) -> Result<(), IcebergError> {
        todo!()
    }
    /// Load the namespace properties from the catalog
    async fn load_namespace(
        &self,
        _namespace: &Namespace,
    ) -> Result<HashMap<String, String>, IcebergError> {
        todo!()
    }
    /// Update the namespace properties in the catalog
    async fn update_namespace(
        &self,
        _namespace: &Namespace,
        _updates: Option<HashMap<String, String>>,
        _removals: Option<Vec<String>>,
    ) -> Result<(), IcebergError> {
        todo!()
    }
    /// Check if a namespace exists
    async fn namespace_exists(&self, _namespace: &Namespace) -> Result<bool, IcebergError> {
        todo!()
    }
    async fn list_tabulars(&self, namespace: &Namespace) -> Result<Vec<Identifier>, IcebergError> {
        let mut tabulars = Vec::new();
        let mut token = None;

        loop {
            let result = self
                .client
                .get_tables()
                .database_name(&namespace[0])
                .set_next_token(token)
                .send()
                .await
                .map_err(Error::from)?;

            if let Some(new) = result.table_list {
                let new = new
                    .into_iter()
                    .map(|x| Identifier::new(namespace, x.name()));
                tabulars.extend(new);
            }
            token = result.next_token;

            if token.is_none() {
                break;
            }
        }

        Ok(tabulars)
    }
    async fn list_namespaces(&self, parent: Option<&str>) -> Result<Vec<Namespace>, IcebergError> {
        if parent.is_some() {
            return Ok(Vec::new());
        }

        let mut namespaces = Vec::new();
        let mut token = None;

        loop {
            let result = self
                .client
                .get_databases()
                .set_next_token(token)
                .send()
                .await
                .map_err(Error::from)?;

            let new = result
                .database_list
                .into_iter()
                .map(|x| Namespace::try_new(&[x.name; 1]))
                .collect::<Result<Vec<_>, spec::error::Error>>()?;
            namespaces.extend(new.into_iter());

            token = result.next_token;

            if token.is_none() {
                break;
            }
        }

        Ok(namespaces)
    }
    async fn tabular_exists(&self, identifier: &Identifier) -> Result<bool, IcebergError> {
        Ok(self
            .client
            .get_table()
            .database_name(identifier.namespace().to_string())
            .name(identifier.name())
            .send()
            .await
            .map_err(Error::from)
            .is_ok())
    }
    async fn drop_table(&self, identifier: &Identifier) -> Result<(), IcebergError> {
        self.client
            .delete_table()
            .database_name(identifier.namespace().to_string())
            .name(identifier.name())
            .send()
            .await
            .map_err(Error::from)?;

        Ok(())
    }
    async fn drop_view(&self, identifier: &Identifier) -> Result<(), IcebergError> {
        self.client
            .delete_table()
            .database_name(identifier.namespace().to_string())
            .name(identifier.name())
            .send()
            .await
            .map_err(Error::from)?;

        Ok(())
    }
    async fn drop_materialized_view(&self, identifier: &Identifier) -> Result<(), IcebergError> {
        self.client
            .delete_table()
            .database_name(identifier.namespace().to_string())
            .name(identifier.name())
            .send()
            .await
            .map_err(Error::from)?;

        Ok(())
    }
    async fn load_tabular(
        self: Arc<Self>,
        identifier: &Identifier,
    ) -> Result<Tabular, IcebergError> {
        let table = self
            .client
            .get_table()
            .database_name(identifier.namespace().to_string())
            .name(identifier.name())
            .send()
            .await
            .map_err(|_| IcebergError::CatalogNotFound)?
            .table
            .ok_or(IcebergError::NotFound(
                "Glue create table didn't return a table.".to_owned(),
            ))?;

        let metadata_location = table.parameters().
            and_then(|parameter| parameter.get(METADATA_LOCATION))
            .ok_or(IcebergError::NotFound(format!("Glue table {} metadata location not found", &table.name())))?;

        let version_id = table
            .version_id()
            .ok_or(IcebergError::NotFound(
                "Glue create table didn't return a table.".to_owned(),
            ))?
            .to_string();

        let bucket = Bucket::from_path(metadata_location)?;
        let object_store = self.object_store(bucket);

        let bytes = object_store
            .get(&strip_prefix(metadata_location).as_str().into())
            .await?
            .bytes()
            .await?;
        let metadata: TabularMetadata = serde_json::from_slice(&bytes)?;

        self.cache
            .write()
            .unwrap()
            .insert(identifier.clone(), (version_id, metadata.clone()));

        match metadata {
            TabularMetadata::Table(metadata) => Ok(Tabular::Table(
                Table::new(identifier.clone(), self.clone(), metadata).await?,
            )),
            TabularMetadata::View(metadata) => Ok(Tabular::View(
                View::new(identifier.clone(), self.clone(), metadata).await?,
            )),
            TabularMetadata::MaterializedView(metadata) => Ok(Tabular::MaterializedView(
                MaterializedView::new(identifier.clone(), self.clone(), metadata).await?,
            )),
        }
    }

    async fn create_table(
        self: Arc<Self>,
        identifier: Identifier,
        create_table: CreateTable,
    ) -> Result<Table, IcebergError> {
        let metadata: TableMetadata = create_table.try_into()?;
        // Create metadata
        let location = metadata.location.to_string();

        // Write metadata to object_store
        let bucket = Bucket::from_path(&location)?;
        let object_store = self.object_store(bucket);

        let metadata_location = new_metadata_location(&metadata);
        object_store
            .put_metadata(&metadata_location, metadata.as_ref())
            .await?;

        object_store.put_version_hint(&metadata_location).await.ok();

        let schema = metadata.current_schema(None)?;

        self.client
            .create_table()
            .database_name(identifier.namespace().to_string())
            .table_input(
                TableInput::builder()
                    .name(identifier.name())
                    .storage_descriptor(
                        StorageDescriptor::builder()
                            .location(&location)
                            .set_columns(schema_to_glue(schema.fields()).ok())
                            .build()
                    )
                    .set_parameters(Some(get_parameters(&metadata_location)))
                    .build()
                    .map_err(Error::from)?,
            )
            .send()
            .await
            .map_err(Error::from)?;

        let table = self
            .client
            .get_table()
            .database_name(identifier.namespace().to_string())
            .name(identifier.name())
            .send()
            .await
            .map_err(Error::from)?
            .table
            .ok_or(IcebergError::NotFound(
                "Glue create table didn't return a table.".to_owned(),
            ))?;

        self.cache.write().unwrap().insert(
            identifier.clone(),
            (
                table
                    .version_id()
                    .ok_or(IcebergError::NotFound(
                        "Glue create table didn't return a table.".to_owned(),
                    ))?
                    .to_string(),
                metadata.clone().into(),
            ),
        );
        Ok(Table::new(identifier.clone(), self.clone(), metadata).await?)
    }

    async fn create_view(
        self: Arc<Self>,
        identifier: Identifier,
        create_view: CreateView<Option<()>>,
    ) -> Result<View, IcebergError> {
        let metadata: ViewMetadata = create_view.try_into()?;
        // Create metadata
        let location = metadata.location.to_string();

        // Write metadata to object_store
        let bucket = Bucket::from_path(&location)?;
        let object_store = self.object_store(bucket);

        let metadata_location = new_metadata_location(&metadata);
        object_store
            .put_metadata(&metadata_location, metadata.as_ref())
            .await?;

        object_store.put_version_hint(&metadata_location).await.ok();

        let schema = metadata.current_schema(None)?;

        self.client
            .create_table()
            .database_name(identifier.namespace().to_string())
            .table_input(
                TableInput::builder()
                    .name(identifier.name())
                    .storage_descriptor(
                        StorageDescriptor::builder()
                            .location(&metadata_location)
                            .set_columns(schema_to_glue(schema.fields()).ok())
                            .build(),
                    )
                    .build()
                    .map_err(Error::from)?,
            )
            .send()
            .await
            .map_err(Error::from)?;

        let table = self
            .client
            .get_table()
            .database_name(identifier.namespace().to_string())
            .name(identifier.name())
            .send()
            .await
            .map_err(Error::from)?
            .table
            .ok_or(IcebergError::NotFound(
                "Glue create table didn't return a table.".to_owned(),
            ))?;

        self.cache.write().unwrap().insert(
            identifier.clone(),
            (
                table
                    .version_id()
                    .ok_or(IcebergError::NotFound(
                        "Glue create table didn't return a table.".to_owned(),
                    ))?
                    .to_string(),
                metadata.clone().into(),
            ),
        );
        Ok(View::new(identifier.clone(), self.clone(), metadata).await?)
    }

    async fn create_materialized_view(
        self: Arc<Self>,
        identifier: Identifier,
        create_view: CreateMaterializedView,
    ) -> Result<MaterializedView, IcebergError> {
        let (create_view, create_table) = create_view.into();
        let metadata: MaterializedViewMetadata = create_view.try_into()?;
        let table_metadata: TableMetadata = create_table.try_into()?;
        // Create metadata
        let location = metadata.location.to_string();

        // Write metadata to object_store
        let bucket = Bucket::from_path(&location)?;
        let object_store = self.object_store(bucket);

        let metadata_location = new_metadata_location(&metadata);

        let table_metadata_location = new_metadata_location(&table_metadata);
        let table_identifier = metadata.current_version(None)?.storage_table();
        object_store
            .put_metadata(&metadata_location, metadata.as_ref())
            .await?;

        object_store.put_version_hint(&metadata_location).await.ok();

        object_store
            .put_metadata(&table_metadata_location, table_metadata.as_ref())
            .await?;

        let schema = metadata.current_schema(None)?;

        let table_schema = table_metadata.current_schema(None)?;

        // Create view
        self.client
            .create_table()
            .database_name(identifier.namespace().to_string())
            .table_input(
                TableInput::builder()
                    .name(identifier.name())
                    .storage_descriptor(
                        StorageDescriptor::builder()
                            .location(&metadata_location)
                            .set_columns(schema_to_glue(schema.fields()).ok())
                            .build(),
                    )
                    .build()
                    .map_err(Error::from)?,
            )
            .send()
            .await
            .map_err(Error::from)?;

        // Create storage table
        self.client
            .create_table()
            .database_name(table_identifier.namespace().to_string())
            .table_input(
                TableInput::builder()
                    .name(table_identifier.name())
                    .storage_descriptor(
                        StorageDescriptor::builder()
                            .location(&table_metadata_location)
                            .set_columns(schema_to_glue(table_schema.fields()).ok())
                            .build(),
                    )
                    .build()
                    .map_err(Error::from)?,
            )
            .send()
            .await
            .map_err(Error::from)?;

        let table = self
            .client
            .get_table()
            .database_name(identifier.namespace().to_string())
            .name(identifier.name())
            .send()
            .await
            .map_err(Error::from)?
            .table
            .ok_or(IcebergError::NotFound(
                "Glue create table didn't return a table.".to_owned(),
            ))?;

        self.cache.write().unwrap().insert(
            identifier.clone(),
            (
                table
                    .version_id()
                    .ok_or(IcebergError::NotFound(
                        "Glue create table didn't return a table.".to_owned(),
                    ))?
                    .to_string(),
                metadata.clone().into(),
            ),
        );
        Ok(MaterializedView::new(identifier.clone(), self.clone(), metadata).await?)
    }

    async fn update_table(self: Arc<Self>, commit: CommitTable) -> Result<Table, IcebergError> {
        let identifier = commit.identifier;
        let Some(entry) = self.cache.read().unwrap().get(&identifier).cloned() else {
            #[allow(clippy::if_same_then_else)]
            if !matches!(commit.requirements[0], TableRequirement::AssertCreate) {
                return Err(IcebergError::InvalidFormat(
                    "Create table assertion".to_owned(),
                ));
            } else {
                return Err(IcebergError::InvalidFormat(
                    "Create table assertion".to_owned(),
                ));
            }
        };
        let (version_id, metadata) = entry;

        let TabularMetadata::Table(mut metadata) = metadata else {
            return Err(IcebergError::InvalidFormat(
                "Table update on entity that is not a table".to_owned(),
            ));
        };
        if !check_table_requirements(&commit.requirements, &metadata) {
            return Err(IcebergError::InvalidFormat(
                "Table requirements not valid".to_owned(),
            ));
        }
        apply_table_updates(&mut metadata, commit.updates)?;
        let metadata_location = new_metadata_location(&metadata);

        let bucket = Bucket::from_path(&metadata_location)?;
        let object_store = self.object_store(bucket);

        object_store
            .put_metadata(&metadata_location, metadata.as_ref())
            .await?;

        object_store.put_version_hint(&metadata_location).await.ok();

        let schema = metadata.current_schema(None)?;

        self.client
            .update_table()
            .database_name(identifier.namespace().to_string())
            .version_id(version_id)
            .table_input(
                TableInput::builder()
                    .name(identifier.name())
                    .storage_descriptor(
                        StorageDescriptor::builder()
                            .location(&metadata_location)
                            .set_columns(schema_to_glue(schema.fields()).ok())
                            .build(),
                    )
                    .build()
                    .map_err(Error::from)?,
            )
            .send()
            .await
            .map_err(Error::from)?;

        let table = self
            .client
            .get_table()
            .database_name(identifier.namespace().to_string())
            .name(identifier.name())
            .send()
            .await
            .map_err(Error::from)?
            .table
            .ok_or(IcebergError::NotFound(
                "Glue create table didn't return a table.".to_owned(),
            ))?;

        let version_id = table
            .version_id()
            .ok_or(IcebergError::NotFound(
                "Glue create table didn't return a table.".to_owned(),
            ))?
            .to_string();

        let new_metadata_location = table
            .storage_descriptor()
            .and_then(|x| x.location())
            .ok_or(IcebergError::NotFound(format!(
                "Location for table {} not found.",
                identifier.name()
            )))?;

        let metadata = if new_metadata_location == metadata_location {
            metadata
        } else {
            let bytes = object_store
                .get(&strip_prefix(new_metadata_location).as_str().into())
                .await?
                .bytes()
                .await?;
            let metadata: TableMetadata = serde_json::from_slice(&bytes)?;
            metadata
        };

        self.cache
            .write()
            .unwrap()
            .insert(identifier.clone(), (version_id, metadata.clone().into()));

        Ok(Table::new(identifier.clone(), self.clone(), metadata).await?)
    }

    async fn update_view(
        self: Arc<Self>,
        commit: CommitView<Option<()>>,
    ) -> Result<View, IcebergError> {
        let identifier = commit.identifier;
        let Some(entry) = self.cache.read().unwrap().get(&identifier).cloned() else {
            return Err(IcebergError::InvalidFormat(
                "Create table assertion".to_owned(),
            ));
        };
        let (version_id, mut metadata) = entry;

        let metadata_ref = metadata.as_ref();
        let bucket = Bucket::from_path(metadata_ref.location())?;
        let object_store = self.object_store(bucket);

        let metadata_location = match &mut metadata {
            TabularMetadata::View(metadata) => {
                if !check_view_requirements(&commit.requirements, metadata) {
                    return Err(IcebergError::InvalidFormat(
                        "View requirements not valid".to_owned(),
                    ));
                }
                apply_view_updates(metadata, commit.updates)?;
                let metadata_location = new_metadata_location(&*metadata);
                object_store
                    .put_metadata(&metadata_location, metadata.as_ref())
                    .await?;

                object_store.put_version_hint(&metadata_location).await.ok();

                Ok(metadata_location)
            }
            _ => Err(IcebergError::InvalidFormat(
                "View update on entity that is not a view".to_owned(),
            )),
        }?;

        let metadata_ref = metadata.as_ref();
        let schema = metadata_ref.current_schema(None)?;

        self.client
            .update_table()
            .database_name(identifier.namespace().to_string())
            .version_id(version_id)
            .table_input(
                TableInput::builder()
                    .name(identifier.name())
                    .set_parameters(
                        Some(get_parameters(&metadata_location))
                    )
                    .build()
                    .map_err(Error::from)?,
            )
            .send()
            .await
            .map_err(Error::from)?;

        let table = self
            .client
            .get_table()
            .database_name(identifier.namespace().to_string())
            .name(identifier.name())
            .send()
            .await
            .map_err(Error::from)?
            .table
            .ok_or(IcebergError::NotFound(
                "Glue create table didn't return a table.".to_owned(),
            ))?;

        let version_id = table
            .version_id()
            .ok_or(IcebergError::NotFound(
                "Glue create table didn't return a table.".to_owned(),
            ))?
            .to_string();

        let new_metadata_location = table
            .storage_descriptor()
            .and_then(|x| x.location())
            .ok_or(IcebergError::NotFound(format!(
                "Location for table {} not found.",
                identifier.name()
            )))?;

        let metadata = if new_metadata_location == metadata_location {
            metadata
        } else {
            object_store.get_metadata(new_metadata_location).await?
        };

        self.cache
            .write()
            .unwrap()
            .insert(identifier.clone(), (version_id, metadata.clone()));

        if let TabularMetadata::View(metadata) = metadata {
            Ok(View::new(identifier.clone(), self.clone(), metadata).await?)
        } else {
            Err(IcebergError::InvalidFormat(
                "Entity is not a view".to_owned(),
            ))
        }
    }
    async fn update_materialized_view(
        self: Arc<Self>,
        commit: CommitView<FullIdentifier>,
    ) -> Result<MaterializedView, IcebergError> {
        let identifier = commit.identifier;
        let Some(entry) = self.cache.read().unwrap().get(&identifier).cloned() else {
            return Err(IcebergError::InvalidFormat(
                "Create table assertion".to_owned(),
            ));
        };
        let (version_id, mut metadata) = entry;

        let metadata_ref = metadata.as_ref();
        let bucket = Bucket::from_path(metadata_ref.location())?;
        let object_store = self.object_store(bucket);

        let metadata_location = match &mut metadata {
            TabularMetadata::MaterializedView(metadata) => {
                if !check_view_requirements(&commit.requirements, metadata) {
                    return Err(IcebergError::InvalidFormat(
                        "Materialized view requirements not valid".to_owned(),
                    ));
                }
                apply_view_updates(metadata, commit.updates)?;
                let metadata_location = new_metadata_location(&*metadata);
                object_store
                    .put_metadata(&metadata_location, metadata.as_ref())
                    .await?;

                object_store.put_version_hint(&metadata_location).await.ok();

                Ok(metadata_location)
            }
            _ => Err(IcebergError::InvalidFormat(
                "Materialized view update on entity that is not a materialized view".to_owned(),
            )),
        }?;

        let metadata_ref = metadata.as_ref();
        let schema = metadata_ref.current_schema(None)?;

        self.client
            .update_table()
            .database_name(identifier.namespace().to_string())
            .version_id(version_id)
            .table_input(
                TableInput::builder()
                    .name(identifier.name())
                    .set_parameters(
                    Some(get_parameters(&metadata_location))
                    )
                    .build()
                    .map_err(Error::from)?,
            )
            .send()
            .await
            .map_err(Error::from)?;

        let table = self
            .client
            .get_table()
            .database_name(identifier.namespace().to_string())
            .name(identifier.name())
            .send()
            .await
            .map_err(Error::from)?
            .table
            .ok_or(IcebergError::NotFound(
                "Glue create table didn't return a table.".to_owned(),
            ))?;

        let version_id = table
            .version_id()
            .ok_or(IcebergError::NotFound(
                "Glue create table didn't return a table.".to_owned(),
            ))?
            .to_string();

        let new_metadata_location = table
            .storage_descriptor()
            .and_then(|x| x.location())
            .ok_or(IcebergError::NotFound(format!(
                "Location for table {} not found.",
                identifier.name()
            )))?;

        let metadata = if new_metadata_location == metadata_location {
            metadata
        } else {
            object_store.get_metadata(new_metadata_location).await?
        };

        self.cache
            .write()
            .unwrap()
            .insert(identifier.clone(), (version_id, metadata.clone()));
        if let TabularMetadata::MaterializedView(metadata) = metadata {
            Ok(MaterializedView::new(identifier.clone(), self.clone(), metadata).await?)
        } else {
            Err(IcebergError::InvalidFormat(
                "Entity is not a materialized view".to_owned(),
            ))
        }
    }

    async fn register_table(
        self: Arc<Self>,
        identifier: Identifier,
        metadata_location: &str,
    ) -> Result<Table, IcebergError> {
        let bucket = Bucket::from_path(metadata_location)?;
        let object_store = self.object_store(bucket);

        let metadata: TableMetadata = serde_json::from_slice(
            &object_store
                .get(&metadata_location.into())
                .await?
                .bytes()
                .await?,
        )?;

        let schema = metadata.current_schema(None)?;

        self.client
            .create_table()
            .database_name(identifier.namespace().to_string())
            .table_input(
                TableInput::builder()
                    .name(identifier.name())
                    .storage_descriptor(
                        StorageDescriptor::builder()
                            .location(metadata.location.clone())
                            .set_columns(schema_to_glue(schema.fields()).ok())
                            .build(),
                    )
                    .set_parameters(
                        Some(
                            HashMap::from([

                            ])
                        )
                    )
                    .build()
                    .map_err(Error::from)?,
            )
            .send()
            .await
            .map_err(Error::from)?;

        let table = self
            .client
            .get_table()
            .database_name(identifier.namespace().to_string())
            .name(identifier.name())
            .send()
            .await
            .map_err(Error::from)?
            .table
            .ok_or(IcebergError::NotFound(
                "Glue create table didn't return a table.".to_owned(),
            ))?;

        self.cache.write().unwrap().insert(
            identifier.clone(),
            (
                table
                    .version_id()
                    .ok_or(IcebergError::NotFound(
                        "Glue create table didn't return a table.".to_owned(),
                    ))?
                    .to_string(),
                metadata.clone().into(),
            ),
        );
        Ok(Table::new(identifier.clone(), self.clone(), metadata).await?)
    }

    fn object_store(&self, bucket: Bucket) -> Arc<dyn object_store::ObjectStore> {
        Arc::new(self.object_store.build(bucket).unwrap())
    }
}

#[cfg(test)]
pub mod tests {
    use aws_config::{BehaviorVersion, Region};
    use datafusion::{
        arrow::array::{Float64Array, Int64Array},
        common::tree_node::{TransformedResult, TreeNode},
        execution::SessionStateBuilder,
        prelude::SessionContext,
    };
    use datafusion_iceberg::{
        catalog::catalog::IcebergCatalog,
        planner::{iceberg_transform, IcebergQueryPlanner},
    };
    use iceberg_rust::{
        catalog::{namespace::Namespace, Catalog},
        object_store::ObjectStoreBuilder,
        spec::util::strip_prefix,
    };
    use testcontainers_modules::localstack::LocalStack;
    use tokio::time::sleep;

    use std::{sync::Arc, time::Duration};
    use testcontainers::{
        core::{wait::LogWaitStrategy, ContainerPort, ExecCommand, WaitFor},
        runners::AsyncRunner,
        GenericImage, ImageExt,
    };

    use crate::GlueCatalog;

    #[tokio::test]
    async fn test_create_update_drop_table() {
        let moto = GenericImage::new("motoserver/moto", "5.0.20")
            .with_exposed_port(ContainerPort::Tcp(5000))
            .with_wait_for(WaitFor::Log(LogWaitStrategy::stderr(
                " * Running on all addresses (0.0.0.0)",
            )))
            .start()
            .await
            .unwrap();

        let moto_host = moto.get_host().await.unwrap();
        let moto_port = moto.get_host_port_ipv4(5000).await.unwrap();

        let config = aws_config::defaults(BehaviorVersion::v2024_03_28())
            .endpoint_url(
                "http://".to_owned() + &moto_host.to_string() + ":" + &moto_port.to_string(),
            )
            .region(Region::new("us-east-1"))
            .test_credentials()
            .load()
            .await;

        let localstack = LocalStack::default()
            .with_env_var("SERVICES", "s3")
            .with_env_var("AWS_ACCESS_KEY_ID", "user")
            .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
            .start()
            .await
            .unwrap();

        let command = localstack
            .exec(ExecCommand::new(vec![
                "awslocal",
                "s3api",
                "create-bucket",
                "--bucket",
                "warehouse",
            ]))
            .await
            .unwrap();

        while command.exit_code().await.unwrap().is_none() {
            sleep(Duration::from_millis(100)).await;
        }

        let localstack_host = localstack.get_host().await.unwrap();
        let localstack_port = localstack.get_host_port_ipv4(4566).await.unwrap();

        let object_store = ObjectStoreBuilder::s3()
            .with_config("aws_access_key_id".parse().unwrap(), "user")
            .with_config("aws_secret_access_key".parse().unwrap(), "password")
            .with_config(
                "endpoint".parse().unwrap(),
                format!("http://{}:{}", localstack_host, localstack_port),
            )
            .with_config("region".parse().unwrap(), "us-east-1")
            .with_config("allow_http".parse().unwrap(), "true");

        // let object_store = ObjectStoreBuilder::memory();
        let iceberg_catalog: Arc<dyn Catalog> =
            Arc::new(GlueCatalog::new(&config, "warehouse", object_store).unwrap());

        iceberg_catalog
            .create_namespace(&Namespace::try_new(&["tpch".to_owned()]).unwrap(), None)
            .await
            .expect("Failed to create namespace");

        let namespaces = iceberg_catalog
            .clone()
            .list_namespaces(None)
            .await
            .expect("Failed to list namespaces");
        assert_eq!(namespaces[0].to_string(), "tpch");

        let catalog = Arc::new(
            IcebergCatalog::new(iceberg_catalog.clone(), None)
                .await
                .unwrap(),
        );

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(IcebergQueryPlanner {}))
            .build();

        let ctx = SessionContext::new_with_state(state);

        ctx.register_catalog("warehouse", catalog);

        let sql = "CREATE EXTERNAL TABLE lineitem ( 
    L_ORDERKEY BIGINT NOT NULL, 
    L_PARTKEY BIGINT NOT NULL, 
    L_SUPPKEY BIGINT NOT NULL, 
    L_LINENUMBER INT NOT NULL, 
    L_QUANTITY DOUBLE NOT NULL, 
    L_EXTENDED_PRICE DOUBLE NOT NULL, 
    L_DISCOUNT DOUBLE NOT NULL, 
    L_TAX DOUBLE NOT NULL, 
    L_RETURNFLAG CHAR NOT NULL, 
    L_LINESTATUS CHAR NOT NULL, 
    L_SHIPDATE DATE NOT NULL, 
    L_COMMITDATE DATE NOT NULL, 
    L_RECEIPTDATE DATE NOT NULL, 
    L_SHIPINSTRUCT VARCHAR NOT NULL, 
    L_SHIPMODE VARCHAR NOT NULL, 
    L_COMMENT VARCHAR NOT NULL ) STORED AS CSV LOCATION '../../datafusion_iceberg/testdata/tpch/lineitem.csv' OPTIONS ('has_header' 'false');";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let sql = "CREATE EXTERNAL TABLE warehouse.tpch.lineitem ( 
    L_ORDERKEY BIGINT NOT NULL, 
    L_PARTKEY BIGINT NOT NULL, 
    L_SUPPKEY BIGINT NOT NULL, 
    L_LINENUMBER INT NOT NULL, 
    L_QUANTITY DOUBLE NOT NULL, 
    L_EXTENDED_PRICE DOUBLE NOT NULL, 
    L_DISCOUNT DOUBLE NOT NULL, 
    L_TAX DOUBLE NOT NULL, 
    L_RETURNFLAG CHAR NOT NULL, 
    L_LINESTATUS CHAR NOT NULL, 
    L_SHIPDATE DATE NOT NULL, 
    L_COMMITDATE DATE NOT NULL, 
    L_RECEIPTDATE DATE NOT NULL, 
    L_SHIPINSTRUCT VARCHAR NOT NULL, 
    L_SHIPMODE VARCHAR NOT NULL, 
    L_COMMENT VARCHAR NOT NULL ) STORED AS ICEBERG LOCATION 's3://warehouse/tpch/lineitem' PARTITIONED BY ( \"month(L_SHIPDATE)\" );";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let tables = iceberg_catalog
            .clone()
            .list_tabulars(
                &Namespace::try_new(&["tpch".to_owned()]).expect("Failed to create namespace"),
            )
            .await
            .expect("Failed to list Tables");
        assert_eq!(tables[0].to_string(), "tpch.lineitem".to_owned());

        let sql = "insert into warehouse.tpch.lineitem select * from lineitem;";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let batches = ctx
        .sql("select sum(L_QUANTITY), L_PARTKEY from warehouse.tpch.lineitem group by L_PARTKEY;")
        .await
        .expect("Failed to create plan for select")
        .collect()
        .await
        .expect("Failed to execute select query");

        let mut once = false;

        for batch in batches {
            if batch.num_rows() != 0 {
                let (amounts, product_ids) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                );
                for (product_id, amount) in product_ids.iter().zip(amounts) {
                    if product_id.unwrap() == 24027 {
                        assert_eq!(amount.unwrap(), 24.0)
                    } else if product_id.unwrap() == 63700 {
                        assert_eq!(amount.unwrap(), 8.0)
                    }
                }
                once = true
            }
        }

        assert!(once);

        let object_store =
            iceberg_catalog.object_store(iceberg_rust::object_store::Bucket::S3("warehouse"));

        let version_hint = object_store
            .get(&strip_prefix("s3://warehouse/tpch/lineitem/metadata/version-hint.text").into())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        assert!(std::str::from_utf8(&version_hint)
            .unwrap()
            .ends_with(".metadata.json"));

        moto.rm().await.unwrap();
        localstack.rm().await.unwrap();
    }
}
