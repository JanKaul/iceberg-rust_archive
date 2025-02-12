/*!
Defining the [Table] struct that represents an iceberg table.
*/

use std::{io::Cursor, sync::Arc};

use futures::future;
use itertools::Itertools;
use manifest::ManifestReader;
use manifest_list::read_snapshot;
use object_store::{path::Path, ObjectStore};

use futures::{stream, Stream, StreamExt, TryFutureExt, TryStreamExt};
use iceberg_rust_spec::util::{self};
use iceberg_rust_spec::{
    spec::{
        manifest::{Content, ManifestEntry},
        manifest_list::ManifestListEntry,
        schema::Schema,
        table_metadata::TableMetadata,
    },
    table_metadata::{
        WRITE_OBJECT_STORAGE_ENABLED, WRITE_PARQUET_COMPRESSION_CODEC,
        WRITE_PARQUET_COMPRESSION_LEVEL,
    },
};

use crate::{
    catalog::{create::CreateTableBuilder, identifier::Identifier, Catalog},
    error::Error,
    object_store::Bucket,
    table::transaction::TableTransaction,
};

pub mod manifest;
pub mod manifest_list;
pub mod transaction;

#[derive(Debug, Clone)]
/// Iceberg table
pub struct Table {
    identifier: Identifier,
    catalog: Arc<dyn Catalog>,
    metadata: TableMetadata,
}

/// Public interface of the table.
impl Table {
    /// Creates a new table builder with default configuration
    ///
    /// Returns a `CreateTableBuilder` initialized with default properties:
    /// * WRITE_PARQUET_COMPRESSION_CODEC: "zstd"
    /// * WRITE_PARQUET_COMPRESSION_LEVEL: "1"
    /// * WRITE_OBJECT_STORAGE_ENABLED: "false"
    ///
    /// # Returns
    /// * `CreateTableBuilder` - A builder for configuring and creating a new table
    ///
    /// # Example
    /// ```
    /// use iceberg_rust::table::Table;
    ///
    /// let builder = Table::builder()
    ///     .with_name("my_table")
    ///     .with_schema(schema);
    /// ```
    pub fn builder() -> CreateTableBuilder {
        let mut builder = CreateTableBuilder::default();
        builder
            .with_property((
                WRITE_PARQUET_COMPRESSION_CODEC.to_owned(),
                "zstd".to_owned(),
            ))
            .with_property((WRITE_PARQUET_COMPRESSION_LEVEL.to_owned(), 1.to_string()))
            .with_property((WRITE_OBJECT_STORAGE_ENABLED.to_owned(), "false".to_owned()));
        builder
    }

    /// Creates a new table instance with the given identifier, catalog and metadata
    ///
    /// # Arguments
    /// * `identifier` - The unique identifier for this table in the catalog
    /// * `catalog` - The catalog that this table belongs to
    /// * `metadata` - The table's metadata containing schema, partitioning, etc.
    ///
    /// # Returns
    /// * `Result<Table, Error>` - The newly created table instance or an error
    ///
    /// This is typically called by catalog implementations rather than directly by users.
    /// For creating new tables, use [`Table::builder()`] instead.
    pub async fn new(
        identifier: Identifier,
        catalog: Arc<dyn Catalog>,
        metadata: TableMetadata,
    ) -> Result<Self, Error> {
        Ok(Table {
            identifier,
            catalog,
            metadata,
        })
    }
    #[inline]
    /// Returns the unique identifier for this table in the catalog
    ///
    /// The identifier contains both the namespace and name that uniquely identify
    /// this table within its catalog.
    ///
    /// # Returns
    /// * `&Identifier` - A reference to this table's identifier
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }
    #[inline]
    /// Returns a reference to the catalog containing this table
    ///
    /// The returned catalog reference is wrapped in an Arc to allow shared ownership
    /// and thread-safe access to the catalog implementation.
    ///
    /// # Returns
    /// * `Arc<dyn Catalog>` - A thread-safe reference to the table's catalog
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog.clone()
    }
    #[inline]
    /// Returns the object store for this table's location
    ///
    /// The object store is determined by the table's location and is used for
    /// reading and writing table data files. The returned store is wrapped in
    /// an Arc to allow shared ownership and thread-safe access.
    ///
    /// # Returns
    /// * `Arc<dyn ObjectStore>` - A thread-safe reference to the table's object store
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.catalog
            .object_store(Bucket::from_path(&self.metadata.location).unwrap())
    }
    #[inline]
    /// Get the schema of the table for a given branch. Defaults to main.
    pub fn current_schema(&self, branch: Option<&str>) -> Result<&Schema, Error> {
        self.metadata.current_schema(branch).map_err(Error::from)
    }
    #[inline]
    /// Get the metadata of the table
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }
    #[inline]
    /// Get the metadata of the table
    pub fn into_metadata(self) -> TableMetadata {
        self.metadata
    }
    /// Get list of current manifest files within an optional snapshot range. The start snapshot is excluded from the range.
    pub async fn manifests(
        &self,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<Vec<ManifestListEntry>, Error> {
        let metadata = self.metadata();
        let end_snapshot = match end.and_then(|id| metadata.snapshots.get(&id)) {
            Some(snapshot) => snapshot,
            None => {
                if let Some(current) = metadata.current_snapshot(None)? {
                    current
                } else {
                    return Ok(vec![]);
                }
            }
        };
        let start_sequence_number =
            start
                .and_then(|id| metadata.snapshots.get(&id))
                .and_then(|snapshot| {
                    let sequence_number = *snapshot.sequence_number();
                    if sequence_number == 0 {
                        None
                    } else {
                        Some(sequence_number)
                    }
                });
        let iter = read_snapshot(end_snapshot, metadata, self.object_store().clone()).await?;
        match start_sequence_number {
            Some(start) => iter
                .filter_ok(|manifest| manifest.sequence_number > start)
                .collect(),
            None => iter.collect(),
        }
    }
    /// Get list of datafiles corresponding to the given manifest files
    #[inline]
    pub async fn datafiles<'a>(
        &self,
        manifests: &'a [ManifestListEntry],
        filter: Option<Vec<bool>>,
        sequence_number_range: (Option<i64>, Option<i64>),
    ) -> Result<impl Stream<Item = Result<ManifestEntry, Error>> + 'a, Error> {
        datafiles(
            self.object_store(),
            manifests,
            filter,
            sequence_number_range,
        )
        .await
    }
    /// Check if datafiles contain deletes
    pub async fn datafiles_contains_delete(
        &self,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<bool, Error> {
        let manifests = self.manifests(start, end).await?;
        let datafiles = self.datafiles(&manifests, None, (None, None)).await?;
        datafiles
            .try_any(|entry| async move { !matches!(entry.data_file().content(), Content::Data) })
            .await
    }
    /// Create a new transaction for this table
    pub fn new_transaction(&mut self, branch: Option<&str>) -> TableTransaction {
        TableTransaction::new(self, branch)
    }
}

async fn datafiles(
    object_store: Arc<dyn ObjectStore>,
    manifests: &'_ [ManifestListEntry],
    filter: Option<Vec<bool>>,
    sequence_number_range: (Option<i64>, Option<i64>),
) -> Result<impl Stream<Item = Result<ManifestEntry, Error>> + '_, Error> {
    // filter manifest files according to filter vector
    let iter: Box<dyn Iterator<Item = &ManifestListEntry> + Send + Sync> = match filter {
        Some(predicate) => {
            let iter = manifests
                .iter()
                .zip(predicate.into_iter())
                .filter(|(_, predicate)| *predicate)
                .map(|(manifest, _)| manifest);
            Box::new(iter)
        }
        None => Box::new(manifests.iter()),
    };

    // Collect a vector of data files by creating a stream over the manifst files, fetch their content and return a flatten stream over their entries.
    Ok(stream::iter(iter)
        .then(move |file| {
            let object_store = object_store.clone();
            async move {
                let path: Path = util::strip_prefix(&file.manifest_path).into();
                let bytes = Cursor::new(Vec::from(
                    object_store
                        .get(&path)
                        .and_then(|file| file.bytes())
                        .await?,
                ));
                Ok::<_, Error>((bytes, file.sequence_number))
            }
        })
        .flat_map_unordered(None, move |result| {
            let (bytes, sequence_number) = result.unwrap();

            let reader = ManifestReader::new(bytes).unwrap();
            stream::iter(reader).try_filter_map(move |mut x| {
                future::ready({
                    let sequence_number = if let Some(sequence_number) = x.sequence_number() {
                        *sequence_number
                    } else {
                        *x.sequence_number_mut() = Some(sequence_number);
                        sequence_number
                    };

                    let filter = match sequence_number_range {
                        (Some(start), Some(end)) => {
                            start < sequence_number && sequence_number <= end
                        }
                        (Some(start), None) => start < sequence_number,
                        (None, Some(end)) => sequence_number <= end,
                        _ => true,
                    };
                    if filter {
                        Ok(Some(x))
                    } else {
                        Ok(None)
                    }
                })
            })
        }))
}

/// delete all datafiles, manifests and metadata files, does not remove table from catalog
pub(crate) async fn delete_all_table_files(
    metadata: &TableMetadata,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(), Error> {
    let Some(snapshot) = metadata.current_snapshot(None)? else {
        return Ok(());
    };
    let manifests: Vec<ManifestListEntry> = read_snapshot(snapshot, metadata, object_store.clone())
        .await?
        .collect::<Result<_, _>>()?;

    let datafiles = datafiles(object_store.clone(), &manifests, None, (None, None)).await?;
    let snapshots = &metadata.snapshots;

    // stream::iter(datafiles.into_iter())
    datafiles
        .try_for_each_concurrent(None, |datafile| {
            let object_store = object_store.clone();
            async move {
                object_store
                    .delete(&datafile.data_file().file_path().as_str().into())
                    .await?;
                Ok(())
            }
        })
        .await?;

    stream::iter(manifests.into_iter())
        .map(Ok::<_, Error>)
        .try_for_each_concurrent(None, |manifest| {
            let object_store = object_store.clone();
            async move {
                object_store.delete(&manifest.manifest_path.into()).await?;
                Ok(())
            }
        })
        .await?;

    stream::iter(snapshots.values())
        .map(Ok::<_, Error>)
        .try_for_each_concurrent(None, |snapshot| {
            let object_store = object_store.clone();
            async move {
                object_store
                    .delete(&snapshot.manifest_list().as_str().into())
                    .await?;
                Ok(())
            }
        })
        .await?;

    Ok(())
}
