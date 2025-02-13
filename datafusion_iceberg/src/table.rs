/*!
 * Tableprovider to use iceberg table with datafusion.
*/

use async_trait::async_trait;
use chrono::DateTime;
use datafusion_expr::{dml::InsertOp, utils::conjunction, JoinType};
use futures::{stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use object_store::ObjectMeta;
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    fmt,
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio::sync::{RwLock, RwLockWriteGuard};

use datafusion::{
    arrow::datatypes::{Field, Schema as ArrowSchema, SchemaRef},
    catalog::Session,
    common::{not_impl_err, plan_err, Constraints, DataFusionError, SchemaExt},
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        listing::PartitionedFile,
        object_store::ObjectStoreUrl,
        physical_plan::FileScanConfig,
        TableProvider, ViewTable,
    },
    execution::{context::SessionState, TaskContext},
    logical_expr::{TableProviderFilterPushDown, TableType},
    physical_expr::create_physical_expr,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{
        expressions::Column,
        insert::{DataSink, DataSinkExec},
        joins::{HashJoinExec, PartitionMode},
        metrics::MetricsSet,
        projection::ProjectionExec,
        union::UnionExec,
        DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, SendableRecordBatchStream,
        Statistics,
    },
    prelude::Expr,
    scalar::ScalarValue,
    sql::parser::DFParser,
};

use crate::{
    error::Error as DataFusionIcebergError,
    pruning_statistics::{transform_predicate, PruneDataFiles, PruneManifests},
    statistics::manifest_statistics,
};

use iceberg_rust::spec::{
    arrow::schema::PARQUET_FIELD_ID_META_KEY,
    manifest::{Content, ManifestEntry, Status},
    util,
    values::{Struct, Value},
};
use iceberg_rust::spec::{schema::Schema, view_metadata::ViewRepresentation};
use iceberg_rust::{
    arrow::write::write_parquet_partitioned, catalog::tabular::Tabular, error::Error,
    materialized_view::MaterializedView, table::Table, view::View,
};
// mod value;

#[derive(Debug, Clone)]
/// Iceberg table for datafusion
pub struct DataFusionTable {
    pub tabular: Arc<RwLock<Tabular>>,
    pub schema: SchemaRef,
    pub snapshot_range: (Option<i64>, Option<i64>),
    pub branch: Option<String>,
}

impl From<Tabular> for DataFusionTable {
    fn from(value: Tabular) -> Self {
        Self::new(value, None, None, None)
    }
}

impl From<Table> for DataFusionTable {
    fn from(value: Table) -> Self {
        Self::new(Tabular::Table(value), None, None, None)
    }
}

impl From<View> for DataFusionTable {
    fn from(value: View) -> Self {
        Self::new(Tabular::View(value), None, None, None)
    }
}

impl From<MaterializedView> for DataFusionTable {
    fn from(value: MaterializedView) -> Self {
        Self::new(Tabular::MaterializedView(value), None, None, None)
    }
}

impl DataFusionTable {
    pub fn new(
        tabular: Tabular,
        start: Option<i64>,
        end: Option<i64>,
        branch: Option<&str>,
    ) -> Self {
        let schema = match &tabular {
            Tabular::Table(table) => {
                let schema = end
                    .and_then(|snapshot_id| table.metadata().schema(snapshot_id).ok().cloned())
                    .unwrap_or_else(|| table.current_schema(None).unwrap().clone());
                Arc::new((schema.fields()).try_into().unwrap())
            }
            Tabular::View(view) => {
                let schema = end
                    .and_then(|version_id| view.metadata().schema(version_id).ok().cloned())
                    .unwrap_or_else(|| view.current_schema(None).unwrap().clone());
                Arc::new((schema.fields()).try_into().unwrap())
            }
            Tabular::MaterializedView(matview) => {
                let schema = end
                    .and_then(|version_id| matview.metadata().schema(version_id).ok().cloned())
                    .unwrap_or_else(|| matview.current_schema(None).unwrap().clone());
                Arc::new((schema.fields()).try_into().unwrap())
            }
        };
        DataFusionTable {
            tabular: Arc::new(RwLock::new(tabular)),
            snapshot_range: (start, end),
            schema,
            branch: branch.map(ToOwned::to_owned),
        }
    }
    #[inline]
    pub fn new_table(
        table: Table,
        start: Option<i64>,
        end: Option<i64>,
        branch: Option<&str>,
    ) -> Self {
        Self::new(Tabular::Table(table), start, end, branch)
    }

    pub async fn inner_mut(&self) -> RwLockWriteGuard<'_, Tabular> {
        self.tabular.write().await
    }
}

#[async_trait]
impl TableProvider for DataFusionTable {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let session_state = session.as_any().downcast_ref::<SessionState>().unwrap();
        match self.tabular.read().await.deref() {
            Tabular::View(view) => {
                let metadata = view.metadata();
                let version = self
                    .snapshot_range
                    .1
                    .and_then(|version_id| metadata.versions.get(&version_id))
                    .unwrap_or(
                        metadata
                            .current_version(None)
                            .map_err(DataFusionIcebergError::from)?,
                    );
                let sql = match &version.representations[0] {
                    ViewRepresentation::Sql { sql, .. } => sql,
                };
                let statement = DFParser::new(sql)?.parse_statement()?;
                let logical_plan = session_state.statement_to_plan(statement).await?;
                ViewTable::try_new(logical_plan, Some(sql.clone()))?
                    .scan(session, projection, filters, limit)
                    .await
            }
            Tabular::Table(table) => {
                let schema = self.schema();
                let statistics = self
                    .statistics()
                    .await
                    .map_err(DataFusionIcebergError::from)?;
                table_scan(
                    table,
                    &self.snapshot_range,
                    schema,
                    statistics,
                    session_state,
                    projection,
                    filters,
                    limit,
                )
                .await
            }
            Tabular::MaterializedView(mv) => {
                let table = mv
                    .storage_table()
                    .await
                    .map_err(DataFusionIcebergError::from)?;
                let schema = self.schema();
                let statistics = self
                    .statistics()
                    .await
                    .map_err(DataFusionIcebergError::from)?;
                table_scan(
                    &table,
                    &self.snapshot_range,
                    schema,
                    statistics,
                    session_state,
                    projection,
                    filters,
                    limit,
                )
                .await
            }
        }
    }
    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Create a physical plan from the logical plan.
        // Check that the schema of the plan matches the schema of this table.
        if !self.schema().equivalent_names_and_types(&input.schema()) {
            return plan_err!("Inserting query must have the same schema with the table.");
        }
        let InsertOp::Append = insert_op else {
            return not_impl_err!("Overwrite not implemented for MemoryTable yet");
        };
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(self.clone().into_data_sink()),
            None,
        )))
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }
}

#[allow(clippy::too_many_arguments)]
async fn table_scan(
    table: &Table,
    snapshot_range: &(Option<i64>, Option<i64>),
    arrow_schema: SchemaRef,
    statistics: Statistics,
    session: &SessionState,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let schema = snapshot_range
        .1
        .and_then(|snapshot_id| table.metadata().schema(snapshot_id).ok().cloned())
        .unwrap_or_else(|| table.current_schema(None).unwrap().clone());

    // Create a unique URI for this particular object store
    let object_store_url = ObjectStoreUrl::parse(&table.metadata().location)
        .unwrap_or_else(|_| ObjectStoreUrl::local_filesystem());
    session
        .runtime_env()
        .register_object_store(object_store_url.as_ref(), table.object_store());

    let partition_fields = &snapshot_range
        .1
        .and_then(|snapshot_id| table.metadata().partition_fields(snapshot_id).ok())
        .unwrap_or_else(|| table.metadata().current_partition_fields(None).unwrap());

    let sequence_number_range = [snapshot_range.0, snapshot_range.1]
        .iter()
        .map(|x| x.and_then(|y| table.metadata().sequence_number(y)))
        .collect_tuple::<(Option<i64>, Option<i64>)>()
        .unwrap();

    // If there is a filter expression the manifests to read are pruned based on the pruning statistics available in the manifest_list file.
    let physical_predicate = if let Some(predicate) = conjunction(filters.iter().cloned()) {
        Some(create_physical_expr(
            &predicate,
            &arrow_schema.as_ref().clone().try_into()?,
            session.execution_props(),
        )?)
    } else {
        None
    };

    // Get all partition columns
    let table_partition_cols: Vec<Field> = partition_fields
        .iter()
        .map(|partition_field| {
            Ok(Field::new(
                partition_field.name().to_owned(),
                (&partition_field
                    .field_type()
                    .tranform(partition_field.transform())
                    .map_err(DataFusionIcebergError::from)?)
                    .try_into()
                    .map_err(DataFusionIcebergError::from)?,
                !partition_field.required(),
            )
            .with_metadata(HashMap::from_iter(vec![(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                partition_field.field_id().to_string(),
            )])))
        })
        .collect::<Result<Vec<_>, DataFusionError>>()
        .map_err(DataFusionIcebergError::from)?;

    // All files have to be grouped according to their partition values. This is done by using a HashMap with the partition values as the key.
    // This way data files with the same partition value are mapped to the same vector.
    let mut data_file_groups: HashMap<Struct, Vec<ManifestEntry>> = HashMap::new();
    let mut equality_delete_file_groups: HashMap<Struct, Vec<ManifestEntry>> = HashMap::new();

    // Prune data & delete file and insert them into the according map
    if let Some(physical_predicate) = physical_predicate.clone() {
        let partition_schema = Arc::new(ArrowSchema::new(table_partition_cols.clone()));
        let partition_column_names = partition_fields
            .iter()
            .map(|field| Ok(field.source_name().to_owned()))
            .collect::<Result<HashSet<_>, Error>>()
            .map_err(DataFusionIcebergError::from)?;

        let partition_predicates = conjunction(
            filters
                .iter()
                .filter(|expr| {
                    let set: HashSet<String> = expr
                        .column_refs()
                        .into_iter()
                        .map(|x| x.name.clone())
                        .collect();
                    set.is_subset(&partition_column_names)
                })
                .cloned()
                .map(|x| transform_predicate(x, partition_fields).unwrap()),
        );

        let manifests = table
            .manifests(snapshot_range.0, snapshot_range.1)
            .await
            .map_err(Into::<Error>::into)
            .map_err(DataFusionIcebergError::from)?;

        // If there is a filter expression on the partition column, the manifest files to read are pruned.
        let data_files: Vec<ManifestEntry> = if let Some(predicate) = partition_predicates {
            let physical_partition_predicate = create_physical_expr(
                &predicate,
                &partition_schema.clone().try_into()?,
                session.execution_props(),
            )?;
            let pruning_predicate =
                PruningPredicate::try_new(physical_partition_predicate, partition_schema.clone())?;
            let manifests_to_prune =
                pruning_predicate.prune(&PruneManifests::new(partition_fields, &manifests))?;

            table
                .datafiles(&manifests, Some(manifests_to_prune), sequence_number_range)
                .await
                .map_err(DataFusionIcebergError::from)?
                .try_collect()
                .await
                .map_err(DataFusionIcebergError::from)?
        } else {
            table
                .datafiles(&manifests, None, sequence_number_range)
                .await
                .map_err(DataFusionIcebergError::from)?
                .try_collect()
                .await
                .map_err(DataFusionIcebergError::from)?
        };

        let pruning_predicate =
            PruningPredicate::try_new(physical_predicate, arrow_schema.clone())?;
        // After the first pruning stage the data_files are pruned again based on the pruning statistics in the manifest files.
        let files_to_prune = pruning_predicate.prune(&PruneDataFiles::new(
            &schema,
            &partition_schema,
            &data_files,
        ))?;

        data_files
            .into_iter()
            .zip(files_to_prune.into_iter())
            .for_each(|(manifest, prune_file)| {
                if prune_file && *manifest.status() != Status::Deleted {
                    match manifest.data_file().content() {
                        Content::Data => {
                            data_file_groups
                                .entry(manifest.data_file().partition().clone())
                                .or_default()
                                .push(manifest);
                        }
                        Content::EqualityDeletes => {
                            equality_delete_file_groups
                                .entry(manifest.data_file().partition().clone())
                                .or_default()
                                .push(manifest);
                        }
                        Content::PositionDeletes => {
                            panic!("Position deletes not supported.")
                        }
                    }
                };
            });
    } else {
        let manifests = table
            .manifests(snapshot_range.0, snapshot_range.1)
            .await
            .map_err(DataFusionIcebergError::from)?;
        let data_files: Vec<ManifestEntry> = table
            .datafiles(&manifests, None, sequence_number_range)
            .await
            .map_err(DataFusionIcebergError::from)?
            .try_collect()
            .await
            .map_err(DataFusionIcebergError::from)?;
        data_files.into_iter().for_each(|manifest| {
            if *manifest.status() != Status::Deleted {
                match manifest.data_file().content() {
                    Content::Data => {
                        data_file_groups
                            .entry(manifest.data_file().partition().clone())
                            .or_default()
                            .push(manifest);
                    }
                    Content::EqualityDeletes => {
                        equality_delete_file_groups
                            .entry(manifest.data_file().partition().clone())
                            .or_default()
                            .push(manifest);
                    }
                    Content::PositionDeletes => {
                        panic!("Position deletes not supported.")
                    }
                }
            }
        });
    };

    let file_schema: SchemaRef = Arc::new((schema.fields()).try_into().unwrap());

    let projection = projection
        .cloned()
        .or_else(|| Some(schema.iter().enumerate().map(|(i, _)| i).collect()));

    let projection_expr: Option<Vec<_>> = projection.as_ref().map(|projection| {
        projection
            .iter()
            .enumerate()
            .map(|(i, id)| {
                let name = file_schema.fields[*id].name();
                (
                    Arc::new(Column::new(name, i)) as Arc<dyn PhysicalExpr>,
                    name.to_owned(),
                )
            })
            .collect()
    });

    // Create plan for every parition with delete files
    let mut plans = stream::iter(equality_delete_file_groups.into_iter())
        .then(|(partition_value, mut delete_files)| {
            let object_store_url = object_store_url.clone();
            let table_partition_cols = table_partition_cols.clone();
            let statistics = statistics.clone();
            let physical_predicate = physical_predicate.clone();
            let schema = &schema;
            let file_schema = file_schema.clone();
            let projection_expr = projection_expr.clone();
            let projection = &projection;
            let mut data_files = data_file_groups
                .remove(&partition_value)
                .unwrap_or_default();
            async move {
                // Sort data & delete files by sequence_number
                delete_files.sort_by(|x, y| {
                    x.sequence_number()
                        .unwrap()
                        .cmp(&y.sequence_number().unwrap())
                });
                data_files.sort_by(|x, y| {
                    x.sequence_number()
                        .unwrap()
                        .cmp(&y.sequence_number().unwrap())
                });

                let mut data_file_iter = data_files.into_iter().peekable();

                let mut plan = stream::iter(delete_files.iter())
                    .map(Ok::<_, DataFusionError>)
                    .try_fold(None, |acc, delete_manifest| {
                        let object_store_url = object_store_url.clone();
                        let table_partition_cols = table_partition_cols.clone();
                        let statistics = statistics.clone();
                        let physical_predicate = physical_predicate.clone();
                        let schema = &schema;
                        let file_schema = file_schema.clone();
                        let mut data_files = Vec::new();
                        while let Some(data_manifest) = data_file_iter.next_if(|x| {
                            x.sequence_number().unwrap()
                                < delete_manifest.sequence_number().unwrap()
                        }) {
                            let last_updated_ms = table.metadata().last_updated_ms;
                            let data_file =
                                generate_partitioned_file(schema, &data_manifest, last_updated_ms)
                                    .unwrap();
                            data_files.push(data_file);
                        }
                        async move {
                            let delete_schema = schema.project(
                                delete_manifest.data_file().equality_ids().as_ref().unwrap(),
                            );
                            let delete_file_schema: SchemaRef =
                                Arc::new((delete_schema.fields()).try_into().unwrap());
                            let equality_projection: Option<Vec<usize>> =
                                match (&projection, delete_manifest.data_file().equality_ids()) {
                                    (Some(projection), Some(equality_ids)) => {
                                        let collect: Vec<usize> = schema
                                            .iter()
                                            .enumerate()
                                            .filter_map(|(id, x)| {
                                                if equality_ids.contains(&x.id)
                                                    && !projection.contains(&id)
                                                {
                                                    Some(id)
                                                } else {
                                                    None
                                                }
                                            })
                                            .collect();
                                        Some([projection.as_slice(), &collect].concat())
                                    }
                                    _ => None,
                                };

                            let last_updated_ms = table.metadata().last_updated_ms;
                            let delete_file = generate_partitioned_file(
                                &delete_schema,
                                delete_manifest,
                                last_updated_ms,
                            )?;

                            let delete_file_scan_config = FileScanConfig {
                                object_store_url: object_store_url.clone(),
                                file_schema: delete_file_schema,
                                file_groups: vec![vec![delete_file]],
                                constraints: Constraints::empty(),
                                statistics: statistics.clone(),
                                projection: None,
                                limit,
                                table_partition_cols: table_partition_cols.clone(),
                                output_ordering: vec![],
                            };

                            let left = ParquetFormat::default()
                                .create_physical_plan(
                                    session,
                                    delete_file_scan_config,
                                    physical_predicate.as_ref(),
                                )
                                .await?;

                            let file_scan_config = FileScanConfig {
                                object_store_url,
                                file_schema: file_schema.clone(),
                                file_groups: vec![data_files],
                                constraints: Constraints::empty(),
                                statistics,
                                projection: equality_projection,
                                limit,
                                table_partition_cols,
                                output_ordering: vec![],
                            };

                            let data_files_scan = ParquetFormat::default()
                                .create_physical_plan(
                                    session,
                                    file_scan_config,
                                    physical_predicate.as_ref(),
                                )
                                .await?;

                            let right = if let Some(acc) = acc {
                                Arc::new(UnionExec::new(vec![acc, data_files_scan]))
                            } else {
                                data_files_scan
                            };

                            let join_on = delete_manifest
                                .data_file()
                                .equality_ids()
                                .as_ref()
                                .unwrap()
                                .iter()
                                .map(|id| {
                                    let column_name =
                                        &schema.get(*id as usize).as_ref().unwrap().name;
                                    let left_column: Arc<dyn PhysicalExpr> = Arc::new(
                                        Column::new_with_schema(column_name, &left.schema())?,
                                    );
                                    let right_column: Arc<dyn PhysicalExpr> = Arc::new(
                                        Column::new_with_schema(column_name, &right.schema())?,
                                    );
                                    Ok((left_column, right_column))
                                })
                                .collect::<Result<Vec<_>, DataFusionError>>()?;

                            Ok(Some(Arc::new(HashJoinExec::try_new(
                                left,
                                right,
                                join_on,
                                None,
                                &JoinType::RightAnti,
                                None,
                                PartitionMode::CollectLeft,
                                false,
                            )?)
                                as Arc<dyn ExecutionPlan>))
                        }
                    })
                    .await
                    .transpose()
                    .ok_or(DataFusionError::External(Box::new(Error::InvalidFormat(
                        "Delete plan".to_owned(),
                    ))))??;

                let additional_data_files = data_file_iter
                    .map(|x| {
                        let last_updated_ms = table.metadata().last_updated_ms;
                        generate_partitioned_file(schema, &x, last_updated_ms)
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                if !additional_data_files.is_empty() {
                    let file_scan_config = FileScanConfig {
                        object_store_url,
                        file_schema: file_schema.clone(),
                        file_groups: vec![additional_data_files],
                        constraints: Constraints::empty(),
                        statistics,
                        projection: projection.as_ref().cloned(),
                        limit,
                        table_partition_cols,
                        output_ordering: vec![],
                    };

                    let data_files_scan = ParquetFormat::default()
                        .create_physical_plan(
                            session,
                            file_scan_config,
                            physical_predicate.as_ref(),
                        )
                        .await?;

                    plan = Arc::new(UnionExec::new(vec![plan, data_files_scan]));
                }

                if let Some(projection_expr) = projection_expr {
                    Ok::<_, DataFusionError>(Arc::new(ProjectionExec::try_new(
                        projection_expr,
                        plan,
                    )?) as Arc<dyn ExecutionPlan>)
                } else {
                    Ok(plan)
                }
            }
        })
        .try_collect::<Vec<_>>()
        .await?;

    // Create plan for partitions without delete files
    let file_scan_config = FileScanConfig {
        object_store_url,
        file_schema,
        file_groups: data_file_groups
            .into_values()
            .map(|x| {
                x.into_iter()
                    .map(|x| {
                        let last_updated_ms = table.metadata().last_updated_ms;
                        generate_partitioned_file(&schema, &x, last_updated_ms).unwrap()
                    })
                    .collect()
            })
            .collect(),
        constraints: Constraints::empty(),
        statistics,
        projection,
        limit,
        table_partition_cols,
        output_ordering: vec![],
    };

    let other_plan = ParquetFormat::default()
        .create_physical_plan(session, file_scan_config, physical_predicate.as_ref())
        .await?;

    if plans.is_empty() {
        Ok(other_plan)
    } else {
        plans.push(other_plan);

        Ok(Arc::new(UnionExec::new(plans)))
    }
}

impl DisplayAs for DataFusionTable {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "IcebergTable")
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct IcebergDataSink(DataFusionTable);

impl DataFusionTable {
    pub(crate) fn into_data_sink(self) -> IcebergDataSink {
        IcebergDataSink(self)
    }
}

impl DisplayAs for IcebergDataSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt_as(t, f)
    }
}

#[async_trait]
impl DataSink for IcebergDataSink {
    fn as_any(&self) -> &dyn Any {
        self.0.as_any()
    }
    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64, DataFusionError> {
        let mut lock = self.0.tabular.write().await;
        let table = if let Tabular::Table(table) = lock.deref_mut() {
            Ok(table)
        } else {
            Err(Error::InvalidFormat("database entity".to_string()))
        }
        .map_err(DataFusionIcebergError::from)?;

        let metadata_files =
            write_parquet_partitioned(table, data.map_err(Into::into), self.0.branch.as_deref())
                .await?;

        table
            .new_transaction(self.0.branch.as_deref())
            .append_data(metadata_files)
            .commit()
            .await
            .map_err(DataFusionIcebergError::from)?;

        Ok(0)
    }
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }
    fn schema(&self) -> &SchemaRef {
        &self.0.schema
    }
}

fn generate_partitioned_file(
    schema: &Schema,
    manifest: &ManifestEntry,
    last_updated_ms: i64,
) -> Result<PartitionedFile, DataFusionError> {
    let manifest_statistics = manifest_statistics(schema, manifest);
    let partition_values = manifest
        .data_file()
        .partition()
        .iter()
        .map(|x| {
            x.as_ref()
                .map(value_to_scalarvalue)
                .unwrap_or(Ok(ScalarValue::Null))
        })
        .collect::<Result<Vec<ScalarValue>, _>>()?;
    let object_meta = ObjectMeta {
        location: util::strip_prefix(manifest.data_file().file_path()).into(),
        size: *manifest.data_file().file_size_in_bytes() as usize,
        last_modified: {
            let secs = last_updated_ms / 1000;
            let nsecs = (last_updated_ms % 1000) as u32 * 1000000;
            DateTime::from_timestamp(secs, nsecs).unwrap()
        },
        e_tag: None,
        version: None,
    };
    let file = PartitionedFile {
        object_meta,
        partition_values,
        range: None,
        statistics: Some(manifest_statistics),
        extensions: None,
        metadata_size_hint: None,
    };
    Ok(file)
}

fn value_to_scalarvalue(value: &Value) -> Result<ScalarValue, DataFusionError> {
    match value {
        Value::Boolean(b) => Ok(ScalarValue::Boolean(Some(*b))),
        Value::Int(i) => Ok(ScalarValue::Int32(Some(*i))),
        Value::LongInt(l) => Ok(ScalarValue::Int64(Some(*l))),
        Value::Float(f) => Ok(ScalarValue::Float32(Some(f.into_inner()))),
        Value::Double(d) => Ok(ScalarValue::Float64(Some(d.into_inner()))),
        Value::Date(d) => Ok(ScalarValue::Date32(Some(*d))),
        Value::Time(t) => Ok(ScalarValue::Time64Microsecond(Some(*t))),
        Value::Timestamp(ts) => Ok(ScalarValue::TimestampMicrosecond(Some(*ts), None)),
        Value::TimestampTZ(ts) => Ok(ScalarValue::TimestampMicrosecond(
            Some(*ts),
            Some("UTC".into()),
        )),
        Value::String(s) => Ok(ScalarValue::Utf8(Some(s.clone()))),
        Value::UUID(u) => Ok(ScalarValue::FixedSizeBinary(
            16,
            Some(u.as_bytes().to_vec()),
        )),
        Value::Fixed(size, bytes) => Ok(ScalarValue::FixedSizeBinary(
            *size as i32,
            Some(bytes.clone()),
        )),
        Value::Binary(bytes) => Ok(ScalarValue::Binary(Some(bytes.clone()))),
        x => Err(DataFusionError::External(Box::new(Error::NotSupported(
            format!("Conversion from Value {x} to ScalarValue"),
        )))),
    }
}

#[cfg(test)]
mod tests {

    use datafusion::{arrow::array::Int64Array, prelude::SessionContext};
    use iceberg_rust::{
        catalog::tabular::Tabular,
        object_store::ObjectStoreBuilder,
        spec::{
            partition::{PartitionField, Transform},
            schema::Schema,
            types::{PrimitiveType, StructField, Type},
        },
    };
    use iceberg_rust::{
        catalog::Catalog,
        spec::{
            partition::PartitionSpec,
            view_metadata::{Version, ViewRepresentation},
        },
        table::Table,
        view::View,
    };
    use iceberg_sql_catalog::SqlCatalog;

    use std::{ops::Deref, sync::Arc};

    use crate::{catalog::catalog::IcebergCatalog, DataFusionTable};

    #[tokio::test]
    pub async fn test_datafusion_table_insert() {
        let object_store = ObjectStoreBuilder::memory();

        let catalog: Arc<dyn Catalog> = Arc::new(
            SqlCatalog::new("sqlite://", "test", object_store)
                .await
                .unwrap(),
        );

        let schema = Schema::builder()
            .with_struct_field(StructField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 2,
                name: "customer_id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 3,
                name: "product_id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 4,
                name: "date".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Date),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 5,
                name: "amount".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Int),
                doc: None,
            })
            .build()
            .unwrap();

        let table = Table::builder()
            .with_name("orders")
            .with_location("/test/orders")
            .with_schema(schema)
            .build(&["test".to_owned()], catalog)
            .await
            .expect("Failed to create table");

        let table = Arc::new(DataFusionTable::from(table));

        let ctx = SessionContext::new();

        ctx.register_table("orders", table.clone()).unwrap();

        ctx.sql(
            "INSERT INTO orders (id, customer_id, product_id, date, amount) VALUES 
                (1, 1, 1, '2020-01-01', 1),
                (2, 2, 1, '2020-01-01', 1),
                (3, 3, 1, '2020-01-01', 3),
                (4, 1, 2, '2020-02-02', 1),
                (5, 1, 1, '2020-02-02', 2),
                (6, 3, 3, '2020-02-02', 3);",
        )
        .await
        .expect("Failed to create query plan for insert")
        .collect()
        .await
        .expect("Failed to insert values into table");

        let batches = ctx
            .sql("select product_id, sum(amount) from orders where customer_id = 1 group by product_id;")
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        for batch in batches {
            if batch.num_rows() != 0 {
                let (product_ids, amounts) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                );
                for (product_id, amount) in product_ids.iter().zip(amounts) {
                    if product_id.unwrap() == 1 {
                        assert_eq!(amount.unwrap(), 3)
                    } else if product_id.unwrap() == 2 {
                        assert_eq!(amount.unwrap(), 1)
                    } else if product_id.unwrap() == 3 {
                        assert_eq!(amount.unwrap(), 0)
                    } else {
                        panic!("Unexpected order id")
                    }
                }
            }
        }

        ctx.sql(
            "INSERT INTO orders (id, customer_id, product_id, date, amount) VALUES 
                (7, 1, 3, '2020-01-03', 1),
                (8, 2, 1, '2020-01-03', 2),
                (9, 2, 2, '2020-01-03', 1);",
        )
        .await
        .expect("Failed to create query plan for insert")
        .collect()
        .await
        .expect("Failed to insert values into table");

        ctx.sql(
            "INSERT INTO orders (id, customer_id, product_id, date, amount) VALUES 
                (10, 1, 2, '2020-01-04', 3),
                (11, 3, 1, '2020-01-04', 2),
                (12, 2, 3, '2020-01-04', 1);",
        )
        .await
        .expect("Failed to create query plan for insert")
        .collect()
        .await
        .expect("Failed to insert values into table");

        ctx.sql(
            "INSERT INTO orders (id, customer_id, product_id, date, amount) VALUES 
                (13, 1, 1, '2020-01-05', 4),
                (14, 3, 2, '2020-01-05', 2),
                (15, 2, 3, '2020-01-05', 3);",
        )
        .await
        .expect("Failed to create query plan for insert")
        .collect()
        .await
        .expect("Failed to insert values into table");

        ctx.sql(
            "INSERT INTO orders (id, customer_id, product_id, date, amount) VALUES 
                (16, 2, 3, '2020-01-05', 3),
                (17, 1, 3, '2020-01-06', 1),
                (18, 2, 1, '2020-01-06', 2);",
        )
        .await
        .expect("Failed to create query plan for insert")
        .collect()
        .await
        .expect("Failed to insert values into table");

        ctx.sql(
            "INSERT INTO orders (id, customer_id, product_id, date, amount) VALUES 
                (19, 2, 2, '2020-01-06', 1),
                (20, 1, 2, '2020-01-07', 3),
                (21, 3, 1, '2020-01-07', 2);",
        )
        .await
        .expect("Failed to create query plan for insert")
        .collect()
        .await
        .expect("Failed to insert values into table");

        ctx.sql(
            "INSERT INTO orders (id, customer_id, product_id, date, amount) VALUES 
                (21, 3, 1, '2020-01-07', 2),
                (22, 2, 3, '2020-01-07', 1),
                (23, 1, 1, '2020-01-08', 4),
                (24, 3, 2, '2020-01-08', 2),
                (25, 2, 3, '2020-01-08', 3);",
        )
        .await
        .expect("Failed to create query plan for insert")
        .collect()
        .await
        .expect("Failed to insert values into table");

        let batches = ctx
            .sql("select product_id, sum(amount) from orders where customer_id = 1 group by product_id;")
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        for batch in batches {
            if batch.num_rows() != 0 {
                let (product_ids, amounts) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                );
                for (product_id, amount) in product_ids.iter().zip(amounts) {
                    match product_id.unwrap() {
                        1 => assert_eq!(amount.unwrap(), 11),
                        2 => assert_eq!(amount.unwrap(), 7),
                        3 => assert_eq!(amount.unwrap(), 2),
                        _ => panic!("Unexpected order id"),
                    }
                }
            }
        }

        if let Tabular::Table(table) = table.tabular.read().await.deref() {
            assert_eq!(table.manifests(None, None).await.unwrap().len(), 2);
        };
    }

    #[tokio::test]
    pub async fn test_datafusion_table_insert_partitioned() {
        let object_store = ObjectStoreBuilder::memory();

        let catalog: Arc<dyn Catalog> = Arc::new(
            SqlCatalog::new("sqlite://", "test", object_store)
                .await
                .unwrap(),
        );

        let schema = Schema::builder()
            .with_struct_field(StructField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 2,
                name: "customer_id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 3,
                name: "product_id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 4,
                name: "date".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Date),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 5,
                name: "amount".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Int),
                doc: None,
            })
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder()
            .with_partition_field(PartitionField::new(4, 1000, "day", Transform::Day))
            .build()
            .expect("Failed to create partition spec");

        let table = Table::builder()
            .with_name("orders")
            .with_location("/test/orders")
            .with_schema(schema)
            .with_partition_spec(partition_spec)
            .build(&["test".to_owned()], catalog)
            .await
            .expect("Failed to create table");

        let table = Arc::new(DataFusionTable::from(table));

        let ctx = SessionContext::new();

        ctx.register_table("orders", table.clone()).unwrap();

        ctx.sql(
            "INSERT INTO orders (id, customer_id, product_id, date, amount) VALUES 
                (1, 1, 1, '2020-01-01', 1),
                (2, 2, 1, '2020-01-01', 1),
                (3, 3, 1, '2020-01-01', 3),
                (4, 1, 2, '2020-02-02', 1),
                (5, 1, 1, '2020-02-02', 2),
                (6, 3, 3, '2020-02-02', 3);",
        )
        .await
        .expect("Failed to create query plan for insert")
        .collect()
        .await
        .expect("Failed to insert values into table");

        let batches = ctx
            .sql("select product_id, sum(amount) from orders where customer_id = 1 group by product_id;")
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        for batch in batches {
            if batch.num_rows() != 0 {
                let (product_ids, amounts) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                );
                for (product_id, amount) in product_ids.iter().zip(amounts) {
                    if product_id.unwrap() == 1 {
                        assert_eq!(amount.unwrap(), 3)
                    } else if product_id.unwrap() == 2 {
                        assert_eq!(amount.unwrap(), 1)
                    } else if product_id.unwrap() == 3 {
                        assert_eq!(amount.unwrap(), 0)
                    } else {
                        panic!("Unexpected order id")
                    }
                }
            }
        }

        ctx.sql(
            "INSERT INTO orders (id, customer_id, product_id, date, amount) VALUES 
                (7, 1, 3, '2020-01-03', 1),
                (8, 2, 1, '2020-01-03', 2),
                (9, 2, 2, '2020-01-03', 1),
                (10, 1, 2, '2020-01-04', 3),
                (11, 3, 1, '2020-01-04', 2),
                (12, 2, 3, '2020-01-04', 1),
                (13, 1, 1, '2020-01-05', 4),
                (14, 3, 2, '2020-01-05', 2),
                (15, 2, 3, '2020-01-05', 3),
                (16, 2, 3, '2020-01-05', 3),
                (17, 1, 3, '2020-01-06', 1),
                (18, 2, 1, '2020-01-06', 2),
                (19, 2, 2, '2020-01-06', 1),
                (20, 1, 2, '2020-01-07', 3),
                (21, 3, 1, '2020-01-07', 2),
                (22, 2, 3, '2020-01-07', 1),
                (23, 1, 1, '2020-01-08', 4),
                (24, 3, 2, '2020-01-08', 2),
                (25, 2, 3, '2020-01-08', 3);",
        )
        .await
        .expect("Failed to create query plan for insert")
        .collect()
        .await
        .expect("Failed to insert values into table");

        let batches = ctx
            .sql("select product_id, sum(amount) from orders where customer_id = 1 group by product_id;")
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        for batch in batches {
            if batch.num_rows() != 0 {
                let (product_ids, amounts) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                );
                for (product_id, amount) in product_ids.iter().zip(amounts) {
                    match product_id.unwrap() {
                        1 => assert_eq!(amount.unwrap(), 11),
                        2 => assert_eq!(amount.unwrap(), 7),
                        3 => assert_eq!(amount.unwrap(), 2),
                        _ => panic!("Unexpected order id"),
                    }
                }
            }
        }

        if let Tabular::Table(table) = table.tabular.read().await.deref() {
            assert_eq!(table.manifests(None, None).await.unwrap().len(), 2);
        };
    }

    #[tokio::test]
    pub async fn test_datafusion_table_branch_insert() {
        let object_store = ObjectStoreBuilder::memory();

        let catalog: Arc<dyn Catalog> = Arc::new(
            SqlCatalog::new("sqlite://", "iceberg", object_store)
                .await
                .unwrap(),
        );

        let schema = Schema::builder()
            .with_struct_field(StructField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 2,
                name: "customer_id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 3,
                name: "product_id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 4,
                name: "date".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Date),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 5,
                name: "amount".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Int),
                doc: None,
            })
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder()
            .with_partition_field(PartitionField::new(4, 1000, "day", Transform::Day))
            .build()
            .expect("Failed to create partition spec");

        Table::builder()
            .with_name("orders")
            .with_location("/test/orders")
            .with_schema(schema)
            .with_partition_spec(partition_spec)
            .build(&["test".to_owned()], catalog.clone())
            .await
            .expect("Failed to create table");

        // Datafusion

        let datafusion_catalog = Arc::new(
            IcebergCatalog::new(catalog.clone(), Some("dev"))
                .await
                .expect("Failed to create datafusion catalog"),
        );

        let ctx = SessionContext::new();

        ctx.register_catalog("iceberg", datafusion_catalog);

        ctx.sql(
            "INSERT INTO iceberg.test.orders (id, customer_id, product_id, date, amount) VALUES 
                (1, 1, 1, '2020-01-01', 1),
                (2, 2, 1, '2020-01-01', 1),
                (3, 3, 1, '2020-01-01', 3),
                (4, 1, 2, '2020-02-02', 1),
                (5, 1, 1, '2020-02-02', 2),
                (6, 3, 3, '2020-02-02', 3);",
        )
        .await
        .expect("Failed to create query plan for insert")
        .collect()
        .await
        .expect("Failed to insert values into table");

        let batches = ctx
            .sql("select product_id, sum(amount) from iceberg.test.orders group by product_id;")
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        for batch in batches {
            if batch.num_rows() != 0 {
                let (product_ids, amounts) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                );
                for (product_id, amount) in product_ids.iter().zip(amounts) {
                    if product_id.unwrap() == 1 {
                        assert_eq!(amount.unwrap(), 7)
                    } else if product_id.unwrap() == 2 {
                        assert_eq!(amount.unwrap(), 1)
                    } else if product_id.unwrap() == 3 {
                        assert_eq!(amount.unwrap(), 3)
                    } else {
                        panic!("Unexpected order id")
                    }
                }
            }
        }

        ctx.sql(
            "INSERT INTO iceberg.test.orders (id, customer_id, product_id, date, amount) VALUES 
                (7, 1, 3, '2020-01-03', 1),
                (8, 2, 1, '2020-01-03', 2),
                (9, 2, 2, '2020-01-03', 1);",
        )
        .await
        .expect("Failed to create query plan for insert")
        .collect()
        .await
        .expect("Failed to insert values into table");

        let batches = ctx
            .sql("select product_id, sum(amount) from iceberg.test.orders group by product_id;")
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        for batch in batches {
            if batch.num_rows() != 0 {
                let (product_ids, amounts) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                );
                for (product_id, amount) in product_ids.iter().zip(amounts) {
                    if product_id.unwrap() == 1 {
                        assert_eq!(amount.unwrap(), 9)
                    } else if product_id.unwrap() == 2 {
                        assert_eq!(amount.unwrap(), 2)
                    } else if product_id.unwrap() == 3 {
                        assert_eq!(amount.unwrap(), 4)
                    } else {
                        panic!("Unexpected order id")
                    }
                }
            }
        }
    }

    #[tokio::test]
    pub async fn test_datafusion_view_scan() {
        let object_store = ObjectStoreBuilder::memory();

        let catalog: Arc<dyn Catalog> = Arc::new(
            SqlCatalog::new("sqlite://", "test", object_store)
                .await
                .unwrap(),
        );

        let schema = Schema::builder()
            .with_struct_field(StructField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 2,
                name: "customer_id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 3,
                name: "product_id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 4,
                name: "date".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Date),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 5,
                name: "amount".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Int),
                doc: None,
            })
            .build()
            .unwrap();
        let partition_spec = PartitionSpec::builder()
            .with_partition_field(PartitionField::new(4, 1000, "day", Transform::Day))
            .build()
            .expect("Failed to create partition spec");

        let table = Table::builder()
            .with_name("orders")
            .with_location("/test/orders")
            .with_schema(schema)
            .with_partition_spec(partition_spec)
            .build(&["schema".to_owned()], catalog.clone())
            .await
            .expect("Failed to create table");

        let table = Arc::new(DataFusionTable::from(table));

        let ctx = SessionContext::new();

        ctx.register_table("orders", table).unwrap();

        ctx.sql(
            "INSERT INTO orders (id, customer_id, product_id, date, amount) VALUES 
                (1, 1, 1, '2020-01-01', 1),
                (2, 2, 1, '2020-01-01', 1),
                (3, 3, 1, '2020-01-01', 3),
                (4, 1, 2, '2020-02-02', 1),
                (5, 1, 1, '2020-02-02', 2),
                (6, 3, 3, '2020-02-02', 3),
                (7, 1, 3, '2020-01-03', 1),
                (8, 2, 1, '2020-01-03', 2),
                (9, 2, 2, '2020-01-03', 1);",
        )
        .await
        .expect("Failed to create query plan for insert")
        .collect()
        .await
        .expect("Failed to insert values into table");

        let view_schema = Schema::builder()
            .with_struct_field(StructField {
                id: 3,
                name: "product_id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 5,
                name: "amount".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Int),
                doc: None,
            })
            .build()
            .unwrap();

        let view = View::builder()
            .with_name("orders_view")
            .with_location("test/orders_view")
            .with_schema(view_schema)
            .with_view_version(
                Version::builder()
                    .with_representation(ViewRepresentation::sql(
                        "select product_id, amount from orders where product_id < 3;",
                        None,
                    ))
                    .build()
                    .unwrap(),
            )
            .build(&["test".to_owned()], catalog)
            .await
            .expect("Failed to build view");

        let view = Arc::new(DataFusionTable::from(view));

        ctx.register_table("orders_view", view).unwrap();

        let batches = ctx
            .sql("select product_id, sum(amount) from orders_view group by product_id;")
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        for batch in batches {
            if batch.num_rows() != 0 {
                let (product_ids, amounts) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                );
                for (product_id, amount) in product_ids.iter().zip(amounts) {
                    if product_id.unwrap() == 1 {
                        assert_eq!(amount.unwrap(), 9)
                    } else if product_id.unwrap() == 2 {
                        assert_eq!(amount.unwrap(), 2)
                    } else {
                        panic!("Unexpected order id")
                    }
                }
            }
        }
    }
}
