use std::{collections::HashMap, sync::Arc};

use datafusion::{
    arrow::error::ArrowError, catalog::TableProvider, common::tree_node::TreeNode,
    execution::SessionStateBuilder, prelude::SessionContext, sql::TableReference,
};
use datafusion_expr::LogicalPlan;
use delta_queries::{
    delta_node::{NegDeltaNode, PosDeltaNode},
    transform::delta_transform_down,
};
use futures::{stream, StreamExt, TryStreamExt};
use iceberg_rust::{
    arrow::write::{write_equality_deletes_parquet_partitioned, write_parquet_partitioned},
    catalog::{identifier::Identifier, tabular::Tabular, CatalogList},
    materialized_view::MaterializedView,
    spec::materialized_view_metadata::{SourceTables, SourceViews},
};
use iceberg_rust::{
    error::Error,
    spec::{materialized_view_metadata::RefreshState, view_metadata::ViewRepresentation},
    sql::find_relations,
};

use crate::{
    catalog::catalog_list::IcebergCatalogList, error::Error as DatafusionIcebergError,
    planner::IcebergQueryPlanner, DataFusionTable,
};

pub(crate) mod delta_queries;

pub async fn refresh_materialized_view(
    matview: &mut MaterializedView,
    catalog_list: Arc<dyn CatalogList>,
    branch: Option<&str>,
) -> Result<(), Error> {
    let identifier = matview.identifier();

    let state = SessionStateBuilder::default()
        .with_catalog_list(Arc::new(
            IcebergCatalogList::new(catalog_list.clone()).await?,
        ))
        .with_query_planner(Arc::new(IcebergQueryPlanner::new()))
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);

    let version = matview.metadata().current_version(branch)?;

    let sql = match &version.representations[0] {
        ViewRepresentation::Sql { sql, dialect: _ } => sql,
    };

    let storage_table = matview.storage_table().await?;

    let relations = find_relations(sql)?;

    let branch = branch.map(ToString::to_string);

    let old_refresh_state = Arc::new(
        storage_table
            .refresh_state(matview.metadata().current_version_id, branch.clone())
            .await?,
    );

    // Load source tables
    let (source_tables, source_table_states) =
        get_source_tables(relations, catalog_list, &branch, old_refresh_state, version).await?;

    let source_tables = Arc::new(source_tables);

    // If all source table states are fresh, then nothing has to be done
    if source_tables.values().all(|x| x.1.is_none()) {
        return Ok(());
    }

    let logical_plan = ctx
        .state()
        .create_logical_plan(sql)
        .await
        .map_err(DatafusionIcebergError::from)?;

    let refresh_strategy =
        determine_refresh_strategy(&logical_plan).map_err(DatafusionIcebergError::from)?;

    let refresh_version_id = matview.metadata().current_version_id;

    let refresh_state = RefreshState {
        refresh_version_id,
        source_table_states: SourceTables(source_table_states),
        source_view_states: SourceViews(HashMap::new()),
    };

    let storage_table_provider = Arc::new(DataFusionTable::new(
        Tabular::Table(storage_table.clone()),
        None,
        None,
        branch.as_deref(),
    ));

    let storage_table_reference = TableReference::full(
        matview.catalog().name(),
        identifier.namespace().to_string(),
        identifier.name(),
    );

    // Potentially register source table deltas and rewrite logical plan
    let pos_plan = match refresh_strategy {
        RefreshStrategy::FullOverwrite => logical_plan.clone(),
        RefreshStrategy::IncrementalAppend => {
            let delta_plan: LogicalPlan = PosDeltaNode::new(Arc::new(logical_plan.clone())).into();
            delta_plan
                .transform_down(|plan| {
                    delta_transform_down(
                        plan,
                        &source_tables,
                        (
                            storage_table_reference.clone(),
                            storage_table_provider.clone(),
                        ),
                    )
                })
                .map_err(DatafusionIcebergError::from)?
                .data
        }
        RefreshStrategy::IncrementalOverwrite => logical_plan.clone(),
    };

    // Calculate arrow record batches from logical plan
    let pos_batches = ctx
        .execute_logical_plan(pos_plan)
        .await
        .map_err(DatafusionIcebergError::from)?
        .execute_stream()
        .await
        .map_err(DatafusionIcebergError::from)?
        .map_err(ArrowError::from);

    // Write arrow record batches to datafiles
    let pos_files =
        write_parquet_partitioned(&storage_table, pos_batches, branch.as_deref()).await?;

    match refresh_strategy {
        RefreshStrategy::FullOverwrite | RefreshStrategy::IncrementalOverwrite => {
            matview
                .new_transaction(branch.as_deref())
                .full_refresh(pos_files, refresh_state)?
                .commit()
                .await?;
        }
        RefreshStrategy::IncrementalAppend => {
            // Potentially register source table deltas and rewrite logical plan
            let delta_plan: LogicalPlan = NegDeltaNode::new(Arc::new(logical_plan)).into();
            let neg_plan = delta_plan
                .transform_down(|plan| {
                    delta_transform_down(
                        plan,
                        &source_tables,
                        (
                            storage_table_reference.clone(),
                            storage_table_provider.clone(),
                        ),
                    )
                })
                .map_err(DatafusionIcebergError::from)?
                .data;

            let delete_schema = neg_plan.schema();

            let equality_ids = delete_schema
                .fields()
                .iter()
                .map(|x| {
                    let schema = storage_table.current_schema(branch.as_deref())?;
                    Ok(schema
                        .get_name(x.name())
                        .ok_or(Error::Schema(x.name().to_string(), schema.to_string()))?
                        .id)
                })
                .collect::<Result<Vec<_>, Error>>()?;

            // Calculate arrow record batches from logical plan
            let neg_batches = ctx
                .execute_logical_plan(neg_plan)
                .await
                .map_err(DatafusionIcebergError::from)?
                .execute_stream()
                .await
                .map_err(DatafusionIcebergError::from)?
                .map_err(ArrowError::from);

            // Write arrow record batches to datafiles
            let neg_files = write_equality_deletes_parquet_partitioned(
                &storage_table,
                neg_batches,
                branch.as_deref(),
                &equality_ids,
            )
            .await?;

            let transaction = matview
                .new_transaction(branch.as_deref())
                .append(pos_files, refresh_state.clone())?;
            let transaction = if !neg_files.is_empty() {
                transaction.delete(neg_files, refresh_state)?
            } else {
                transaction
            };
            transaction.commit().await?;
        }
    }

    Ok(())
}

async fn get_source_tables(
    relations: Vec<String>,
    catalog_list: Arc<dyn CatalogList>,
    branch: &Option<String>,
    old_refresh_state: Arc<Option<RefreshState>>,
    version: &iceberg_rust::spec::view_metadata::Version<
        iceberg_rust::spec::identifier::FullIdentifier,
    >,
) -> Result<
    (
        HashMap<
            TableReference,
            (
                Option<Arc<dyn TableProvider>>,
                Option<Arc<dyn TableProvider>>,
            ),
        >,
        HashMap<(uuid::Uuid, Option<String>), i64>,
    ),
    Error,
> {
    stream::iter(relations.iter())
        .then(|relation| {
            let catalog_list = catalog_list.clone();
            let branch = branch.clone();
            let old_refresh_state = old_refresh_state.clone();
            async move {
                let reference = TableReference::parse_str(relation);
                let resolved_reference = reference.clone().resolve(
                    version.default_catalog().as_deref().unwrap_or("datafusion"),
                    &version.default_namespace()[0],
                );
                let catalog_name = resolved_reference.catalog.to_string();
                let identifier = Identifier::new(
                    &[resolved_reference.schema.to_string()],
                    &resolved_reference.table,
                );
                let catalog = catalog_list
                    .catalog(&catalog_name)
                    .ok_or(Error::NotFound(format!("Catalog {catalog_name}")))?;

                let tabular = match catalog.load_tabular(&identifier).await? {
                    Tabular::View(_) => {
                        return Err(Error::InvalidFormat("storage table".to_string()))
                    }
                    x => x,
                };
                let uuid = *tabular.metadata().as_ref().uuid();
                let current_snapshot_id = match &tabular {
                    Tabular::Table(table) => Ok(*table
                        .metadata()
                        .current_snapshot(branch.as_deref())?
                        // Fallback to main branch
                        .or(table.metadata().current_snapshot(None)?)
                        .ok_or(Error::NotFound(format!(
                            "Snapshot in source table {}",
                            (&identifier.name()),
                        )))?
                        .snapshot_id()),
                    Tabular::MaterializedView(mv) => {
                        let storage_table = mv.storage_table().await?;
                        Ok(*storage_table
                            .metadata()
                            .current_snapshot(branch.as_deref())?
                            // Fallback to main branch
                            .or(storage_table.metadata().current_snapshot(None)?)
                            .ok_or(Error::NotFound(format!(
                                "Snapshot in source table {}",
                                (&identifier.name()),
                            )))?
                            .snapshot_id())
                    }
                    _ => Err(Error::InvalidFormat("storage table".to_string())),
                }?;

                #[allow(clippy::type_complexity)]
                let source_table_provider: (
                    Option<Arc<dyn TableProvider>>,
                    Option<Arc<dyn TableProvider>>,
                ) = if let Some(old_refresh_state) = old_refresh_state.as_ref() {
                    let revision_id = old_refresh_state.source_table_states.get(&(uuid, None));
                    if Some(&current_snapshot_id) == revision_id {
                        // Fresh
                        (
                            Some(Arc::new(DataFusionTable::new(
                                tabular,
                                None,
                                None,
                                branch.as_deref(),
                            ))),
                            None,
                        )
                    } else if Some(&-1) == revision_id {
                        // Invalid
                        (
                            None,
                            Some(Arc::new(DataFusionTable::new(
                                tabular,
                                None,
                                None,
                                branch.as_deref(),
                            ))),
                        )
                    } else if let Some(revision_id) = revision_id {
                        // Outdated
                        (
                            Some(Arc::new(DataFusionTable::new(
                                tabular.clone(),
                                None,
                                Some(*revision_id),
                                branch.as_deref(),
                            ))),
                            Some(Arc::new(DataFusionTable::new(
                                tabular,
                                Some(*revision_id),
                                None,
                                branch.as_deref(),
                            ))),
                        )
                    } else {
                        // Invalid
                        (
                            None,
                            Some(Arc::new(DataFusionTable::new(
                                tabular,
                                None,
                                None,
                                branch.as_deref(),
                            ))),
                        )
                    }
                } else {
                    // Invalid
                    (
                        None,
                        Some(Arc::new(DataFusionTable::new(
                            tabular,
                            None,
                            None,
                            branch.as_deref(),
                        ))),
                    )
                };

                Ok((
                    (reference, source_table_provider),
                    ((uuid, None), current_snapshot_id),
                ))
            }
        })
        .try_collect::<(HashMap<TableReference, _>, HashMap<_, _>)>()
        .await
}

#[derive(Debug)]
/// Refresh strategy that can be used for the operation
enum RefreshStrategy {
    /// Changes can be computed incrementally and appended to the last state, i.e. Projection, Filter
    IncrementalAppend,
    /// Changes can be computed incrementally and the last state needs to be overwritten, i.e. Aggregate
    IncrementalOverwrite,
    /// The entire query needs to be computed, i.e. WindowFunction
    FullOverwrite,
}

fn determine_refresh_strategy(
    node: &LogicalPlan,
) -> Result<RefreshStrategy, datafusion::error::DataFusionError> {
    if requires_full_overwrite(node)? {
        Ok(RefreshStrategy::FullOverwrite)
    } else if requires_overwrite_from_last(node)? {
        Ok(RefreshStrategy::IncrementalOverwrite)
    } else {
        Ok(RefreshStrategy::IncrementalAppend)
    }
}

/// Check if the entire query has to be reexecuted
fn requires_full_overwrite(node: &LogicalPlan) -> Result<bool, datafusion::error::DataFusionError> {
    node.exists(|node| {
        Ok(!matches!(
            node,
            &LogicalPlan::Projection(_)
                | &LogicalPlan::Filter(_)
                | &LogicalPlan::Join(_)
                | &LogicalPlan::Union(_)
                | &LogicalPlan::Aggregate(_)
                | &LogicalPlan::Sort(_)
                | &LogicalPlan::TableScan(_)
                | &LogicalPlan::SubqueryAlias(_)
        ))
    })
}

/// Checks if the storage table has to be overwritten but the data from the last refresh can be reused
fn requires_overwrite_from_last(
    node: &LogicalPlan,
) -> Result<bool, datafusion::error::DataFusionError> {
    node.exists(|node| Ok(matches!(node, &LogicalPlan::Sort(_))))
}

#[cfg(test)]
mod tests {

    use datafusion::{
        arrow::array::{Int32Array, Int64Array},
        common::tree_node::{TransformedResult, TreeNode},
        execution::SessionStateBuilder,
        prelude::SessionContext,
    };
    use datafusion_expr::ScalarUDF;
    use iceberg_rust::object_store::Bucket;
    use iceberg_rust::object_store::ObjectStoreBuilder;
    use iceberg_sql_catalog::SqlCatalogList;
    use std::{sync::Arc, time::Duration};
    use tokio::time::sleep;
    use url::Url;

    use crate::{
        catalog::catalog_list::IcebergCatalogList,
        planner::{iceberg_transform, IcebergQueryPlanner, RefreshMaterializedView},
    };

    #[tokio::test]
    pub async fn test_datafusion_materialized_view_refresh_incremental() {
        let object_store = ObjectStoreBuilder::memory();

        let iceberg_catalog_list = Arc::new(
            SqlCatalogList::new("sqlite://", object_store.clone())
                .await
                .unwrap(),
        );

        let catalog_list = {
            Arc::new(
                IcebergCatalogList::new(iceberg_catalog_list.clone())
                    .await
                    .unwrap(),
            )
        };

        let state = SessionStateBuilder::default()
            .with_default_features()
            .with_catalog_list(catalog_list)
            .with_query_planner(Arc::new(IcebergQueryPlanner::new()))
            .with_object_store(
                &Url::try_from("file://").unwrap(),
                object_store.build(Bucket::Local).unwrap(),
            )
            .build();

        let ctx = SessionContext::new_with_state(state);

        ctx.register_udf(ScalarUDF::from(RefreshMaterializedView::new(
            iceberg_catalog_list,
        )));

        let sql = "CREATE EXTERNAL TABLE warehouse.public.orders (
      id BIGINT NOT NULL,
      order_date DATE NOT NULL,
      customer_id INTEGER NOT NULL,
      product_id INTEGER NOT NULL,
      quantity INTEGER NOT NULL
)
STORED AS ICEBERG
LOCATION '/warehouse/public/orders/';";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let plan = ctx
            .state()
            .create_logical_plan(
                "CREATE TEMPORARY VIEW warehouse.public.orders_view AS select product_id, quantity from warehouse.public.orders where product_id < 3;",
            )
            .await
            .expect("Failed to create plan for select");

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        ctx.sql(
            "INSERT INTO warehouse.public.orders (id, customer_id, product_id, order_date, quantity) VALUES 
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

        ctx.sql("select refresh_materialized_view('warehouse.public.orders_view');")
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        sleep(Duration::from_millis(1_000)).await;

        let batches = ctx
            .sql(
                "select product_id, sum(quantity) from warehouse.public.orders_view group by product_id;",
            )
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        for batch in batches {
            if batch.num_rows() != 0 {
                let (order_ids, amounts) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                );
                for (order_id, amount) in order_ids.iter().zip(amounts) {
                    if order_id.unwrap() == 1 {
                        assert_eq!(amount.unwrap(), 7)
                    } else if order_id.unwrap() == 2 {
                        assert_eq!(amount.unwrap(), 1)
                    } else {
                        panic!("Unexpected order id")
                    }
                }
            }
        }

        ctx.sql(
            "INSERT INTO warehouse.public.orders (id, customer_id, product_id, order_date, quantity) VALUES 
                (7, 1, 3, '2020-01-03', 1),
                (8, 2, 1, '2020-01-03', 2),
                (9, 2, 2, '2020-01-03', 1);",
        )
        .await
        .expect("Failed to create query plan for insert")
        .collect()
        .await
        .expect("Failed to insert values into table");

        ctx.sql("select refresh_materialized_view('warehouse.public.orders_view');")
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        sleep(Duration::from_millis(1_000)).await;

        let batches = ctx
            .sql(
                "select product_id, sum(quantity) from warehouse.public.orders_view group by product_id;",
            )
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        for batch in batches {
            if batch.num_rows() != 0 {
                let (order_ids, amounts) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                );
                for (order_id, amount) in order_ids.iter().zip(amounts) {
                    if order_id.unwrap() == 1 {
                        assert_eq!(amount.unwrap(), 9)
                    } else if order_id.unwrap() == 2 {
                        assert_eq!(amount.unwrap(), 2)
                    } else {
                        panic!("Unexpected order id")
                    }
                }
            }
        }
    }
}
