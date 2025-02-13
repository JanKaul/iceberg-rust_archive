use std::sync::Arc;

use datafusion::{
    arrow::{array::Int64Array, error::ArrowError},
    prelude::SessionContext,
};
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use futures::stream;
use iceberg_rust::{
    arrow::write::write_equality_deletes_parquet_partitioned,
    catalog::Catalog,
    object_store::ObjectStoreBuilder,
    spec::{
        partition::{PartitionField, PartitionSpec, Transform},
        schema::Schema,
        types::{PrimitiveType, StructField, Type},
    },
    table::Table,
};
use iceberg_sql_catalog::SqlCatalog;

#[tokio::test]
pub async fn test_equality_delete() {
    let object_store = ObjectStoreBuilder::memory();

    let catalog: Arc<dyn Catalog> = Arc::new(
        SqlCatalog::new("sqlite://", "warehouse", object_store.clone())
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
        .with_partition_field(PartitionField::new(4, 1000, "date_day", Transform::Day))
        .build()
        .expect("Failed to create partition spec");

    let mut table = Table::builder()
        .with_name("orders")
        .with_location("/test/orders")
        .with_schema(schema)
        .with_partition_spec(partition_spec)
        .build(&["test".to_owned()], catalog.clone())
        .await
        .expect("Failed to create table");

    let ctx = SessionContext::new();

    let datafusion_catalog = Arc::new(IcebergCatalog::new(catalog, None).await.unwrap());

    ctx.register_catalog("warehouse", datafusion_catalog);

    ctx.sql(
        "INSERT INTO warehouse.test.orders (id, customer_id, product_id, date, amount) VALUES 
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
        .sql("select product_id, sum(amount) from warehouse.test.orders group by product_id;")
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

    let batches = ctx
        .sql(
            "SELECT id, customer_id, product_id, date FROM warehouse.test.orders WHERE customer_id = 1;",
        )
        .await
        .expect("Failed to create query plan for insert")
        .collect()
        .await
        .expect("Failed to insert values into table");

    let files = write_equality_deletes_parquet_partitioned(
        &table,
        stream::iter(batches.into_iter().map(Ok::<_, ArrowError>)),
        None,
        &[1, 2, 3, 4],
    )
    .await
    .unwrap();

    table
        .new_transaction(None)
        .append_delete(files)
        .commit()
        .await
        .unwrap();

    let batches = ctx
        .sql("select product_id, sum(amount) from warehouse.test.orders group by product_id;")
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
                    assert_eq!(amount.unwrap(), 4)
                } else if product_id.unwrap() == 2 {
                    assert_eq!(amount.unwrap(), 0)
                } else if product_id.unwrap() == 3 {
                    assert_eq!(amount.unwrap(), 3)
                } else {
                    panic!("Unexpected order id")
                }
            }
        }
    }
}
