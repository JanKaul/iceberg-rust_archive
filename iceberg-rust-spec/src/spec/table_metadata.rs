/*!
Defines the [table metadata](https://iceberg.apache.org/spec/#table-metadata).
The main struct here is [TableMetadataV2] which defines the data for a table.
*/

use std::{
    collections::HashMap,
    fmt, str,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    error::Error,
    partition::BoundPartitionField,
    spec::{
        partition::PartitionSpec,
        sort::{self, SortOrder},
    },
};

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use uuid::Uuid;

use derive_builder::Builder;

use super::{
    schema::Schema,
    snapshot::{Snapshot, SnapshotReference},
    tabular::TabularMetadataRef,
};

pub static MAIN_BRANCH: &str = "main";
static DEFAULT_SORT_ORDER_ID: i32 = 0;
static DEFAULT_SPEC_ID: i32 = 0;

// Properties

pub const WRITE_PARQUET_COMPRESSION_CODEC: &str = "write.parquet.compression-codec";
pub const WRITE_PARQUET_COMPRESSION_LEVEL: &str = "write.parquet.compression-level";
pub const WRITE_OBJECT_STORAGE_ENABLED: &str = "write.object-storage.enabled";
pub const WRITE_DATA_PATH: &str = "write.data.path";

pub use _serde::{TableMetadataV1, TableMetadataV2};

use _serde::TableMetadataEnum;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default, Builder)]
#[serde(try_from = "TableMetadataEnum", into = "TableMetadataEnum")]
/// Fields for the version 2 of the table metadata.
pub struct TableMetadata {
    #[builder(default)]
    /// Integer Version for the format.
    pub format_version: FormatVersion,
    #[builder(default = "Uuid::new_v4()")]
    /// A UUID that identifies the table
    pub table_uuid: Uuid,
    #[builder(setter(into))]
    /// Location tables base location
    pub location: String,
    #[builder(default)]
    /// The tables highest sequence number
    pub last_sequence_number: i64,
    #[builder(
        default = "SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64"
    )]
    /// Timestamp in milliseconds from the unix epoch when the table was last updated.
    pub last_updated_ms: i64,
    #[builder(default)]
    /// An integer; the highest assigned column ID for the table.
    pub last_column_id: i32,
    #[builder(setter(each(name = "with_schema")))]
    /// A list of schemas, stored as objects with schema-id.
    pub schemas: HashMap<i32, Schema>,
    /// ID of the table’s current schema.
    pub current_schema_id: i32,
    #[builder(
        setter(each(name = "with_partition_spec")),
        default = "HashMap::from_iter(vec![(0,PartitionSpec::default())])"
    )]
    /// A list of partition specs, stored as full partition spec objects.
    pub partition_specs: HashMap<i32, PartitionSpec>,
    #[builder(default)]
    /// ID of the “current” spec that writers should use by default.
    pub default_spec_id: i32,
    #[builder(default)]
    /// An integer; the highest assigned partition field ID across all partition specs for the table.
    pub last_partition_id: i32,
    ///A string to string map of table properties. This is used to control settings that
    /// affect reading and writing and is not intended to be used for arbitrary metadata.
    /// For example, commit.retry.num-retries is used to control the number of commit retries.
    #[builder(default)]
    pub properties: HashMap<String, String>,
    /// long ID of the current table snapshot; must be the same as the current
    /// ID of the main branch in refs.
    #[builder(default)]
    pub current_snapshot_id: Option<i64>,
    /// A list of valid snapshots. Valid snapshots are snapshots for which all
    /// data files exist in the file system. A data file must not be deleted
    /// from the file system until the last snapshot in which it was listed is
    /// garbage collected.
    #[builder(default)]
    pub snapshots: HashMap<i64, Snapshot>,
    /// A list (optional) of timestamp and snapshot ID pairs that encodes changes
    /// to the current snapshot for the table. Each time the current-snapshot-id
    /// is changed, a new entry should be added with the last-updated-ms
    /// and the new current-snapshot-id. When snapshots are expired from
    /// the list of valid snapshots, all entries before a snapshot that has
    /// expired should be removed.
    #[builder(default)]
    pub snapshot_log: Vec<SnapshotLog>,

    /// A list (optional) of timestamp and metadata file location pairs
    /// that encodes changes to the previous metadata files for the table.
    /// Each time a new metadata file is created, a new entry of the
    /// previous metadata file location should be added to the list.
    /// Tables can be configured to remove oldest metadata log entries and
    /// keep a fixed-size log of the most recent entries after a commit.
    #[builder(default)]
    pub metadata_log: Vec<MetadataLog>,
    #[builder(
        setter(each(name = "with_sort_order")),
        default = "HashMap::from_iter(vec![(0, SortOrder::default())])"
    )]
    /// A list of sort orders, stored as full sort order objects.
    pub sort_orders: HashMap<i32, sort::SortOrder>,
    #[builder(default)]
    /// Default sort order id of the table. Note that this could be used by
    /// writers, but is not used when reading because reads use the specs
    /// stored in manifest files.
    pub default_sort_order_id: i32,
    ///A map of snapshot references. The map keys are the unique snapshot reference
    /// names in the table, and the map values are snapshot reference objects.
    /// There is always a main branch reference pointing to the current-snapshot-id
    /// even if the refs map is null.
    #[builder(default)]
    pub refs: HashMap<String, SnapshotReference>,
}

impl TableMetadata {
    /// Get current schema
    #[inline]
    pub fn current_schema(&self, branch: Option<&str>) -> Result<&Schema, Error> {
        let schema_id = self
            .current_snapshot(branch)?
            .and_then(|x| *x.schema_id())
            .unwrap_or(self.current_schema_id);
        self.schemas
            .get(&schema_id)
            .ok_or_else(|| Error::InvalidFormat("schema".to_string()))
    }

    /// Get schema for snapshot
    #[inline]
    pub fn schema(&self, snapshot_id: i64) -> Result<&Schema, Error> {
        let schema_id = self
            .snapshots
            .get(&snapshot_id)
            .and_then(|x| *x.schema_id())
            .unwrap_or(self.current_schema_id);
        self.schemas
            .get(&schema_id)
            .ok_or_else(|| Error::InvalidFormat("schema".to_string()))
    }

    /// Get default partition spec
    #[inline]
    pub fn default_partition_spec(&self) -> Result<&PartitionSpec, Error> {
        self.partition_specs
            .get(&self.default_spec_id)
            .ok_or_else(|| Error::InvalidFormat("partition spec".to_string()))
    }

    /// Get partition fields
    pub fn current_partition_fields(
        &self,
        branch: Option<&str>,
    ) -> Result<Vec<BoundPartitionField>, Error> {
        let schema = self.current_schema(branch)?;
        self.default_partition_spec()?
            .fields()
            .iter()
            .map(|partition_field| {
                let field =
                    schema
                        .get(*partition_field.source_id() as usize)
                        .ok_or(Error::NotFound(
                            "Field".to_owned(),
                            partition_field.source_id().to_string(),
                        ))?;
                Ok(BoundPartitionField::new(partition_field, field))
            })
            .collect()
    }

    /// Get partition fields for snapshot
    pub fn partition_fields(&self, snapshot_id: i64) -> Result<Vec<BoundPartitionField>, Error> {
        let schema = self.schema(snapshot_id)?;
        self.default_partition_spec()?
            .fields()
            .iter()
            .map(|partition_field| {
                let field =
                    schema
                        .get(*partition_field.source_id() as usize)
                        .ok_or(Error::NotFound(
                            "Field".to_owned(),
                            partition_field.source_id().to_string(),
                        ))?;
                Ok(BoundPartitionField::new(partition_field, field))
            })
            .collect()
    }

    /// Get current snapshot
    #[inline]
    pub fn current_snapshot(&self, snapshot_ref: Option<&str>) -> Result<Option<&Snapshot>, Error> {
        let snapshot_id = match snapshot_ref {
            None => self
                .refs
                .get("main")
                .map(|x| x.snapshot_id)
                .or(self.current_snapshot_id),
            Some(reference) => self.refs.get(reference).map(|x| x.snapshot_id),
        };
        match snapshot_id {
            Some(snapshot_id) => Ok(self.snapshots.get(&snapshot_id)),
            None => {
                if self.snapshots.is_empty()
                    || (snapshot_ref.is_some() && snapshot_ref != Some("main"))
                {
                    Ok(None)
                } else {
                    Err(Error::InvalidFormat("snapshots".to_string()))
                }
            }
        }
    }

    /// Get current snapshot
    #[inline]
    pub fn current_snapshot_mut(
        &mut self,
        snapshot_ref: Option<String>,
    ) -> Result<Option<&mut Snapshot>, Error> {
        let snapshot_id = match &snapshot_ref {
            None => self
                .refs
                .get("main")
                .map(|x| x.snapshot_id)
                .or(self.current_snapshot_id),
            Some(reference) => self.refs.get(reference).map(|x| x.snapshot_id),
        };
        match snapshot_id {
            Some(-1) => {
                if self.snapshots.is_empty()
                    || (snapshot_ref.is_some() && snapshot_ref.as_deref() != Some("main"))
                {
                    Ok(None)
                } else {
                    Err(Error::InvalidFormat("snapshots".to_string()))
                }
            }
            Some(snapshot_id) => Ok(self.snapshots.get_mut(&snapshot_id)),
            None => {
                if self.snapshots.is_empty()
                    || (snapshot_ref.is_some() && snapshot_ref.as_deref() != Some("main"))
                {
                    Ok(None)
                } else {
                    Err(Error::InvalidFormat("snapshots".to_string()))
                }
            }
        }
    }

    /// Get sequence_number of snapshot
    pub fn sequence_number(&self, snapshot_id: i64) -> Option<i64> {
        self.snapshots
            .get(&snapshot_id)
            .map(|x| *x.sequence_number())
    }

    pub fn as_ref(&self) -> TabularMetadataRef {
        TabularMetadataRef::Table(self)
    }
}

pub fn new_metadata_location<'a, T: Into<TabularMetadataRef<'a>>>(metadata: T) -> String {
    let metadata: TabularMetadataRef = metadata.into();
    let transaction_uuid = Uuid::new_v4();
    let version = metadata.sequence_number();

    metadata.location().to_string()
        + "/metadata/"
        + &version.to_string()
        + "-"
        + &transaction_uuid.to_string()
        + ".metadata.json"
}

impl fmt::Display for TableMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            &serde_json::to_string(self).map_err(|_| fmt::Error)?,
        )
    }
}

impl str::FromStr for TableMetadata {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(Error::from)
    }
}

pub mod _serde {
    use std::collections::HashMap;

    use itertools::Itertools;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use crate::{
        error::Error,
        spec::{
            partition::{PartitionField, PartitionSpec},
            schema,
            snapshot::{
                SnapshotReference, SnapshotRetention,
                _serde::{SnapshotV1, SnapshotV2},
            },
            sort,
        },
    };

    use super::{
        FormatVersion, MetadataLog, SnapshotLog, TableMetadata, VersionNumber,
        DEFAULT_SORT_ORDER_ID, DEFAULT_SPEC_ID, MAIN_BRANCH,
    };

    /// Metadata of an iceberg table
    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(untagged)]
    pub(super) enum TableMetadataEnum {
        /// Version 2 of the table metadata
        V2(TableMetadataV2),
        /// Version 1 of the table metadata
        V1(TableMetadataV1),
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    /// Fields for the version 2 of the table metadata.
    pub struct TableMetadataV2 {
        /// Integer Version for the format.
        pub format_version: VersionNumber<2>,
        /// A UUID that identifies the table
        pub table_uuid: Uuid,
        /// Location tables base location
        pub location: String,
        /// The tables highest sequence number
        pub last_sequence_number: i64,
        /// Timestamp in milliseconds from the unix epoch when the table was last updated.
        pub last_updated_ms: i64,
        /// An integer; the highest assigned column ID for the table.
        pub last_column_id: i32,
        /// A list of schemas, stored as objects with schema-id.
        pub schemas: Vec<schema::SchemaV2>,
        /// ID of the table’s current schema.
        pub current_schema_id: i32,
        /// A list of partition specs, stored as full partition spec objects.
        pub partition_specs: Vec<PartitionSpec>,
        /// ID of the “current” spec that writers should use by default.
        pub default_spec_id: i32,
        /// An integer; the highest assigned partition field ID across all partition specs for the table.
        pub last_partition_id: i32,
        ///A string to string map of table properties. This is used to control settings that
        /// affect reading and writing and is not intended to be used for arbitrary metadata.
        /// For example, commit.retry.num-retries is used to control the number of commit retries.
        #[serde(skip_serializing_if = "HashMap::is_empty", default)]
        pub properties: HashMap<String, String>,
        /// long ID of the current table snapshot; must be the same as the current
        /// ID of the main branch in refs.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub current_snapshot_id: Option<i64>,
        ///A list of valid snapshots. Valid snapshots are snapshots for which all
        /// data files exist in the file system. A data file must not be deleted
        /// from the file system until the last snapshot in which it was listed is
        /// garbage collected.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub snapshots: Option<Vec<SnapshotV2>>,
        /// A list (optional) of timestamp and snapshot ID pairs that encodes changes
        /// to the current snapshot for the table. Each time the current-snapshot-id
        /// is changed, a new entry should be added with the last-updated-ms
        /// and the new current-snapshot-id. When snapshots are expired from
        /// the list of valid snapshots, all entries before a snapshot that has
        /// expired should be removed.
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        pub snapshot_log: Vec<SnapshotLog>,

        /// A list (optional) of timestamp and metadata file location pairs
        /// that encodes changes to the previous metadata files for the table.
        /// Each time a new metadata file is created, a new entry of the
        /// previous metadata file location should be added to the list.
        /// Tables can be configured to remove oldest metadata log entries and
        /// keep a fixed-size log of the most recent entries after a commit.
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        pub metadata_log: Vec<MetadataLog>,

        /// A list of sort orders, stored as full sort order objects.
        pub sort_orders: Vec<sort::SortOrder>,
        /// Default sort order id of the table. Note that this could be used by
        /// writers, but is not used when reading because reads use the specs
        /// stored in manifest files.
        pub default_sort_order_id: i32,
        ///A map of snapshot references. The map keys are the unique snapshot reference
        /// names in the table, and the map values are snapshot reference objects.
        /// There is always a main branch reference pointing to the current-snapshot-id
        /// even if the refs map is null.
        #[serde(skip_serializing_if = "HashMap::is_empty", default)]
        pub refs: HashMap<String, SnapshotReference>,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    /// Fields for the version 1 of the table metadata.
    pub struct TableMetadataV1 {
        /// Integer Version for the format.
        pub format_version: VersionNumber<1>,
        /// A UUID that identifies the table
        #[serde(skip_serializing_if = "Option::is_none")]
        pub table_uuid: Option<Uuid>,
        /// Location tables base location
        pub location: String,
        /// Timestamp in milliseconds from the unix epoch when the table was last updated.
        pub last_updated_ms: i64,
        /// An integer; the highest assigned column ID for the table.
        pub last_column_id: i32,
        /// The table’s current schema.
        pub schema: schema::SchemaV1,
        /// A list of schemas, stored as objects with schema-id.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schemas: Option<Vec<schema::SchemaV1>>,
        /// ID of the table’s current schema.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub current_schema_id: Option<i32>,
        /// The table’s current partition spec, stored as only fields. Note that this is used by writers to partition data,
        /// but is not used when reading because reads use the specs stored in manifest files.
        pub partition_spec: Vec<PartitionField>,
        /// A list of partition specs, stored as full partition spec objects.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub partition_specs: Option<Vec<PartitionSpec>>,
        /// ID of the “current” spec that writers should use by default.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub default_spec_id: Option<i32>,
        /// An integer; the highest assigned partition field ID across all partition specs for the table.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub last_partition_id: Option<i32>,
        ///A string to string map of table properties. This is used to control settings that
        /// affect reading and writing and is not intended to be used for arbitrary metadata.
        /// For example, commit.retry.num-retries is used to control the number of commit retries.
        #[serde(skip_serializing_if = "HashMap::is_empty", default)]
        pub properties: HashMap<String, String>,
        /// long ID of the current table snapshot; must be the same as the current
        /// ID of the main branch in refs.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub current_snapshot_id: Option<i64>,
        ///A list of valid snapshots. Valid snapshots are snapshots for which all
        /// data files exist in the file system. A data file must not be deleted
        /// from the file system until the last snapshot in which it was listed is
        /// garbage collected.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub snapshots: Option<Vec<SnapshotV1>>,
        /// A list (optional) of timestamp and snapshot ID pairs that encodes changes
        /// to the current snapshot for the table. Each time the current-snapshot-id
        /// is changed, a new entry should be added with the last-updated-ms
        /// and the new current-snapshot-id. When snapshots are expired from
        /// the list of valid snapshots, all entries before a snapshot that has
        /// expired should be removed.
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        pub snapshot_log: Vec<SnapshotLog>,

        /// A list (optional) of timestamp and metadata file location pairs
        /// that encodes changes to the previous metadata files for the table.
        /// Each time a new metadata file is created, a new entry of the
        /// previous metadata file location should be added to the list.
        /// Tables can be configured to remove oldest metadata log entries and
        /// keep a fixed-size log of the most recent entries after a commit.
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        pub metadata_log: Vec<MetadataLog>,

        /// A list of sort orders, stored as full sort order objects.
        pub sort_orders: Option<Vec<sort::SortOrder>>,
        /// Default sort order id of the table. Note that this could be used by
        /// writers, but is not used when reading because reads use the specs
        /// stored in manifest files.
        pub default_sort_order_id: Option<i32>,
    }

    impl TryFrom<TableMetadataEnum> for TableMetadata {
        type Error = Error;
        fn try_from(value: TableMetadataEnum) -> Result<Self, Error> {
            match value {
                TableMetadataEnum::V2(value) => value.try_into(),
                TableMetadataEnum::V1(value) => value.try_into(),
            }
        }
    }

    impl From<TableMetadata> for TableMetadataEnum {
        fn from(value: TableMetadata) -> Self {
            match value.format_version {
                FormatVersion::V2 => TableMetadataEnum::V2(value.into()),
                FormatVersion::V1 => TableMetadataEnum::V1(value.into()),
            }
        }
    }

    impl TryFrom<TableMetadataV2> for TableMetadata {
        type Error = Error;
        fn try_from(value: TableMetadataV2) -> Result<Self, Error> {
            let current_snapshot_id = if let &Some(-1) = &value.current_snapshot_id {
                None
            } else {
                value.current_snapshot_id
            };
            let schemas = HashMap::from_iter(
                value
                    .schemas
                    .into_iter()
                    .map(|schema| Ok((schema.schema_id, schema.try_into()?)))
                    .collect::<Result<Vec<_>, Error>>()?,
            );
            let mut refs = value.refs;
            if let Some(snapshot_id) = current_snapshot_id {
                refs.entry(MAIN_BRANCH.to_string())
                    .or_insert(SnapshotReference {
                        snapshot_id,
                        retention: SnapshotRetention::default(),
                    });
            }
            Ok(TableMetadata {
                format_version: FormatVersion::V2,
                table_uuid: value.table_uuid,
                location: value.location,
                last_sequence_number: value.last_sequence_number,
                last_updated_ms: value.last_updated_ms,
                last_column_id: value.last_column_id,
                current_schema_id: if schemas.keys().contains(&value.current_schema_id) {
                    Ok(value.current_schema_id)
                } else {
                    Err(Error::InvalidFormat("schema".to_string()))
                }?,
                schemas,
                partition_specs: HashMap::from_iter(
                    value.partition_specs.into_iter().map(|x| (*x.spec_id(), x)),
                ),
                default_spec_id: value.default_spec_id,
                last_partition_id: value.last_partition_id,
                properties: value.properties,
                current_snapshot_id,
                snapshots: value
                    .snapshots
                    .map(|snapshots| {
                        HashMap::from_iter(snapshots.into_iter().map(|x| (x.snapshot_id, x.into())))
                    })
                    .unwrap_or_default(),
                snapshot_log: value.snapshot_log,
                metadata_log: value.metadata_log,
                sort_orders: HashMap::from_iter(
                    value.sort_orders.into_iter().map(|x| (x.order_id, x)),
                ),
                default_sort_order_id: value.default_sort_order_id,
                refs,
            })
        }
    }

    impl TryFrom<TableMetadataV1> for TableMetadata {
        type Error = Error;
        fn try_from(value: TableMetadataV1) -> Result<Self, Error> {
            let schemas = value
                .schemas
                .map(|schemas| {
                    Ok::<_, Error>(HashMap::from_iter(
                        schemas
                            .into_iter()
                            .enumerate()
                            .map(|(i, schema)| {
                                Ok((schema.schema_id.unwrap_or(i as i32), schema.try_into()?))
                            })
                            .collect::<Result<Vec<_>, Error>>()?
                            .into_iter(),
                    ))
                })
                .or_else(|| {
                    Some(Ok(HashMap::from_iter(vec![(
                        value.schema.schema_id.unwrap_or(0),
                        value.schema.try_into().ok()?,
                    )])))
                })
                .transpose()?
                .unwrap_or_default();
            let partition_specs = HashMap::from_iter(
                value
                    .partition_specs
                    .unwrap_or_else(|| {
                        vec![PartitionSpec::builder()
                            .with_spec_id(DEFAULT_SPEC_ID)
                            .with_fields(value.partition_spec)
                            .build()
                            .unwrap()]
                    })
                    .into_iter()
                    .map(|x| (*x.spec_id(), x)),
            );
            Ok(TableMetadata {
                format_version: FormatVersion::V1,
                table_uuid: value.table_uuid.unwrap_or_default(),
                location: value.location,
                last_sequence_number: 0,
                last_updated_ms: value.last_updated_ms,
                last_column_id: value.last_column_id,
                current_schema_id: value
                    .current_schema_id
                    .unwrap_or_else(|| schemas.keys().copied().max().unwrap_or_default()),
                default_spec_id: value
                    .default_spec_id
                    .unwrap_or_else(|| partition_specs.keys().copied().max().unwrap_or_default()),
                last_partition_id: value
                    .last_partition_id
                    .unwrap_or_else(|| partition_specs.keys().copied().max().unwrap_or_default()),
                partition_specs,
                schemas,

                properties: value.properties,
                current_snapshot_id: if let &Some(id) = &value.current_snapshot_id {
                    if id == -1 {
                        None
                    } else {
                        Some(id)
                    }
                } else {
                    value.current_snapshot_id
                },
                snapshots: value
                    .snapshots
                    .map(|snapshots| {
                        Ok::<_, Error>(HashMap::from_iter(
                            snapshots
                                .into_iter()
                                .map(|x| Ok((x.snapshot_id, x.into())))
                                .collect::<Result<Vec<_>, Error>>()?,
                        ))
                    })
                    .transpose()?
                    .unwrap_or_default(),
                snapshot_log: value.snapshot_log,
                metadata_log: value.metadata_log,
                sort_orders: match value.sort_orders {
                    Some(sort_orders) => {
                        HashMap::from_iter(sort_orders.into_iter().map(|x| (x.order_id, x)))
                    }
                    None => HashMap::new(),
                },
                default_sort_order_id: value.default_sort_order_id.unwrap_or(DEFAULT_SORT_ORDER_ID),
                refs: HashMap::from_iter(vec![(
                    MAIN_BRANCH.to_string(),
                    SnapshotReference {
                        snapshot_id: value.current_snapshot_id.unwrap_or_default(),
                        retention: SnapshotRetention::Branch {
                            min_snapshots_to_keep: None,
                            max_snapshot_age_ms: None,
                            max_ref_age_ms: None,
                        },
                    },
                )]),
            })
        }
    }

    impl From<TableMetadata> for TableMetadataV2 {
        fn from(v: TableMetadata) -> Self {
            TableMetadataV2 {
                format_version: VersionNumber::<2>,
                table_uuid: v.table_uuid,
                location: v.location,
                last_sequence_number: v.last_sequence_number,
                last_updated_ms: v.last_updated_ms,
                last_column_id: v.last_column_id,
                schemas: v.schemas.into_values().map(|x| x.into()).collect(),
                current_schema_id: v.current_schema_id,
                partition_specs: v.partition_specs.into_values().collect(),
                default_spec_id: v.default_spec_id,
                last_partition_id: v.last_partition_id,
                properties: v.properties,
                current_snapshot_id: v.current_snapshot_id.or(Some(-1)),
                snapshots: Some(v.snapshots.into_values().map(|x| x.into()).collect()),
                snapshot_log: v.snapshot_log,
                metadata_log: v.metadata_log,
                sort_orders: v.sort_orders.into_values().collect(),
                default_sort_order_id: v.default_sort_order_id,
                refs: v.refs,
            }
        }
    }

    impl From<TableMetadata> for TableMetadataV1 {
        fn from(v: TableMetadata) -> Self {
            TableMetadataV1 {
                format_version: VersionNumber::<1>,
                table_uuid: Some(v.table_uuid),
                location: v.location,
                last_updated_ms: v.last_updated_ms,
                last_column_id: v.last_column_id,
                schema: v.schemas.get(&v.current_schema_id).unwrap().clone().into(),
                schemas: Some(v.schemas.into_values().map(|x| x.into()).collect()),
                current_schema_id: Some(v.current_schema_id),
                partition_spec: v
                    .partition_specs
                    .get(&v.default_spec_id)
                    .map(|x| x.fields().clone())
                    .unwrap_or_default(),
                partition_specs: Some(v.partition_specs.into_values().collect()),
                default_spec_id: Some(v.default_spec_id),
                last_partition_id: Some(v.last_partition_id),
                properties: v.properties,
                current_snapshot_id: v.current_snapshot_id.or(Some(-1)),
                snapshots: Some(v.snapshots.into_values().map(|x| x.into()).collect()),
                snapshot_log: v.snapshot_log,
                metadata_log: v.metadata_log,
                sort_orders: Some(v.sort_orders.into_values().collect()),
                default_sort_order_id: Some(v.default_sort_order_id),
            }
        }
    }
}

/// Helper to serialize and deserialize the format version.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct VersionNumber<const V: u8>;

impl<const V: u8> Serialize for VersionNumber<V> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u8(V)
    }
}

impl<'de, const V: u8> Deserialize<'de> for VersionNumber<V> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = u8::deserialize(deserializer)?;
        if value == V {
            Ok(VersionNumber::<V>)
        } else {
            Err(serde::de::Error::custom("Invalid Version"))
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Encodes changes to the previous metadata files for the table
pub struct MetadataLog {
    /// The file for the log.
    pub metadata_file: String,
    /// Time new metadata was created
    pub timestamp_ms: i64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// A log of when each snapshot was made.
pub struct SnapshotLog {
    /// Id of the snapshot.
    pub snapshot_id: i64,
    /// Last updated timestamp
    pub timestamp_ms: i64,
}

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
/// Iceberg format version
#[derive(Default)]
pub enum FormatVersion {
    /// Iceberg spec version 1
    V1 = b'1',
    /// Iceberg spec version 2
    #[default]
    V2 = b'2',
}

impl TryFrom<u8> for FormatVersion {
    type Error = Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(FormatVersion::V1),
            2 => Ok(FormatVersion::V2),
            _ => Err(Error::Conversion(
                "u8".to_string(),
                "format version".to_string(),
            )),
        }
    }
}

impl From<FormatVersion> for u8 {
    fn from(value: FormatVersion) -> Self {
        match value {
            FormatVersion::V1 => b'1',
            FormatVersion::V2 => b'2',
        }
    }
}

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, fs};

    use uuid::Uuid;

    use crate::{
        error::Error,
        spec::{
            partition::{PartitionField, PartitionSpec, Transform},
            schema::SchemaBuilder,
            snapshot::{Operation, SnapshotBuilder, SnapshotReference, SnapshotRetention, Summary},
            sort::{NullOrder, SortDirection, SortField, SortOrderBuilder},
            table_metadata::TableMetadata,
            types::{PrimitiveType, StructField, StructTypeBuilder, Type},
        },
    };

    use super::{FormatVersion, SnapshotLog};

    fn check_table_metadata_serde(json: &str, expected_type: TableMetadata) {
        let desered_type: TableMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(desered_type, expected_type);

        let sered_json = serde_json::to_string(&expected_type).unwrap();
        let parsed_json_value = serde_json::from_str::<TableMetadata>(&sered_json).unwrap();

        assert_eq!(parsed_json_value, desered_type);
    }

    #[test]
    fn test_deserialize_table_data_v2() -> Result<(), Error> {
        let data = r#"
            {
                "format-version" : 2,
                "table-uuid": "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
                "location": "s3://b/wh/data.db/table",
                "last-sequence-number" : 1,
                "last-updated-ms": 1515100955770,
                "last-column-id": 1,
                "schemas": [
                    {
                        "schema-id" : 1,
                        "type" : "struct",
                        "fields" :[
                            {
                                "id": 1,
                                "name": "struct_name",
                                "required": true,
                                "type": "fixed[1]"
                            }
                        ]
                    }
                ],
                "current-schema-id" : 1,
                "partition-specs": [
                    {
                        "spec-id": 1,
                        "fields": [
                            {  
                                "source-id": 4,  
                                "field-id": 1000,  
                                "name": "ts_day",  
                                "transform": "day"
                            } 
                        ]
                    }
                ],
                "default-spec-id": 1,
                "last-partition-id": 1,
                "properties": {
                    "commit.retry.num-retries": "1"
                },
                "metadata-log": [
                    {  
                        "metadata-file": "s3://bucket/.../v1.json",  
                        "timestamp-ms": 1515100
                    }
                ],
                "sort-orders": [],
                "default-sort-order-id": 0
            }
        "#;
        let metadata =
            serde_json::from_str::<TableMetadata>(data).expect("Failed to deserialize json");
        //test serialise deserialise works.
        let metadata_two: TableMetadata = serde_json::from_str(
            &serde_json::to_string(&metadata).expect("Failed to serialize metadata"),
        )
        .expect("Failed to serialize json");
        assert_eq!(metadata, metadata_two);

        Ok(())
    }

    #[test]
    fn test_deserialize_table_data_v1() -> Result<(), Error> {
        let data = r#"
        {
            "format-version" : 1,
            "table-uuid" : "df838b92-0b32-465d-a44e-d39936e538b7",
            "location" : "/home/iceberg/warehouse/nyc/taxis",
            "last-updated-ms" : 1662532818843,
            "last-column-id" : 5,
            "schema" : {
              "type" : "struct",
              "schema-id" : 0,
              "fields" : [ {
                "id" : 1,
                "name" : "vendor_id",
                "required" : false,
                "type" : "long"
              }, {
                "id" : 2,
                "name" : "trip_id",
                "required" : false,
                "type" : "long"
              }, {
                "id" : 3,
                "name" : "trip_distance",
                "required" : false,
                "type" : "float"
              }, {
                "id" : 4,
                "name" : "fare_amount",
                "required" : false,
                "type" : "double"
              }, {
                "id" : 5,
                "name" : "store_and_fwd_flag",
                "required" : false,
                "type" : "string"
              } ]
            },
            "current-schema-id" : 0,
            "schemas" : [ {
              "type" : "struct",
              "schema-id" : 0,
              "fields" : [ {
                "id" : 1,
                "name" : "vendor_id",
                "required" : false,
                "type" : "long"
              }, {
                "id" : 2,
                "name" : "trip_id",
                "required" : false,
                "type" : "long"
              }, {
                "id" : 3,
                "name" : "trip_distance",
                "required" : false,
                "type" : "float"
              }, {
                "id" : 4,
                "name" : "fare_amount",
                "required" : false,
                "type" : "double"
              }, {
                "id" : 5,
                "name" : "store_and_fwd_flag",
                "required" : false,
                "type" : "string"
              } ]
            } ],
            "partition-spec" : [ {
              "name" : "vendor_id",
              "transform" : "identity",
              "source-id" : 1,
              "field-id" : 1000
            } ],
            "default-spec-id" : 0,
            "partition-specs" : [ {
              "spec-id" : 0,
              "fields" : [ {
                "name" : "vendor_id",
                "transform" : "identity",
                "source-id" : 1,
                "field-id" : 1000
              } ]
            } ],
            "last-partition-id" : 1000,
            "default-sort-order-id" : 0,
            "sort-orders" : [ {
              "order-id" : 0,
              "fields" : [ ]
            } ],
            "properties" : {
              "owner" : "root"
            },
            "current-snapshot-id" : 638933773299822130,
            "refs" : {
              "main" : {
                "snapshot-id" : 638933773299822130,
                "type" : "branch"
              }
            },
            "snapshots" : [ {
              "snapshot-id" : 638933773299822130,
              "timestamp-ms" : 1662532818843,
              "summary" : {
                "operation" : "append",
                "spark.app.id" : "local-1662532784305",
                "added-data-files" : "4",
                "added-records" : "4",
                "added-files-size" : "6001",
                "changed-partition-count" : "2",
                "total-records" : "4",
                "total-files-size" : "6001",
                "total-data-files" : "4",
                "total-delete-files" : "0",
                "total-position-deletes" : "0",
                "total-equality-deletes" : "0"
              },
              "manifest-list" : "/home/iceberg/warehouse/nyc/taxis/metadata/snap-638933773299822130-1-7e6760f0-4f6c-4b23-b907-0a5a174e3863.avro",
              "schema-id" : 0
            } ],
            "snapshot-log" : [ {
              "timestamp-ms" : 1662532818843,
              "snapshot-id" : 638933773299822130
            } ],
            "metadata-log" : [ {
              "timestamp-ms" : 1662532805245,
              "metadata-file" : "/home/iceberg/warehouse/nyc/taxis/metadata/00000-8a62c37d-4573-4021-952a-c0baef7d21d0.metadata.json"
            } ]
          }
        "#;
        let metadata =
            serde_json::from_str::<TableMetadata>(data).expect("Failed to deserialize json");
        //test serialise deserialise works.
        let metadata_two: TableMetadata = serde_json::from_str(
            &serde_json::to_string(&metadata).expect("Failed to serialize metadata"),
        )
        .expect("Failed to serialize json");
        assert_eq!(metadata, metadata_two);

        Ok(())
    }

    #[test]
    fn test_table_metadata_v2_file_valid() {
        let metadata =
            fs::read_to_string("testdata/table_metadata/TableMetadataV2Valid.json").unwrap();

        let schema1 = SchemaBuilder::default()
            .with_schema_id(0)
            .with_fields(
                StructTypeBuilder::default()
                    .with_struct_field(StructField {
                        id: 1,
                        name: "x".to_owned(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: None,
                    })
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        let schema2 = SchemaBuilder::default()
            .with_schema_id(1)
            .with_fields(
                StructTypeBuilder::default()
                    .with_struct_field(StructField {
                        id: 1,
                        name: "x".to_owned(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: None,
                    })
                    .with_struct_field(StructField {
                        id: 2,
                        name: "y".to_owned(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: Some("comment".to_owned()),
                    })
                    .with_struct_field(StructField {
                        id: 3,
                        name: "z".to_owned(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: None,
                    })
                    .build()
                    .unwrap(),
            )
            .with_identifier_field_ids(vec![1, 2])
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder()
            .with_partition_field(PartitionField::new(1, 1000, "x", Transform::Identity))
            .build()
            .unwrap();

        let sort_order = SortOrderBuilder::default()
            .with_order_id(3)
            .with_sort_field(SortField {
                source_id: 2,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            })
            .with_sort_field(SortField {
                source_id: 3,
                transform: Transform::Bucket(4),
                direction: SortDirection::Descending,
                null_order: NullOrder::Last,
            })
            .build()
            .unwrap();

        let snapshot1 = SnapshotBuilder::default()
            .with_snapshot_id(3051729675574597004)
            .with_timestamp_ms(1515100955770)
            .with_sequence_number(0)
            .with_manifest_list("s3://a/b/1.avro".to_string())
            .with_summary(Summary {
                operation: Operation::Append,
                other: HashMap::new(),
            })
            .build()
            .expect("Failed to create snapshot");

        let snapshot2 = SnapshotBuilder::default()
            .with_snapshot_id(3055729675574597004)
            .with_parent_snapshot_id(3051729675574597004)
            .with_timestamp_ms(1555100955770)
            .with_sequence_number(1)
            .with_schema_id(1)
            .with_manifest_list("s3://a/b/2.avro".to_string())
            .with_summary(Summary {
                operation: Operation::Append,
                other: HashMap::new(),
            })
            .build()
            .expect("Failed to create snapshot");

        let expected = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: Uuid::parse_str("9c12d441-03fe-4693-9a96-a0705ddf69c1").unwrap(),
            location: "s3://bucket/test/location".to_string(),
            last_updated_ms: 1602638573590,
            last_column_id: 3,
            schemas: HashMap::from_iter(vec![(0, schema1), (1, schema2)]),
            current_schema_id: 1,
            partition_specs: HashMap::from_iter(vec![(0, partition_spec)]),
            default_spec_id: 0,
            last_partition_id: 1000,
            default_sort_order_id: 3,
            sort_orders: HashMap::from_iter(vec![(3, sort_order)]),
            snapshots: HashMap::from_iter(vec![
                (3051729675574597004, snapshot1),
                (3055729675574597004, snapshot2),
            ]),
            current_snapshot_id: Some(3055729675574597004),
            last_sequence_number: 34,
            properties: HashMap::new(),
            snapshot_log: vec![
                SnapshotLog {
                    snapshot_id: 3051729675574597004,
                    timestamp_ms: 1515100955770,
                },
                SnapshotLog {
                    snapshot_id: 3055729675574597004,
                    timestamp_ms: 1555100955770,
                },
            ],
            metadata_log: Vec::new(),
            refs: HashMap::from_iter(vec![(
                "main".to_string(),
                SnapshotReference {
                    snapshot_id: 3055729675574597004,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            )]),
        };

        check_table_metadata_serde(&metadata, expected);
    }

    #[test]
    fn test_table_metadata_v2_file_valid_minimal() {
        let metadata =
            fs::read_to_string("testdata/table_metadata/TableMetadataV2ValidMinimal.json").unwrap();

        let schema = SchemaBuilder::default()
            .with_schema_id(0)
            .with_fields(
                StructTypeBuilder::default()
                    .with_struct_field(StructField {
                        id: 1,
                        name: "x".to_owned(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: None,
                    })
                    .with_struct_field(StructField {
                        id: 2,
                        name: "y".to_owned(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: Some("comment".to_owned()),
                    })
                    .with_struct_field(StructField {
                        id: 3,
                        name: "z".to_owned(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: None,
                    })
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder()
            .with_partition_field(PartitionField::new(1, 1000, "x", Transform::Identity))
            .build()
            .unwrap();

        let sort_order = SortOrderBuilder::default()
            .with_order_id(3)
            .with_sort_field(SortField {
                source_id: 2,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            })
            .with_sort_field(SortField {
                source_id: 3,
                transform: Transform::Bucket(4),
                direction: SortDirection::Descending,
                null_order: NullOrder::Last,
            })
            .build()
            .unwrap();

        let expected = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: Uuid::parse_str("9c12d441-03fe-4693-9a96-a0705ddf69c1").unwrap(),
            location: "s3://bucket/test/location".to_string(),
            last_updated_ms: 1602638573590,
            last_column_id: 3,
            schemas: HashMap::from_iter(vec![(0, schema)]),
            current_schema_id: 0,
            partition_specs: HashMap::from_iter(vec![(0, partition_spec)]),
            default_spec_id: 0,
            last_partition_id: 1000,
            default_sort_order_id: 3,
            sort_orders: HashMap::from_iter(vec![(3, sort_order)]),
            snapshots: HashMap::default(),
            current_snapshot_id: None,
            last_sequence_number: 34,
            properties: HashMap::new(),
            snapshot_log: vec![],
            metadata_log: Vec::new(),
            refs: HashMap::new(),
        };

        check_table_metadata_serde(&metadata, expected);
    }

    #[test]
    fn test_table_metadata_v1_file_valid() {
        let metadata =
            fs::read_to_string("testdata/table_metadata/TableMetadataV1Valid.json").unwrap();

        let schema = SchemaBuilder::default()
            .with_schema_id(0)
            .with_fields(
                StructTypeBuilder::default()
                    .with_struct_field(StructField {
                        id: 1,
                        name: "x".to_owned(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: None,
                    })
                    .with_struct_field(StructField {
                        id: 2,
                        name: "y".to_owned(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: Some("comment".to_owned()),
                    })
                    .with_struct_field(StructField {
                        id: 3,
                        name: "z".to_owned(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: None,
                    })
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder()
            .with_partition_field(PartitionField::new(1, 1000, "x", Transform::Identity))
            .build()
            .unwrap();

        let expected = TableMetadata {
            format_version: FormatVersion::V1,
            table_uuid: Uuid::parse_str("d20125c8-7284-442c-9aea-15fee620737c").unwrap(),
            location: "s3://bucket/test/location".to_string(),
            last_updated_ms: 1602638573874,
            last_column_id: 3,
            schemas: HashMap::from_iter(vec![(0, schema)]),
            current_schema_id: 0,
            partition_specs: HashMap::from_iter(vec![(0, partition_spec)]),
            default_spec_id: 0,
            last_partition_id: 0,
            default_sort_order_id: 0,
            sort_orders: HashMap::new(),
            snapshots: HashMap::new(),
            current_snapshot_id: None,
            last_sequence_number: 0,
            properties: HashMap::new(),
            snapshot_log: vec![],
            metadata_log: Vec::new(),
            refs: HashMap::from_iter(vec![(
                "main".to_string(),
                SnapshotReference {
                    snapshot_id: -1,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            )]),
        };

        check_table_metadata_serde(&metadata, expected);
    }

    #[test]
    fn test_table_metadata_v2_missing_sort_order() {
        let metadata =
            fs::read_to_string("testdata/table_metadata/TableMetadataV2MissingSortOrder.json")
                .unwrap();

        let desered: Result<TableMetadata, serde_json::Error> = serde_json::from_str(&metadata);

        assert_eq!(
            desered.unwrap_err().to_string(),
            "data did not match any variant of untagged enum TableMetadataEnum"
        )
    }
}
