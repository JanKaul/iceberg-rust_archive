/*!
Manifest files
*/
use std::collections::HashMap;

use apache_avro::Schema as AvroSchema;
use derive_builder::Builder;
use derive_getters::Getters;
use serde::{de::DeserializeOwned, ser::SerializeSeq, Deserialize, Serialize};
use serde_bytes::ByteBuf;
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::{error::Error, partition::BoundPartitionField};

use super::{
    partition::PartitionSpec,
    schema::Schema,
    table_metadata::{FormatVersion, TableMetadata},
    types::{PrimitiveType, StructType, Type},
    values::{Struct, Value},
};

/// Entry in manifest with the iceberg spec version 2.
#[derive(Debug, Serialize, PartialEq, Clone, Getters, Builder)]
#[serde(into = "ManifestEntryEnum")]
#[builder(setter(prefix = "with"))]
pub struct ManifestEntry {
    /// Table format version
    format_version: FormatVersion,
    /// Used to track additions and deletions
    status: Status,
    /// Snapshot id where the file was added, or deleted if status is 2.
    /// Inherited when null.
    #[builder(setter(strip_option), default)]
    snapshot_id: Option<i64>,
    /// Sequence number when the file was added. Inherited when null.
    #[builder(setter(strip_option), default)]
    sequence_number: Option<i64>,
    /// File path, partition tuple, metrics, …
    data_file: DataFile,
}

impl ManifestEntry {
    pub fn builder() -> ManifestEntryBuilder {
        ManifestEntryBuilder::default()
    }

    /// Change a field status to `Existing`, and assign the appropriate sequence number from the table metadata.
    pub fn mark_existing(&mut self, table_metadata: &TableMetadata) -> Result<(), Error> {
        self.status = Status::Existing;
        let entry_snapshot_id = self
            .snapshot_id
            .ok_or(apache_avro::Error::DeserializeValue(
                "snapshot_id missing in ManifestEntry.".to_owned(),
            ))?;
        let entry_sequence_number =
            table_metadata
                .sequence_number(entry_snapshot_id)
                .ok_or(Error::NotFound(
                    "Snapshot Id".to_owned(),
                    entry_snapshot_id.to_string(),
                ))?;
        self.sequence_number = Some(entry_sequence_number);
        Ok(())
    }
}

impl ManifestEntry {
    pub fn try_from_v2(
        value: ManifestEntryV2,
        schema: &Schema,
        partition_spec: &PartitionSpec,
    ) -> Result<Self, Error> {
        Ok(ManifestEntry {
            format_version: FormatVersion::V2,
            status: value.status,
            snapshot_id: value.snapshot_id,
            sequence_number: value.sequence_number,
            data_file: DataFile::try_from_v2(value.data_file, schema, partition_spec)?,
        })
    }

    pub fn try_from_v1(
        value: ManifestEntryV1,
        schema: &Schema,
        partition_spec: &PartitionSpec,
    ) -> Result<Self, Error> {
        Ok(ManifestEntry {
            format_version: FormatVersion::V2,
            status: value.status,
            snapshot_id: Some(value.snapshot_id),
            sequence_number: None,
            data_file: DataFile::try_from_v1(value.data_file, schema, partition_spec)?,
        })
    }
}

/// Entry in manifest
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(untagged)]
pub enum ManifestEntryEnum {
    /// Manifest entry version 2
    V2(ManifestEntryV2),
    /// Manifest entry version 1
    V1(ManifestEntryV1),
}

/// Entry in manifest with the iceberg spec version 2.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ManifestEntryV2 {
    /// Used to track additions and deletions
    pub status: Status,
    /// Snapshot id where the file was added, or deleted if status is 2.
    /// Inherited when null.
    pub snapshot_id: Option<i64>,
    /// Sequence number when the file was added. Inherited when null.
    pub sequence_number: Option<i64>,
    /// File path, partition tuple, metrics, …
    pub data_file: DataFileV2,
}

/// Entry in manifest with the iceberg spec version 1.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ManifestEntryV1 {
    /// Used to track additions and deletions
    pub status: Status,
    /// Snapshot id where the file was added, or deleted if status is 2.
    /// Inherited when null.
    pub snapshot_id: i64,
    /// File path, partition tuple, metrics, …
    pub data_file: DataFileV1,
}

impl From<ManifestEntry> for ManifestEntryEnum {
    fn from(value: ManifestEntry) -> Self {
        match value.format_version {
            FormatVersion::V2 => ManifestEntryEnum::V2(value.into()),
            FormatVersion::V1 => ManifestEntryEnum::V1(value.into()),
        }
    }
}

impl From<ManifestEntry> for ManifestEntryV2 {
    fn from(value: ManifestEntry) -> Self {
        ManifestEntryV2 {
            status: value.status,
            snapshot_id: value.snapshot_id,
            sequence_number: value.sequence_number,
            data_file: value.data_file.into(),
        }
    }
}

impl From<ManifestEntry> for ManifestEntryV1 {
    fn from(v1: ManifestEntry) -> Self {
        ManifestEntryV1 {
            status: v1.status,
            snapshot_id: v1.snapshot_id.unwrap_or(0),
            data_file: v1.data_file.into(),
        }
    }
}

impl From<ManifestEntryV1> for ManifestEntryV2 {
    fn from(v1: ManifestEntryV1) -> Self {
        ManifestEntryV2 {
            status: v1.status,
            snapshot_id: Some(v1.snapshot_id),
            sequence_number: Some(0),
            data_file: v1.data_file.into(),
        }
    }
}

impl ManifestEntry {
    /// Get schema of manifest entry.
    pub fn schema(
        partition_schema: &str,
        format_version: &FormatVersion,
    ) -> Result<AvroSchema, Error> {
        let schema = match format_version {
            FormatVersion::V1 => {
                let datafile_schema = DataFileV1::schema(partition_schema);
                r#"{
            "type": "record",
            "name": "manifest_entry",
            "fields": [
                {
                    "name": "status",
                    "type": "int",
                    "field_id": 0
                },
                {
                    "name": "snapshot_id",
                    "type": "long",
                    "field_id": 1
                },
                {
                    "name": "data_file",
                    "type": "#
                    .to_owned()
                    + &datafile_schema
                    + r#",
                    "field_id": 2
                }
            ]
        }"#
            }
            FormatVersion::V2 => {
                let datafile_schema = DataFileV2::schema(partition_schema);
                r#"{
            "type": "record",
            "name": "manifest_entry",
            "fields": [
                {
                    "name": "status",
                    "type": "int",
                    "field_id": 0
                },
                {
                    "name": "snapshot_id",
                    "type": [
                        "null",
                        "long"
                    ],
                    "default": null,
                    "field_id": 1
                },
                {
                    "name": "sequence_number",
                    "type": [
                        "null",
                        "long"
                    ],
                    "default": null,
                    "field_id": 3
                },
                {
                    "name": "data_file",
                    "type": "#
                    .to_owned()
                    + &datafile_schema
                    + r#",
                    "field_id": 2
                }
            ]
        }"#
            }
        };
        AvroSchema::parse_str(&schema).map_err(Into::into)
    }
}

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
/// Used to track additions and deletions
pub enum Status {
    /// Existing files
    Existing = 0,
    /// Added files
    Added = 1,
    /// Deleted files
    Deleted = 2,
}

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Eq, Clone)]
#[repr(u8)]
/// Type of content stored by the data file.
pub enum Content {
    /// Data.
    Data = 0,
    /// Deletes at position.
    PositionDeletes = 1,
    /// Delete by equality.
    EqualityDeletes = 2,
}

impl TryFrom<Vec<u8>> for Content {
    type Error = Error;
    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        match String::from_utf8(value)?.to_uppercase().as_str() {
            "DATA" => Ok(Content::Data),
            "POSITION DELETES" => Ok(Content::PositionDeletes),
            "EQUALITY DELETES" => Ok(Content::EqualityDeletes),
            _ => Err(Error::Conversion(
                "string".to_string(),
                "content".to_string(),
            )),
        }
    }
}

impl From<Content> for Vec<u8> {
    fn from(value: Content) -> Self {
        match value {
            Content::Data => "DATA".as_bytes().to_owned(),
            Content::PositionDeletes => "POSITION DELETES".as_bytes().to_owned(),
            Content::EqualityDeletes => "EQUALITY DELETES".as_bytes().to_owned(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
#[repr(u8)]
/// Name of file format
pub enum FileFormat {
    /// Avro file
    Avro = 0,
    /// Orc file
    Orc = 1,
    /// Parquet file
    Parquet = 2,
}

/// Serialize for PrimitiveType wit special handling for
/// Decimal and Fixed types.
impl Serialize for FileFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use FileFormat::*;
        match self {
            Avro => serializer.serialize_str("AVRO"),
            Orc => serializer.serialize_str("ORC"),
            Parquet => serializer.serialize_str("PARQUET"),
        }
    }
}

/// Serialize for PrimitiveType wit special handling for
/// Decimal and Fixed types.
impl<'de> Deserialize<'de> for FileFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s == "AVRO" {
            Ok(FileFormat::Avro)
        } else if s == "ORC" {
            Ok(FileFormat::Orc)
        } else if s == "PARQUET" {
            Ok(FileFormat::Parquet)
        } else {
            Err(serde::de::Error::custom("Invalid data file format."))
        }
    }
}

/// Get schema for partition values depending on partition spec and table schema
pub fn partition_value_schema(spec: &[BoundPartitionField<'_>]) -> Result<String, Error> {
    Ok(spec
        .iter()
        .map(|field| {
            let data_type = avro_schema_datatype(field.field_type());
            Ok::<_, Error>(
                r#"
                {
                    "name": ""#
                    .to_owned()
                    + field.name()
                    + r#"", 
                    "type":  ["null",""#
                    + &format!("{}", &data_type)
                    + r#""],
                    "field_id": "#
                    + &field.field_id().to_string()
                    + r#",
                    "default": null
                },"#,
            )
        })
        .try_fold(
            r#"{"type": "record","name": "r102","fields": ["#.to_owned(),
            |acc, x| {
                let result = acc + &x?;
                Ok::<_, Error>(result)
            },
        )?
        .trim_end_matches(',')
        .to_owned()
        + r#"]}"#)
}

fn avro_schema_datatype(data_type: &Type) -> Type {
    match data_type {
        Type::Primitive(prim) => match prim {
            PrimitiveType::Date => Type::Primitive(PrimitiveType::Int),
            PrimitiveType::Time => Type::Primitive(PrimitiveType::Long),
            PrimitiveType::Timestamp => Type::Primitive(PrimitiveType::Long),
            PrimitiveType::Timestamptz => Type::Primitive(PrimitiveType::Long),
            p => Type::Primitive(p.clone()),
        },
        t => t.clone(),
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct KeyValue<T: Serialize + Clone> {
    key: i32,
    value: T,
}

/// Utility struct to convert avro maps to rust hashmaps. Derefences to a Hashmap.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct AvroMap<T: Serialize + Clone>(pub HashMap<i32, T>);

impl<T: Serialize + Clone> core::ops::Deref for AvroMap<T> {
    type Target = HashMap<i32, T>;

    fn deref(self: &'_ AvroMap<T>) -> &'_ Self::Target {
        &self.0
    }
}

impl<T: Serialize + Clone> core::ops::DerefMut for AvroMap<T> {
    fn deref_mut(self: &'_ mut AvroMap<T>) -> &'_ mut Self::Target {
        &mut self.0
    }
}

impl<T: Serialize + Clone> Serialize for AvroMap<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let entries = self
            .0
            .iter()
            .map(|(key, value)| KeyValue {
                key: *key,
                value: (*value).clone(),
            })
            .collect::<Vec<KeyValue<T>>>();
        let mut seq = serializer.serialize_seq(Some(entries.len()))?;
        for element in entries {
            seq.serialize_element(&element)?;
        }
        seq.end()
    }
}

impl<'de, T: Serialize + DeserializeOwned + Clone> Deserialize<'de> for AvroMap<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let vec: Vec<KeyValue<T>> = Vec::deserialize(deserializer)?;
        Ok(AvroMap(HashMap::from_iter(
            vec.into_iter().map(|x| (x.key, x.value)),
        )))
    }
}

impl AvroMap<ByteBuf> {
    fn into_value_map(self, schema: &StructType) -> Result<HashMap<i32, Value>, Error> {
        Ok(HashMap::from_iter(
            self.0
                .into_iter()
                .map(|(k, v)| {
                    Ok((
                        k,
                        Value::try_from_bytes(
                            &v,
                            &schema
                                .get(k as usize)
                                .ok_or(Error::Schema(k.to_string(), format!("{:?}", schema)))?
                                .field_type,
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>, Error>>()?,
        ))
    }
}

impl From<HashMap<i32, Value>> for AvroMap<ByteBuf> {
    fn from(value: HashMap<i32, Value>) -> Self {
        AvroMap(HashMap::from_iter(
            value.into_iter().map(|(k, v)| (k, v.into())),
        ))
    }
}

#[derive(Debug, PartialEq, Clone, Getters, Builder)]
#[builder(setter(prefix = "with"))]
/// DataFile found in Manifest.
pub struct DataFile {
    ///Type of content in data file.
    content: Content,
    /// Full URI for the file with a FS scheme.
    file_path: String,
    /// String file format name, avro, orc or parquet
    file_format: FileFormat,
    /// Partition data tuple, schema based on the partition spec output using partition field ids for the struct field ids
    partition: Struct,
    /// Number of records in this file
    record_count: i64,
    /// Total file size in bytes
    file_size_in_bytes: i64,
    /// Map from column id to total size on disk
    column_sizes: Option<AvroMap<i64>>,
    /// Map from column id to number of values in the column (including null and NaN values)
    value_counts: Option<AvroMap<i64>>,
    /// Map from column id to number of null values
    null_value_counts: Option<AvroMap<i64>>,
    /// Map from column id to number of NaN values
    nan_value_counts: Option<AvroMap<i64>>,
    /// Map from column id to number of distinct values in the column.
    distinct_counts: Option<AvroMap<i64>>,
    /// Map from column id to lower bound in the column
    lower_bounds: Option<HashMap<i32, Value>>,
    /// Map from column id to upper bound in the column
    upper_bounds: Option<HashMap<i32, Value>>,
    /// Implementation specific key metadata for encryption
    #[builder(default)]
    key_metadata: Option<ByteBuf>,
    /// Split offsets for the data file.
    #[builder(default)]
    split_offsets: Option<Vec<i64>>,
    /// Field ids used to determine row equality in equality delete files.
    #[builder(default)]
    equality_ids: Option<Vec<i32>>,
    /// ID representing sort order for this file
    #[builder(default)]
    sort_order_id: Option<i32>,
}

impl DataFile {
    pub fn builder() -> DataFileBuilder {
        DataFileBuilder::default()
    }

    pub(crate) fn try_from_v2(
        value: DataFileV2,
        schema: &Schema,
        partition_spec: &PartitionSpec,
    ) -> Result<Self, Error> {
        Ok(DataFile {
            content: value.content,
            file_path: value.file_path,
            file_format: value.file_format,
            partition: value
                .partition
                .cast(schema.fields(), partition_spec.fields())?,
            record_count: value.record_count,
            file_size_in_bytes: value.file_size_in_bytes,
            column_sizes: value.column_sizes,
            value_counts: value.value_counts,
            null_value_counts: value.null_value_counts,
            nan_value_counts: value.nan_value_counts,
            distinct_counts: value.distinct_counts,
            lower_bounds: value
                .lower_bounds
                .map(|map| map.into_value_map(schema.fields()))
                .transpose()?,
            upper_bounds: value
                .upper_bounds
                .map(|map| map.into_value_map(schema.fields()))
                .transpose()?,
            key_metadata: value.key_metadata,
            split_offsets: value.split_offsets,
            equality_ids: value.equality_ids,
            sort_order_id: value.sort_order_id,
        })
    }

    pub(crate) fn try_from_v1(
        value: DataFileV1,
        schema: &Schema,
        partition_spec: &PartitionSpec,
    ) -> Result<Self, Error> {
        Ok(DataFile {
            content: Content::Data,
            file_path: value.file_path,
            file_format: value.file_format,
            partition: value
                .partition
                .cast(schema.fields(), partition_spec.fields())?,
            record_count: value.record_count,
            file_size_in_bytes: value.file_size_in_bytes,
            column_sizes: value.column_sizes,
            value_counts: value.value_counts,
            null_value_counts: value.null_value_counts,
            nan_value_counts: value.nan_value_counts,
            distinct_counts: value.distinct_counts,
            lower_bounds: value
                .lower_bounds
                .map(|map| map.into_value_map(schema.fields()))
                .transpose()?,
            upper_bounds: value
                .upper_bounds
                .map(|map| map.into_value_map(schema.fields()))
                .transpose()?,
            key_metadata: value.key_metadata,
            split_offsets: value.split_offsets,
            equality_ids: None,
            sort_order_id: value.sort_order_id,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
/// DataFile found in Manifest.
pub struct DataFileV2 {
    ///Type of content in data file.
    pub content: Content,
    /// Full URI for the file with a FS scheme.
    pub file_path: String,
    /// String file format name, avro, orc or parquet
    pub file_format: FileFormat,
    /// Partition data tuple, schema based on the partition spec output using partition field ids for the struct field ids
    pub partition: Struct,
    /// Number of records in this file
    pub record_count: i64,
    /// Total file size in bytes
    pub file_size_in_bytes: i64,
    /// Map from column id to total size on disk
    pub column_sizes: Option<AvroMap<i64>>,
    /// Map from column id to number of values in the column (including null and NaN values)
    pub value_counts: Option<AvroMap<i64>>,
    /// Map from column id to number of null values
    pub null_value_counts: Option<AvroMap<i64>>,
    /// Map from column id to number of NaN values
    pub nan_value_counts: Option<AvroMap<i64>>,
    /// Map from column id to number of distinct values in the column.
    pub distinct_counts: Option<AvroMap<i64>>,
    /// Map from column id to lower bound in the column
    pub lower_bounds: Option<AvroMap<ByteBuf>>,
    /// Map from column id to upper bound in the column
    pub upper_bounds: Option<AvroMap<ByteBuf>>,
    /// Implementation specific key metadata for encryption
    pub key_metadata: Option<ByteBuf>,
    /// Split offsets for the data file.
    pub split_offsets: Option<Vec<i64>>,
    /// Field ids used to determine row equality in equality delete files.
    pub equality_ids: Option<Vec<i32>>,
    /// ID representing sort order for this file
    pub sort_order_id: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
/// DataFile found in Manifest.
pub struct DataFileV1 {
    /// Full URI for the file with a FS scheme.
    pub file_path: String,
    /// String file format name, avro, orc or parquet
    pub file_format: FileFormat,
    /// Partition data tuple, schema based on the partition spec output using partition field ids for the struct field ids
    pub partition: Struct,
    /// Number of records in this file
    pub record_count: i64,
    /// Total file size in bytes
    pub file_size_in_bytes: i64,
    /// Block size
    pub block_size_in_bytes: i64,
    /// File ordinal
    pub file_ordinal: Option<i32>,
    /// Columns to sort
    pub sort_columns: Option<Vec<i32>>,
    /// Map from column id to total size on disk
    pub column_sizes: Option<AvroMap<i64>>,
    /// Map from column id to number of values in the column (including null and NaN values)
    pub value_counts: Option<AvroMap<i64>>,
    /// Map from column id to number of null values
    pub null_value_counts: Option<AvroMap<i64>>,
    /// Map from column id to number of NaN values
    pub nan_value_counts: Option<AvroMap<i64>>,
    /// Map from column id to number of distinct values in the column.
    pub distinct_counts: Option<AvroMap<i64>>,
    /// Map from column id to lower bound in the column
    pub lower_bounds: Option<AvroMap<ByteBuf>>,
    /// Map from column id to upper bound in the column
    pub upper_bounds: Option<AvroMap<ByteBuf>>,
    /// Implementation specific key metadata for encryption
    pub key_metadata: Option<ByteBuf>,
    /// Split offsets for the data file.
    pub split_offsets: Option<Vec<i64>>,
    /// ID representing sort order for this file
    pub sort_order_id: Option<i32>,
}

impl From<DataFile> for DataFileV2 {
    fn from(value: DataFile) -> Self {
        DataFileV2 {
            content: value.content,
            file_path: value.file_path,
            file_format: value.file_format,
            partition: value.partition,
            record_count: value.record_count,
            file_size_in_bytes: value.file_size_in_bytes,
            column_sizes: value.column_sizes,
            value_counts: value.value_counts,
            null_value_counts: value.null_value_counts,
            nan_value_counts: value.nan_value_counts,
            distinct_counts: value.distinct_counts,
            lower_bounds: value.lower_bounds.map(Into::into),
            upper_bounds: value.upper_bounds.map(Into::into),
            key_metadata: value.key_metadata,
            split_offsets: value.split_offsets,
            equality_ids: value.equality_ids,
            sort_order_id: value.sort_order_id,
        }
    }
}

impl From<DataFile> for DataFileV1 {
    fn from(value: DataFile) -> Self {
        DataFileV1 {
            file_path: value.file_path,
            file_format: value.file_format,
            partition: value.partition,
            record_count: value.record_count,
            file_size_in_bytes: value.file_size_in_bytes,
            column_sizes: value.column_sizes,
            value_counts: value.value_counts,
            null_value_counts: value.null_value_counts,
            nan_value_counts: value.nan_value_counts,
            distinct_counts: value.distinct_counts,
            lower_bounds: value.lower_bounds.map(Into::into),
            upper_bounds: value.upper_bounds.map(Into::into),
            key_metadata: value.key_metadata,
            split_offsets: value.split_offsets,
            sort_order_id: value.sort_order_id,
            block_size_in_bytes: 0,
            file_ordinal: None,
            sort_columns: None,
        }
    }
}

impl From<DataFileV1> for DataFileV2 {
    fn from(v1: DataFileV1) -> Self {
        DataFileV2 {
            content: Content::Data,
            file_path: v1.file_path,
            file_format: v1.file_format,
            partition: v1.partition,
            record_count: v1.record_count,
            file_size_in_bytes: v1.file_size_in_bytes,
            column_sizes: v1.column_sizes,
            value_counts: v1.value_counts,
            null_value_counts: v1.null_value_counts,
            nan_value_counts: v1.nan_value_counts,
            distinct_counts: v1.distinct_counts,
            lower_bounds: v1.lower_bounds,
            upper_bounds: v1.upper_bounds,
            key_metadata: v1.key_metadata,
            split_offsets: v1.split_offsets,
            equality_ids: None,
            sort_order_id: v1.sort_order_id,
        }
    }
}

impl DataFileV1 {
    /// Get schema
    pub fn schema(partition_schema: &str) -> String {
        r#"{
            "type": "record",
            "name": "r2",
            "fields": [
                {
                    "name": "file_path",
                    "type": "string",
                    "field_id": 100
                },
                {
                    "name": "file_format",
                    "type": "string",
                    "field_id": 101
                },
                {
                    "name": "partition",
                    "type": "#
            .to_owned()
            + partition_schema
            + r#",
                    "field_id": 102
                },
                {
                    "name": "record_count",
                    "type": "long",
                    "field_id": 103
                },
                {
                    "name": "file_size_in_bytes",
                    "type": "long",
                    "field_id": 104
                },
                {
                    "name": "block_size_in_bytes",
                    "type": "long",
                    "field_id": 105
                },
                {
                    "name": "file_ordinal",
                    "type": [
                        "null",
                        "int"
                    ],
                    "default": null,
                    "field_id": 106
                },
                {
                    "name": "sort_columns",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "items": "int",
                            "element-id": 112
                        }
                    ],
                    "default": null,
                    "field_id": 107
                },
                {
                    "name": "column_sizes",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k117_v118",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 117
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 118
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 108
                },
                {
                    "name": "value_counts",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k119_v120",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 119
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 120
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 109
                },
                {
                    "name": "null_value_counts",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k121_v122",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 121
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 122
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 110
                },
                {
                    "name": "nan_value_counts",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k138_v139",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 138
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 139
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 137
                },
                {
                    "name": "distinct_counts",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k123_v124",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 123
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 124
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 111
                },
                {
                    "name": "lower_bounds",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k126_v127",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 126
                                    },
                                    {
                                        "name": "value",
                                        "type": "bytes",
                                        "field-id": 127
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 125
                },
                {
                    "name": "upper_bounds",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k129_v130",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 129
                                    },
                                    {
                                        "name": "value",
                                        "type": "bytes",
                                        "field-id": 130
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 128
                },
                {
                    "name": "key_metadata",
                    "type": [
                        "null",
                        "bytes"
                    ],
                    "default": null,
                    "field_id": 131
                },
                {
                    "name": "split_offsets",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "items": "long",
                            "element-id": 133
                        }
                    ],
                    "default": null,
                    "field_id": 132
                },
                {
                    "name": "sort_order_id",
                    "type": [
                        "null",
                        "int"
                    ],
                    "default": null,
                    "field_id": 140
                }
            ]
        }"#
    }
}

impl DataFileV2 {
    /// Get schema
    pub fn schema(partition_schema: &str) -> String {
        r#"{
            "type": "record",
            "name": "r2",
            "fields": [
                {
                    "name": "content",
                    "type": "int",
                    "field_id": 134
                },
                {
                    "name": "file_path",
                    "type": "string",
                    "field_id": 100
                },
                {
                    "name": "file_format",
                    "type": "string",
                    "field_id": 101
                },
                {
                    "name": "partition",
                    "type": "#
            .to_owned()
            + partition_schema
            + r#",
                    "field_id": 102
                },
                {
                    "name": "record_count",
                    "type": "long",
                    "field_id": 103
                },
                {
                    "name": "file_size_in_bytes",
                    "type": "long",
                    "field_id": 104
                },
                {
                    "name": "column_sizes",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k117_v118",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 117
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 118
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 108
                },
                {
                    "name": "value_counts",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k119_v120",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 119
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 120
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 109
                },
                {
                    "name": "null_value_counts",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k121_v122",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 121
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 122
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 110
                },
                {
                    "name": "nan_value_counts",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k138_v139",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 138
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 139
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 137
                },
                {
                    "name": "distinct_counts",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k123_v124",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 123
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 124
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 111
                },
                {
                    "name": "lower_bounds",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k126_v127",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 126
                                    },
                                    {
                                        "name": "value",
                                        "type": "bytes",
                                        "field-id": 127
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 125
                },
                {
                    "name": "upper_bounds",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k129_v130",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 129
                                    },
                                    {
                                        "name": "value",
                                        "type": "bytes",
                                        "field-id": 130
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 128
                },
                {
                    "name": "key_metadata",
                    "type": [
                        "null",
                        "bytes"
                    ],
                    "default": null,
                    "field_id": 131
                },
                {
                    "name": "split_offsets",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "items": "long",
                            "element-id": 133
                        }
                    ],
                    "default": null,
                    "field_id": 132
                },
                {
                    "name": "equality_ids",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "items": "int",
                            "element-id": 136
                        }
                    ],
                    "default": null,
                    "field_id": 135
                },
                {
                    "name": "sort_order_id",
                    "type": [
                        "null",
                        "int"
                    ],
                    "default": null,
                    "field_id": 140
                }
            ]
        }"#
    }
}

#[cfg(test)]
mod tests {
    use crate::spec::{
        partition::{PartitionField, Transform},
        table_metadata::TableMetadataBuilder,
        types::{PrimitiveType, StructField, StructType, Type},
        values::Value,
    };

    use super::*;
    use apache_avro::{self, types::Value as AvroValue};

    #[test]
    fn manifest_entry() {
        let table_metadata = TableMetadataBuilder::default()
            .location("/")
            .current_schema_id(0)
            .schemas(HashMap::from_iter(vec![(
                0,
                Schema::builder()
                    .with_fields(
                        StructType::builder()
                            .with_struct_field(StructField {
                                id: 0,
                                name: "date".to_string(),
                                required: true,
                                field_type: Type::Primitive(PrimitiveType::Date),
                                doc: None,
                            })
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap(),
            )]))
            .default_spec_id(0)
            .partition_specs(HashMap::from_iter(vec![(
                0,
                PartitionSpec::builder()
                    .with_partition_field(PartitionField::new(0, 1000, "day", Transform::Day))
                    .build()
                    .unwrap(),
            )]))
            .build()
            .unwrap();

        let manifest_entry = ManifestEntry {
            format_version: FormatVersion::V2,
            status: Status::Added,
            snapshot_id: Some(638933773299822130),
            sequence_number: Some(1),
            data_file: DataFile {
                content: Content::Data,
                file_path: "/".to_string(),
                file_format: FileFormat::Parquet,
                partition: Struct::from_iter(vec![("day".to_owned(), Some(Value::Int(1)))]),
                record_count: 4,
                file_size_in_bytes: 1200,
                column_sizes: None,
                value_counts: None,
                null_value_counts: None,
                nan_value_counts: None,
                distinct_counts: None,
                lower_bounds: Some(HashMap::from_iter(vec![(0, Value::Date(0))])),
                upper_bounds: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
                sort_order_id: None,
            },
        };

        let partition_schema =
            partition_value_schema(&table_metadata.current_partition_fields(None).unwrap())
                .unwrap();

        let schema = ManifestEntry::schema(&partition_schema, &FormatVersion::V2).unwrap();

        // TODO: make this a correct partition spec
        let partition_spec = r#"[{
            "source-id": 4,
            "field-id": 1000,
            "name": "date",
            "transform": "day"
          }]"#;
        let partition_spec_id = "0";
        // TODO: make this a correct schema
        let table_schema = r#"{"schema": "0"}"#;
        let table_schema_id = "1";
        let format_version = FormatVersion::V1;
        let content = "DATA";

        let meta: std::collections::HashMap<String, apache_avro::types::Value> =
            std::collections::HashMap::from_iter(vec![
                ("schema".to_string(), AvroValue::Bytes(table_schema.into())),
                (
                    "schema-id".to_string(),
                    AvroValue::Bytes(table_schema_id.into()),
                ),
                (
                    "partition-spec".to_string(),
                    AvroValue::Bytes(partition_spec.into()),
                ),
                (
                    "partition-spec-id".to_string(),
                    AvroValue::Bytes(partition_spec_id.into()),
                ),
                (
                    "format-version".to_string(),
                    AvroValue::Bytes(vec![u8::from(format_version)]),
                ),
                ("content".to_string(), AvroValue::Bytes(content.into())),
            ]);
        let mut writer = apache_avro::Writer::builder()
            .schema(&schema)
            .writer(vec![])
            .user_metadata(meta)
            .build();
        writer.append_ser(manifest_entry.clone()).unwrap();

        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&encoded[..]).unwrap();

        for value in reader {
            let entry = apache_avro::from_value::<ManifestEntryV2>(&value.unwrap()).unwrap();
            assert_eq!(
                manifest_entry,
                ManifestEntry::try_from_v2(
                    entry,
                    table_metadata.current_schema(None).unwrap(),
                    table_metadata.default_partition_spec().unwrap()
                )
                .unwrap()
            )
        }
    }

    #[test]
    fn test_read_manifest_entry() {
        let table_metadata = TableMetadataBuilder::default()
            .location("/")
            .current_schema_id(0)
            .schemas(HashMap::from_iter(vec![(
                0,
                Schema::builder()
                    .with_fields(
                        StructType::builder()
                            .with_struct_field(StructField {
                                id: 0,
                                name: "date".to_string(),
                                required: true,
                                field_type: Type::Primitive(PrimitiveType::Date),
                                doc: None,
                            })
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap(),
            )]))
            .default_spec_id(0)
            .partition_specs(HashMap::from_iter(vec![(
                0,
                PartitionSpec::builder()
                    .with_partition_field(PartitionField::new(0, 1000, "day", Transform::Day))
                    .build()
                    .unwrap(),
            )]))
            .build()
            .unwrap();

        let manifest_entry = ManifestEntry {
            format_version: FormatVersion::V2,
            status: Status::Added,
            snapshot_id: Some(638933773299822130),
            sequence_number: Some(1),
            data_file: DataFile {
                content: Content::Data,
                file_path: "/".to_string(),
                file_format: FileFormat::Parquet,
                partition: Struct::from_iter(vec![("day".to_owned(), Some(Value::Int(1)))]),
                record_count: 4,
                file_size_in_bytes: 1200,
                column_sizes: None,
                value_counts: None,
                null_value_counts: None,
                nan_value_counts: None,
                distinct_counts: None,
                lower_bounds: Some(HashMap::from_iter(vec![(0, Value::Date(0))])),
                upper_bounds: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
                sort_order_id: None,
            },
        };

        let partition_schema =
            partition_value_schema(&table_metadata.current_partition_fields(None).unwrap())
                .unwrap();

        let schema = ManifestEntry::schema(&partition_schema, &FormatVersion::V2).unwrap();

        // TODO: make this a correct partition spec
        let partition_spec = r#"[{
                "source-id": 4,
                "field-id": 1000,
                "name": "date",
                "transform": "day"
              }]"#;
        let partition_spec_id = "0";
        // TODO: make this a correct schema
        let table_schema = r#"{"schema": "0"}"#;
        let table_schema_id = "1";
        let format_version = "1";
        let content = "DATA";

        let meta: std::collections::HashMap<String, apache_avro::types::Value> =
            std::collections::HashMap::from_iter(vec![
                ("schema".to_string(), AvroValue::Bytes(table_schema.into())),
                (
                    "schema-id".to_string(),
                    AvroValue::Bytes(table_schema_id.into()),
                ),
                (
                    "partition-spec".to_string(),
                    AvroValue::Bytes(partition_spec.into()),
                ),
                (
                    "partition-spec-id".to_string(),
                    AvroValue::Bytes(partition_spec_id.into()),
                ),
                (
                    "format-version".to_string(),
                    AvroValue::Bytes(format_version.into()),
                ),
                ("content".to_string(), AvroValue::Bytes(content.into())),
            ]);
        let mut writer = apache_avro::Writer::builder()
            .schema(&schema)
            .writer(vec![])
            .user_metadata(meta)
            .build();
        writer.append_ser(manifest_entry.clone()).unwrap();

        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&encoded[..]).unwrap();
        let record = reader.into_iter().next().unwrap().unwrap();

        let metadata_entry = apache_avro::from_value::<ManifestEntryV2>(&record).unwrap();
        assert_eq!(
            manifest_entry,
            ManifestEntry::try_from_v2(
                metadata_entry,
                table_metadata.current_schema(None).unwrap(),
                table_metadata.default_partition_spec().unwrap()
            )
            .unwrap()
        );
    }

    #[test]
    pub fn test_partition_values() {
        let partition_values = Struct::from_iter(vec![("day".to_owned(), Some(Value::Int(1)))]);

        let part_field = PartitionField::new(4, 1000, "day", Transform::Day);
        let field = StructField {
            id: 4,
            name: "day".to_owned(),
            required: false,
            field_type: Type::Primitive(PrimitiveType::Int),
            doc: None,
        };
        let partition_fields = vec![BoundPartitionField::new(&part_field, &field)];

        let raw_schema = partition_value_schema(&partition_fields).unwrap();

        let schema = apache_avro::Schema::parse_str(&raw_schema).unwrap();

        let mut writer = apache_avro::Writer::new(&schema, Vec::new());

        writer.append_ser(partition_values.clone()).unwrap();

        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&*encoded).unwrap();

        for record in reader {
            let result = apache_avro::from_value::<Struct>(&record.unwrap()).unwrap();
            assert_eq!(partition_values, result);
        }
    }
}
