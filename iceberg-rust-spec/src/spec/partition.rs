/*!
 * Partitioning
*/

use std::{
    fmt::{self, Display},
    str,
};

use derive_getters::Getters;
use serde::{
    de::{Error as SerdeError, IntoDeserializer},
    Deserialize, Deserializer, Serialize, Serializer,
};

use derive_builder::Builder;

use crate::{error::Error, types::StructField};

use super::types::{StructType, Type};

pub static DEFAULT_PARTITION_SPEC_ID: i32 = 0;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase", remote = "Self")]
/// A Transform that is applied to each source column to produce a partition value.
pub enum Transform {
    /// Source value, unmodified
    Identity,
    /// Hash of value, mod N
    Bucket(u32),
    /// Value truncated to width
    Truncate(u32),
    /// Extract a date or timestamp year as years from 1970
    Year,
    /// Extract a date or timestamp month as months from 1970-01-01
    Month,
    /// Extract a date or timestamp day as days from 1970-01-01
    Day,
    /// Extract a date or timestamp hour as hours from 1970-01-01 00:00:00
    Hour,
    /// Always produces `null`
    Void,
}

impl<'de> Deserialize<'de> for Transform {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.starts_with("bucket") {
            deserialize_bucket(s.into_deserializer())
        } else if s.starts_with("truncate") {
            deserialize_truncate(s.into_deserializer())
        } else {
            Transform::deserialize(s.into_deserializer())
        }
    }
}

impl Serialize for Transform {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Transform::Bucket(bucket) => serialize_bucket(bucket, serializer),
            Transform::Truncate(truncate) => serialize_truncate(truncate, serializer),
            x => Transform::serialize(x, serializer),
        }
    }
}

fn deserialize_bucket<'de, D>(deserializer: D) -> Result<Transform, D::Error>
where
    D: Deserializer<'de>,
{
    let bucket = String::deserialize(deserializer)?
        .trim_start_matches(r"bucket[")
        .trim_end_matches(']')
        .to_owned();

    bucket
        .parse()
        .map(Transform::Bucket)
        .map_err(D::Error::custom)
}

fn serialize_bucket<S>(value: &u32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("bucket[{value}]"))
}

fn deserialize_truncate<'de, D>(deserializer: D) -> Result<Transform, D::Error>
where
    D: Deserializer<'de>,
{
    let truncate = String::deserialize(deserializer)?
        .trim_start_matches(r"truncate[")
        .trim_end_matches(']')
        .to_owned();

    truncate
        .parse()
        .map(Transform::Truncate)
        .map_err(D::Error::custom)
}

fn serialize_truncate<S>(value: &u32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("truncate[{value}]"))
}

impl Display for Transform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Transform::Identity => write!(f, "identity"),
            Transform::Year => write!(f, "year"),
            Transform::Month => write!(f, "month"),
            Transform::Day => write!(f, "day"),
            Transform::Hour => write!(f, "hour"),
            Transform::Bucket(i) => write!(f, "bucket[{}]", i),
            Transform::Truncate(i) => write!(f, "truncate[{}]", i),
            Transform::Void => write!(f, "void"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Getters)]
#[serde(rename_all = "kebab-case")]
/// Partition fields capture the transform from table data to partition values.
pub struct PartitionField {
    /// A source column id from the table’s schema
    source_id: i32,
    /// A partition field id that is used to identify a partition field and is unique within a partition spec.
    /// In v2 table metadata, it is unique across all partition specs.
    field_id: i32,
    /// A partition name.
    name: String,
    /// A transform that is applied to the source column to produce a partition value.
    transform: Transform,
}

impl PartitionField {
    /// Create a new PartitionField
    pub fn new(source_id: i32, field_id: i32, name: &str, transform: Transform) -> Self {
        Self {
            source_id,
            field_id,
            name: name.to_string(),
            transform,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default, Builder, Getters)]
#[serde(rename_all = "kebab-case")]
#[builder(setter(prefix = "with"))]
///  Partition spec that defines how to produce a tuple of partition values from a record.
pub struct PartitionSpec {
    /// Identifier for PartitionSpec
    #[builder(default = "DEFAULT_PARTITION_SPEC_ID")]
    spec_id: i32,
    /// Details of the partition spec
    #[builder(setter(each(name = "with_partition_field")))]
    fields: Vec<PartitionField>,
}

impl PartitionSpec {
    /// Create partition spec builder
    pub fn builder() -> PartitionSpecBuilder {
        PartitionSpecBuilder::default()
    }
    /// Get datatypes of partition fields
    pub fn data_types(&self, schema: &StructType) -> Result<Vec<Type>, Error> {
        self.fields
            .iter()
            .map(|field| {
                schema
                    .get(field.source_id as usize)
                    .ok_or(Error::NotFound(
                        "Partition field".to_owned(),
                        field.name.clone(),
                    ))
                    .and_then(|x| x.field_type.clone().tranform(&field.transform))
            })
            .collect::<Result<Vec<_>, Error>>()
    }

    pub fn unpartitioned(spec_id: i32) -> Self {
        Self {
            spec_id,
            fields: vec![],
        }
    }
}

impl fmt::Display for PartitionSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            &serde_json::to_string(self).map_err(|_| fmt::Error)?,
        )
    }
}

impl str::FromStr for PartitionSpec {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(Error::from)
    }
}

#[derive(Debug)]
pub struct BoundPartitionField<'a> {
    partition_field: &'a PartitionField,
    struct_field: &'a StructField,
}

impl<'a> BoundPartitionField<'a> {
    pub fn new(partition_field: &'a PartitionField, struct_field: &'a StructField) -> Self {
        Self {
            partition_field,
            struct_field,
        }
    }

    /// Name of partition field
    pub fn name(&self) -> &str {
        &self.partition_field.name
    }

    /// Name of source field
    pub fn source_name(&self) -> &str {
        &self.struct_field.name
    }

    /// Datatype of source field
    pub fn field_type(&self) -> &Type {
        &self.struct_field.field_type
    }

    /// Datatype of source field
    pub fn transform(&self) -> &Transform {
        &self.partition_field.transform
    }

    /// Field id if partition field
    pub fn field_id(&self) -> i32 {
        self.partition_field.field_id
    }

    /// Field id if partition field
    pub fn source_id(&self) -> i32 {
        self.partition_field.source_id
    }

    /// Field id if partition field
    pub fn required(&self) -> bool {
        self.struct_field.required
    }

    /// Partition field
    pub fn partition_field(&self) -> &PartitionField {
        self.partition_field
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_spec() {
        let sort_order = r#"
        {
        "spec-id": 1,
        "fields": [ {
            "source-id": 4,
            "field-id": 1000,
            "name": "ts_day",
            "transform": "day"
            }, {
            "source-id": 1,
            "field-id": 1001,
            "name": "id_bucket",
            "transform": "bucket[16]"
            }, {
            "source-id": 2,
            "field-id": 1002,
            "name": "id_truncate",
            "transform": "truncate[4]"
            } ]
        }
        "#;

        let partition_spec: PartitionSpec = serde_json::from_str(sort_order).unwrap();
        assert_eq!(4, partition_spec.fields[0].source_id);
        assert_eq!(1000, partition_spec.fields[0].field_id);
        assert_eq!("ts_day", partition_spec.fields[0].name);
        assert_eq!(Transform::Day, partition_spec.fields[0].transform);

        assert_eq!(1, partition_spec.fields[1].source_id);
        assert_eq!(1001, partition_spec.fields[1].field_id);
        assert_eq!("id_bucket", partition_spec.fields[1].name);
        assert_eq!(Transform::Bucket(16), partition_spec.fields[1].transform);

        assert_eq!(2, partition_spec.fields[2].source_id);
        assert_eq!(1002, partition_spec.fields[2].field_id);
        assert_eq!("id_truncate", partition_spec.fields[2].name);
        assert_eq!(Transform::Truncate(4), partition_spec.fields[2].transform);
    }
}
