/*!
 * Value in iceberg
 */

use core::panic;
use std::{
    any::Any,
    collections::{btree_map::Keys, BTreeMap, HashMap},
    fmt,
    hash::{DefaultHasher, Hash, Hasher},
    io::Cursor,
    ops::Sub,
    slice::Iter,
};

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use rust_decimal::Decimal;
use serde::{
    de::{MapAccess, Visitor},
    ser::SerializeStruct,
    Deserialize, Deserializer, Serialize,
};
use serde_bytes::ByteBuf;
use serde_json::{Map as JsonMap, Number, Value as JsonValue};
use uuid::Uuid;

use crate::error::Error;

use super::{
    partition::{PartitionField, Transform},
    types::{PrimitiveType, StructType, Type},
};

static DAYS_BEFORE_UNIX_EPOCH: i32 = 719163;
static YEARS_BEFORE_UNIX_EPOCH: i32 = 1970;

/// Values present in iceberg type
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(untagged)]
pub enum Value {
    /// 0x00 for false, non-zero byte for true
    Boolean(bool),
    /// Stored as 4-byte little-endian
    Int(i32),
    /// Stored as 8-byte little-endian
    LongInt(i64),
    /// Stored as 4-byte little-endian
    Float(OrderedFloat<f32>),
    /// Stored as 8-byte little-endian
    Double(OrderedFloat<f64>),
    /// Stores days from the 1970-01-01 in an 4-byte little-endian int
    Date(i32),
    /// Stores microseconds from midnight in an 8-byte little-endian long
    Time(i64),
    /// Stores microseconds from 1970-01-01 00:00:00.000000 in an 8-byte little-endian long
    Timestamp(i64),
    /// Stores microseconds from 1970-01-01 00:00:00.000000 in an 8-byte little-endian long
    TimestampTZ(i64),
    /// UTF-8 bytes (without length)
    String(String),
    /// 16-byte big-endian value
    UUID(Uuid),
    /// Binary value
    Fixed(usize, Vec<u8>),
    /// Binary value (without length)
    Binary(Vec<u8>),
    /// Stores unscaled value as two’s-complement big-endian binary,
    /// using the minimum number of bytes for the value
    Decimal(Decimal),
    /// A struct is a tuple of typed values. Each field in the tuple is named and has an integer id that is unique in the table schema.
    /// Each field can be either optional or required, meaning that values can (or cannot) be null. Fields may be any type.
    /// Fields may have an optional comment or doc string. Fields can have default values.
    Struct(Struct),
    /// A list is a collection of values with some element type.
    /// The element field has an integer id that is unique in the table schema.
    /// Elements can be either optional or required. Element types may be any type.
    List(Vec<Option<Value>>),
    /// A map is a collection of key-value pairs with a key type and a value type.
    /// Both the key field and value field each have an integer id that is unique in the table schema.
    /// Map keys are required and map values can be either optional or required. Both map keys and map values may be any type, including nested types.
    Map(BTreeMap<Value, Option<Value>>),
}

impl From<Value> for ByteBuf {
    fn from(value: Value) -> Self {
        match value {
            Value::Boolean(val) => {
                if val {
                    ByteBuf::from([1u8])
                } else {
                    ByteBuf::from([0u8])
                }
            }
            Value::Int(val) => ByteBuf::from(val.to_le_bytes()),
            Value::LongInt(val) => ByteBuf::from(val.to_le_bytes()),
            Value::Float(val) => ByteBuf::from(val.to_le_bytes()),
            Value::Double(val) => ByteBuf::from(val.to_le_bytes()),
            Value::Date(val) => ByteBuf::from(val.to_le_bytes()),
            Value::Time(val) => ByteBuf::from(val.to_le_bytes()),
            Value::Timestamp(val) => ByteBuf::from(val.to_le_bytes()),
            Value::TimestampTZ(val) => ByteBuf::from(val.to_le_bytes()),
            Value::String(val) => ByteBuf::from(val.as_bytes()),
            Value::UUID(val) => ByteBuf::from(val.as_u128().to_be_bytes()),
            Value::Fixed(_, val) => ByteBuf::from(val),
            Value::Binary(val) => ByteBuf::from(val),
            _ => todo!(),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Int(i) => write!(f, "{}", i),
            Value::LongInt(l) => write!(f, "{}", l),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Double(d) => write!(f, "{}", d),
            Value::Date(d) => write!(f, "{}", d),
            Value::Time(t) => write!(f, "{}", t),
            Value::Timestamp(ts) => write!(f, "{}", ts),
            Value::TimestampTZ(ts) => write!(f, "{}", ts),
            Value::String(s) => write!(f, "{}", s),
            Value::UUID(u) => write!(f, "{}", u),
            Value::Fixed(size, data) => write!(f, "{:?} ({} bytes)", data, size),
            Value::Binary(data) => write!(f, "{:?} ({} bytes)", data, data.len()),
            Value::Decimal(d) => write!(f, "{}", d),
            _ => panic!("Printing of compound types is not supported"),
        }
    }
}

/// The partition struct stores the tuple of partition values for each file.
/// Its type is derived from the partition fields of the partition spec used to write the manifest file.
/// In v2, the partition struct’s field ids must match the ids from the partition spec.
#[derive(Debug, Clone, Eq, PartialOrd, Ord)]
pub struct Struct {
    /// Vector to store the field values
    pub fields: Vec<Option<Value>>,
    /// A lookup that matches the field name to the entry in the vector
    pub lookup: BTreeMap<String, usize>,
}

impl Struct {
    /// Get reference to partition value
    pub fn get(&self, name: &str) -> Option<&Option<Value>> {
        self.fields.get(*self.lookup.get(name)?)
    }
    /// Get mutable reference to partition value
    pub fn get_mut(&mut self, name: &str) -> Option<&mut Option<Value>> {
        self.fields.get_mut(*self.lookup.get(name)?)
    }

    pub fn iter(&self) -> Iter<'_, Option<Value>> {
        self.fields.iter()
    }

    pub fn keys(&self) -> Keys<'_, String, usize> {
        self.lookup.keys()
    }

    pub(crate) fn cast(
        self,
        schema: &StructType,
        partition_spec: &[PartitionField],
    ) -> Result<Self, Error> {
        // Returns a HashMap mapping partition field names to transformed types.
        let map = partition_spec
            .iter()
            .map(|partition_field| {
                let field = schema.get(*partition_field.source_id() as usize).ok_or(
                    Error::InvalidFormat(format!(
                        "partition spec references unknown column id {}",
                        partition_field.source_id()
                    )),
                )?;

                Ok((
                    partition_field.name().clone(),
                    field.field_type.tranform(partition_field.transform())?,
                ))
            })
            .collect::<Result<HashMap<_, _>, Error>>()?;
        Ok(Struct::from_iter(
            self.fields
                .into_iter()
                .enumerate()
                .map(|(idx, field)| {
                    // Get name of the column
                    let name = self
                        .lookup
                        .iter()
                        .find(|(_, v)| **v == idx)
                        .ok_or(Error::InvalidFormat("partition struct".to_string()))?
                        .0;

                    // Get datatype after tranform
                    let datatype = map
                        .get(name)
                        .ok_or(Error::InvalidFormat("partition_struct".to_string()))?;
                    // Cast the value to the datatype
                    let value = field.map(|value| value.cast(datatype)).transpose()?;
                    Ok((name.clone(), value))
                })
                .collect::<Result<Vec<_>, Error>>()?,
        ))
    }

    pub fn unpartitioned() -> Self {
        Struct {
            fields: Vec::new(),
            lookup: BTreeMap::new(),
        }
    }
}

impl FromIterator<(String, Option<Value>)> for Struct {
    fn from_iter<I: IntoIterator<Item = (String, Option<Value>)>>(iter: I) -> Self {
        let mut fields = Vec::new();
        let mut lookup = BTreeMap::new();

        for (i, (key, value)) in iter.into_iter().enumerate() {
            fields.push(value);
            lookup.insert(key, i);
        }

        Struct { fields, lookup }
    }
}

impl Serialize for Struct {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut record = serializer.serialize_struct("r102", self.fields.len())?;
        for (i, value) in self.fields.iter().enumerate() {
            let (key, _) = self.lookup.iter().find(|(_, value)| **value == i).unwrap();
            record.serialize_field(Box::leak(key.clone().into_boxed_str()), value)?;
        }
        record.end()
    }
}

impl<'de> Deserialize<'de> for Struct {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PartitionStructVisitor;

        impl<'de> Visitor<'de> for PartitionStructVisitor {
            type Value = Struct;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("map")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Struct, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut fields: Vec<Option<Value>> = Vec::new();
                let mut lookup: BTreeMap<String, usize> = BTreeMap::new();
                let mut index = 0;
                while let Some(key) = map.next_key()? {
                    fields.push(map.next_value()?);
                    lookup.insert(key, index);
                    index += 1;
                }
                Ok(Struct { fields, lookup })
            }
        }
        deserializer.deserialize_struct(
            "r102",
            Box::leak(vec![].into_boxed_slice()),
            PartitionStructVisitor,
        )
    }
}

impl PartialEq for Struct {
    fn eq(&self, other: &Self) -> bool {
        self.keys().all(|key| self.get(key).eq(&other.get(key)))
    }
}

impl Hash for Struct {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for key in self.keys().sorted() {
            key.hash(state);
            self.get(key).hash(state);
        }
    }
}

impl Value {
    /// Perform a partition transformation for the given value
    pub fn transform(&self, transform: &Transform) -> Result<Value, Error> {
        match transform {
            Transform::Identity => Ok(self.clone()),
            Transform::Bucket(n) => {
                let mut bytes = Cursor::new(<Value as Into<ByteBuf>>::into(self.clone()));
                let hash = murmur3::murmur3_32(&mut bytes, 0).unwrap();
                Ok(Value::Int((hash % n) as i32))
            }
            Transform::Truncate(w) => match self {
                Value::Int(i) => Ok(Value::Int(i - i.rem_euclid(*w as i32))),
                Value::LongInt(i) => Ok(Value::LongInt(i - i.rem_euclid(*w as i64))),
                Value::String(s) => {
                    let mut s = s.clone();
                    s.truncate(*w as usize);
                    Ok(Value::String(s))
                }
                _ => Err(Error::NotSupported(
                    "Datatype for truncate partition transform.".to_string(),
                )),
            },
            Transform::Year => match self {
                Value::Date(date) => Ok(Value::Int(
                    NaiveDate::from_num_days_from_ce_opt(*date + DAYS_BEFORE_UNIX_EPOCH)
                        .ok_or(Error::InvalidFormat("Date".to_owned()))?
                        .year()
                        - YEARS_BEFORE_UNIX_EPOCH,
                )),
                Value::Timestamp(time) => Ok(Value::Int(
                    DateTime::from_timestamp_millis(time / 1000)
                        .unwrap()
                        .naive_utc()
                        .year()
                        - YEARS_BEFORE_UNIX_EPOCH,
                )),
                Value::TimestampTZ(time) => Ok(Value::Int(
                    DateTime::from_timestamp_millis(time / 1000)
                        .unwrap()
                        .naive_utc()
                        .year()
                        - YEARS_BEFORE_UNIX_EPOCH,
                )),
                _ => Err(Error::NotSupported(
                    "Datatype for year partition transform.".to_string(),
                )),
            },
            Transform::Month => match self {
                Value::Date(date) => Ok(Value::Int(
                    NaiveDate::from_num_days_from_ce_opt(*date + DAYS_BEFORE_UNIX_EPOCH)
                        .ok_or(Error::InvalidFormat("Date".to_owned()))?
                        .month() as i32,
                )),
                Value::Timestamp(time) => Ok(Value::Int(
                    DateTime::from_timestamp_millis(time / 1000)
                        .unwrap()
                        .naive_utc()
                        .month() as i32,
                )),
                Value::TimestampTZ(time) => Ok(Value::Int(
                    DateTime::from_timestamp_millis(time / 1000)
                        .unwrap()
                        .naive_utc()
                        .month() as i32,
                )),
                _ => Err(Error::NotSupported(
                    "Datatype for month partition transform.".to_string(),
                )),
            },
            Transform::Day => match self {
                Value::Date(date) => Ok(Value::Int(
                    NaiveDate::from_num_days_from_ce_opt(*date + DAYS_BEFORE_UNIX_EPOCH)
                        .ok_or(Error::InvalidFormat("Date".to_owned()))?
                        .day() as i32,
                )),
                Value::Timestamp(time) => Ok(Value::Int(
                    DateTime::from_timestamp_millis(time / 1000)
                        .unwrap()
                        .naive_utc()
                        .day() as i32,
                )),
                Value::TimestampTZ(time) => Ok(Value::Int(
                    DateTime::from_timestamp_millis(time / 1000)
                        .unwrap()
                        .naive_utc()
                        .day() as i32,
                )),
                _ => Err(Error::NotSupported(
                    "Datatype for day partition transform.".to_string(),
                )),
            },
            Transform::Hour => match self {
                Value::Timestamp(time) => Ok(Value::Int(
                    DateTime::from_timestamp_millis(time / 1000)
                        .unwrap()
                        .naive_utc()
                        .hour() as i32,
                )),
                Value::TimestampTZ(time) => Ok(Value::Int(
                    DateTime::from_timestamp_millis(time / 1000)
                        .unwrap()
                        .naive_utc()
                        .hour() as i32,
                )),
                _ => Err(Error::NotSupported(
                    "Datatype for hour partition transform.".to_string(),
                )),
            },
            _ => Err(Error::NotSupported(
                "Partition transform operation".to_string(),
            )),
        }
    }

    #[inline]
    /// Create iceberg value from bytes
    pub fn try_from_bytes(bytes: &[u8], data_type: &Type) -> Result<Self, Error> {
        match data_type {
            Type::Primitive(primitive) => match primitive {
                PrimitiveType::Boolean => {
                    if bytes.len() == 1 && bytes[0] == 0u8 {
                        Ok(Value::Boolean(false))
                    } else {
                        Ok(Value::Boolean(true))
                    }
                }
                PrimitiveType::Int => Ok(Value::Int(i32::from_le_bytes(bytes.try_into()?))),
                PrimitiveType::Long => Ok(Value::LongInt(i64::from_le_bytes(bytes.try_into()?))),
                PrimitiveType::Float => Ok(Value::Float(OrderedFloat(f32::from_le_bytes(
                    bytes.try_into()?,
                )))),
                PrimitiveType::Double => Ok(Value::Double(OrderedFloat(f64::from_le_bytes(
                    bytes.try_into()?,
                )))),
                PrimitiveType::Date => Ok(Value::Date(i32::from_le_bytes(bytes.try_into()?))),
                PrimitiveType::Time => Ok(Value::Time(i64::from_le_bytes(bytes.try_into()?))),
                PrimitiveType::Timestamp => {
                    Ok(Value::Timestamp(i64::from_le_bytes(bytes.try_into()?)))
                }
                PrimitiveType::Timestamptz => {
                    Ok(Value::TimestampTZ(i64::from_le_bytes(bytes.try_into()?)))
                }
                PrimitiveType::String => Ok(Value::String(std::str::from_utf8(bytes)?.to_string())),
                PrimitiveType::Uuid => Ok(Value::UUID(Uuid::from_u128(u128::from_be_bytes(
                    bytes.try_into()?,
                )))),
                PrimitiveType::Fixed(len) => Ok(Value::Fixed(*len as usize, Vec::from(bytes))),
                PrimitiveType::Binary => Ok(Value::Binary(Vec::from(bytes))),
                _ => Err(Error::Type("decimal".to_string(), "bytes".to_string())),
            },
            _ => Err(Error::NotSupported("Complex types as bytes".to_string())),
        }
    }

    /// Create iceberg value from a json value
    pub fn try_from_json(value: JsonValue, data_type: &Type) -> Result<Option<Self>, Error> {
        match data_type {
            Type::Primitive(primitive) => match (primitive, value) {
                (PrimitiveType::Boolean, JsonValue::Bool(bool)) => Ok(Some(Value::Boolean(bool))),
                (PrimitiveType::Int, JsonValue::Number(number)) => Ok(Some(Value::Int(
                    number
                        .as_i64()
                        .ok_or(Error::Conversion(
                            "json number".to_string(),
                            "int".to_string(),
                        ))?
                        .try_into()?,
                ))),
                (PrimitiveType::Long, JsonValue::Number(number)) => {
                    Ok(Some(Value::LongInt(number.as_i64().ok_or(
                        Error::Conversion("json number".to_string(), "long".to_string()),
                    )?)))
                }
                (PrimitiveType::Float, JsonValue::Number(number)) => Ok(Some(Value::Float(
                    OrderedFloat(number.as_f64().ok_or(Error::Conversion(
                        "json number".to_string(),
                        "float".to_string(),
                    ))? as f32),
                ))),
                (PrimitiveType::Double, JsonValue::Number(number)) => {
                    Ok(Some(Value::Double(OrderedFloat(number.as_f64().ok_or(
                        Error::Conversion("json number".to_string(), "double".to_string()),
                    )?))))
                }
                (PrimitiveType::Date, JsonValue::String(s)) => Ok(Some(Value::Date(
                    datetime::date_to_days(&NaiveDate::parse_from_str(&s, "%Y-%m-%d")?),
                ))),
                (PrimitiveType::Time, JsonValue::String(s)) => Ok(Some(Value::Time(
                    datetime::time_to_microseconds(&NaiveTime::parse_from_str(&s, "%H:%M:%S%.f")?),
                ))),
                (PrimitiveType::Timestamp, JsonValue::String(s)) => {
                    Ok(Some(Value::Timestamp(datetime::datetime_to_microseconds(
                        &NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f")?,
                    ))))
                }
                (PrimitiveType::Timestamptz, JsonValue::String(s)) => Ok(Some(Value::TimestampTZ(
                    datetime::datetimetz_to_microseconds(&Utc.from_utc_datetime(
                        &NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f+00:00")?,
                    )),
                ))),
                (PrimitiveType::String, JsonValue::String(s)) => Ok(Some(Value::String(s))),
                (PrimitiveType::Uuid, JsonValue::String(s)) => {
                    Ok(Some(Value::UUID(Uuid::parse_str(&s)?)))
                }
                (PrimitiveType::Fixed(_), JsonValue::String(_)) => todo!(),
                (PrimitiveType::Binary, JsonValue::String(_)) => todo!(),
                (
                    PrimitiveType::Decimal {
                        precision: _,
                        scale: _,
                    },
                    JsonValue::String(_),
                ) => todo!(),
                (_, JsonValue::Null) => Ok(None),
                (i, j) => Err(Error::Type(i.to_string(), j.to_string())),
            },
            Type::Struct(schema) => {
                if let JsonValue::Object(mut object) = value {
                    Ok(Some(Value::Struct(Struct::from_iter(schema.iter().map(
                        |field| {
                            (
                                field.name.clone(),
                                object.remove(&field.name).and_then(|value| {
                                    Value::try_from_json(value, &field.field_type)
                                        .and_then(|value| {
                                            value.ok_or(Error::InvalidFormat(
                                                "key of map".to_string(),
                                            ))
                                        })
                                        .ok()
                                }),
                            )
                        },
                    )))))
                } else {
                    Err(Error::Type(
                        "json for a struct".to_string(),
                        "object".to_string(),
                    ))
                }
            }
            Type::List(list) => {
                if let JsonValue::Array(array) = value {
                    Ok(Some(Value::List(
                        array
                            .into_iter()
                            .map(|value| Value::try_from_json(value, &list.element))
                            .collect::<Result<Vec<_>, Error>>()?,
                    )))
                } else {
                    Err(Error::Type(
                        "json for a list".to_string(),
                        "array".to_string(),
                    ))
                }
            }
            Type::Map(map) => {
                if let JsonValue::Object(mut object) = value {
                    if let (Some(JsonValue::Array(keys)), Some(JsonValue::Array(values))) =
                        (object.remove("keys"), object.remove("values"))
                    {
                        Ok(Some(Value::Map(BTreeMap::from_iter(
                            keys.into_iter()
                                .zip(values.into_iter())
                                .map(|(key, value)| {
                                    Ok((
                                        Value::try_from_json(key, &map.key).and_then(|value| {
                                            value.ok_or(Error::InvalidFormat(
                                                "key of map".to_string(),
                                            ))
                                        })?,
                                        Value::try_from_json(value, &map.value)?,
                                    ))
                                })
                                .collect::<Result<Vec<_>, Error>>()?,
                        ))))
                    } else {
                        Err(Error::Type(
                            "json for a list".to_string(),
                            "array".to_string(),
                        ))
                    }
                } else {
                    Err(Error::Type(
                        "json for a list".to_string(),
                        "array".to_string(),
                    ))
                }
            }
        }
    }

    /// Get datatype of value
    pub fn datatype(&self) -> Type {
        match self {
            Value::Boolean(_) => Type::Primitive(PrimitiveType::Boolean),
            Value::Int(_) => Type::Primitive(PrimitiveType::Int),
            Value::LongInt(_) => Type::Primitive(PrimitiveType::Long),
            Value::Float(_) => Type::Primitive(PrimitiveType::Float),
            Value::Double(_) => Type::Primitive(PrimitiveType::Double),
            Value::Date(_) => Type::Primitive(PrimitiveType::Date),
            Value::Time(_) => Type::Primitive(PrimitiveType::Time),
            Value::Timestamp(_) => Type::Primitive(PrimitiveType::Timestamp),
            Value::TimestampTZ(_) => Type::Primitive(PrimitiveType::Timestamptz),
            Value::Fixed(len, _) => Type::Primitive(PrimitiveType::Fixed(*len as u64)),
            Value::Binary(_) => Type::Primitive(PrimitiveType::Binary),
            Value::String(_) => Type::Primitive(PrimitiveType::String),
            Value::UUID(_) => Type::Primitive(PrimitiveType::Uuid),
            Value::Decimal(dec) => Type::Primitive(PrimitiveType::Decimal {
                precision: 38,
                scale: dec.scale(),
            }),
            _ => unimplemented!(),
        }
    }

    /// Convert Value to the any type
    pub fn into_any(self) -> Box<dyn Any> {
        match self {
            Value::Boolean(any) => Box::new(any),
            Value::Int(any) => Box::new(any),
            Value::LongInt(any) => Box::new(any),
            Value::Float(any) => Box::new(any),
            Value::Double(any) => Box::new(any),
            Value::Date(any) => Box::new(any),
            Value::Time(any) => Box::new(any),
            Value::Timestamp(any) => Box::new(any),
            Value::TimestampTZ(any) => Box::new(any),
            Value::Fixed(_, any) => Box::new(any),
            Value::Binary(any) => Box::new(any),
            Value::String(any) => Box::new(any),
            Value::UUID(any) => Box::new(any),
            Value::Decimal(any) => Box::new(any),
            _ => unimplemented!(),
        }
    }
    /// Cast value to different type
    pub fn cast(self, data_type: &Type) -> Result<Self, Error> {
        if self.datatype() == *data_type {
            Ok(self)
        } else {
            match (self, data_type) {
                (Value::Int(input), Type::Primitive(PrimitiveType::Long)) => {
                    Ok(Value::LongInt(input as i64))
                }
                (Value::Int(input), Type::Primitive(PrimitiveType::Date)) => Ok(Value::Date(input)),
                (Value::LongInt(input), Type::Primitive(PrimitiveType::Time)) => {
                    Ok(Value::Time(input))
                }
                (Value::LongInt(input), Type::Primitive(PrimitiveType::Timestamp)) => {
                    Ok(Value::Timestamp(input))
                }
                (Value::LongInt(input), Type::Primitive(PrimitiveType::Timestamptz)) => {
                    Ok(Value::TimestampTZ(input))
                }
                _ => Err(Error::NotSupported("cast".to_string())),
            }
        }
    }
}

impl From<&Value> for JsonValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::Boolean(val) => JsonValue::Bool(*val),
            Value::Int(val) => JsonValue::Number((*val).into()),
            Value::LongInt(val) => JsonValue::Number((*val).into()),
            Value::Float(val) => match Number::from_f64(val.0 as f64) {
                Some(number) => JsonValue::Number(number),
                None => JsonValue::Null,
            },
            Value::Double(val) => match Number::from_f64(val.0) {
                Some(number) => JsonValue::Number(number),
                None => JsonValue::Null,
            },
            Value::Date(val) => JsonValue::String(datetime::days_to_date(*val).to_string()),
            Value::Time(val) => JsonValue::String(datetime::microseconds_to_time(*val).to_string()),
            Value::Timestamp(val) => JsonValue::String(
                datetime::microseconds_to_datetime(*val)
                    .format("%Y-%m-%dT%H:%M:%S%.f")
                    .to_string(),
            ),
            Value::TimestampTZ(val) => JsonValue::String(
                datetime::microseconds_to_datetimetz(*val)
                    .format("%Y-%m-%dT%H:%M:%S%.f+00:00")
                    .to_string(),
            ),
            Value::String(val) => JsonValue::String(val.clone()),
            Value::UUID(val) => JsonValue::String(val.to_string()),
            Value::Fixed(_, val) => {
                JsonValue::String(val.iter().fold(String::new(), |mut acc, x| {
                    acc.push_str(&format!("{:x}", x));
                    acc
                }))
            }
            Value::Binary(val) => {
                JsonValue::String(val.iter().fold(String::new(), |mut acc, x| {
                    acc.push_str(&format!("{:x}", x));
                    acc
                }))
            }
            Value::Decimal(_) => todo!(),

            Value::Struct(s) => JsonValue::Object(JsonMap::from_iter(
                s.lookup
                    .iter()
                    .map(|(k, v)| (k, &s.fields[*v]))
                    .map(|(id, value)| {
                        let json: JsonValue = match value {
                            Some(val) => val.into(),
                            None => JsonValue::Null,
                        };
                        (id.to_string(), json)
                    }),
            )),
            Value::List(list) => JsonValue::Array(
                list.iter()
                    .map(|opt| match opt {
                        Some(literal) => literal.into(),
                        None => JsonValue::Null,
                    })
                    .collect(),
            ),
            Value::Map(map) => {
                let mut object = JsonMap::with_capacity(2);
                object.insert(
                    "keys".to_string(),
                    JsonValue::Array(map.keys().map(|literal| literal.into()).collect()),
                );
                object.insert(
                    "values".to_string(),
                    JsonValue::Array(
                        map.values()
                            .map(|literal| match literal {
                                Some(literal) => literal.into(),
                                None => JsonValue::Null,
                            })
                            .collect(),
                    ),
                );
                JsonValue::Object(object)
            }
        }
    }
}

mod datetime {
    pub(crate) fn date_to_days(date: &NaiveDate) -> i32 {
        date.signed_duration_since(
            // This is always the same and shouldn't fail
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
        )
        .num_days() as i32
    }

    pub(crate) fn days_to_date(days: i32) -> NaiveDate {
        // This shouldn't fail until the year 262000
        DateTime::from_timestamp(days as i64 * 86_400, 0)
            .unwrap()
            .naive_utc()
            .date()
    }

    pub(crate) fn time_to_microseconds(time: &NaiveTime) -> i64 {
        time.signed_duration_since(
            // This is always the same and shouldn't fail
            NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap(),
        )
        .num_microseconds()
        .unwrap()
    }

    pub(crate) fn microseconds_to_time(micros: i64) -> NaiveTime {
        let (secs, rem) = (micros / 1_000_000, micros % 1_000_000);

        NaiveTime::from_num_seconds_from_midnight_opt(secs as u32, rem as u32 * 1_000).unwrap()
    }

    pub(crate) fn datetime_to_microseconds(time: &NaiveDateTime) -> i64 {
        time.and_utc().timestamp_micros()
    }

    pub(crate) fn microseconds_to_datetime(micros: i64) -> NaiveDateTime {
        let (secs, rem) = (micros / 1_000_000, micros % 1_000_000);

        // This shouldn't fail until the year 262000
        DateTime::from_timestamp(secs, rem as u32 * 1_000)
            .unwrap()
            .naive_utc()
    }

    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};

    pub(crate) fn datetimetz_to_microseconds(time: &DateTime<Utc>) -> i64 {
        time.timestamp_micros()
    }

    pub(crate) fn microseconds_to_datetimetz(micros: i64) -> DateTime<Utc> {
        let (secs, rem) = (micros / 1_000_000, micros % 1_000_000);

        Utc.from_utc_datetime(
            // This shouldn't fail until the year 262000
            &DateTime::from_timestamp(secs, rem as u32 * 1_000)
                .unwrap()
                .naive_utc(),
        )
    }
}

pub trait TrySub: Sized {
    fn try_sub(&self, other: &Self) -> Result<Self, Error>;
}

impl<T: Sub<Output = T> + Copy> TrySub for T {
    fn try_sub(&self, other: &Self) -> Result<Self, Error> {
        Ok(*self - *other)
    }
}

impl TrySub for Value {
    fn try_sub(&self, other: &Self) -> Result<Self, Error> {
        match (self, other) {
            (Value::Int(own), Value::Int(other)) => Ok(Value::Int(own - other)),
            (Value::LongInt(own), Value::LongInt(other)) => Ok(Value::LongInt(own - other)),
            (Value::Float(own), Value::Float(other)) => Ok(Value::Float(*own - *other)),
            (Value::Double(own), Value::Double(other)) => Ok(Value::Double(*own - *other)),
            (Value::Date(own), Value::Date(other)) => Ok(Value::Date(own - other)),
            (Value::Time(own), Value::Time(other)) => Ok(Value::Time(own - other)),
            (Value::Timestamp(own), Value::Timestamp(other)) => Ok(Value::Timestamp(own - other)),
            (Value::TimestampTZ(own), Value::TimestampTZ(other)) => {
                Ok(Value::TimestampTZ(own - other))
            }
            (Value::String(own), Value::String(other)) => {
                Ok(Value::LongInt(sub_string(own, other) as i64))
            }
            (Value::UUID(own), Value::UUID(other)) => {
                let (own1, own2, own3, own4) = own.to_fields_le();
                let (other1, other2, other3, other4) = other.to_fields_le();
                let mut sub4 = [0; 8];
                for i in 0..own4.len() {
                    sub4[i] = own4[i] - other4[i];
                }
                Ok(Value::UUID(Uuid::from_fields_le(
                    own1 - other1,
                    own2 - other2,
                    own3 - other3,
                    &sub4,
                )))
            }
            (Value::Fixed(own_size, own), Value::Fixed(other_size, other)) => Ok(Value::Fixed(
                if own_size <= other_size {
                    *own_size
                } else if own_size > other_size {
                    *other_size
                } else {
                    panic!("Size must be either smaller, equal or larger");
                },
                own.iter()
                    .zip(other.iter())
                    .map(|(own, other)| own - other)
                    .collect(),
            )),
            (x, y) => Err(Error::Type(
                x.datatype().to_string(),
                y.datatype().to_string(),
            )),
        }
    }
}

fn sub_string(left: &str, right: &str) -> u64 {
    if let Some(distance) = left
        .chars()
        .zip(right.chars())
        .take(256)
        .skip_while(|(l, r)| l == r)
        .try_fold(0, |acc, (l, r)| {
            if let (Some(l), Some(r)) = (l.to_digit(36), r.to_digit(36)) {
                Some(acc + (l - r).pow(2))
            } else {
                None
            }
        })
    {
        distance as u64
    } else {
        let mut hasher = DefaultHasher::new();
        hasher.write(left.as_bytes());
        let left = hasher.finish();
        hasher.write(right.as_bytes());
        let right = hasher.finish();
        left - right
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        spec::types::{ListType, MapType, StructType},
        types::StructField,
    };

    fn check_json_serde(json: &str, expected_literal: Value, expected_type: &Type) {
        let raw_json_value = serde_json::from_str::<JsonValue>(json).unwrap();
        let desered_literal = Value::try_from_json(raw_json_value.clone(), expected_type).unwrap();
        assert_eq!(desered_literal, Some(expected_literal.clone()));

        let expected_json_value: JsonValue = (&expected_literal).into();
        let sered_json = serde_json::to_string(&expected_json_value).unwrap();
        let parsed_json_value = serde_json::from_str::<JsonValue>(&sered_json).unwrap();

        assert_eq!(parsed_json_value, raw_json_value);
    }

    fn check_avro_bytes_serde(input: Vec<u8>, expected_literal: Value, expected_type: &Type) {
        let raw_schema = r#""bytes""#;
        let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();

        let bytes = ByteBuf::from(input);
        let literal = Value::try_from_bytes(&bytes, expected_type).unwrap();
        assert_eq!(literal, expected_literal);

        let mut writer = apache_avro::Writer::new(&schema, Vec::new());
        writer.append_ser(bytes).unwrap();
        let encoded = writer.into_inner().unwrap();
        let reader = apache_avro::Reader::new(&*encoded).unwrap();

        for record in reader {
            let result = apache_avro::from_value::<ByteBuf>(&record.unwrap()).unwrap();
            let desered_literal = Value::try_from_bytes(&result, expected_type).unwrap();
            assert_eq!(desered_literal, expected_literal);
        }
    }

    #[test]
    fn json_boolean() {
        let record = r#"true"#;

        check_json_serde(
            record,
            Value::Boolean(true),
            &Type::Primitive(PrimitiveType::Boolean),
        );
    }

    #[test]
    fn json_int() {
        let record = r#"32"#;

        check_json_serde(record, Value::Int(32), &Type::Primitive(PrimitiveType::Int));
    }

    #[test]
    fn json_long() {
        let record = r#"32"#;

        check_json_serde(
            record,
            Value::LongInt(32),
            &Type::Primitive(PrimitiveType::Long),
        );
    }

    #[test]
    fn json_float() {
        let record = r#"1.0"#;

        check_json_serde(
            record,
            Value::Float(OrderedFloat(1.0)),
            &Type::Primitive(PrimitiveType::Float),
        );
    }

    #[test]
    fn json_double() {
        let record = r#"1.0"#;

        check_json_serde(
            record,
            Value::Double(OrderedFloat(1.0)),
            &Type::Primitive(PrimitiveType::Double),
        );
    }

    #[test]
    fn json_date() {
        let record = r#""2017-11-16""#;

        check_json_serde(
            record,
            Value::Date(17486),
            &Type::Primitive(PrimitiveType::Date),
        );
    }

    #[test]
    fn json_time() {
        let record = r#""22:31:08.123456""#;

        check_json_serde(
            record,
            Value::Time(81068123456),
            &Type::Primitive(PrimitiveType::Time),
        );
    }

    #[test]
    fn json_timestamp() {
        let record = r#""2017-11-16T22:31:08.123456""#;

        check_json_serde(
            record,
            Value::Timestamp(1510871468123456),
            &Type::Primitive(PrimitiveType::Timestamp),
        );
    }

    #[test]
    fn json_timestamptz() {
        let record = r#""2017-11-16T22:31:08.123456+00:00""#;

        check_json_serde(
            record,
            Value::TimestampTZ(1510871468123456),
            &Type::Primitive(PrimitiveType::Timestamptz),
        );
    }

    #[test]
    fn json_string() {
        let record = r#""iceberg""#;

        check_json_serde(
            record,
            Value::String("iceberg".to_string()),
            &Type::Primitive(PrimitiveType::String),
        );
    }

    #[test]
    fn json_uuid() {
        let record = r#""f79c3e09-677c-4bbd-a479-3f349cb785e7""#;

        check_json_serde(
            record,
            Value::UUID(Uuid::parse_str("f79c3e09-677c-4bbd-a479-3f349cb785e7").unwrap()),
            &Type::Primitive(PrimitiveType::Uuid),
        );
    }

    #[test]
    fn json_struct() {
        let record = r#"{"id": 1, "name": "bar", "address": null}"#;

        check_json_serde(
            record,
            Value::Struct(Struct::from_iter(vec![
                ("id".to_string(), Some(Value::Int(1))),
                ("name".to_string(), Some(Value::String("bar".to_string()))),
                ("address".to_string(), None),
            ])),
            &Type::Struct(StructType::new(vec![
                StructField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: Type::Primitive(PrimitiveType::Int),
                    doc: None,
                },
                StructField {
                    id: 2,
                    name: "name".to_string(),
                    required: false,
                    field_type: Type::Primitive(PrimitiveType::String),
                    doc: None,
                },
                StructField {
                    id: 3,
                    name: "address".to_string(),
                    required: false,
                    field_type: Type::Primitive(PrimitiveType::String),
                    doc: None,
                },
            ])),
        );
    }

    #[test]
    fn json_list() {
        let record = r#"[1, 2, 3, null]"#;

        check_json_serde(
            record,
            Value::List(vec![
                Some(Value::Int(1)),
                Some(Value::Int(2)),
                Some(Value::Int(3)),
                None,
            ]),
            &Type::List(ListType {
                element_id: 0,
                element_required: true,
                element: Box::new(Type::Primitive(PrimitiveType::Int)),
            }),
        );
    }

    #[test]
    fn json_map() {
        let record = r#"{ "keys": ["a", "b", "c"], "values": [1, 2, null] }"#;

        check_json_serde(
            record,
            Value::Map(BTreeMap::from([
                (Value::String("a".to_string()), Some(Value::Int(1))),
                (Value::String("b".to_string()), Some(Value::Int(2))),
                (Value::String("c".to_string()), None),
            ])),
            &Type::Map(MapType {
                key_id: 0,
                key: Box::new(Type::Primitive(PrimitiveType::String)),
                value_id: 1,
                value: Box::new(Type::Primitive(PrimitiveType::Int)),
                value_required: true,
            }),
        );
    }

    #[test]
    fn avro_bytes_boolean() {
        let bytes = vec![1u8];

        check_avro_bytes_serde(
            bytes,
            Value::Boolean(true),
            &Type::Primitive(PrimitiveType::Boolean),
        );
    }

    #[test]
    fn avro_bytes_int() {
        let bytes = vec![32u8, 0u8, 0u8, 0u8];

        check_avro_bytes_serde(bytes, Value::Int(32), &Type::Primitive(PrimitiveType::Int));
    }

    #[test]
    fn avro_bytes_long() {
        let bytes = vec![32u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];

        check_avro_bytes_serde(
            bytes,
            Value::LongInt(32),
            &Type::Primitive(PrimitiveType::Long),
        );
    }

    #[test]
    fn avro_bytes_float() {
        let bytes = vec![0u8, 0u8, 128u8, 63u8];

        check_avro_bytes_serde(
            bytes,
            Value::Float(OrderedFloat(1.0)),
            &Type::Primitive(PrimitiveType::Float),
        );
    }

    #[test]
    fn avro_bytes_double() {
        let bytes = vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 240u8, 63u8];

        check_avro_bytes_serde(
            bytes,
            Value::Double(OrderedFloat(1.0)),
            &Type::Primitive(PrimitiveType::Double),
        );
    }

    #[test]
    fn avro_bytes_string() {
        let bytes = vec![105u8, 99u8, 101u8, 98u8, 101u8, 114u8, 103u8];

        check_avro_bytes_serde(
            bytes,
            Value::String("iceberg".to_string()),
            &Type::Primitive(PrimitiveType::String),
        );
    }

    #[test]
    fn test_transform_identity() {
        let value = Value::Int(42);
        let result = value.transform(&Transform::Identity).unwrap();
        assert_eq!(result, Value::Int(42));
    }

    #[test]
    fn test_transform_bucket() {
        let value = Value::String("test".to_string());
        let result = value.transform(&Transform::Bucket(10)).unwrap();
        assert!(matches!(result, Value::Int(_)));
    }

    #[test]
    fn test_transform_truncate_int() {
        let value = Value::Int(42);
        let result = value.transform(&Transform::Truncate(10)).unwrap();
        assert_eq!(result, Value::Int(40));
    }

    #[test]
    fn test_transform_truncate_long_int() {
        let value = Value::LongInt(1234567890);
        let result = value.transform(&Transform::Truncate(1000000)).unwrap();
        assert_eq!(result, Value::LongInt(1234000000));
    }

    #[test]
    fn test_transform_truncate_string() {
        let value = Value::String("Hello, World!".to_string());
        let result = value.transform(&Transform::Truncate(5)).unwrap();
        assert_eq!(result, Value::String("Hello".to_string()));
    }

    #[test]
    fn test_transform_truncate_unsupported() {
        let value = Value::Boolean(true);
        let result = value.transform(&Transform::Truncate(5));
        assert!(matches!(result, Err(Error::NotSupported(_))));
    }

    #[test]
    fn test_transform_year_date() {
        let value = Value::Date(19478);
        let result = value.transform(&Transform::Year).unwrap();
        assert_eq!(result, Value::Int(53));

        let value = Value::Date(19523);
        let result = value.transform(&Transform::Year).unwrap();
        assert_eq!(result, Value::Int(53));

        let value = Value::Date(19723);
        let result = value.transform(&Transform::Year).unwrap();
        assert_eq!(result, Value::Int(54));
    }

    #[test]
    fn test_transform_year_timestamp() {
        let value = Value::Timestamp(1682937000000000);
        let result = value.transform(&Transform::Year).unwrap();
        assert_eq!(result, Value::Int(53));

        let value = Value::Timestamp(1686840330000000);
        let result = value.transform(&Transform::Year).unwrap();
        assert_eq!(result, Value::Int(53));

        let value = Value::Timestamp(1704067200000000);
        let result = value.transform(&Transform::Year).unwrap();
        assert_eq!(result, Value::Int(54));
    }

    #[test]
    fn test_transform_month_date() {
        let value = Value::Date(19478);
        let result = value.transform(&Transform::Month).unwrap();
        assert_eq!(result, Value::Int(5)); // 0-based month index

        let value = Value::Date(19523);
        let result = value.transform(&Transform::Month).unwrap();
        assert_eq!(result, Value::Int(6)); // 0-based month index

        let value = Value::Date(19723);
        let result = value.transform(&Transform::Month).unwrap();
        assert_eq!(result, Value::Int(1)); // 0-based month index
    }

    #[test]
    fn test_transform_month_timestamp() {
        let value = Value::Timestamp(1682937000000000);
        let result = value.transform(&Transform::Month).unwrap();
        assert_eq!(result, Value::Int(5)); // 0-based month index

        let value = Value::Timestamp(1686840330000000);
        let result = value.transform(&Transform::Month).unwrap();
        assert_eq!(result, Value::Int(6)); // 0-based month index

        let value = Value::Timestamp(1704067200000000);
        let result = value.transform(&Transform::Month).unwrap();
        assert_eq!(result, Value::Int(1)); // 0-based month index
    }

    #[test]
    fn test_transform_month_unsupported() {
        let value = Value::Boolean(true);
        let result = value.transform(&Transform::Month);
        assert!(matches!(result, Err(Error::NotSupported(_))));

        let value = Value::Boolean(true);
        let result = value.transform(&Transform::Month);
        assert!(matches!(result, Err(Error::NotSupported(_))));

        let value = Value::Boolean(true);
        let result = value.transform(&Transform::Month);
        assert!(matches!(result, Err(Error::NotSupported(_))));
    }

    #[test]
    fn test_transform_day_date() {
        let value = Value::Date(19478);
        let result = value.transform(&Transform::Day).unwrap();
        assert_eq!(result, Value::Int(1)); // 0-based day index

        let value = Value::Date(19523);
        let result = value.transform(&Transform::Day).unwrap();
        assert_eq!(result, Value::Int(15)); // 0-based day index

        let value = Value::Date(19723);
        let result = value.transform(&Transform::Day).unwrap();
        assert_eq!(result, Value::Int(1)); // 0-based day index
    }

    #[test]
    fn test_transform_day_timestamp() {
        let value = Value::Timestamp(1682937000000000);
        let result = value.transform(&Transform::Day).unwrap();
        assert_eq!(result, Value::Int(1)); // 0-based day index

        let value = Value::Timestamp(1686840330000000);
        let result = value.transform(&Transform::Day).unwrap();
        assert_eq!(result, Value::Int(15)); // 0-based day index

        let value = Value::Timestamp(1704067200000000);
        let result = value.transform(&Transform::Day).unwrap();
        assert_eq!(result, Value::Int(1)); // 0-based day index
    }

    #[test]
    fn test_transform_day_unsupported() {
        let value = Value::Boolean(true);
        let result = value.transform(&Transform::Day);
        assert!(matches!(result, Err(Error::NotSupported(_))));
    }

    #[test]
    fn test_transform_hour_timestamp() {
        let value = Value::Timestamp(1682937000000000);
        let result = value.transform(&Transform::Hour).unwrap();
        assert_eq!(result, Value::Int(10)); // Assuming the timestamp is at 12:00 UTC

        let value = Value::Timestamp(1686840330000000);
        let result = value.transform(&Transform::Hour).unwrap();
        assert_eq!(result, Value::Int(14)); // Assuming the timestamp is at 12:00 UTC

        let value = Value::Timestamp(1704067200000000);
        let result = value.transform(&Transform::Hour).unwrap();
        assert_eq!(result, Value::Int(0)); // Assuming the timestamp is at 12:00 UTC
    }

    #[test]
    fn test_transform_hour_unsupported() {
        let value = Value::Date(0);
        let result = value.transform(&Transform::Hour);
        assert!(matches!(result, Err(Error::NotSupported(_))));
    }
}
