/*!
Error type for iceberg
*/

use thiserror::Error;

#[derive(Error, Debug)]
/// Iceberg error
pub enum Error {
    /// Invalid format
    #[error("{0} doesn't have the right format")]
    InvalidFormat(String),
    /// Type error
    #[error("Value {0} doesn't have the {1} type.")]
    Type(String, String),
    /// Schema error
    #[error("Column {0} not in schema {1}.")]
    Schema(String, String),
    /// Conversion error
    #[error("Failed to convert {0} to {1}.")]
    Conversion(String, String),
    /// Not found
    #[error("{0} {1} not found.")]
    NotFound(String, String),
    /// Not supported
    #[error("Feature {0} is not supported.")]
    NotSupported(String),
    /// Avro error
    #[error("avro error")]
    Avro(#[from] apache_avro::Error),
    /// Serde json
    #[error("serde json error")]
    JSONSerde(#[from] serde_json::Error),
    /// Chrono parse
    #[error("chrono parse error")]
    Chrono(#[from] chrono::ParseError),
    /// Chrono parse
    #[error("uuid error")]
    Uuid(#[from] uuid::Error),
    /// Io error
    #[error("io error")]
    IO(#[from] std::io::Error),
    /// Objectstore error
    #[error("object store error")]
    ObjectStore(#[from] object_store::Error),
    /// Try from slice error
    #[error("try from slice error")]
    TryFromSlice(#[from] std::array::TryFromSliceError),
    /// Try from int error
    #[error("try from int error")]
    TryFromInt(#[from] std::num::TryFromIntError),
    /// Utf8 error
    #[error("utf8 error")]
    UTF8(#[from] std::str::Utf8Error),
    /// from utf8 error
    #[error("from utf8 error")]
    FromUTF8(#[from] std::string::FromUtf8Error),
    /// parse int error
    #[error("parse int error")]
    ParseInt(#[from] std::num::ParseIntError),
    /// table metadata builder
    #[error("table metadata builder")]
    TableMetadataBuilder(#[from] crate::spec::table_metadata::TableMetadataBuilderError),
    /// view metadata builder
    #[error("view metadata builder")]
    ViewMetadataBuilder(#[from] crate::spec::view_metadata::GeneralViewMetadataBuilderError),
    /// version builder
    #[error("version builder")]
    VersionBuilder(#[from] crate::spec::view_metadata::VersionBuilderError),
}
