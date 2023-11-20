use iceberg_rust::error::Error as IcebergError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("parse error")]
    ParseError(#[from] url::ParseError),
    #[error("sql error")]
    SqlError(#[from] sqlx::Error),
}

impl From<Error> for IcebergError {
    fn from(value: Error) -> Self {
        IcebergError::InvalidFormat(value.to_string())
    }
}
