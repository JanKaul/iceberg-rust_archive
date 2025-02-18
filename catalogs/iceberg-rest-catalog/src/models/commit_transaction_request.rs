/*
 * Apache Iceberg REST Catalog API
 *
 * Defines the specification for the first version of the REST Catalog API. Implementations should ideally support both Iceberg table specs v1 and v2, with priority given to v2.
 *
 * The version of the OpenAPI document: 0.0.1
 *
 * Generated by: https://openapi-generator.tech
 */

use crate::models;
use iceberg_rust::catalog::commit::CommitTable;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommitTransactionRequest {
    #[serde(rename = "table-changes")]
    pub table_changes: Vec<CommitTable>,
}

impl CommitTransactionRequest {
    pub fn new(table_changes: Vec<CommitTable>) -> CommitTransactionRequest {
        CommitTransactionRequest { table_changes }
    }
}
