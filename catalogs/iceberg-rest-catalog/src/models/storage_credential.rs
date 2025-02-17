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
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct StorageCredential {
    /// Indicates a storage location prefix where the credential is relevant. Clients should choose the most specific prefix (by selecting the longest prefix) if several credentials of the same type are available.
    #[serde(rename = "prefix")]
    pub prefix: String,
    #[serde(rename = "config")]
    pub config: std::collections::HashMap<String, String>,
}

impl StorageCredential {
    pub fn new(prefix: String, config: std::collections::HashMap<String, String>) -> StorageCredential {
        StorageCredential {
            prefix,
            config,
        }
    }
}

