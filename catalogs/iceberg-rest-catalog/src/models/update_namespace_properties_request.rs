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
pub struct UpdateNamespacePropertiesRequest {
    #[serde(rename = "removals", skip_serializing_if = "Option::is_none")]
    pub removals: Option<Vec<String>>,
    #[serde(rename = "updates", skip_serializing_if = "Option::is_none")]
    pub updates: Option<std::collections::HashMap<String, String>>,
}

impl UpdateNamespacePropertiesRequest {
    pub fn new() -> UpdateNamespacePropertiesRequest {
        UpdateNamespacePropertiesRequest {
            removals: None,
            updates: None,
        }
    }
}

