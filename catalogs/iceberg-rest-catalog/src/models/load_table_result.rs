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
use iceberg_rust::spec::table_metadata::TableMetadata;
use serde::{Deserialize, Serialize};

/// LoadTableResult : Result used when a table is successfully loaded.   The table metadata JSON is returned in the `metadata` field. The corresponding file location of table metadata should be returned in the `metadata-location` field, unless the metadata is not yet committed. For example, a create transaction may return metadata that is staged but not committed. Clients can check whether metadata has changed by comparing metadata locations after the table has been created.   The `config` map returns table-specific configuration for the table's resources, including its HTTP client and FileIO. For example, config may contain a specific FileIO implementation class for the table depending on its underlying storage.   The following configurations should be respected by clients:  ## General Configurations  - `token`: Authorization bearer token to use for table requests if OAuth2 security is enabled   ## AWS Configurations  The following configurations should be respected when working with tables stored in AWS S3  - `client.region`: region to configure client for making requests to AWS  - `s3.access-key-id`: id for credentials that provide access to the data in S3  - `s3.secret-access-key`: secret for credentials that provide access to data in S3   - `s3.session-token`: if present, this value should be used for as the session token   - `s3.remote-signing-enabled`: if `true` remote signing should be performed as described in the `s3-signer-open-api.yaml` specification  - `s3.cross-region-access-enabled`: if `true`, S3 Cross-Region bucket access is enabled  ## Storage Credentials  Credentials for ADLS / GCS / S3 / ... are provided through the `storage-credentials` field. Clients must first check whether the respective credentials exist in the `storage-credentials` field before checking the `config` for credentials.
#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct LoadTableResult {
    /// May be null if the table is staged as part of a transaction
    #[serde(rename = "metadata-location", skip_serializing_if = "Option::is_none")]
    pub metadata_location: Option<String>,
    #[serde(rename = "metadata")]
    pub metadata: TableMetadata,
    #[serde(rename = "config", skip_serializing_if = "Option::is_none")]
    pub config: Option<std::collections::HashMap<String, String>>,
    #[serde(
        rename = "storage-credentials",
        skip_serializing_if = "Option::is_none"
    )]
    pub storage_credentials: Option<Vec<models::StorageCredential>>,
}

impl LoadTableResult {
    /// Result used when a table is successfully loaded.   The table metadata JSON is returned in the `metadata` field. The corresponding file location of table metadata should be returned in the `metadata-location` field, unless the metadata is not yet committed. For example, a create transaction may return metadata that is staged but not committed. Clients can check whether metadata has changed by comparing metadata locations after the table has been created.   The `config` map returns table-specific configuration for the table's resources, including its HTTP client and FileIO. For example, config may contain a specific FileIO implementation class for the table depending on its underlying storage.   The following configurations should be respected by clients:  ## General Configurations  - `token`: Authorization bearer token to use for table requests if OAuth2 security is enabled   ## AWS Configurations  The following configurations should be respected when working with tables stored in AWS S3  - `client.region`: region to configure client for making requests to AWS  - `s3.access-key-id`: id for credentials that provide access to the data in S3  - `s3.secret-access-key`: secret for credentials that provide access to data in S3   - `s3.session-token`: if present, this value should be used for as the session token   - `s3.remote-signing-enabled`: if `true` remote signing should be performed as described in the `s3-signer-open-api.yaml` specification  - `s3.cross-region-access-enabled`: if `true`, S3 Cross-Region bucket access is enabled  ## Storage Credentials  Credentials for ADLS / GCS / S3 / ... are provided through the `storage-credentials` field. Clients must first check whether the respective credentials exist in the `storage-credentials` field before checking the `config` for credentials.
    pub fn new(metadata: TableMetadata) -> LoadTableResult {
        LoadTableResult {
            metadata_location: None,
            metadata,
            config: None,
            storage_credentials: None,
        }
    }
}
