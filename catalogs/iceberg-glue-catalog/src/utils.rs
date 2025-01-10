use std::collections::HashMap;

/// Property `metadata_location` for `TableInput`
pub(crate) const METADATA_LOCATION: &str = "metadata_location";
/// Parameter key `table_type` for `TableInput`
pub(crate) const TABLE_TYPE: &str = "table_type";
/// Parameter value `table_type` for `TableInput`
pub(crate) const ICEBERG: &str = "ICEBERG";

pub(crate) fn get_parameters (metadata_location: &str) -> HashMap<String, String> {
    HashMap::from([
        (TABLE_TYPE.to_string(), ICEBERG.to_string()),
        (METADATA_LOCATION.to_string(), metadata_location.to_string())
    ])
}

