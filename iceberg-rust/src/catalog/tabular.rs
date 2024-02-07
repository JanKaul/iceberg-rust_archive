/*!
 * Provides the [Relation] enum to refer to any queriable entity like a table or a view
*/

use std::sync::Arc;

use iceberg_rust_spec::spec::materialized_view_metadata::MaterializedViewMetadata;
use iceberg_rust_spec::spec::table_metadata::TableMetadata;
use iceberg_rust_spec::spec::view_metadata::ViewMetadata;
use object_store::ObjectStore;
use serde::{self, Deserialize, Serialize};

use crate::error::Error;
use crate::materialized_view::MaterializedView;
use crate::table::Table;
use crate::view::View;

use super::identifier::Identifier;
use super::Catalog;

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
/// Enum for different types that can be queried like a table, for example view
pub enum Tabular {
    /// An iceberg table
    Table(Table),
    /// An iceberg view
    View(View),
    /// An iceberg materialized view
    MaterializedView(MaterializedView),
}

impl Tabular {
    #[inline]
    /// Return metadata location for relation.
    pub fn identifier(&self) -> &Identifier {
        match self {
            Tabular::Table(table) => table.identifier(),
            Tabular::View(view) => view.identifier(),
            Tabular::MaterializedView(mv) => mv.identifier(),
        }
    }

    #[inline]
    /// Return metadata location for relation.
    pub fn metadata(&self) -> TabularMetadata {
        match self {
            Tabular::Table(table) => TabularMetadata::Table(table.metadata().clone()),
            Tabular::View(view) => TabularMetadata::View(view.metadata().clone()),
            Tabular::MaterializedView(mv) => {
                TabularMetadata::MaterializedView(mv.metadata().clone())
            }
        }
    }

    #[inline]
    /// Return catalog for relation.
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        match self {
            Tabular::Table(table) => table.catalog(),
            Tabular::View(view) => view.catalog(),
            Tabular::MaterializedView(mv) => mv.catalog(),
        }
    }

    /// Reload relation from catalog
    pub async fn reload(&mut self) -> Result<(), Error> {
        match self {
            Tabular::Table(table) => {
                let new = if let Tabular::Table(table) =
                    table.catalog().load_table(table.identifier()).await?
                {
                    Ok(table)
                } else {
                    Err(Error::InvalidFormat(
                        "Tabular type from catalog response".to_string(),
                    ))
                }?;
                let _ = std::mem::replace(table, new);
            }
            Tabular::View(view) => {
                let new = if let Tabular::View(view) =
                    view.catalog().load_table(view.identifier()).await?
                {
                    Ok(view)
                } else {
                    Err(Error::InvalidFormat(
                        "Tabular type from catalog response".to_string(),
                    ))
                }?;
                let _ = std::mem::replace(view, new);
            }
            Tabular::MaterializedView(matview) => {
                let new = if let Tabular::MaterializedView(matview) =
                    matview.catalog().load_table(matview.identifier()).await?
                {
                    Ok(matview)
                } else {
                    Err(Error::InvalidFormat(
                        "Tabular type from catalog response".to_string(),
                    ))
                }?;
                let _ = std::mem::replace(matview, new);
            }
        };
        Ok(())
    }
}

/// Metadata of an iceberg relation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum TabularMetadata {
    /// Table metadata
    Table(TableMetadata),
    /// View metadata
    View(ViewMetadata),
    /// Materialized view metadata
    MaterializedView(MaterializedViewMetadata),
}

impl TabularMetadata {
    /// Get location for tabular
    pub fn location(&self) -> &str {
        match self {
            TabularMetadata::Table(table) => &table.location,
            TabularMetadata::View(view) => &view.location,
            TabularMetadata::MaterializedView(matview) => &matview.location,
        }
    }
    /// Get sequence number for tabular
    pub fn sequence_number(&self) -> i64 {
        match self {
            TabularMetadata::Table(table) => table.last_sequence_number,
            TabularMetadata::View(view) => view.current_version_id,
            TabularMetadata::MaterializedView(matview) => matview.current_version_id,
        }
    }
}

impl From<TableMetadata> for TabularMetadata {
    fn from(value: TableMetadata) -> Self {
        TabularMetadata::Table(value)
    }
}

impl From<ViewMetadata> for TabularMetadata {
    fn from(value: ViewMetadata) -> Self {
        TabularMetadata::View(value)
    }
}

impl From<MaterializedViewMetadata> for TabularMetadata {
    fn from(value: MaterializedViewMetadata) -> Self {
        TabularMetadata::MaterializedView(value)
    }
}

/// Fetch metadata of a tabular(table, view, materialized view) structure from an object_store
pub async fn get_tabular_metadata(
    metadata_location: &str,
    object_store: Arc<dyn ObjectStore>,
) -> Result<TabularMetadata, Error> {
    let bytes = object_store
        .get(&metadata_location.into())
        .await?
        .bytes()
        .await?;
    Ok(serde_json::from_slice(&bytes)?)
}
