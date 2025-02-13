//! Transaction module for atomic table operations
//!
//! This module provides the transaction system for Iceberg tables, allowing multiple
//! operations to be grouped and executed atomically. The main types are:
//!
//! * [`TableTransaction`] - Builder for creating and executing atomic transactions
//! * [`Operation`] - Individual operations that can be part of a transaction
//!
//! Transactions ensure that either all operations succeed or none do, maintaining
//! table consistency. Common operations include:
//!
//! * Adding/updating schemas
//! * Appending data files
//! * Replacing data files
//! * Updating table properties
//! * Managing snapshots and branches

use std::collections::HashMap;

use iceberg_rust_spec::spec::{manifest::DataFile, schema::Schema, snapshot::SnapshotReference};

use crate::{catalog::commit::CommitTable, error::Error, table::Table};

use self::operation::Operation;

use super::delete_all_table_files;

pub(crate) mod append;
pub(crate) mod operation;

pub(crate) static APPEND_KEY: &str = "append";
pub(crate) static REPLACE_KEY: &str = "replace";
pub(crate) static ADD_SCHEMA_KEY: &str = "add-schema";
pub(crate) static SET_DEFAULT_SPEC_KEY: &str = "set-default-spec";
pub(crate) static UPDATE_PROPERTIES_KEY: &str = "update-properties";
pub(crate) static SET_SNAPSHOT_REF_KEY: &str = "set-ref";

/// A transaction that can perform multiple operations on a table atomically
///
/// TableTransaction allows grouping multiple table operations (like schema updates,
/// appends, overwrites) into a single atomic transaction. The transaction must be
/// committed for changes to take effect.
///
/// # Type Parameters
/// * `'table` - Lifetime of the reference to the table being modified
///
/// # Examples
/// ```
/// let mut table = // ... get table reference
/// table.new_transaction(None)
///     .add_schema(new_schema)
///     .append(data_files)
///     .commit()
///     .await?;
/// ```
pub struct TableTransaction<'table> {
    table: &'table mut Table,
    operations: HashMap<String, Operation>,
    branch: Option<String>,
}

impl<'table> TableTransaction<'table> {
    /// Create a transaction for the given table.
    pub(crate) fn new(table: &'table mut Table, branch: Option<&str>) -> Self {
        TableTransaction {
            table,
            operations: HashMap::new(),
            branch: branch.map(ToString::to_string),
        }
    }
    /// Adds a new schema to the table
    ///
    /// This operation adds a new schema version to the table. The schema ID will be
    /// automatically assigned when the transaction is committed.
    ///
    /// # Arguments
    /// * `schema` - The new schema to add to the table
    ///
    /// # Returns
    /// * `Self` - The transaction builder for method chaining
    pub fn add_schema(mut self, schema: Schema) -> Self {
        self.operations
            .insert(ADD_SCHEMA_KEY.to_owned(), Operation::AddSchema(schema));
        self
    }
    /// Sets the default partition specification ID for the table
    ///
    /// # Arguments
    /// * `spec_id` - The ID of the partition specification to set as default
    ///
    /// # Returns
    /// * `Self` - The transaction builder for method chaining
    ///
    /// The specified partition specification must already exist in the table metadata.
    pub fn set_default_spec(mut self, spec_id: i32) -> Self {
        self.operations.insert(
            SET_DEFAULT_SPEC_KEY.to_owned(),
            Operation::SetDefaultSpec(spec_id),
        );
        self
    }
    /// Appends new data files to the table
    ///
    /// This operation adds new data files to the table's current snapshot. Multiple
    /// append operations in the same transaction will be combined.
    ///
    /// # Arguments
    /// * `files` - Vector of data files to append to the table
    ///
    /// # Returns
    /// * `Self` - The transaction builder for method chaining
    ///
    /// # Examples
    /// ```
    /// let transaction = table.new_transaction(None)
    ///     .append_data(data_files)
    ///     .commit()
    ///     .await?;
    /// ```
    pub fn append_data(mut self, files: Vec<DataFile>) -> Self {
        self.operations
            .entry(APPEND_KEY.to_owned())
            .and_modify(|mut x| {
                if let Operation::Append {
                    branch: _,
                    data_files: old,
                    delete_files: _,
                    additional_summary: None,
                } = &mut x
                {
                    old.extend_from_slice(&files)
                }
            })
            .or_insert(Operation::Append {
                branch: self.branch.clone(),
                data_files: files,
                delete_files: Vec::new(),
                additional_summary: None,
            });
        self
    }
    /// Appends delete files to the table
    ///
    /// This operation adds files that mark records for deletion in the table's current snapshot.
    /// Multiple delete operations in the same transaction will be combined. The delete files
    /// specify which records should be removed when reading the table.
    ///
    /// # Arguments
    /// * `files` - Vector of delete files to append to the table
    ///
    /// # Returns
    /// * `Self` - The transaction builder for method chaining
    ///
    /// # Examples
    /// ```
    /// let transaction = table.new_transaction(None)
    ///     .append_delete(delete_files)
    ///     .commit()
    ///     .await?;
    /// ```
    pub fn append_delete(mut self, files: Vec<DataFile>) -> Self {
        self.operations
            .entry(APPEND_KEY.to_owned())
            .and_modify(|mut x| {
                if let Operation::Append {
                    branch: _,
                    data_files: _,
                    delete_files: old,
                    additional_summary: None,
                } = &mut x
                {
                    old.extend_from_slice(&files)
                }
            })
            .or_insert(Operation::Append {
                branch: self.branch.clone(),
                data_files: Vec::new(),
                delete_files: files,
                additional_summary: None,
            });
        self
    }
    /// Replaces all data files in the table with new ones
    ///
    /// This operation removes all existing data files and replaces them with the provided
    /// files. Multiple replace operations in the same transaction will be combined.
    ///
    /// # Arguments
    /// * `files` - Vector of data files that will replace the existing ones
    ///
    /// # Returns
    /// * `Self` - The transaction builder for method chaining
    ///
    /// # Examples
    /// ```
    /// let transaction = table.new_transaction(None)
    ///     .replace(new_files)
    ///     .commit()
    ///     .await?;
    /// ```
    pub fn replace(mut self, files: Vec<DataFile>) -> Self {
        self.operations
            .entry(REPLACE_KEY.to_owned())
            .and_modify(|mut x| {
                if let Operation::Replace {
                    branch: _,
                    files: old,
                    additional_summary: None,
                } = &mut x
                {
                    old.extend_from_slice(&files)
                }
            })
            .or_insert(Operation::Replace {
                branch: self.branch.clone(),
                files,
                additional_summary: None,
            });
        self
    }
    /// Quickly append files to the table
    pub fn replace_with_lineage(
        mut self,
        files: Vec<DataFile>,
        additional_summary: HashMap<String, String>,
    ) -> Self {
        self.operations
            .entry(REPLACE_KEY.to_owned())
            .and_modify(|mut x| {
                if let Operation::Replace {
                    branch: _,
                    files: old,
                    additional_summary: old_lineage,
                } = &mut x
                {
                    old.extend_from_slice(&files);
                    *old_lineage = Some(additional_summary.clone());
                }
            })
            .or_insert(Operation::Replace {
                branch: self.branch.clone(),
                files,
                additional_summary: Some(additional_summary),
            });
        self
    }
    /// Updates the table properties with new key-value pairs
    ///
    /// This operation adds or updates table properties. Multiple update operations
    /// in the same transaction will be combined.
    ///
    /// # Arguments
    /// * `entries` - Vector of (key, value) pairs to update in the table properties
    ///
    /// # Returns
    /// * `Self` - The transaction builder for method chaining
    ///
    /// # Examples
    /// ```
    /// let transaction = table.new_transaction(None)
    ///     .update_properties(vec![
    ///         ("write.format.default".to_string(), "parquet".to_string()),
    ///         ("write.metadata.compression-codec".to_string(), "gzip".to_string())
    ///     ])
    ///     .commit()
    ///     .await?;
    /// ```
    pub fn update_properties(mut self, entries: Vec<(String, String)>) -> Self {
        self.operations
            .entry(UPDATE_PROPERTIES_KEY.to_owned())
            .and_modify(|mut x| {
                if let Operation::UpdateProperties(props) = &mut x {
                    props.extend_from_slice(&entries)
                }
            })
            .or_insert(Operation::UpdateProperties(entries));
        self
    }
    /// Sets a snapshot reference for the table
    ///
    /// This operation creates or updates a named reference to a specific snapshot,
    /// allowing for features like branches and tags.
    ///
    /// # Arguments
    /// * `entry` - Tuple of (reference name, snapshot reference) defining the reference
    ///
    /// # Returns
    /// * `Self` - The transaction builder for method chaining
    ///
    /// # Examples
    /// ```
    /// let transaction = table.new_transaction(None)
    ///     .set_snapshot_ref((
    ///         "test-branch".to_string(),
    ///         SnapshotReference {
    ///             snapshot_id: 123,
    ///             retention: SnapshotRetention::default(),
    ///         }
    ///     ))
    ///     .commit()
    ///     .await?;
    /// ```
    pub fn set_snapshot_ref(mut self, entry: (String, SnapshotReference)) -> Self {
        self.operations.insert(
            SET_SNAPSHOT_REF_KEY.to_owned(),
            Operation::SetSnapshotRef(entry),
        );
        self
    }
    /// Commits all operations in this transaction atomically
    ///
    /// This method executes all operations in the transaction and updates the table
    /// metadata. The changes are atomic - either all operations succeed or none do.
    /// After commit, the transaction is consumed and the table is updated with the
    /// new metadata.
    ///
    /// # Returns
    /// * `Result<(), Error>` - Ok(()) if the commit succeeds, Error if it fails
    ///
    /// # Errors
    /// Returns an error if:
    /// * Any operation fails to execute
    /// * The catalog update fails
    /// * Cleanup of old data files fails (for replace operations)
    ///
    /// # Examples
    /// ```
    /// let result = table.new_transaction(None)
    ///     .append(data_files)
    ///     .update_properties(properties)
    ///     .commit()
    ///     .await?;
    /// ```
    pub async fn commit(self) -> Result<(), Error> {
        let catalog = self.table.catalog();
        let object_store = self.table.object_store();
        let identifier = self.table.identifier.clone();

        // Save old metadata to be able to remove old data after a rewrite operation
        let delete_data = if self.operations.values().any(|x| {
            matches!(
                x,
                Operation::Replace {
                    branch: _,
                    files: _,
                    additional_summary: _,
                }
            )
        }) {
            Some(self.table.metadata())
        } else {
            None
        };

        // Execute the table operations
        let (mut requirements, mut updates) = (Vec::new(), Vec::new());
        for operation in self.operations.into_values() {
            let (requirement, update) = operation
                .execute(self.table.metadata(), self.table.object_store())
                .await?;

            if let Some(requirement) = requirement {
                requirements.push(requirement);
            }
            updates.extend(update);
        }

        let new_table = catalog
            .clone()
            .update_table(CommitTable {
                identifier,
                requirements,
                updates,
            })
            .await?;

        if let Some(old_metadata) = delete_data {
            delete_all_table_files(old_metadata, object_store).await?;
        }

        *self.table = new_table;
        Ok(())
    }
}
