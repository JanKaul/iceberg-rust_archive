/*!
 * Defines the different [Operation]s on a [Table].
*/

use std::{collections::HashMap, sync::Arc};

use iceberg_rust_spec::spec::table_metadata::TableMetadata;
use iceberg_rust_spec::spec::{
    manifest::{DataFile, ManifestEntry, Status},
    schema::Schema,
    snapshot::{Operation as SpecOperation, SnapshotReference, SnapshotRetention},
};
use object_store::ObjectStore;
use smallvec::SmallVec;

use crate::table::manifest::ManifestReader;
use crate::table::manifest_list::{ManifestListReader, ManifestListWriter};

use crate::{
    catalog::commit::{TableRequirement, TableUpdate},
    error::Error,
    util::{partition_struct_to_vec, summary_to_rectangle, Rectangle},
};

use super::append::{
    select_manifest_partitioned, select_manifest_unpartitioned, split_datafiles, SelectedManifest,
};

/// The target number of datafiles per manifest is dynamic, but we don't want to go below this number.
static MIN_DATAFILES_PER_MANIFEST: usize = 4;

#[derive(Debug)]
///Table operations
pub enum Operation {
    /// Update schema
    AddSchema(Schema),
    /// Update spec
    SetDefaultSpec(i32),
    /// Update table properties
    UpdateProperties(Vec<(String, String)>),
    /// Set Ref
    SetSnapshotRef((String, SnapshotReference)),
    /// Replace the sort order
    // ReplaceSortOrder,
    // /// Update the table location
    // UpdateLocation,
    /// Append new files to the table
    Append {
        branch: Option<String>,
        files: Vec<DataFile>,
        additional_summary: Option<HashMap<String, String>>,
    },
    // /// Quickly append new files to the table
    // NewFastAppend {
    //     paths: Vec<String>,
    //     partition_values: Vec<Struct>,
    // },
    /// Replace all files in the table and commit
    Rewrite {
        branch: Option<String>,
        files: Vec<DataFile>,
        additional_summary: Option<HashMap<String, String>>,
    },
    // /// Replace manifests files and commit
    // RewriteManifests,
    // /// Replace files in the table by a filter expression
    // NewOverwrite,
    // /// Remove or replace rows in existing data files
    // NewRowDelta,
    // /// Delete files in the table and commit
    // NewDelete,
    // /// Expire snapshots in the table
    // ExpireSnapshots,
    // /// Manage snapshots in the table
    // ManageSnapshots,
    // /// Read and write table data and metadata files
    // IO,
}

impl Operation {
    pub async fn execute(
        self,
        table_metadata: &TableMetadata,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<(Option<TableRequirement>, Vec<TableUpdate>), Error> {
        match self {
            Operation::Append {
                branch,
                files: new_files,
                additional_summary,
            } => {
                let partition_fields =
                    table_metadata.current_partition_fields(branch.as_deref())?;
                let schema = table_metadata.current_schema(branch.as_deref())?;
                let old_snapshot = table_metadata.current_snapshot(branch.as_deref())?;

                let partition_column_names = partition_fields
                    .iter()
                    .map(|x| x.name())
                    .collect::<SmallVec<[_; 4]>>();

                let bounding_partition_values = new_files
                    .iter()
                    .try_fold(None, |acc, x| {
                        let node = partition_struct_to_vec(x.partition(), &partition_column_names)?;
                        let Some(mut acc) = acc else {
                            return Ok::<_, Error>(Some(Rectangle::new(node.clone(), node)));
                        };
                        acc.expand_with_node(node);
                        Ok(Some(acc))
                    })?
                    .ok_or(Error::NotFound(
                        "Bounding".to_owned(),
                        "rectangle".to_owned(),
                    ))?;

                let mut manifest_list_writer = ManifestListWriter::try_new(
                    object_store.clone(),
                    table_metadata,
                    branch.clone(),
                )?;

                // Find a manifest to add the new datafiles
                let mut existing_file_count = 0;
                let selected_manifest_opt = if let Some(old_snapshot) = old_snapshot {
                    let manifest_list_reader = ManifestListReader::from_snapshot(
                        old_snapshot,
                        table_metadata,
                        object_store.clone(),
                    )
                    .await?;

                    let SelectedManifest {
                        manifest,
                        file_count_all_entries,
                    } = if partition_column_names.is_empty() {
                        select_manifest_unpartitioned(
                            manifest_list_reader,
                            &mut manifest_list_writer,
                        )?
                    } else {
                        select_manifest_partitioned(
                            manifest_list_reader,
                            &mut manifest_list_writer,
                            &bounding_partition_values,
                        )?
                    };
                    existing_file_count = file_count_all_entries;
                    Some(manifest)
                } else {
                    // If manifest list doesn't exist, there is no manifest
                    None
                };

                let selected_manifest_file_count = selected_manifest_opt
                    .as_ref()
                    .and_then(|selected_manifest| {
                        match (
                            selected_manifest.existing_files_count,
                            selected_manifest.added_files_count,
                        ) {
                            (Some(x), Some(y)) => Some(x + y),
                            (Some(x), None) => Some(x),
                            (None, Some(y)) => Some(y),
                            (None, None) => None,
                        }
                    })
                    .unwrap_or(0) as usize;

                let n_splits = compute_n_splits(
                    existing_file_count,
                    new_files.len(),
                    selected_manifest_file_count,
                );

                let bounds = selected_manifest_opt
                    .as_ref()
                    .and_then(|x| x.partitions.as_deref())
                    .map(summary_to_rectangle)
                    .transpose()?
                    .map(|mut x| {
                        x.expand(&bounding_partition_values);
                        x
                    })
                    .unwrap_or(bounding_partition_values);

                let new_manifest_entries = new_files
                    .into_iter()
                    .map(|data_file| {
                        ManifestEntry::builder()
                            .with_format_version(table_metadata.format_version)
                            .with_status(Status::Added)
                            .with_snapshot_id(manifest_list_writer.snapshot_id())
                            .with_data_file(data_file)
                            .build()
                            .map_err(crate::spec::error::Error::from)
                            .map_err(Error::from)
                    })
                    .collect::<Result<_, Error>>()?;

                // Write manifest files, splitting them if the limit is exceeded
                if n_splits == 0 {
                    if let Some(manifest) = selected_manifest_opt {
                        manifest_list_writer
                            .append_manifests_entries_to_existing_manifest(
                                manifest,
                                new_manifest_entries,
                            )
                            .await?;
                    } else {
                        manifest_list_writer
                            .append_manifest_entries_to_new_manifests(vec![new_manifest_entries])
                            .await?;
                    };
                } else {
                    let existing_manifest_entries = if let Some(manifest) = selected_manifest_opt {
                        let manifest_reader = ManifestReader::from_object_store(
                            object_store.clone(),
                            &manifest.manifest_path,
                        )
                        .await?;
                        manifest_reader
                            .map(|entry| {
                                let mut entry = entry?;
                                if *entry.status() == Status::Added {
                                    entry.mark_existing(table_metadata)?;
                                }
                                Ok(entry)
                            })
                            .collect::<Result<_, Error>>()?
                    } else {
                        vec![]
                    };
                    let splits = split_datafiles(
                        existing_manifest_entries
                            .into_iter()
                            .chain(new_manifest_entries)
                            .collect(),
                        bounds,
                        &partition_column_names,
                        n_splits,
                    )?;
                    manifest_list_writer
                        .append_manifest_entries_to_new_manifests(splits)
                        .await?;
                };

                let branch = branch.unwrap_or("main".to_owned());
                let snapshot = manifest_list_writer
                    .finish(SpecOperation::Append, additional_summary, schema)
                    .await?;
                let snapshot_id = *snapshot.snapshot_id();

                Ok((
                    old_snapshot.map(|x| TableRequirement::AssertRefSnapshotId {
                        r#ref: branch.clone(),
                        snapshot_id: *x.snapshot_id(),
                    }),
                    vec![
                        TableUpdate::AddSnapshot { snapshot },
                        TableUpdate::SetSnapshotRef {
                            ref_name: branch,
                            snapshot_reference: SnapshotReference {
                                snapshot_id,
                                retention: SnapshotRetention::default(),
                            },
                        },
                    ],
                ))
            }
            Operation::Rewrite {
                branch,
                files,
                additional_summary,
            } => {
                let partition_fields =
                    table_metadata.current_partition_fields(branch.as_deref())?;
                let old_snapshot = table_metadata.current_snapshot(branch.as_deref())?;
                let schema = table_metadata.current_schema(branch.as_deref())?.clone();

                let partition_column_names = partition_fields
                    .iter()
                    .map(|x| x.name())
                    .collect::<SmallVec<[_; 4]>>();

                let bounding_partition_values = files
                    .iter()
                    .try_fold(None, |acc, x| {
                        let node = partition_struct_to_vec(x.partition(), &partition_column_names)?;
                        let Some(mut acc) = acc else {
                            return Ok::<_, Error>(Some(Rectangle::new(node.clone(), node)));
                        };
                        acc.expand_with_node(node);
                        Ok(Some(acc))
                    })?
                    .ok_or(Error::NotFound(
                        "Bounding".to_owned(),
                        "rectangle".to_owned(),
                    ))?;

                let mut manifest_list_writer = ManifestListWriter::try_new(
                    object_store.clone(),
                    table_metadata,
                    branch.clone(),
                )?;

                let n_splits = compute_n_splits(0, files.len(), 0);

                let new_datafiles = files
                    .into_iter()
                    .map(|data_file| {
                        ManifestEntry::builder()
                            .with_format_version(table_metadata.format_version)
                            .with_status(Status::Added)
                            .with_snapshot_id(manifest_list_writer.snapshot_id())
                            .with_data_file(data_file)
                            .build()
                            .map_err(crate::spec::error::Error::from)
                            .map_err(Error::from)
                    })
                    .collect::<Result<_, Error>>()?;

                // Write manifest files, splitting tem if the limit is exceeded
                if n_splits == 0 {
                    manifest_list_writer
                        .append_manifest_entries_to_new_manifests(vec![new_datafiles])
                        .await?;
                } else {
                    let splits = split_datafiles(
                        new_datafiles,
                        bounding_partition_values,
                        &partition_column_names,
                        n_splits,
                    )?;

                    manifest_list_writer
                        .append_manifest_entries_to_new_manifests(splits)
                        .await?;
                };

                let new_snapshot_id = manifest_list_writer.snapshot_id();
                let snapshot = manifest_list_writer
                    .finish(SpecOperation::Overwrite, additional_summary, &schema)
                    .await?;

                let old_snapshot_ids: Vec<i64> =
                    table_metadata.snapshots.keys().map(Clone::clone).collect();

                Ok((
                    old_snapshot.map(|x| TableRequirement::AssertRefSnapshotId {
                        r#ref: branch.clone().unwrap_or("main".to_owned()),
                        snapshot_id: *x.snapshot_id(),
                    }),
                    vec![
                        TableUpdate::RemoveSnapshots {
                            snapshot_ids: old_snapshot_ids,
                        },
                        TableUpdate::AddSnapshot { snapshot },
                        TableUpdate::SetSnapshotRef {
                            ref_name: branch.unwrap_or("main".to_owned()),
                            snapshot_reference: SnapshotReference {
                                snapshot_id: new_snapshot_id,
                                retention: SnapshotRetention::default(),
                            },
                        },
                    ],
                ))
            }
            Operation::UpdateProperties(entries) => Ok((
                None,
                vec![TableUpdate::SetProperties {
                    updates: HashMap::from_iter(entries),
                }],
            )),
            Operation::SetSnapshotRef((key, value)) => Ok((
                table_metadata
                    .refs
                    .get(&key)
                    .map(|x| TableRequirement::AssertRefSnapshotId {
                        r#ref: key.clone(),
                        snapshot_id: x.snapshot_id,
                    }),
                vec![TableUpdate::SetSnapshotRef {
                    ref_name: key,
                    snapshot_reference: value,
                }],
            )),
            Operation::AddSchema(schema) => {
                let last_column_id = schema.fields().iter().map(|x| x.id).max();
                Ok((
                    None,
                    vec![TableUpdate::AddSchema {
                        schema,
                        last_column_id,
                    }],
                ))
            }
            Operation::SetDefaultSpec(spec_id) => {
                Ok((None, vec![TableUpdate::SetDefaultSpec { spec_id }]))
            }
        }
    }
}

/// To achieve fast lookups of the datafiles, the manifest tree should be somewhat balanced, meaning that manifest files should contain a similar number of datafiles.
/// This means that manifest files might need to be split up when they get too large. Since the number of datafiles being added by a append operation might be really large,
/// it might even be required to split the manifest file multiple times. *n_splits* stores how many times a manifest file needs to be split to give at most *limit* datafiles per manifest.
fn compute_n_splits(
    existing_file_count: usize,
    new_file_count: usize,
    selected_manifest_file_count: usize,
) -> u32 {
    // We want:
    //   nb manifests per manifest list ~= nb data files per manifest
    // Since:
    //   total number of data files = nb manifests per manifest list * nb data files per manifest
    // We shall have:
    //   limit = sqrt(total number of data files)
    let limit = MIN_DATAFILES_PER_MANIFEST
        + ((existing_file_count + new_file_count) as f64).sqrt() as usize;
    let new_manifest_file_count = selected_manifest_file_count + new_file_count;
    match new_manifest_file_count / limit {
        0 => 0,
        x => x.ilog2() + 1,
    }
}
