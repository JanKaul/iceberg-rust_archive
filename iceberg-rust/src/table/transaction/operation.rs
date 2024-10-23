/*!
 * Defines the different [Operation]s on a [Table].
*/

use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use apache_avro::from_value;
use futures::{lock::Mutex, stream, StreamExt, TryStreamExt};
use iceberg_rust_spec::util::strip_prefix;
use iceberg_rust_spec::{error::Error as SpecError, spec::table_metadata::TableMetadata};
use iceberg_rust_spec::{
    manifest::ManifestReader,
    spec::{
        manifest::{partition_value_schema, DataFile, ManifestEntry, ManifestWriter, Status},
        manifest_list::{Content, FieldSummary, ManifestListEntry, ManifestListEntryEnum},
        partition::PartitionField,
        schema::Schema,
        snapshot::{
            generate_snapshot_id, SnapshotBuilder, SnapshotReference, SnapshotRetention, Summary,
        },
        values::{Struct, Value},
    },
};
use object_store::ObjectStore;
use smallvec::SmallVec;

use crate::{
    catalog::commit::{TableRequirement, TableUpdate},
    error::Error,
    util::{struct_to_smallvec, summary_to_rectangle, Rectangle},
};

use super::append::split_datafiles;

static MIN_DATAFILES: usize = 4;

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
    // /// Replace files in the table and commit
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
                files,
                additional_summary,
            } => {
                let partition_spec = table_metadata.default_partition_spec()?;
                let schema = table_metadata.current_schema(branch.as_deref())?;
                let old_snapshot = table_metadata.current_snapshot(branch.as_deref())?;

                let partition_column_names = table_metadata
                    .default_partition_spec()?
                    .fields()
                    .iter()
                    .map(|x| x.name().as_str())
                    .collect::<SmallVec<[_; 4]>>();

                let bounding_partition_values = files
                    .iter()
                    .fold(Ok::<_, Error>(None), |acc, x| {
                        let acc = acc?;
                        let node = struct_to_smallvec(x.partition(), &partition_column_names)?;
                        let Some(mut acc) = acc else {
                            return Ok(Some(Rectangle::new(node.clone(), node)));
                        };
                        acc.expand_with_node(node);
                        Ok(Some(acc))
                    })?
                    .ok_or(Error::NotFound(
                        "Bounding".to_owned(),
                        "rectangle".to_owned(),
                    ))?;

                let manifest_list_schema =
                    ManifestListEntry::schema(&table_metadata.format_version)?;

                let mut manifest_list_writer =
                    apache_avro::Writer::new(&manifest_list_schema, Vec::new());

                let old_manifest_list_location = old_snapshot.map(|x| x.manifest_list()).cloned();

                let mut file_count = 0;

                // Find a manifest to add the new datafiles
                let manifest = if let Some(old_manifest_list_location) = &old_manifest_list_location
                {
                    let old_manifest_list_bytes = object_store
                        .get(&strip_prefix(&old_manifest_list_location).as_str().into())
                        .await?
                        .bytes()
                        .await?;

                    let manifest_list_reader =
                        apache_avro::Reader::new(old_manifest_list_bytes.as_ref())?;

                    // Check if table is partitioned
                    let manifest = if partition_column_names.is_empty() {
                        // Find the manifest with the lowest row count
                        manifest_list_reader
                            .fold(Ok::<_, Error>(None), |acc, x| {
                                let acc = acc?;

                                let manifest = x
                                    .map_err(Into::into)
                                    .and_then(|value| {
                                        ManifestListEntry::try_from_enum(
                                            from_value::<ManifestListEntryEnum>(&value)?,
                                            table_metadata,
                                        )
                                    })
                                    .unwrap();

                                let row_count = manifest.added_rows_count;

                                file_count += manifest.added_files_count.unwrap_or(0) as usize;

                                let Some((old_row_count, old_manifest)) = acc else {
                                    return Ok(Some((row_count, manifest)));
                                };

                                let Some(row_count) = row_count else {
                                    return Ok(Some((old_row_count, old_manifest)));
                                };

                                if old_row_count.is_none()
                                    || old_row_count.is_some_and(|x| x > row_count)
                                {
                                    manifest_list_writer.append_ser(old_manifest)?;
                                    Ok(Some((Some(row_count), manifest)))
                                } else {
                                    manifest_list_writer.append_ser(manifest)?;
                                    Ok(Some((old_row_count, old_manifest)))
                                }
                            })?
                            .ok_or(Error::NotFound("Manifest".to_owned(), "file".to_owned()))?
                            .1
                    } else {
                        // Find the manifest with the smallest bounding partition values
                        manifest_list_reader
                            .fold(Ok::<_, Error>(None), |acc, x| {
                                let acc = acc?;

                                let manifest = x
                                    .map_err(Into::into)
                                    .and_then(|value| {
                                        ManifestListEntry::try_from_enum(
                                            from_value::<ManifestListEntryEnum>(&value)?,
                                            table_metadata,
                                        )
                                    })
                                    .unwrap();

                                let mut bounds = summary_to_rectangle(
                                    manifest.partitions.as_ref().ok_or(Error::NotFound(
                                        "Partition".to_owned(),
                                        "struct".to_owned(),
                                    ))?,
                                )?;

                                bounds.expand(&bounding_partition_values);

                                file_count += manifest.added_files_count.unwrap_or(0) as usize;

                                let Some((old_bounds, old_manifest)) = acc else {
                                    return Ok(Some((bounds, manifest)));
                                };

                                match old_bounds.cmp_with_priority(&bounds)? {
                                    Ordering::Greater => {
                                        manifest_list_writer.append_ser(old_manifest)?;
                                        Ok(Some((bounds, manifest)))
                                    }
                                    _ => {
                                        manifest_list_writer.append_ser(manifest)?;
                                        Ok(Some((old_bounds, old_manifest)))
                                    }
                                }
                            })?
                            .ok_or(Error::NotFound("Manifest".to_owned(), "file".to_owned()))?
                            .1
                    };
                    Some(manifest)
                } else {
                    // If manifest list doesn't exist, there is no manifest
                    None
                };

                let limit = MIN_DATAFILES + ((file_count + files.len()) as f64).sqrt() as usize;

                let new_file_count = manifest
                    .as_ref()
                    .and_then(|x| x.added_files_count)
                    .unwrap_or(0) as usize
                    + files.len();

                // How many times do the files need to be split to give at most *limit* files per manifest
                let n_splits = match new_file_count / limit {
                    0 => 0,
                    x => x.ilog2() + 1,
                };

                let bounds = manifest
                    .as_ref()
                    .and_then(|x| x.partitions.as_deref())
                    .map(summary_to_rectangle)
                    .transpose()?
                    .map(|mut x| {
                        x.expand(&bounding_partition_values);
                        x
                    })
                    .unwrap_or(bounding_partition_values);

                let snapshot_id = generate_snapshot_id();
                let sequence_number = table_metadata.last_sequence_number + 1;

                let new_datafile_iter = files.into_iter().map(|data_file| {
                    ManifestEntry::builder()
                        .with_format_version(table_metadata.format_version)
                        .with_status(Status::Added)
                        .with_snapshot_id(snapshot_id)
                        .with_sequence_number(sequence_number)
                        .with_data_file(data_file)
                        .build()
                        .map_err(crate::spec::error::Error::from)
                        .map_err(Error::from)
                });

                let manifest_schema = ManifestEntry::schema(
                    &partition_value_schema(partition_spec.fields(), schema)?,
                    &table_metadata.format_version,
                )?;

                let snapshot_uuid = &uuid::Uuid::new_v4().to_string();
                let new_manifest_list_location = table_metadata.location.to_string()
                    + "/metadata/snap-"
                    + &snapshot_id.to_string()
                    + snapshot_uuid
                    + ".avro";

                // Write manifest files
                // Split manifest file if limit is exceeded
                if n_splits == 0 {
                    // If manifest doesn't need to be split
                    let mut manifest_writer = ManifestWriter::new(
                        Vec::new(),
                        &manifest_schema,
                        table_metadata,
                        branch.as_deref(),
                    )?;

                    // Copy potential existing entries
                    if let Some(manifest) = &manifest {
                        let manifest_bytes: Vec<u8> = object_store
                            .get(&strip_prefix(&manifest.manifest_path).as_str().into())
                            .await?
                            .bytes()
                            .await?
                            .into();

                        let manifest_reader = apache_avro::Reader::new(&*manifest_bytes)?;
                        manifest_writer.extend(manifest_reader.filter_map(Result::ok))?;
                    };

                    // If there is no manifest, create one
                    let mut manifest = manifest.unwrap_or_else(|| {
                        let manifest_location = table_metadata.location.to_string()
                            + "/metadata/"
                            + snapshot_uuid
                            + "-m"
                            + &0.to_string()
                            + ".avro";
                        let manifest = ManifestListEntry {
                            format_version: table_metadata.format_version.clone(),
                            manifest_path: manifest_location,
                            manifest_length: 0,
                            partition_spec_id: table_metadata.default_spec_id,
                            content: Content::Data,
                            sequence_number: table_metadata.last_sequence_number,
                            min_sequence_number: 0,
                            added_snapshot_id: snapshot_id,
                            added_files_count: Some(0),
                            existing_files_count: Some(0),
                            deleted_files_count: Some(0),
                            added_rows_count: Some(0),
                            existing_rows_count: Some(0),
                            deleted_rows_count: Some(0),
                            partitions: None,
                            key_metadata: None,
                        };
                        manifest
                    });

                    for manifest_entry in new_datafile_iter {
                        {
                            let manifest_entry = manifest_entry?;

                            let mut added_rows_count = 0;

                            if manifest.partitions.is_none() {
                                manifest.partitions = Some(
                                    table_metadata
                                        .default_partition_spec()?
                                        .fields()
                                        .iter()
                                        .map(|_| FieldSummary {
                                            contains_null: false,
                                            contains_nan: None,
                                            lower_bound: None,
                                            upper_bound: None,
                                        })
                                        .collect::<Vec<FieldSummary>>(),
                                );
                            }

                            added_rows_count += manifest_entry.data_file().record_count();
                            update_partitions(
                                manifest.partitions.as_mut().unwrap(),
                                manifest_entry.data_file().partition(),
                                table_metadata.default_partition_spec()?.fields(),
                            )?;

                            manifest_writer.append_ser(manifest_entry)?;

                            manifest.added_files_count = match manifest.added_files_count {
                                Some(count) => Some(count + new_file_count as i32),
                                None => Some(new_file_count as i32),
                            };
                            manifest.added_rows_count = match manifest.added_rows_count {
                                Some(count) => Some(count + added_rows_count),
                                None => Some(added_rows_count),
                            };
                        }
                    }

                    let manifest_bytes = manifest_writer.into_inner()?;

                    let manifest_length: i64 = manifest_bytes.len() as i64;

                    manifest.manifest_length += manifest_length;

                    object_store
                        .put(
                            &strip_prefix(&manifest.manifest_path).as_str().into(),
                            manifest_bytes.into(),
                        )
                        .await?;

                    manifest_list_writer.append_ser(manifest)?;
                } else {
                    // Split datafiles
                    let splits = if let Some(manifest) = manifest {
                        let manifest_bytes: Vec<u8> = object_store
                            .get(&strip_prefix(&manifest.manifest_path).as_str().into())
                            .await?
                            .bytes()
                            .await?
                            .into();

                        let manifest_reader =
                            ManifestReader::new(&*manifest_bytes)?.map(|x| x.map_err(Error::from));

                        split_datafiles(
                            new_datafile_iter.chain(manifest_reader),
                            bounds,
                            &partition_column_names,
                            n_splits,
                        )?
                    } else {
                        split_datafiles(
                            new_datafile_iter,
                            bounds,
                            &partition_column_names,
                            n_splits,
                        )?
                    };

                    for (i, entries) in splits.into_iter().enumerate() {
                        let mut manifest_writer = ManifestWriter::new(
                            Vec::new(),
                            &manifest_schema,
                            table_metadata,
                            branch.as_deref(),
                        )?;

                        let manifest_location = table_metadata.location.to_string()
                            + "/metadata/"
                            + snapshot_uuid
                            + "-m"
                            + &i.to_string()
                            + ".avro";
                        let mut manifest = ManifestListEntry {
                            format_version: table_metadata.format_version.clone(),
                            manifest_path: manifest_location,
                            manifest_length: 0,
                            partition_spec_id: table_metadata.default_spec_id,
                            content: Content::Data,
                            sequence_number: table_metadata.last_sequence_number,
                            min_sequence_number: 0,
                            added_snapshot_id: snapshot_id,
                            added_files_count: Some(0),
                            existing_files_count: Some(0),
                            deleted_files_count: Some(0),
                            added_rows_count: Some(0),
                            existing_rows_count: Some(0),
                            deleted_rows_count: Some(0),
                            partitions: None,
                            key_metadata: None,
                        };

                        for manifest_entry in entries {
                            {
                                let mut added_rows_count = 0;

                                if manifest.partitions.is_none() {
                                    manifest.partitions = Some(
                                        table_metadata
                                            .default_partition_spec()?
                                            .fields()
                                            .iter()
                                            .map(|_| FieldSummary {
                                                contains_null: false,
                                                contains_nan: None,
                                                lower_bound: None,
                                                upper_bound: None,
                                            })
                                            .collect::<Vec<FieldSummary>>(),
                                    );
                                }

                                added_rows_count += manifest_entry.data_file().record_count();
                                update_partitions(
                                    manifest.partitions.as_mut().unwrap(),
                                    manifest_entry.data_file().partition(),
                                    table_metadata.default_partition_spec()?.fields(),
                                )?;

                                manifest_writer.append_ser(manifest_entry)?;

                                manifest.added_files_count = match manifest.added_files_count {
                                    Some(count) => Some(count + new_file_count as i32),
                                    None => Some(new_file_count as i32),
                                };
                                manifest.added_rows_count = match manifest.added_rows_count {
                                    Some(count) => Some(count + added_rows_count),
                                    None => Some(added_rows_count),
                                };
                            }
                        }

                        let manifest_bytes = manifest_writer.into_inner()?;

                        let manifest_length: i64 = manifest_bytes.len() as i64;

                        manifest.manifest_length += manifest_length;

                        object_store
                            .put(
                                &strip_prefix(&manifest.manifest_path).as_str().into(),
                                manifest_bytes.into(),
                            )
                            .await?;

                        manifest_list_writer.append_ser(manifest)?;
                    }
                };

                let manifest_list_bytes = manifest_list_writer.into_inner()?;

                object_store
                    .put(
                        &strip_prefix(&new_manifest_list_location).into(),
                        manifest_list_bytes.into(),
                    )
                    .await?;

                let mut snapshot_builder = SnapshotBuilder::default();
                snapshot_builder
                    .with_snapshot_id(snapshot_id)
                    .with_manifest_list(new_manifest_list_location)
                    .with_sequence_number(
                        old_snapshot
                            .map(|x| *x.sequence_number() + 1)
                            .unwrap_or_default(),
                    )
                    .with_summary(Summary {
                        operation: iceberg_rust_spec::spec::snapshot::Operation::Append,
                        other: additional_summary.unwrap_or_default(),
                    })
                    .with_schema_id(*schema.schema_id());
                let snapshot = snapshot_builder
                    .build()
                    .map_err(iceberg_rust_spec::error::Error::from)?;

                Ok((
                    old_snapshot.map(|x| TableRequirement::AssertRefSnapshotId {
                        r#ref: branch.clone().unwrap_or("main".to_owned()),
                        snapshot_id: *x.snapshot_id(),
                    }),
                    vec![
                        TableUpdate::AddSnapshot { snapshot },
                        TableUpdate::SetSnapshotRef {
                            ref_name: branch.unwrap_or("main".to_owned()),
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
                let old_snapshot = table_metadata.current_snapshot(branch.as_deref())?;
                let schema = table_metadata.current_schema(branch.as_deref())?.clone();

                // Split datafils by partition
                let datafiles = Arc::new(files.into_iter().map(Ok::<_, Error>).try_fold(
                    HashMap::<Struct, Vec<DataFile>>::new(),
                    |mut acc, x| {
                        let x = x?;
                        let partition_value = x.partition().clone();
                        acc.entry(partition_value).or_default().push(x);
                        Ok::<_, Error>(acc)
                    },
                )?);

                let snapshot_id = generate_snapshot_id();
                let snapshot_uuid = &uuid::Uuid::new_v4().to_string();
                let manifest_list_location = table_metadata.location.to_string()
                    + "/metadata/snap-"
                    + &snapshot_id.to_string()
                    + "-"
                    + snapshot_uuid
                    + ".avro";

                let manifest_iter = datafiles.keys().enumerate().map(|(i, partition_value)| {
                    let manifest_location = table_metadata.location.to_string()
                        + "/metadata/"
                        + snapshot_uuid
                        + "-m"
                        + &(i).to_string()
                        + ".avro";
                    let manifest = ManifestListEntry {
                        format_version: table_metadata.format_version.clone(),
                        manifest_path: manifest_location,
                        manifest_length: 0,
                        partition_spec_id: table_metadata.default_spec_id,
                        content: Content::Data,
                        sequence_number: table_metadata.last_sequence_number,
                        min_sequence_number: 0,
                        added_snapshot_id: snapshot_id,
                        added_files_count: Some(0),
                        existing_files_count: Some(0),
                        deleted_files_count: Some(0),
                        added_rows_count: Some(0),
                        existing_rows_count: Some(0),
                        deleted_rows_count: Some(0),
                        partitions: None,
                        key_metadata: None,
                    };
                    (ManifestStatus::New(manifest), vec![partition_value.clone()])
                });

                let manifest_list_schema =
                    ManifestListEntry::schema(&table_metadata.format_version)?;

                let manifest_list_writer = Arc::new(Mutex::new(apache_avro::Writer::new(
                    &manifest_list_schema,
                    Vec::new(),
                )));

                stream::iter(manifest_iter)
                    .then(|(manifest, files): (ManifestStatus, Vec<Struct>)| {
                        let object_store = object_store.clone();
                        let datafiles = datafiles.clone();
                        let branch = branch.clone();
                        let schema = &schema;
                        let old_storage_table_metadata = &table_metadata;
                        async move {
                            write_manifest(
                                old_storage_table_metadata,
                                manifest,
                                files,
                                datafiles,
                                schema,
                                object_store,
                                branch,
                            )
                            .await
                        }
                    })
                    .try_for_each_concurrent(None, |manifest| {
                        let manifest_list_writer = manifest_list_writer.clone();
                        async move {
                            manifest_list_writer.lock().await.append_ser(manifest)?;
                            Ok(())
                        }
                    })
                    .await?;

                let manifest_list_bytes = Arc::into_inner(manifest_list_writer)
                    .unwrap()
                    .into_inner()
                    .into_inner()?;

                object_store
                    .put(
                        &strip_prefix(&manifest_list_location).into(),
                        manifest_list_bytes.into(),
                    )
                    .await?;

                let mut snapshot_builder = SnapshotBuilder::default();
                snapshot_builder
                    .with_snapshot_id(snapshot_id)
                    .with_sequence_number(0)
                    .with_schema_id(*schema.schema_id())
                    .with_manifest_list(manifest_list_location)
                    .with_summary(Summary {
                        operation: iceberg_rust_spec::spec::snapshot::Operation::Append,
                        other: additional_summary.unwrap_or_default(),
                    });
                let snapshot = snapshot_builder
                    .build()
                    .map_err(iceberg_rust_spec::error::Error::from)?;

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
                                snapshot_id,
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

pub enum ManifestStatus {
    New(ManifestListEntry),
}

pub(crate) async fn write_manifest(
    table_metadata: &TableMetadata,
    manifest: ManifestStatus,
    files: Vec<Struct>,
    datafiles: Arc<HashMap<Struct, Vec<DataFile>>>,
    schema: &Schema,
    object_store: Arc<dyn ObjectStore>,
    branch: Option<String>,
) -> Result<ManifestListEntry, Error> {
    let partition_spec = table_metadata.default_partition_spec()?;
    let manifest_schema = ManifestEntry::schema(
        &partition_value_schema(partition_spec.fields(), schema)?,
        &table_metadata.format_version,
    )?;

    let mut manifest_writer = ManifestWriter::new(
        Vec::new(),
        &manifest_schema,
        table_metadata,
        branch.as_deref(),
    )?;

    let mut manifest = match manifest {
        ManifestStatus::New(manifest) => manifest,
    };
    let files_count = manifest.added_files_count.unwrap_or_default() + files.len() as i32;
    for path in files {
        for datafile in datafiles.get(&path).ok_or(Error::InvalidFormat(
            "Datafiles for partition value".to_string(),
        ))? {
            let mut added_rows_count = 0;

            if manifest.partitions.is_none() {
                manifest.partitions = Some(
                    partition_spec
                        .fields()
                        .iter()
                        .map(|_| FieldSummary {
                            contains_null: false,
                            contains_nan: None,
                            lower_bound: None,
                            upper_bound: None,
                        })
                        .collect::<Vec<FieldSummary>>(),
                );
            }

            added_rows_count += datafile.record_count();
            update_partitions(
                manifest.partitions.as_mut().unwrap(),
                datafile.partition(),
                partition_spec.fields(),
            )?;

            let manifest_entry = ManifestEntry::builder()
                .with_format_version(table_metadata.format_version.clone())
                .with_status(Status::Added)
                .with_snapshot_id(
                    table_metadata
                        .current_snapshot_id
                        .ok_or(Error::NotFound("Snapshot".to_owned(), "id".to_owned()))?,
                )
                .with_sequence_number(
                    table_metadata
                        .current_snapshot(branch.as_deref())?
                        .map(|x| *x.sequence_number())
                        .ok_or(Error::NotFound("Sequence".to_owned(), "number".to_owned()))?,
                )
                .with_data_file(datafile.clone())
                .build()
                .map_err(SpecError::from)?;

            manifest_writer.append_ser(manifest_entry)?;

            manifest.added_files_count = match manifest.added_files_count {
                Some(count) => Some(count + files_count),
                None => Some(files_count),
            };
            manifest.added_rows_count = match manifest.added_rows_count {
                Some(count) => Some(count + added_rows_count),
                None => Some(added_rows_count),
            };
        }
    }

    let manifest_bytes = manifest_writer.into_inner()?;

    let manifest_length: i64 = manifest_bytes.len() as i64;

    manifest.manifest_length += manifest_length;

    object_store
        .put(
            &strip_prefix(&manifest.manifest_path).as_str().into(),
            manifest_bytes.into(),
        )
        .await?;

    Ok::<_, Error>(manifest)
}

fn update_partitions(
    partitions: &mut [FieldSummary],
    partition_values: &Struct,
    partition_columns: &[PartitionField],
) -> Result<(), Error> {
    for (field, summary) in partition_columns.iter().zip(partitions.iter_mut()) {
        let value = &partition_values.get(field.name()).and_then(|x| x.as_ref());
        if let Some(value) = value {
            if let Some(lower_bound) = &mut summary.lower_bound {
                match (value, lower_bound) {
                    (Value::Int(val), Value::Int(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::LongInt(val), Value::LongInt(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Float(val), Value::Float(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Double(val), Value::Double(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Date(val), Value::Date(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Time(val), Value::Time(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Timestamp(val), Value::Timestamp(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::TimestampTZ(val), Value::TimestampTZ(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    _ => {}
                }
            }
            if let Some(upper_bound) = &mut summary.upper_bound {
                match (value, upper_bound) {
                    (Value::Int(val), Value::Int(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::LongInt(val), Value::LongInt(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Float(val), Value::Float(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Double(val), Value::Double(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Date(val), Value::Date(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Time(val), Value::Time(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Timestamp(val), Value::Timestamp(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::TimestampTZ(val), Value::TimestampTZ(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}
