use std::{cmp::Ordering, io::Read};

use iceberg_rust_spec::{manifest::ManifestEntry, manifest_list::ManifestListEntry};

use crate::{
    error::Error,
    table::manifest_list::{ManifestListReader, ManifestListWriter},
    util::{cmp_with_priority, partition_struct_to_vec, summary_to_rectangle, try_sub, Rectangle},
};

/// Split sets of datafiles depending on their partition_values
#[allow(clippy::type_complexity)]
fn split_datafiles_once(
    files: Vec<ManifestEntry>,
    rect: Rectangle,
    names: &[&str],
) -> Result<[(Vec<ManifestEntry>, Rectangle); 2], Error> {
    let mut smaller = Vec::new();
    let mut larger = Vec::new();
    let mut smaller_rect = None;
    let mut larger_rect = None;

    for manifest_entry in files {
        let position = partition_struct_to_vec(manifest_entry.data_file().partition(), names)?;
        // Compare distance to upper and lower bound. Since you can't compute a "norm" for a multidimensional vector where the dimensions have different datatypes,
        // the dimensions are compared individually and the norm is computed by weighing the earlier columns more than the later.
        if let Ordering::Greater = cmp_with_priority(
            &try_sub(&position, &rect.min)?,
            &try_sub(&rect.max, &position)?,
        )? {
            // if closer to upper bound
            larger.push(manifest_entry);

            if larger_rect.is_none() {
                larger_rect = Some(Rectangle::new(position.clone(), position));
            } else if let Some(larger_rect) = larger_rect.as_mut() {
                larger_rect.expand_with_node(position);
            }
        } else {
            // if closer to lower bound
            smaller.push(manifest_entry);

            if smaller_rect.is_none() {
                smaller_rect = Some(Rectangle::new(position.clone(), position));
            } else if let Some(smaller_rect) = smaller_rect.as_mut() {
                smaller_rect.expand_with_node(position);
            }
        }
    }
    Ok([
        (
            smaller,
            smaller_rect.ok_or(Error::NotFound("Lower".to_owned(), "rectangle".to_owned()))?,
        ),
        (
            larger,
            larger_rect.ok_or(Error::NotFound("Upper".to_owned(), "rectangle".to_owned()))?,
        ),
    ])
}

/// Splits the datafiles *n_split* times to decrease the number of datafiles per maniefst. Returns *2^n_splits* lists of manifest entries.
pub(crate) fn split_datafiles(
    files: Vec<ManifestEntry>,
    rect: Rectangle,
    names: &[&str],
    n_split: u32,
) -> Result<Vec<Vec<ManifestEntry>>, Error> {
    let [(smaller, smaller_rect), (larger, larger_rect)] =
        split_datafiles_once(files, rect, names)?;
    if n_split == 1 {
        Ok(vec![smaller, larger])
    } else {
        let mut smaller = split_datafiles(smaller, smaller_rect, names, n_split - 1)?;
        let mut larger = split_datafiles(larger, larger_rect, names, n_split - 1)?;

        smaller.append(&mut larger);
        Ok(smaller)
    }
}

pub(crate) struct SelectedManifest {
    pub manifest: ManifestListEntry,
    pub file_count_all_entries: usize,
}

/// Select the manifest that yields the smallest bounding rectangle after the
/// bounding rectangle of the new values has been added.
pub(crate) fn select_manifest_partitioned(
    manifest_list_reader: ManifestListReader<impl Read>,
    manifest_list_writer: &mut ManifestListWriter,
    bounding_partition_values: &Rectangle,
) -> Result<SelectedManifest, Error> {
    let mut selected_state = None;
    let mut file_count_all_entries = 0;
    for manifest_res in manifest_list_reader {
        let manifest = manifest_res?;

        let mut bounds = summary_to_rectangle(
            manifest
                .partitions
                .as_ref()
                .ok_or(Error::NotFound("Partition".to_owned(), "struct".to_owned()))?,
        )?;

        bounds.expand(bounding_partition_values);

        file_count_all_entries += manifest.added_files_count.unwrap_or(0) as usize;

        let Some((selected_bounds, selected_manifest)) = &selected_state else {
            selected_state = Some((bounds, manifest));
            continue;
        };

        match selected_bounds.cmp_with_priority(&bounds)? {
            Ordering::Greater => {
                manifest_list_writer.append_ser(selected_manifest)?;
                selected_state = Some((bounds, manifest));
                continue;
            }
            _ => {
                manifest_list_writer.append_ser(&manifest)?;
                continue;
            }
        }
    }
    selected_state
        .map(|(_, entry)| SelectedManifest {
            manifest: entry,
            file_count_all_entries,
        })
        .ok_or(Error::NotFound("Manifest".to_owned(), "file".to_owned()))
}

/// Select the manifest with the smallest number of rows.
pub(crate) fn select_manifest_unpartitioned(
    manifest_list_reader: ManifestListReader<impl Read>,
    manifest_list_writer: &mut ManifestListWriter,
) -> Result<SelectedManifest, Error> {
    let mut selected_state = None;
    let mut file_count_all_entries = 0;
    for manifest_res in manifest_list_reader {
        let manifest = manifest_res?;
        // TODO: should this also account for existing_rows_count / existing_files_count?
        let row_count = manifest.added_rows_count;
        file_count_all_entries += manifest.added_files_count.unwrap_or(0) as usize;

        let Some((selected_row_count, selected_manifest)) = &selected_state else {
            selected_state = Some((row_count, manifest));
            continue;
        };

        // If the file doesn't have any rows, we select it
        let Some(row_count) = row_count else {
            selected_state = Some((row_count, manifest));
            continue;
        };

        if selected_row_count.is_some_and(|x| x > row_count) {
            manifest_list_writer.append_ser(selected_manifest)?;
            selected_state = Some((Some(row_count), manifest));
            continue;
        } else {
            manifest_list_writer.append_ser(&manifest)?;
            continue;
        }
    }
    selected_state
        .map(|(_, entry)| SelectedManifest {
            manifest: entry,
            file_count_all_entries,
        })
        .ok_or(Error::NotFound("Manifest".to_owned(), "file".to_owned()))
}
