/*!
 * Helpers to deal with manifest lists and files
*/

use apache_avro::{types::Value as AvroValue, Reader as AvroReader, Schema as AvroSchema};
use iceberg_rust_spec::{
    manifest::{partition_value_schema, ManifestEntry},
    manifest_list::{
        avro_value_to_manifest_list_entry, manifest_list_schema_v1, manifest_list_schema_v2,
        ManifestListEntry,
    },
    schema::Schema,
    snapshot::{generate_snapshot_id, Operation, Snapshot, SnapshotBuilder, Summary},
    table_metadata::{FormatVersion, TableMetadata},
    util::strip_prefix,
};
use object_store::ObjectStore;
use std::{
    collections::HashMap,
    io::{Cursor, Read},
    iter::{repeat, Map, Repeat, Zip},
    sync::Arc,
};

use crate::error::Error;

use super::manifest::ManifestWriter;

type ReaderZip<'a, 'metadata, R> = Zip<AvroReader<'a, R>, Repeat<&'metadata TableMetadata>>;
type ReaderMap<'a, 'metadata, R> = Map<
    ReaderZip<'a, 'metadata, R>,
    fn((Result<AvroValue, apache_avro::Error>, &TableMetadata)) -> Result<ManifestListEntry, Error>,
>;

/// Iterator of Manifests
pub struct ManifestListReader<'metadata, R: Read> {
    reader: ReaderMap<'static, 'metadata, R>,
}

impl<'metadata, R: Read> Iterator for ManifestListReader<'metadata, R> {
    type Item = Result<ManifestListEntry, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next()
    }
}

impl<'metadata, R: Read> ManifestListReader<'metadata, R> {
    /// Create a new ManifestFile reader
    pub fn new(reader: R, table_metadata: &'metadata TableMetadata) -> Result<Self, Error> {
        let schema: &AvroSchema = match table_metadata.format_version {
            FormatVersion::V1 => manifest_list_schema_v1(),
            FormatVersion::V2 => manifest_list_schema_v2(),
        };
        Ok(Self {
            reader: AvroReader::with_schema(schema, reader)?
                .zip(repeat(table_metadata))
                .map(|(avro_value_res, meta)| {
                    avro_value_to_manifest_list_entry(avro_value_res, meta).map_err(Error::from)
                }),
        })
    }
}

impl<'metadata> ManifestListReader<'metadata, Cursor<Vec<u8>>> {
    pub(crate) async fn from_snapshot(
        snapshot: &Snapshot,
        table_metadata: &'metadata TableMetadata,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, Error> {
        let bytes: Cursor<Vec<u8>> = Cursor::new(
            object_store
                .get(&strip_prefix(snapshot.manifest_list()).into())
                .await?
                .bytes()
                .await?
                .into(),
        );
        ManifestListReader::new(bytes, table_metadata).map_err(Into::into)
    }
}

/// A helper to write entries into manifest lists and files
pub struct ManifestListWriter<'metadata> {
    table_metadata: &'metadata TableMetadata,
    manifest_list_writer: apache_avro::Writer<'static, Vec<u8>>,
    snapshot_id: i64,
    object_store: Arc<dyn ObjectStore>,
    snapshot_uuid: String,
    manifest_index: usize,
    branch: Option<String>,
    avro_schema: AvroSchema,
}

impl<'schema, 'metadata> ManifestListWriter<'metadata> {
    /// New manifest list writer
    pub fn try_new(
        object_store: Arc<dyn ObjectStore>,
        table_metadata: &'metadata TableMetadata,
        branch: Option<String>,
    ) -> Result<Self, Error> {
        let manifest_list_schema = match table_metadata.format_version {
            FormatVersion::V1 => manifest_list_schema_v1(),
            FormatVersion::V2 => manifest_list_schema_v2(),
        };
        let snapshot_id = generate_snapshot_id();
        let snapshot_uuid = uuid::Uuid::new_v4().to_string();
        let manifest_list_writer = apache_avro::Writer::new(manifest_list_schema, Vec::new());

        let partition_fields = table_metadata.current_partition_fields(branch.as_deref())?;
        let avro_schema = ManifestEntry::schema(
            &partition_value_schema(&partition_fields)?,
            &table_metadata.format_version,
        )?;

        Ok(Self {
            table_metadata,
            manifest_list_writer,
            snapshot_id,
            snapshot_uuid,
            object_store,
            manifest_index: 0,
            branch,
            avro_schema,
        })
    }

    /// Append a serializable ManifestListEntry
    pub fn append_ser(&mut self, entry: &ManifestListEntry) -> Result<usize, Error> {
        self.manifest_list_writer
            .append_ser(entry)
            .map_err(Error::from)
    }

    /// Create new manifests for each group of provided manifest entries and append them to the manifest list
    pub async fn append_manifest_entries_to_new_manifests(
        &mut self,
        grouped_manifest_entries: Vec<Vec<ManifestEntry>>,
    ) -> Result<(), Error> {
        for entry_group in grouped_manifest_entries {
            let manifest_location = format!(
                "{}/metadata/{}-m{}.avro",
                self.table_metadata.location, self.snapshot_uuid, self.manifest_index
            );

            let mut manifest_writer = ManifestWriter::new(
                &manifest_location,
                self.snapshot_id,
                &self.avro_schema,
                self.table_metadata,
                self.branch.as_deref(),
            )?;
            for manifest_entry in entry_group {
                manifest_writer.append(manifest_entry)?;
            }

            let manifest = manifest_writer.finish(self.object_store.clone()).await?;

            self.append_ser(&manifest)?;
        }
        Ok(())
    }

    /// Appends entries to an existing manifest and adds the resulting manifest to this manifest list
    pub async fn append_manifests_entries_to_existing_manifest(
        &mut self,
        existing_list_entry: ManifestListEntry,
        new_manifest_entries: Vec<ManifestEntry>,
    ) -> Result<(), Error> {
        let manifest_bytes: Vec<u8> = self
            .object_store
            .get(
                &strip_prefix(&existing_list_entry.manifest_path)
                    .as_str()
                    .into(),
            )
            .await?
            .bytes()
            .await?
            .into();

        let mut manifest_writer = ManifestWriter::from_existing(
            &manifest_bytes,
            existing_list_entry,
            &self.avro_schema,
            self.table_metadata,
            self.branch.as_deref(),
        )?;
        for manifest_entry in new_manifest_entries {
            manifest_writer.append(manifest_entry)?;
        }

        let manifest = manifest_writer.finish(self.object_store.clone()).await?;

        self.append_ser(&manifest)?;
        Ok(())
    }

    /// Finish writing the manifest list and push it to object storage
    pub async fn finish(
        self,
        operation: Operation,
        additional_summary: Option<HashMap<String, String>>,
        schema: &Schema,
    ) -> Result<Snapshot, Error> {
        let new_manifest_list_location = format!(
            "{}/metadata/snap-{}-{}.avro",
            self.table_metadata.location, self.snapshot_id, self.snapshot_uuid
        );
        self.object_store
            .put(
                &strip_prefix(&new_manifest_list_location).into(),
                self.manifest_list_writer.into_inner()?.into(),
            )
            .await?;
        let mut snapshot_builder = SnapshotBuilder::default();
        snapshot_builder
            .with_snapshot_id(self.snapshot_id)
            .with_manifest_list(new_manifest_list_location)
            .with_sequence_number(self.table_metadata.last_sequence_number)
            .with_summary(Summary {
                operation,
                other: additional_summary.unwrap_or_default(),
            })
            .with_schema_id(*schema.schema_id());
        let snapshot = snapshot_builder
            .build()
            .map_err(iceberg_rust_spec::error::Error::from)?;
        Ok(snapshot)
    }

    /// Snapshot id of the currently written snapshot
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }
}
