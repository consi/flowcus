//! Buffered storage writer: accumulates columnar data and flushes to parts.
//!
//! Maintains one write buffer per unique schema (template). Flushes when
//! the buffer exceeds a configurable size threshold, time timeout, or
//! when the hour partition boundary is crossed.
//! No fsync — all I/O is buffered through BufWriter and kernel page cache.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use tracing::{debug, info, warn};

use crate::schema::system_columns;
use flowcus_ipfix::protocol::{DataRecord, IpfixMessage, SetContents};

use crate::codec;
use crate::column::ColumnBuffer;
use crate::part;
use crate::schema::Schema;
use crate::table::Table;

/// Configuration for the storage writer.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Flush when buffer exceeds this many bytes (across all columns).
    pub flush_bytes: usize,
    /// Flush after this many seconds even if size threshold not reached.
    pub flush_interval_secs: u64,
    /// Pre-allocated row capacity for column buffers.
    pub initial_row_capacity: usize,
    /// Maximum part duration in seconds. Parts crossing this boundary are split.
    /// Default: 3600 (1 hour).
    pub partition_duration_secs: u32,
    /// Ingestion channel capacity.
    pub channel_capacity: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            flush_bytes: 1024 * 1024, // 1 MB
            flush_interval_secs: 1,
            initial_row_capacity: 8192,
            partition_duration_secs: 3600, // 1 hour
            channel_capacity: 4096,
        }
    }
}

/// A write buffer for a specific schema (template).
struct SchemaBuffer {
    schema: Schema,
    columns: Vec<ColumnBuffer>,
    row_count: usize,
    first_export_time: u32,
    last_export_time: u32,
    /// Hour partition this buffer belongs to (export_time / partition_duration).
    partition_hour: u32,
    exporter: std::net::SocketAddr,
    observation_domain_id: u32,
    created_at: Instant,
}

impl SchemaBuffer {
    fn new(schema: Schema, capacity: usize) -> Self {
        let columns = schema
            .columns
            .iter()
            .map(|def| ColumnBuffer::with_capacity(def.storage_type, capacity))
            .collect();

        Self {
            schema,
            columns,
            row_count: 0,
            first_export_time: u32::MAX,
            last_export_time: 0,
            partition_hour: 0,
            exporter: ([0, 0, 0, 0], 0).into(),
            observation_domain_id: 0,
            created_at: Instant::now(),
        }
    }

    fn append_record(
        &mut self,
        record: &DataRecord,
        exporter_ipv4: u32,
        exporter_port: u16,
        export_time: u32,
        observation_domain_id: u32,
    ) {
        let num_sys = system_columns().len();
        // Push system column values into the first 4 column buffers.
        self.columns[0].push_u32(exporter_ipv4);
        self.columns[1].push_u16(exporter_port);
        self.columns[2].push_u32(export_time);
        self.columns[3].push_u32(observation_domain_id);
        // Push template field values into the remaining column buffers.
        for (col_buf, field) in self.columns[num_sys..].iter_mut().zip(record.fields.iter()) {
            col_buf.push(&field.value);
        }
        self.row_count += 1;
    }

    fn mem_size(&self) -> usize {
        self.columns.iter().map(ColumnBuffer::mem_size).sum()
    }

    fn elapsed_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }

    fn clear(&mut self) {
        for col in &mut self.columns {
            col.clear();
        }
        self.row_count = 0;
        self.first_export_time = u32::MAX;
        self.last_export_time = 0;
        self.partition_hour = 0;
        self.created_at = Instant::now();
    }
}

/// Storage writer that ingests IPFIX messages and flushes parts to disk.
pub struct StorageWriter {
    config: WriterConfig,
    table: Table,
    /// Buffers keyed by (schema_fingerprint, partition_hour).
    buffers: HashMap<(u64, u32), SchemaBuffer>,
    /// Sequence counter for part IDs.
    seq: AtomicU32,
    /// Shared pending-hours tracker. Marks hours dirty when parts are written.
    pending: Option<crate::pending::PendingHours>,
}

impl StorageWriter {
    /// Create a new storage writer for a table.
    pub fn new(
        table: Table,
        config: WriterConfig,
        pending: Option<crate::pending::PendingHours>,
    ) -> Self {
        info!(
            table = table.name(),
            flush_bytes = config.flush_bytes,
            flush_interval_secs = config.flush_interval_secs,
            partition_duration_secs = config.partition_duration_secs,
            "Storage writer initialized"
        );
        Self {
            config,
            table,
            buffers: HashMap::new(),
            seq: AtomicU32::new(1),
            pending,
        }
    }

    /// Ingest a decoded IPFIX message. Transposes row data into columnar buffers.
    /// Returns the number of records ingested.
    pub fn ingest(&mut self, msg: &IpfixMessage) -> usize {
        let exporter = msg.exporter;
        let domain = msg.header.observation_domain_id;
        let export_time = msg.header.export_time;
        let partition = export_time / self.config.partition_duration_secs;

        // Extract exporter IPv4 address and port from the socket address.
        let exporter_ipv4: u32 = match exporter {
            std::net::SocketAddr::V4(v4) => u32::from(*v4.ip()),
            std::net::SocketAddr::V6(_) => 0,
        };
        let exporter_port = exporter.port();

        let num_sys = system_columns().len();
        let mut total = 0;

        for set in &msg.sets {
            if let SetContents::Data(data) = &set.contents {
                if data.records.is_empty() {
                    continue;
                }

                let specs: Vec<_> = data.records[0].fields.iter().map(|f| f.spec).collect();
                let schema = Schema::from_template(&specs);
                let fingerprint = schema.fingerprint();
                let key = (fingerprint, partition);

                let capacity = self.config.initial_row_capacity;
                let buf = self.buffers.entry(key).or_insert_with(|| {
                    let mut b = SchemaBuffer::new(schema, capacity);
                    b.partition_hour = partition;
                    b
                });

                buf.exporter = exporter;
                buf.observation_domain_id = domain;
                if export_time < buf.first_export_time {
                    buf.first_export_time = export_time;
                }
                if export_time > buf.last_export_time {
                    buf.last_export_time = export_time;
                }

                // Record fields should match the non-system columns in the schema.
                let expected_fields = buf.schema.columns.len() - num_sys;
                for record in &data.records {
                    if record.fields.len() == expected_fields {
                        buf.append_record(
                            record,
                            exporter_ipv4,
                            exporter_port,
                            export_time,
                            domain,
                        );
                        total += 1;
                    }
                }
            }
        }

        total
    }

    /// Check all buffers and flush any that exceed thresholds.
    pub fn flush_ready(&mut self) -> usize {
        let flush_bytes = self.config.flush_bytes;
        let flush_secs = self.config.flush_interval_secs;

        let to_flush: Vec<(u64, u32)> = self
            .buffers
            .iter()
            .filter(|(_, buf)| {
                buf.row_count > 0
                    && (buf.mem_size() >= flush_bytes || buf.elapsed_secs() >= flush_secs)
            })
            .map(|(key, _)| *key)
            .collect();

        let mut flushed = 0;
        for key in to_flush {
            if self.flush_key(&key) {
                flushed += 1;
            }
        }
        flushed
    }

    /// Number of active write buffers.
    pub fn buffer_count(&self) -> usize {
        self.buffers.len()
    }

    /// Total bytes across all write buffers.
    pub fn buffer_bytes(&self) -> usize {
        self.buffers.values().map(|b| b.mem_size()).sum()
    }

    /// Force-flush all non-empty buffers.
    pub fn flush_all(&mut self) -> usize {
        let keys: Vec<(u64, u32)> = self.buffers.keys().copied().collect();
        let mut flushed = 0;
        for key in keys {
            if self.flush_key(&key) {
                flushed += 1;
            }
        }
        flushed
    }

    /// Flush a specific buffer by key. Returns true if a part was written.
    fn flush_key(&mut self, key: &(u64, u32)) -> bool {
        let Some(buf) = self.buffers.get(key) else {
            return false;
        };
        if buf.row_count == 0 {
            return false;
        }

        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let base_dir = self.table.base_dir().to_path_buf();

        match flush_schema_buffer(buf, &base_dir, seq) {
            Ok(path) => {
                let rows = buf.row_count;
                debug!(part = %path.display(), rows, "Part flushed");

                // Mark this hour as needing merge attention
                if let (Some(pending), Some(hour_dir)) = (&self.pending, path.parent()) {
                    pending.mark_dirty(hour_dir.to_path_buf());
                }

                if let Some(buf) = self.buffers.get_mut(key) {
                    buf.clear();
                }
                true
            }
            Err(e) => {
                warn!(error = %e, "Failed to flush part");
                false
            }
        }
    }
}

/// Default granule size used during ingestion flush.
const INGESTION_GRANULE_SIZE: usize = 8192;
/// Default bloom bits per granule during ingestion.
const INGESTION_BLOOM_BITS: usize = 8192;

/// Encode columns, compute granules, and write a part to disk.
fn flush_schema_buffer(buf: &SchemaBuffer, base_dir: &Path, seq: u32) -> std::io::Result<PathBuf> {
    let _t = flowcus_core::profiling::span_timer("storage;writer;flush");
    let mut column_data = Vec::with_capacity(buf.schema.columns.len());
    let mut disk_bytes: u64 = 0;

    for (def, col_buf) in buf.schema.columns.iter().zip(buf.columns.iter()) {
        let _t_enc = flowcus_core::profiling::span_timer("storage;writer;flush;encode_column");
        let encoded = codec::encode(col_buf);
        disk_bytes += (part::COLUMN_HEADER_SIZE + encoded.data.len()) as u64;

        let _t_gran = flowcus_core::profiling::span_timer("storage;writer;flush;compute_granules");
        let (marks, blooms) = crate::granule::compute_granules(
            col_buf,
            &encoded.data,
            INGESTION_GRANULE_SIZE,
            INGESTION_BLOOM_BITS,
            def.storage_type,
        );

        column_data.push(part::ColumnWriteData {
            def: def.clone(),
            encoded,
            marks,
            blooms,
        });
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let metadata = part::PartMetadata {
        row_count: buf.row_count as u64,
        generation: 0,
        time_min: buf.first_export_time,
        time_max: buf.last_export_time,
        observation_domain_id: buf.observation_domain_id,
        created_at_ms: now,
        disk_bytes,
        column_count: buf.schema.columns.len() as u32,
        schema_fingerprint: buf.schema.fingerprint(),
        exporter: buf.exporter,
        schema: buf.schema.clone(),
    };

    part::write_part(
        base_dir,
        &metadata,
        &column_data,
        INGESTION_GRANULE_SIZE,
        INGESTION_BLOOM_BITS,
        seq,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::Table;
    use flowcus_ipfix::protocol::*;
    use std::net::Ipv4Addr;

    fn make_record(src: Ipv4Addr, dst: Ipv4Addr, bytes: u64) -> DataRecord {
        DataRecord {
            fields: vec![
                DataField {
                    spec: FieldSpecifier {
                        element_id: 8,
                        field_length: 4,
                        enterprise_id: 0,
                    },
                    name: "sourceIPv4Address".into(),
                    value: FieldValue::Ipv4(src),
                },
                DataField {
                    spec: FieldSpecifier {
                        element_id: 12,
                        field_length: 4,
                        enterprise_id: 0,
                    },
                    name: "destinationIPv4Address".into(),
                    value: FieldValue::Ipv4(dst),
                },
                DataField {
                    spec: FieldSpecifier {
                        element_id: 1,
                        field_length: 8,
                        enterprise_id: 0,
                    },
                    name: "octetDeltaCount".into(),
                    value: FieldValue::Unsigned64(bytes),
                },
            ],
        }
    }

    fn make_message(records: Vec<DataRecord>) -> IpfixMessage {
        IpfixMessage {
            header: MessageHeader {
                version: 0x000a,
                length: 0,
                export_time: 1_700_000_000,
                sequence_number: 1,
                observation_domain_id: 1,
            },
            exporter: "10.0.0.1:4739".parse().unwrap(),
            sets: vec![Set {
                set_id: 256,
                contents: SetContents::Data(DataSet {
                    template_id: 256,
                    records,
                }),
            }],
        }
    }

    #[test]
    fn ingest_and_flush() {
        let dir = std::env::temp_dir().join("flowcus_test_writer2");
        let _ = std::fs::remove_dir_all(&dir);

        let table = Table::open(&dir, "flows").unwrap();
        let config = WriterConfig {
            flush_bytes: 1,
            flush_interval_secs: 0,
            initial_row_capacity: 64,
            ..WriterConfig::default()
        };
        let mut writer = StorageWriter::new(table, config, None);

        let msg = make_message(vec![
            make_record(
                Ipv4Addr::new(10, 0, 0, 1),
                Ipv4Addr::new(192, 168, 1, 1),
                1500,
            ),
            make_record(
                Ipv4Addr::new(10, 0, 0, 2),
                Ipv4Addr::new(192, 168, 1, 2),
                2500,
            ),
        ]);

        assert_eq!(writer.ingest(&msg), 2);
        assert_eq!(writer.flush_ready(), 1);

        // Verify part was written in time-partitioned tree
        // Path: flows/2023/11/14/22/00000_..._000001/
        let table = Table::open(&dir, "flows").unwrap();
        let parts = table.list_all_parts().unwrap();
        assert_eq!(parts.len(), 1);

        let part_dir = &parts[0].path;
        assert!(
            part_dir
                .join("columns")
                .join("sourceIPv4Address.col")
                .exists()
        );
        assert!(part_dir.join("meta.bin").exists());
        assert_eq!(parts[0].generation, 0);

        // Verify binary metadata roundtrip
        let header = part::read_meta_bin(&part_dir.join("meta.bin")).unwrap();
        assert_eq!(header.row_count, 2);
        assert_eq!(header.generation, 0);
        assert_eq!(header.column_count, 7); // 4 system + 3 template

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn time_partitioning_splits_buffers() {
        let dir = std::env::temp_dir().join("flowcus_test_partition");
        let _ = std::fs::remove_dir_all(&dir);

        let table = Table::open(&dir, "flows").unwrap();
        let config = WriterConfig {
            flush_bytes: 1,
            flush_interval_secs: 0,
            partition_duration_secs: 3600,
            ..WriterConfig::default()
        };
        let mut writer = StorageWriter::new(table, config, None);

        // Message from hour 472222 (1_700_000_000 / 3600)
        let msg1 = make_message(vec![make_record(
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(192, 168, 1, 1),
            100,
        )]);

        // Message from next hour
        let mut msg2 = make_message(vec![make_record(
            Ipv4Addr::new(10, 0, 0, 2),
            Ipv4Addr::new(192, 168, 1, 2),
            200,
        )]);
        msg2.header.export_time = 1_700_003_600;

        writer.ingest(&msg1);
        writer.ingest(&msg2);
        let flushed = writer.flush_all();
        assert_eq!(flushed, 2, "should produce 2 parts for 2 different hours");

        std::fs::remove_dir_all(&dir).ok();
    }
}
