//! Table schema derived from IPFIX templates.
//!
//! Each unique template produces a schema defining which columns exist and their types.
//! Parts are self-describing: each part stores its column set in metadata.

use serde::{Deserialize, Serialize};

use flowcus_ipfix::ie;
use flowcus_ipfix::protocol::{DataType, FieldSpecifier};

/// A column definition within a table schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name (from IE registry or generated).
    pub name: String,
    /// IPFIX Information Element ID.
    pub element_id: u16,
    /// IPFIX enterprise ID (0 = IANA standard).
    pub enterprise_id: u32,
    /// Abstract data type from IPFIX.
    pub data_type: DataType,
    /// Physical storage type for the column.
    pub storage_type: StorageType,
    /// Wire length from template (65535 = variable).
    pub wire_length: u16,
}

/// Physical storage type for column data. Determines codec and memory layout.
/// Each variant is sized to enable SIMD-aligned packed arrays.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StorageType {
    /// 1-byte values: u8, i8, bool. 64 values per cache line.
    U8,
    /// 2-byte values: u16, i16, ports. 32 values per cache line.
    U16,
    /// 4-byte values: u32, i32, f32, IPv4, timestamps. 16 values per cache line.
    U32,
    /// 8-byte values: u64, i64, f64, millisecond timestamps. 8 values per cache line.
    U64,
    /// 16-byte values: IPv6 addresses. 4 values per cache line.
    U128,
    /// 6-byte values: MAC addresses. Packed, not naturally aligned.
    Mac,
    /// Variable-length: strings, byte arrays. Stored as offsets + data.
    VarLen,
}

impl StorageType {
    /// Fixed element size in bytes, or None for variable-length.
    pub const fn element_size(self) -> Option<usize> {
        match self {
            Self::U8 => Some(1),
            Self::U16 => Some(2),
            Self::U32 => Some(4),
            Self::U64 => Some(8),
            Self::U128 => Some(16),
            Self::Mac => Some(6),
            Self::VarLen => None,
        }
    }

    pub const fn is_fixed(self) -> bool {
        self.element_size().is_some()
    }
}

/// Map an IPFIX DataType to a physical StorageType.
pub const fn storage_type_for(dt: DataType) -> StorageType {
    match dt {
        DataType::Unsigned8 | DataType::Signed8 | DataType::Boolean => StorageType::U8,
        DataType::Unsigned16 | DataType::Signed16 => StorageType::U16,
        DataType::Unsigned32
        | DataType::Signed32
        | DataType::Float32
        | DataType::Ipv4Address
        | DataType::DateTimeSeconds => StorageType::U32,
        DataType::Unsigned64
        | DataType::Signed64
        | DataType::Float64
        | DataType::DateTimeMilliseconds
        | DataType::DateTimeMicroseconds
        | DataType::DateTimeNanoseconds => StorageType::U64,
        DataType::Ipv6Address => StorageType::U128,
        DataType::MacAddress => StorageType::Mac,
        DataType::OctetArray | DataType::String => StorageType::VarLen,
    }
}

/// Sentinel enterprise ID used for system columns (not real IPFIX IEs).
pub const SYSTEM_ENTERPRISE_ID: u32 = u32::MAX;

/// Returns the 4 system column definitions that are prepended to every schema.
///
/// These columns capture IPFIX message metadata (exporter address, port,
/// export time, observation domain ID) rather than flow record fields.
pub fn system_columns() -> Vec<ColumnDef> {
    vec![
        ColumnDef {
            name: "flowcusExporterIPv4".into(),
            element_id: 0,
            enterprise_id: SYSTEM_ENTERPRISE_ID,
            data_type: DataType::Ipv4Address,
            storage_type: StorageType::U32,
            wire_length: 4,
        },
        ColumnDef {
            name: "flowcusExporterPort".into(),
            element_id: 0,
            enterprise_id: SYSTEM_ENTERPRISE_ID,
            data_type: DataType::Unsigned16,
            storage_type: StorageType::U16,
            wire_length: 2,
        },
        ColumnDef {
            name: "flowcusExportTime".into(),
            element_id: 0,
            enterprise_id: SYSTEM_ENTERPRISE_ID,
            data_type: DataType::DateTimeMilliseconds,
            storage_type: StorageType::U64,
            wire_length: 8,
        },
        ColumnDef {
            name: "flowcusObservationDomainId".into(),
            element_id: 0,
            enterprise_id: SYSTEM_ENTERPRISE_ID,
            data_type: DataType::Unsigned32,
            storage_type: StorageType::U32,
            wire_length: 4,
        },
    ]
}

/// Column definition for `flowcusFlowDuration` (milliseconds).
fn flow_duration_column() -> ColumnDef {
    ColumnDef {
        name: "flowcusFlowDuration".into(),
        element_id: 1,
        enterprise_id: SYSTEM_ENTERPRISE_ID,
        data_type: DataType::Unsigned32,
        storage_type: StorageType::U32,
        wire_length: 4,
    }
}

/// Describes which start/end time pair is available for computing flow duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurationSource {
    /// IE 150 + 151: seconds → multiply difference by 1000.
    Seconds { start_idx: usize, end_idx: usize },
    /// IE 152 + 153: milliseconds → difference is already in ms.
    Milliseconds { start_idx: usize, end_idx: usize },
    /// IE 22 + 21: sysUpTime (already ms) → difference is already in ms.
    SysUpTime { start_idx: usize, end_idx: usize },
}

/// Check whether a template has a start/end time pair suitable for computing
/// flow duration, and that it does NOT already contain IE 161
/// (`flowDurationMilliseconds`).
///
/// The returned indices are relative to the `specs` slice (i.e. template field
/// index, NOT schema column index). The caller must add the system-column offset
/// to get the schema column index.
pub fn detect_duration_source(specs: &[FieldSpecifier]) -> Option<DurationSource> {
    // If the template already exports flowDurationMilliseconds, skip.
    if specs
        .iter()
        .any(|s| s.element_id == 161 && s.enterprise_id == 0)
    {
        return None;
    }

    let find = |ie: u16| -> Option<usize> {
        specs
            .iter()
            .position(|s| s.element_id == ie && s.enterprise_id == 0)
    };

    // Prefer millisecond precision, then seconds, then sysUpTime.
    if let (Some(si), Some(ei)) = (find(152), find(153)) {
        return Some(DurationSource::Milliseconds {
            start_idx: si,
            end_idx: ei,
        });
    }
    if let (Some(si), Some(ei)) = (find(150), find(151)) {
        return Some(DurationSource::Seconds {
            start_idx: si,
            end_idx: ei,
        });
    }
    if let (Some(si), Some(ei)) = (find(22), find(21)) {
        return Some(DurationSource::SysUpTime {
            start_idx: si,
            end_idx: ei,
        });
    }
    None
}

/// A table schema: an ordered list of column definitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<ColumnDef>,
    /// If present, flow duration is computed during ingest from this source.
    /// Not serialized to disk — rebuilt from column presence on deserialization.
    #[serde(skip)]
    pub duration_source: Option<DurationSource>,
}

impl Schema {
    /// Build a schema from an IPFIX template's field specifiers.
    /// System columns are automatically prepended before the template-derived columns.
    /// If the template contains a start/end time pair (and no native
    /// `flowDurationMilliseconds`), a computed `flowcusFlowDuration` column
    /// is appended after the system columns, before the template columns.
    pub fn from_template(specs: &[FieldSpecifier]) -> Self {
        let mut columns = system_columns();
        let duration_source = detect_duration_source(specs);

        if duration_source.is_some() {
            columns.push(flow_duration_column());
        }

        columns.extend(specs.iter().map(|spec| {
            let dt = ie::data_type(spec.element_id, spec.enterprise_id);
            let st = storage_type_for(dt);
            ColumnDef {
                name: ie::name(spec.element_id, spec.enterprise_id),
                element_id: spec.element_id,
                enterprise_id: spec.enterprise_id,
                data_type: dt,
                storage_type: st,
                wire_length: spec.field_length,
            }
        }));
        Self {
            columns,
            duration_source,
        }
    }

    /// Number of system/computed columns (columns with `SYSTEM_ENTERPRISE_ID`).
    /// These come before the template-derived columns in the schema.
    pub fn system_column_count(&self) -> usize {
        self.columns
            .iter()
            .filter(|c| c.enterprise_id == SYSTEM_ENTERPRISE_ID)
            .count()
    }

    /// Fingerprint for deduplicating schemas (same columns = same schema).
    /// System columns are skipped — they are always the same and should not
    /// affect schema grouping.
    pub fn fingerprint(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for col in &self.columns {
            if col.enterprise_id == SYSTEM_ENTERPRISE_ID {
                continue;
            }
            col.element_id.hash(&mut hasher);
            col.enterprise_id.hash(&mut hasher);
        }
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_from_ipv4_template() {
        let specs = vec![
            FieldSpecifier {
                element_id: 8,
                field_length: 4,
                enterprise_id: 0,
            },
            FieldSpecifier {
                element_id: 12,
                field_length: 4,
                enterprise_id: 0,
            },
            FieldSpecifier {
                element_id: 7,
                field_length: 2,
                enterprise_id: 0,
            },
            FieldSpecifier {
                element_id: 11,
                field_length: 2,
                enterprise_id: 0,
            },
            FieldSpecifier {
                element_id: 4,
                field_length: 1,
                enterprise_id: 0,
            },
        ];
        let schema = Schema::from_template(&specs);
        // 4 system columns + 5 template columns
        assert_eq!(schema.columns.len(), 9);
        // First 4 are system columns
        assert_eq!(schema.columns[0].name, "flowcusExporterIPv4");
        assert_eq!(schema.columns[1].name, "flowcusExporterPort");
        assert_eq!(schema.columns[2].name, "flowcusExportTime");
        assert_eq!(schema.columns[3].name, "flowcusObservationDomainId");
        // Template columns start at index 4
        assert_eq!(schema.columns[4].name, "sourceIPv4Address");
        assert_eq!(schema.columns[4].storage_type, StorageType::U32);
        assert_eq!(schema.columns[6].name, "sourceTransportPort");
        assert_eq!(schema.columns[6].storage_type, StorageType::U16);
        assert_eq!(schema.columns[8].storage_type, StorageType::U8);
    }

    #[test]
    fn storage_type_sizes() {
        assert_eq!(StorageType::U8.element_size(), Some(1));
        assert_eq!(StorageType::U32.element_size(), Some(4));
        assert_eq!(StorageType::U128.element_size(), Some(16));
        assert_eq!(StorageType::VarLen.element_size(), None);
    }

    #[test]
    fn duration_column_added_for_millisecond_timestamps() {
        let specs = vec![
            FieldSpecifier {
                element_id: 8,
                field_length: 4,
                enterprise_id: 0,
            },
            FieldSpecifier {
                element_id: 152,
                field_length: 8,
                enterprise_id: 0,
            }, // flowStartMilliseconds
            FieldSpecifier {
                element_id: 153,
                field_length: 8,
                enterprise_id: 0,
            }, // flowEndMilliseconds
        ];
        let schema = Schema::from_template(&specs);
        // 4 base system + 1 duration + 3 template = 8
        assert_eq!(schema.columns.len(), 8);
        assert_eq!(schema.columns[4].name, "flowcusFlowDuration");
        assert_eq!(schema.columns[4].storage_type, StorageType::U32);
        assert!(matches!(
            schema.duration_source,
            Some(DurationSource::Milliseconds { .. })
        ));
        assert_eq!(schema.system_column_count(), 5);
    }

    #[test]
    fn duration_column_added_for_second_timestamps() {
        let specs = vec![
            FieldSpecifier {
                element_id: 150,
                field_length: 4,
                enterprise_id: 0,
            }, // flowStartSeconds
            FieldSpecifier {
                element_id: 151,
                field_length: 4,
                enterprise_id: 0,
            }, // flowEndSeconds
        ];
        let schema = Schema::from_template(&specs);
        assert_eq!(schema.columns[4].name, "flowcusFlowDuration");
        assert!(matches!(
            schema.duration_source,
            Some(DurationSource::Seconds { .. })
        ));
    }

    #[test]
    fn duration_column_added_for_sysuptime() {
        let specs = vec![
            FieldSpecifier {
                element_id: 22,
                field_length: 4,
                enterprise_id: 0,
            }, // flowStartSysUpTime
            FieldSpecifier {
                element_id: 21,
                field_length: 4,
                enterprise_id: 0,
            }, // flowEndSysUpTime
        ];
        let schema = Schema::from_template(&specs);
        assert_eq!(schema.columns[4].name, "flowcusFlowDuration");
        assert!(matches!(
            schema.duration_source,
            Some(DurationSource::SysUpTime { .. })
        ));
    }

    #[test]
    fn no_duration_column_when_native_ie161_present() {
        let specs = vec![
            FieldSpecifier {
                element_id: 152,
                field_length: 8,
                enterprise_id: 0,
            },
            FieldSpecifier {
                element_id: 153,
                field_length: 8,
                enterprise_id: 0,
            },
            FieldSpecifier {
                element_id: 161,
                field_length: 4,
                enterprise_id: 0,
            }, // flowDurationMilliseconds
        ];
        let schema = Schema::from_template(&specs);
        assert!(schema.duration_source.is_none());
        assert!(
            !schema
                .columns
                .iter()
                .any(|c| c.name == "flowcusFlowDuration")
        );
        assert_eq!(schema.system_column_count(), 4); // only base system columns
    }

    #[test]
    fn no_duration_column_without_time_pair() {
        let specs = vec![
            FieldSpecifier {
                element_id: 8,
                field_length: 4,
                enterprise_id: 0,
            },
            FieldSpecifier {
                element_id: 12,
                field_length: 4,
                enterprise_id: 0,
            },
        ];
        let schema = Schema::from_template(&specs);
        assert!(schema.duration_source.is_none());
        assert_eq!(schema.system_column_count(), 4);
    }

    #[test]
    fn fingerprint_excludes_duration_column() {
        let specs_with_time = vec![
            FieldSpecifier {
                element_id: 8,
                field_length: 4,
                enterprise_id: 0,
            },
            FieldSpecifier {
                element_id: 152,
                field_length: 8,
                enterprise_id: 0,
            },
            FieldSpecifier {
                element_id: 153,
                field_length: 8,
                enterprise_id: 0,
            },
        ];
        let specs_without_time = vec![
            FieldSpecifier {
                element_id: 8,
                field_length: 4,
                enterprise_id: 0,
            },
            FieldSpecifier {
                element_id: 152,
                field_length: 8,
                enterprise_id: 0,
            },
            FieldSpecifier {
                element_id: 153,
                field_length: 8,
                enterprise_id: 0,
            },
        ];
        let s1 = Schema::from_template(&specs_with_time);
        let s2 = Schema::from_template(&specs_without_time);
        assert_eq!(s1.fingerprint(), s2.fingerprint());
    }

    #[test]
    fn milliseconds_preferred_over_seconds() {
        // Template with both second and millisecond pairs — ms should win.
        let specs = vec![
            FieldSpecifier {
                element_id: 150,
                field_length: 4,
                enterprise_id: 0,
            },
            FieldSpecifier {
                element_id: 151,
                field_length: 4,
                enterprise_id: 0,
            },
            FieldSpecifier {
                element_id: 152,
                field_length: 8,
                enterprise_id: 0,
            },
            FieldSpecifier {
                element_id: 153,
                field_length: 8,
                enterprise_id: 0,
            },
        ];
        let schema = Schema::from_template(&specs);
        assert!(matches!(
            schema.duration_source,
            Some(DurationSource::Milliseconds { .. })
        ));
    }
}
