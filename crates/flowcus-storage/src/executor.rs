//! Query execution engine: reads columnar storage and returns actual results.
//!
//! Execution pipeline:
//! 1. Convert time range to unix second bounds
//! 2. Discover parts via `Table::list_parts` with time pruning
//! 3. For each part: decode filter columns, build matching-row bitmask,
//!    decode output columns only for matches, build `Vec<Vec<Value>>` directly
//! 4. Aggregate if needed (group-by, top-N)
//! 5. Early-terminate for non-aggregate queries once offset+limit rows collected
//! 6. Return results with execution plan

use std::collections::{BinaryHeap, HashMap};
use std::path::{Path, PathBuf};
use std::time::Instant;

use rayon::prelude::*;
use serde::Serialize;
use tracing::{debug, trace};

use flowcus_query::ast::{
    AggCall, AggExpr, AggFunc, CompareOp, Duration as FqlDuration, DurationUnit, FieldFilter,
    FieldValue as AstFieldValue, FilterExpr, GroupByKey, IpDirection, IpFilter, IpValue,
    NumericFilter, NumericValue, PortDirection, PortFilter, PortValue, ProtoFilter, ProtoValue,
    Query, SelectExpr, SelectFieldExpr, Stage, StringFilterExpr, StringOp, TimeRange,
};

use std::sync::Arc;

use crate::cache::StorageCache;
use crate::column::ColumnBuffer;
use crate::decode;
use crate::part;
use crate::schema::StorageType;
use crate::table::{PartEntry, Table};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Maximum aggregate matching rows before the query is rejected to prevent OOM.
const AGG_MAX_MATCHING_ROWS: usize = 10_000_000;

/// The query executor: reads parts from disk and evaluates queries.
pub struct QueryExecutor {
    table_base: PathBuf,
    granule_size: usize,
    cache: Arc<StorageCache>,
    part_locks: crate::part_locks::PartLocks,
}

/// Execution plan with statistics about what happened during the query.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ExecutionPlan {
    pub steps: Vec<PlanStep>,
    pub parts_total: usize,
    pub parts_skipped_by_time: usize,
    pub parts_skipped_by_index: usize,
    pub granules_total: usize,
    pub granules_skipped_by_bloom: usize,
    pub granules_skipped_by_marks: usize,
    pub columns_to_read: Vec<String>,
    /// Total bytes read from disk across all column reads.
    pub bytes_read: u64,
}

/// A single step in the execution plan.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum PlanStep {
    TimeRangePrune {
        start: u64,
        end: u64,
        parts_before: usize,
        parts_after: usize,
        duration_us: Option<u64>,
    },
    ColumnIndexFilter {
        column: String,
        predicate: String,
        parts_skipped: usize,
        duration_us: Option<u64>,
    },
    BloomFilter {
        column: String,
        value: String,
        granules_skipped: usize,
        duration_us: Option<u64>,
    },
    MarkSeek {
        column: String,
        granule_range: (usize, usize),
        duration_us: Option<u64>,
    },
    ColumnRead {
        column: String,
        bytes: u64,
        duration_us: Option<u64>,
    },
    FilterApply {
        expression: String,
        rows_before: usize,
        rows_after: usize,
        duration_us: Option<u64>,
    },
    Aggregate {
        function: String,
        groups: usize,
        duration_us: Option<u64>,
    },
    PartSkippedMerge {
        path: String,
    },
    PartSkippedIncomplete {
        path: String,
    },
    PartSkippedMerging {
        path: String,
    },
    PartSkippedSubsumed {
        path: String,
        subsumed_by: String,
    },
    TopNFastPath {
        sort_field: String,
        n: usize,
        parts_scanned: usize,
        rows_scanned: u64,
        duration_us: Option<u64>,
    },
    ParallelScan {
        parts_count: usize,
        duration_us: Option<u64>,
    },
    /// Per-part scan timing within a ParallelScan. One emitted per part.
    PartScan {
        part: String,
        rows_scanned: u64,
        rows_matched: u64,
        bytes_read: u64,
        duration_us: Option<u64>,
    },
    /// Summary of storage-level LRU cache usage for this query.
    CacheStats {
        hits: u64,
        misses: u64,
    },
}

/// Query result with rows, columns, and the execution plan.
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub plan: ExecutionPlan,
    pub rows_scanned: u64,
    pub parts_scanned: u64,
    pub parts_skipped: u64,
    pub total_matching_rows: u64,
    /// Sum of `row_count` from all part metadata in the time range (no column scan).
    pub total_rows_in_range: u64,
    pub time_start: u64,
    pub time_end: u64,
    /// Unified column list across all parts, before SELECT projection.
    /// Used as `pinned_columns` for subsequent pagination requests.
    pub schema_columns: Vec<String>,
    /// Cursor for next page — hex-encoded flowcusRowId of the last returned row.
    pub next_cursor: Option<String>,
}

/// Result for a single-row lookup by flowcusRowId.
pub struct SingleRowResult {
    pub columns: Vec<String>,
    pub values: HashMap<String, serde_json::Value>,
}

/// Lightweight histogram built from part metadata only (no column scan).
pub struct HistogramResult {
    /// Time-bucketed row counts: (bucket_start_unix_ms, row_count).
    pub buckets: Vec<(u64, u64)>,
    /// Total rows across all parts in the time range.
    pub total_rows: u64,
    /// Resolved time range bounds (unix milliseconds).
    pub time_start: u64,
    pub time_end: u64,
}

// ---------------------------------------------------------------------------
// Pre-resolved filter types — parse constants once before the row loop
// ---------------------------------------------------------------------------

/// A filter with all constants pre-resolved to native types.
/// Avoids repeated string parsing and name resolution inside per-row evaluation.
enum ResolvedFilter {
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>),
    Ip(ResolvedIpFilter),
    Port(ResolvedPortFilter),
    Proto(ResolvedProtoFilter),
    Numeric(ResolvedNumericFilter),
    StringFilter(ResolvedStringFilter),
    Field(ResolvedFieldFilter),
    /// Cursor filter: only rows with flowcusRowId > (hi, lo).
    RowIdCursor {
        hi: u64,
        lo: u64,
    },
}

struct ResolvedIpFilter {
    direction: IpDirection,
    negated: bool,
    value: ResolvedIpValue,
}

enum ResolvedIpValue {
    Addr(u32),
    Cidr {
        base_masked: u32,
        mask: u32,
    },
    List(Vec<u32>),
    Wildcard(WildcardOctets),
    // IPv6 variants
    Addr128([u64; 2]),
    Cidr128 {
        base_masked: [u64; 2],
        mask: [u64; 2],
    },
    List128(Vec<[u64; 2]>),
}

/// Pre-parsed wildcard octets: `None` means `*` (match any).
struct WildcardOctets([Option<u8>; 4]);

struct ResolvedPortFilter {
    direction: PortDirection,
    negated: bool,
    value: ResolvedPortValue,
}

enum ResolvedPortValue {
    Single(u16),
    Range(u16, u16),
    List(Vec<Self>),
    OpenRange(u16),
}

struct ResolvedProtoFilter {
    negated: bool,
    value: ResolvedProtoValue,
}

enum ResolvedProtoValue {
    Single(u8),
    List(Vec<u8>),
}

struct ResolvedNumericFilter {
    col_name: String,
    op: CompareOp,
    target: u64,
}

struct ResolvedStringFilter {
    col_name: String,
    op: StringOp,
    value: String,
    /// Pre-split list items for In/NotIn
    list_items: Vec<String>,
}

struct ResolvedFieldFilter {
    col_name: String,
    op: CompareOp,
    target: u64,
}

/// Resolve an AST filter tree into pre-computed native values.
fn resolve_filter(filter: &FilterExpr) -> ResolvedFilter {
    match filter {
        FilterExpr::And(a, b) => {
            ResolvedFilter::And(Box::new(resolve_filter(a)), Box::new(resolve_filter(b)))
        }
        FilterExpr::Or(a, b) => {
            ResolvedFilter::Or(Box::new(resolve_filter(a)), Box::new(resolve_filter(b)))
        }
        FilterExpr::Not(inner) => ResolvedFilter::Not(Box::new(resolve_filter(inner))),
        FilterExpr::Ip(ip) => ResolvedFilter::Ip(resolve_ip_filter(ip)),
        FilterExpr::Port(port) => ResolvedFilter::Port(resolve_port_filter(port)),
        FilterExpr::Proto(proto) => ResolvedFilter::Proto(resolve_proto_filter(proto)),
        FilterExpr::Numeric(num) => ResolvedFilter::Numeric(resolve_numeric_filter(num)),
        FilterExpr::StringFilter(sf) => ResolvedFilter::StringFilter(resolve_string_filter(sf)),
        FilterExpr::Field(ff) => ResolvedFilter::Field(resolve_field_filter(ff)),
    }
}

/// Convert an IPv6 address string to `[u64; 2]` (hi, lo) in big-endian order.
fn ipv6_to_pair(addr: std::net::Ipv6Addr) -> [u64; 2] {
    let octets = addr.octets();
    let hi = u64::from_be_bytes(octets[..8].try_into().unwrap());
    let lo = u64::from_be_bytes(octets[8..].try_into().unwrap());
    [hi, lo]
}

fn resolve_ip_filter(ip: &IpFilter) -> ResolvedIpFilter {
    let value = match &ip.value {
        IpValue::Addr(addr) => {
            if let Ok(v4) = addr.parse::<std::net::Ipv4Addr>() {
                ResolvedIpValue::Addr(u32::from(v4))
            } else if let Ok(v6) = addr.parse::<std::net::Ipv6Addr>() {
                ResolvedIpValue::Addr128(ipv6_to_pair(v6))
            } else {
                ResolvedIpValue::Addr(0)
            }
        }
        IpValue::Cidr(cidr) => {
            let parts: Vec<&str> = cidr.split('/').collect();
            if parts.len() == 2 {
                let prefix_len: u32 = parts[1].parse().unwrap_or(0);
                if let Ok(v4) = parts[0].parse::<std::net::Ipv4Addr>() {
                    let base = u32::from(v4);
                    let mask = if prefix_len == 0 {
                        0
                    } else {
                        u32::MAX << (32 - prefix_len.min(32))
                    };
                    ResolvedIpValue::Cidr {
                        base_masked: base & mask,
                        mask,
                    }
                } else if let Ok(v6) = parts[0].parse::<std::net::Ipv6Addr>() {
                    let pair = ipv6_to_pair(v6);
                    let mask = ipv6_prefix_mask(prefix_len);
                    ResolvedIpValue::Cidr128 {
                        base_masked: [pair[0] & mask[0], pair[1] & mask[1]],
                        mask,
                    }
                } else {
                    ResolvedIpValue::Addr(0)
                }
            } else {
                ResolvedIpValue::Addr(0)
            }
        }
        IpValue::List(addrs) => {
            // Try IPv4 first; if any parse as IPv6, use the IPv6 path
            let mut v4s: Vec<u32> = Vec::new();
            let mut v6s: Vec<[u64; 2]> = Vec::new();
            let mut has_v6 = false;
            for a in addrs {
                if let Ok(v4) = a.parse::<std::net::Ipv4Addr>() {
                    v4s.push(u32::from(v4));
                } else if let Ok(v6) = a.parse::<std::net::Ipv6Addr>() {
                    v6s.push(ipv6_to_pair(v6));
                    has_v6 = true;
                }
            }
            if has_v6 {
                ResolvedIpValue::List128(v6s)
            } else {
                ResolvedIpValue::List(v4s)
            }
        }
        IpValue::Wildcard(pattern) => {
            let octets: Vec<&str> = pattern.split('.').collect();
            let mut wo = [None; 4];
            for (i, oct) in octets.iter().enumerate().take(4) {
                if *oct != "*" {
                    wo[i] = oct.parse::<u8>().ok();
                }
            }
            ResolvedIpValue::Wildcard(WildcardOctets(wo))
        }
    };
    ResolvedIpFilter {
        direction: ip.direction.clone(),
        negated: ip.negated,
        value,
    }
}

/// Build a 128-bit prefix mask as `[u64; 2]` from a prefix length (0..=128).
fn ipv6_prefix_mask(prefix_len: u32) -> [u64; 2] {
    let pl = prefix_len.min(128);
    let hi = if pl >= 64 {
        u64::MAX
    } else if pl == 0 {
        0
    } else {
        u64::MAX << (64 - pl)
    };
    let lo = if pl >= 128 {
        u64::MAX
    } else if pl <= 64 {
        0
    } else {
        u64::MAX << (128 - pl)
    };
    [hi, lo]
}

fn resolve_port_value(pv: &PortValue) -> ResolvedPortValue {
    match pv {
        PortValue::Single(p) => ResolvedPortValue::Single(*p),
        PortValue::Range(lo, hi) => ResolvedPortValue::Range(*lo, *hi),
        PortValue::List(items) => {
            ResolvedPortValue::List(items.iter().map(resolve_port_value).collect())
        }
        PortValue::Named(name) => ResolvedPortValue::Single(resolve_named_port(name).unwrap_or(0)),
        PortValue::OpenRange(lo) => ResolvedPortValue::OpenRange(*lo),
    }
}

fn resolve_port_filter(port: &PortFilter) -> ResolvedPortFilter {
    ResolvedPortFilter {
        direction: port.direction,
        negated: port.negated,
        value: resolve_port_value(&port.value),
    }
}

fn resolve_proto_value(pv: &ProtoValue) -> ResolvedProtoValue {
    match pv {
        ProtoValue::Named(name) => ResolvedProtoValue::Single(resolve_proto(name).unwrap_or(0)),
        ProtoValue::Number(n) => ResolvedProtoValue::Single(*n),
        ProtoValue::List(items) => {
            let resolved: Vec<u8> = items
                .iter()
                .map(|item| match item {
                    ProtoValue::Named(name) => resolve_proto(name).unwrap_or(0),
                    ProtoValue::Number(n) => *n,
                    ProtoValue::List(_) => 0,
                })
                .collect();
            ResolvedProtoValue::List(resolved)
        }
    }
}

fn resolve_proto_filter(proto: &ProtoFilter) -> ResolvedProtoFilter {
    ResolvedProtoFilter {
        negated: proto.negated,
        value: resolve_proto_value(&proto.value),
    }
}

fn resolve_numeric_target(value: &NumericValue) -> u64 {
    match value {
        NumericValue::Integer(n) => *n,
        NumericValue::WithSuffix(n, suffix) => {
            let multiplier = match suffix.to_lowercase().as_str() {
                "k" => 1_000u64,
                "m" => 1_000_000,
                "g" => 1_000_000_000,
                "t" => 1_000_000_000_000,
                _ => 1,
            };
            n * multiplier
        }
    }
}

fn resolve_numeric_filter(num: &NumericFilter) -> ResolvedNumericFilter {
    ResolvedNumericFilter {
        col_name: resolve_field_name(&num.field).to_string(),
        op: num.op,
        target: resolve_numeric_target(&num.value),
    }
}

fn resolve_string_filter(sf: &StringFilterExpr) -> ResolvedStringFilter {
    let list_items = if matches!(sf.op, StringOp::In | StringOp::NotIn) {
        sf.value.split(',').map(|s| s.trim().to_string()).collect()
    } else {
        Vec::new()
    };
    ResolvedStringFilter {
        col_name: resolve_field_name(&sf.field).to_string(),
        op: sf.op,
        value: sf.value.clone(),
        list_items,
    }
}

fn resolve_field_filter(ff: &FieldFilter) -> ResolvedFieldFilter {
    let target = match &ff.value {
        AstFieldValue::Integer(n) => *n,
        AstFieldValue::String(_) | AstFieldValue::List(_) => 0,
    };
    ResolvedFieldFilter {
        col_name: resolve_field_name(&ff.field).to_string(),
        op: ff.op,
        target,
    }
}

// ---------------------------------------------------------------------------
// Field name resolution
// ---------------------------------------------------------------------------

/// Resolve short aliases to canonical IPFIX field names.
fn resolve_field_name(name: &str) -> &str {
    match name {
        // Core 5-tuple
        "src" => "sourceIPv4Address",
        "dst" => "destinationIPv4Address",
        "sport" => "sourceTransportPort",
        "dport" => "destinationTransportPort",
        "proto" => "protocolIdentifier",
        "port" => "sourceTransportPort", // any-direction handled at filter level
        // Counters
        "bytes" => "octetDeltaCount",
        "packets" => "packetDeltaCount",
        // QoS / classification
        "tos" | "dscp" => "ipClassOfService",
        "flags" | "tcpflags" => "tcpControlBits",
        // Routing
        "nexthop" => "ipNextHopIPv4Address",
        "nexthop6" => "ipNextHopIPv6Address",
        "bgp_nexthop" => "bgpNextHopIPv4Address",
        "src_as" | "srcas" => "bgpSourceAsNumber",
        "dst_as" | "dstas" => "bgpDestinationAsNumber",
        // L2
        "vlan" => "vlanId",
        "src_mac" | "srcmac" => "sourceMacAddress",
        "dst_mac" | "dstmac" => "destinationMacAddress",
        // Interfaces
        "in_if" | "ingress" => "ingressInterface",
        "out_if" | "egress" => "egressInterface",
        // Timing
        "duration" => "flowDurationMilliseconds",
        "start" => "flowStartMilliseconds",
        "end" => "flowEndMilliseconds",
        // ICMP
        "icmp_type" => "icmpTypeCodeIPv4",
        "icmp6_type" => "icmpTypeCodeIPv6",
        // Exporter
        "exporter" => "flowcusExporterIPv4",
        "domain_id" => "flowcusObservationDomainId",
        // Application
        "app" => "applicationName",
        other => other,
    }
}

/// Resolve named port to port number.
fn resolve_named_port(name: &str) -> Option<u16> {
    match name.to_lowercase().as_str() {
        "http" => Some(80),
        "https" => Some(443),
        "dns" => Some(53),
        "ssh" => Some(22),
        "ftp" => Some(21),
        "smtp" => Some(25),
        "ntp" => Some(123),
        "snmp" => Some(161),
        "bgp" => Some(179),
        "ldap" => Some(389),
        "rdp" => Some(3389),
        "mysql" => Some(3306),
        "postgres" => Some(5432),
        _ => None,
    }
}

/// Resolve named protocol to protocol number.
fn resolve_proto(name: &str) -> Option<u8> {
    match name.to_lowercase().as_str() {
        "tcp" => Some(6),
        "udp" => Some(17),
        "icmp" => Some(1),
        "gre" => Some(47),
        "sctp" => Some(132),
        "esp" => Some(50),
        "ah" => Some(51),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Time range conversion
// ---------------------------------------------------------------------------

fn duration_to_secs(d: &FqlDuration) -> u64 {
    d.parts.iter().fold(0u64, |acc, p| {
        acc + p.value
            * match p.unit {
                DurationUnit::Seconds => 1,
                DurationUnit::Minutes => 60,
                DurationUnit::Hours => 3600,
                DurationUnit::Days => 86400,
                DurationUnit::Weeks => 604_800,
                DurationUnit::Months => 2_592_000, // ~30 days
            }
    })
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Convert a `TimeRange` to (start_unix_ms, end_unix_ms).
pub fn time_range_to_bounds(tr: &TimeRange) -> (u64, u64) {
    let now = now_unix_ms();
    match tr {
        TimeRange::Relative { duration, offset } => {
            let dur_ms = duration_to_secs(duration) * 1000;
            let off_ms = offset
                .as_ref()
                .map(|o| duration_to_secs(o) * 1000)
                .unwrap_or(0);
            let end = now.saturating_sub(off_ms);
            let start = end.saturating_sub(dur_ms);
            (start, end)
        }
        TimeRange::Absolute { start, end } => {
            let s = parse_datetime(start).unwrap_or(now.saturating_sub(3_600_000));
            let e = parse_datetime(end).unwrap_or(now);
            (s, e)
        }
        TimeRange::PointInTime { datetime, window } => {
            let t = parse_datetime(datetime).unwrap_or(now);
            match window {
                flowcus_query::ast::PointWindow::PlusMinus(d) => {
                    let w = duration_to_secs(d) * 1000;
                    (t.saturating_sub(w), t.saturating_add(w))
                }
                flowcus_query::ast::PointWindow::Plus(d) => {
                    let w = duration_to_secs(d) * 1000;
                    (t, t.saturating_add(w))
                }
                flowcus_query::ast::PointWindow::Minus(d) => {
                    let w = duration_to_secs(d) * 1000;
                    (t.saturating_sub(w), t)
                }
            }
        }
        TimeRange::Recurring { base, .. } => time_range_to_bounds(base),
        TimeRange::Combined(ranges) => {
            if ranges.is_empty() {
                (now.saturating_sub(3_600_000), now)
            } else {
                let mut min_start = u64::MAX;
                let mut max_end = 0u64;
                for r in ranges {
                    let (s, e) = time_range_to_bounds(r);
                    min_start = min_start.min(s);
                    max_end = max_end.max(e);
                }
                (min_start, max_end)
            }
        }
    }
}

/// Simple datetime parser: handles ISO 8601 date/datetime strings.
/// Supports: "YYYY-MM-DD", "YYYY-MM-DDTHH:MM:SS", "YYYY-MM-DDTHH:MM:SS.sss",
/// with optional trailing "Z". Returns unix milliseconds.
fn parse_datetime(s: &str) -> Option<u64> {
    let s = s.trim_end_matches('Z');
    let parts: Vec<&str> = s.split(['T', ' ']).collect();
    let date_parts: Vec<u32> = parts
        .first()?
        .split('-')
        .filter_map(|p| p.parse().ok())
        .collect();
    if date_parts.len() != 3 {
        return None;
    }
    let (year, month, day) = (date_parts[0], date_parts[1], date_parts[2]);

    let (hour, minute, second, millis) = if parts.len() > 1 {
        let time_parts: Vec<&str> = parts[1].split(':').collect();
        let h: u32 = time_parts.first().and_then(|p| p.parse().ok()).unwrap_or(0);
        let m: u32 = time_parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
        // Seconds may have fractional part: "SS" or "SS.sss"
        let (sec, ms) = if let Some(sec_str) = time_parts.get(2) {
            if let Some((whole, frac)) = sec_str.split_once('.') {
                let s: u32 = whole.parse().unwrap_or(0);
                let frac_padded = format!("{frac:0<3}");
                let ms: u32 = frac_padded[..3].parse().unwrap_or(0);
                (s, ms)
            } else {
                (sec_str.parse().unwrap_or(0), 0u32)
            }
        } else {
            (0, 0)
        };
        (h, m, sec, ms)
    } else {
        (0, 0, 0, 0)
    };

    let days = ymd_to_days(year, month, day)?;
    let secs = u64::from(days) * 86400
        + u64::from(hour) * 3600
        + u64::from(minute) * 60
        + u64::from(second);
    Some(secs * 1000 + u64::from(millis))
}

fn ymd_to_days(year: u32, month: u32, day: u32) -> Option<u32> {
    if month < 1 || month > 12 || day < 1 || day > 31 {
        return None;
    }
    let (y, m) = if month <= 2 {
        (year.wrapping_sub(1), month + 9)
    } else {
        (year, month.wrapping_sub(3))
    };
    let era = y / 400;
    let yoe = y - era * 400;
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe;
    Some(days.wrapping_sub(719_468))
}

// ---------------------------------------------------------------------------
// Column name extraction from query
// ---------------------------------------------------------------------------

/// Collect all column names referenced in the query (filters + select + aggregation).
fn collect_referenced_columns(query: &Query) -> Vec<String> {
    let mut cols = Vec::new();

    for stage in &query.stages {
        match stage {
            Stage::Filter(f) => collect_filter_columns(f, &mut cols),
            Stage::Select(sel) => match sel {
                SelectExpr::Fields(fields) => {
                    for f in fields {
                        match &f.expr {
                            SelectFieldExpr::Field(name) => {
                                cols.push(resolve_field_name(name).to_string());
                            }
                            SelectFieldExpr::BinaryOp { left, right, .. } => {
                                cols.push(resolve_field_name(left).to_string());
                                cols.push(resolve_field_name(right).to_string());
                            }
                        }
                    }
                }
                SelectExpr::All | SelectExpr::AllExcept(_) => {
                    // Will need all columns — handled at read time
                }
            },
            Stage::Aggregate(agg) => match agg {
                AggExpr::GroupBy { keys, functions } => {
                    for key in keys {
                        match key {
                            GroupByKey::Field(name) | GroupByKey::Subnet { field: name, .. } => {
                                cols.push(resolve_field_name(name).to_string());
                            }
                            GroupByKey::TimeBucket(_) => {
                                cols.push("flowcusExportTime".to_string());
                            }
                        }
                    }
                    for func in functions {
                        if let Some(field) = &func.field {
                            cols.push(resolve_field_name(field).to_string());
                        }
                    }
                }
                AggExpr::TopN { by, .. } | AggExpr::Sort { by, .. } => {
                    if let Some(field) = &by.field {
                        cols.push(resolve_field_name(field).to_string());
                    }
                }
                AggExpr::Limit(_) => {}
            },
        }
    }

    cols.sort();
    cols.dedup();
    cols
}

/// Collect column names referenced by filter expressions.
fn collect_filter_column_names(filters: &[&FilterExpr]) -> Vec<String> {
    let mut cols = Vec::new();
    for f in filters {
        collect_filter_columns(f, &mut cols);
    }
    cols.sort();
    cols.dedup();
    cols
}

fn collect_filter_columns(f: &FilterExpr, out: &mut Vec<String>) {
    match f {
        FilterExpr::And(a, b) | FilterExpr::Or(a, b) => {
            collect_filter_columns(a, out);
            collect_filter_columns(b, out);
        }
        FilterExpr::Not(inner) => collect_filter_columns(inner, out),
        FilterExpr::Ip(ip) => match &ip.direction {
            IpDirection::Src => out.push("sourceIPv4Address".to_string()),
            IpDirection::Dst => out.push("destinationIPv4Address".to_string()),
            IpDirection::Any => {
                out.push("sourceIPv4Address".to_string());
                out.push("destinationIPv4Address".to_string());
            }
            IpDirection::Named(col) => out.push(col.clone()),
        },
        FilterExpr::Port(port) => match port.direction {
            PortDirection::Src => out.push("sourceTransportPort".to_string()),
            PortDirection::Dst => out.push("destinationTransportPort".to_string()),
            PortDirection::Any => {
                out.push("sourceTransportPort".to_string());
                out.push("destinationTransportPort".to_string());
            }
        },
        FilterExpr::Proto(_) => out.push("protocolIdentifier".to_string()),
        FilterExpr::Numeric(n) => out.push(resolve_field_name(&n.field).to_string()),
        FilterExpr::StringFilter(s) => out.push(resolve_field_name(&s.field).to_string()),
        FilterExpr::Field(f) => out.push(resolve_field_name(&f.field).to_string()),
    }
}

// ---------------------------------------------------------------------------
// Column value extraction helpers
// ---------------------------------------------------------------------------

fn column_get_u8(buf: &ColumnBuffer, row: usize) -> u8 {
    match buf {
        ColumnBuffer::U8(v) => v.get(row).copied().unwrap_or(0),
        _ => 0,
    }
}

fn column_get_u16(buf: &ColumnBuffer, row: usize) -> u16 {
    match buf {
        ColumnBuffer::U16(v) => v.get(row).copied().unwrap_or(0),
        _ => 0,
    }
}

fn column_get_u32(buf: &ColumnBuffer, row: usize) -> u32 {
    match buf {
        ColumnBuffer::U32(v) => v.get(row).copied().unwrap_or(0),
        _ => 0,
    }
}

fn column_get_u64(buf: &ColumnBuffer, row: usize) -> u64 {
    match buf {
        ColumnBuffer::U64(v) => v.get(row).copied().unwrap_or(0),
        ColumnBuffer::U32(v) => v.get(row).copied().unwrap_or(0) as u64,
        ColumnBuffer::U16(v) => v.get(row).copied().unwrap_or(0) as u64,
        ColumnBuffer::U8(v) => v.get(row).copied().unwrap_or(0) as u64,
        _ => 0,
    }
}

fn column_get_u128(buf: &ColumnBuffer, row: usize) -> [u64; 2] {
    match buf {
        ColumnBuffer::U128(v) => v.get(row).copied().unwrap_or([0, 0]),
        _ => [0, 0],
    }
}

fn column_get_varlen<'a>(buf: &'a ColumnBuffer, row: usize) -> &'a [u8] {
    if let ColumnBuffer::VarLen { offsets, data } = buf {
        if row + 1 < offsets.len() {
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            if end <= data.len() && start <= end {
                return &data[start..end];
            }
        }
    }
    &[]
}

/// Convert a column value at a given row to a JSON value.
fn column_to_json(buf: &ColumnBuffer, row: usize, col_name: &str) -> serde_json::Value {
    match buf {
        ColumnBuffer::U8(v) => serde_json::Value::Number(serde_json::Number::from(
            v.get(row).copied().unwrap_or(0) as u64,
        )),
        ColumnBuffer::U16(v) => serde_json::Value::Number(serde_json::Number::from(
            v.get(row).copied().unwrap_or(0) as u64,
        )),
        ColumnBuffer::U32(v) => {
            let val = v.get(row).copied().unwrap_or(0);
            // Format IPv4 addresses as dotted strings
            if col_name.contains("IPv4")
                || col_name.contains("Ipv4")
                || col_name == "src"
                || col_name == "dst"
                || col_name == "nexthop"
            {
                let ip = std::net::Ipv4Addr::from(val);
                serde_json::Value::String(ip.to_string())
            } else {
                serde_json::Value::Number(serde_json::Number::from(val as u64))
            }
        }
        ColumnBuffer::U64(v) => {
            serde_json::Value::Number(serde_json::Number::from(v.get(row).copied().unwrap_or(0)))
        }
        ColumnBuffer::U128(v) => {
            let pair = v.get(row).copied().unwrap_or([0, 0]);
            if col_name.contains("IPv6") || col_name.contains("Ipv6") {
                let mut octets = [0u8; 16];
                octets[..8].copy_from_slice(&pair[0].to_be_bytes());
                octets[8..].copy_from_slice(&pair[1].to_be_bytes());
                let ip = std::net::Ipv6Addr::from(octets);
                serde_json::Value::String(ip.to_string())
            } else if col_name == "flowcusRowId" {
                serde_json::Value::String(crate::uuid7::format_uuid(pair))
            } else {
                serde_json::Value::String(format!("{:016x}{:016x}", pair[0], pair[1]))
            }
        }
        ColumnBuffer::Mac(v) => {
            let mac = v.get(row).copied().unwrap_or([0; 6]);
            serde_json::Value::String(format!(
                "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
            ))
        }
        ColumnBuffer::VarLen { offsets, data } => {
            if row + 1 < offsets.len() {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                if end <= data.len() && start <= end {
                    let s = String::from_utf8_lossy(&data[start..end]);
                    return serde_json::Value::String(s.into_owned());
                }
            }
            serde_json::Value::String(String::new())
        }
    }
}

// ---------------------------------------------------------------------------
// Fast filter evaluation using pre-resolved filters
// ---------------------------------------------------------------------------

/// Evaluate a pre-resolved filter against a row from decoded column buffers.
fn evaluate_resolved_filter(
    filter: &ResolvedFilter,
    row: usize,
    columns: &HashMap<String, ColumnBuffer>,
) -> bool {
    match filter {
        ResolvedFilter::And(a, b) => {
            evaluate_resolved_filter(a, row, columns) && evaluate_resolved_filter(b, row, columns)
        }
        ResolvedFilter::Or(a, b) => {
            evaluate_resolved_filter(a, row, columns) || evaluate_resolved_filter(b, row, columns)
        }
        ResolvedFilter::Not(inner) => !evaluate_resolved_filter(inner, row, columns),
        ResolvedFilter::Ip(ip) => eval_resolved_ip(ip, row, columns),
        ResolvedFilter::Port(port) => eval_resolved_port(port, row, columns),
        ResolvedFilter::Proto(proto) => eval_resolved_proto(proto, row, columns),
        ResolvedFilter::Numeric(num) => eval_resolved_numeric(num, row, columns),
        ResolvedFilter::StringFilter(sf) => eval_resolved_string(sf, row, columns),
        ResolvedFilter::Field(ff) => eval_resolved_field(ff, row, columns),
        ResolvedFilter::RowIdCursor { hi, lo } => {
            // Default query order is descending (newest first). The cursor is
            // the last (oldest) row's UUID on the previous page. To get the
            // next page of older rows, we need flowcusRowId < cursor.
            if let Some(buf) = columns.get("flowcusRowId") {
                let pair = column_get_u128(buf, row);
                pair[0] < *hi || (pair[0] == *hi && pair[1] < *lo)
            } else {
                true // v1 parts without rowId: include all rows
            }
        }
    }
}

fn eval_resolved_ip(
    ip: &ResolvedIpFilter,
    row: usize,
    columns: &HashMap<String, ColumnBuffer>,
) -> bool {
    let check = |col_name: &str| -> bool {
        let buf = match columns.get(col_name) {
            Some(b) => b,
            None => return false,
        };
        // Dispatch based on column type: U128 columns use IPv6 comparison,
        // U32 columns use IPv4 comparison.
        let matched = if matches!(buf, ColumnBuffer::U128(_)) {
            let val = column_get_u128(buf, row);
            match &ip.value {
                ResolvedIpValue::Addr128(addr) => val == *addr,
                ResolvedIpValue::Cidr128 { base_masked, mask } => {
                    (val[0] & mask[0]) == base_masked[0] && (val[1] & mask[1]) == base_masked[1]
                }
                ResolvedIpValue::List128(addrs) => addrs.contains(&val),
                // IPv4 value against IPv6 column → no match
                _ => false,
            }
        } else {
            let val = column_get_u32(buf, row);
            match &ip.value {
                ResolvedIpValue::Addr(addr) => val == *addr,
                ResolvedIpValue::Cidr { base_masked, mask } => (val & *mask) == *base_masked,
                ResolvedIpValue::List(addrs) => addrs.contains(&val),
                ResolvedIpValue::Wildcard(wo) => {
                    let octets = val.to_be_bytes();
                    wo.0.iter()
                        .enumerate()
                        .all(|(i, expected)| expected.is_none() || *expected == Some(octets[i]))
                }
                // IPv6 value against IPv4 column → no match
                _ => false,
            }
        };
        if ip.negated { !matched } else { matched }
    };

    match &ip.direction {
        IpDirection::Src => check("sourceIPv4Address"),
        IpDirection::Dst => check("destinationIPv4Address"),
        IpDirection::Any => check("sourceIPv4Address") || check("destinationIPv4Address"),
        IpDirection::Named(col_name) => check(col_name),
    }
}

fn match_resolved_port(val: u16, pv: &ResolvedPortValue) -> bool {
    match pv {
        ResolvedPortValue::Single(p) => val == *p,
        ResolvedPortValue::Range(lo, hi) => val >= *lo && val <= *hi,
        ResolvedPortValue::List(items) => items.iter().any(|item| match_resolved_port(val, item)),
        ResolvedPortValue::OpenRange(lo) => val >= *lo,
    }
}

fn eval_resolved_port(
    port: &ResolvedPortFilter,
    row: usize,
    columns: &HashMap<String, ColumnBuffer>,
) -> bool {
    let check = |col_name: &str| -> bool {
        let buf = match columns.get(col_name) {
            Some(b) => b,
            None => return false,
        };
        let val = column_get_u16(buf, row);
        let matched = match_resolved_port(val, &port.value);
        if port.negated { !matched } else { matched }
    };

    match port.direction {
        PortDirection::Src => check("sourceTransportPort"),
        PortDirection::Dst => check("destinationTransportPort"),
        PortDirection::Any => check("sourceTransportPort") || check("destinationTransportPort"),
    }
}

fn eval_resolved_proto(
    proto: &ResolvedProtoFilter,
    row: usize,
    columns: &HashMap<String, ColumnBuffer>,
) -> bool {
    let buf = match columns.get("protocolIdentifier") {
        Some(b) => b,
        None => return false,
    };
    let val = column_get_u8(buf, row);
    let matched = match &proto.value {
        ResolvedProtoValue::Single(n) => val == *n,
        ResolvedProtoValue::List(items) => items.contains(&val),
    };
    if proto.negated { !matched } else { matched }
}

fn eval_resolved_numeric(
    num: &ResolvedNumericFilter,
    row: usize,
    columns: &HashMap<String, ColumnBuffer>,
) -> bool {
    let buf = match columns.get(&num.col_name) {
        Some(b) => b,
        None => return false,
    };
    let val = column_get_u64(buf, row);
    match num.op {
        CompareOp::Eq => val == num.target,
        CompareOp::Ne => val != num.target,
        CompareOp::Gt => val > num.target,
        CompareOp::Ge => val >= num.target,
        CompareOp::Lt => val < num.target,
        CompareOp::Le => val <= num.target,
    }
}

fn eval_resolved_string(
    sf: &ResolvedStringFilter,
    row: usize,
    columns: &HashMap<String, ColumnBuffer>,
) -> bool {
    let buf = match columns.get(&sf.col_name) {
        Some(b) => b,
        None => return false,
    };
    // MAC columns need formatting to string for comparison
    if let ColumnBuffer::Mac(v) = buf {
        let mac = v.get(row).copied().unwrap_or([0; 6]);
        let formatted = format!(
            "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
        );
        let val_lower = sf.value.to_lowercase();
        return match sf.op {
            StringOp::Eq => formatted == val_lower,
            StringOp::Ne => formatted != val_lower,
            StringOp::Regex | StringOp::NotRegex => {
                let matched = formatted.contains(val_lower.as_str());
                if sf.op == StringOp::NotRegex {
                    !matched
                } else {
                    matched
                }
            }
            StringOp::In | StringOp::NotIn => {
                let matched = sf
                    .list_items
                    .iter()
                    .any(|item| formatted == item.to_lowercase());
                if sf.op == StringOp::NotIn {
                    !matched
                } else {
                    matched
                }
            }
        };
    }
    let val_bytes = column_get_varlen(buf, row);
    let val = String::from_utf8_lossy(val_bytes);

    match sf.op {
        StringOp::Eq => val == sf.value.as_str(),
        StringOp::Ne => val != sf.value.as_str(),
        StringOp::Regex | StringOp::NotRegex => {
            let matched = val.contains(&sf.value);
            if sf.op == StringOp::NotRegex {
                !matched
            } else {
                matched
            }
        }
        StringOp::In | StringOp::NotIn => {
            let matched = sf
                .list_items
                .iter()
                .any(|item| val.as_ref() == item.as_str());
            if sf.op == StringOp::NotIn {
                !matched
            } else {
                matched
            }
        }
    }
}

fn eval_resolved_field(
    ff: &ResolvedFieldFilter,
    row: usize,
    columns: &HashMap<String, ColumnBuffer>,
) -> bool {
    let buf = match columns.get(&ff.col_name) {
        Some(b) => b,
        None => return false,
    };
    let val = column_get_u64(buf, row);
    match ff.op {
        CompareOp::Eq => val == ff.target,
        CompareOp::Ne => val != ff.target,
        CompareOp::Gt => val > ff.target,
        CompareOp::Ge => val >= ff.target,
        CompareOp::Lt => val < ff.target,
        CompareOp::Le => val <= ff.target,
    }
}

// ---------------------------------------------------------------------------
// Legacy filter evaluation (needed for index_may_match which uses AST filters)
// ---------------------------------------------------------------------------

// Legacy per-row filter evaluation functions removed — the hot path now uses
// `ResolvedFilter` + `evaluate_resolved_filter` which pre-resolves all
// constants before the row loop. The `index_may_match` function still uses
// AST `FilterExpr` types directly for min/max pruning (no per-row eval).

// ---------------------------------------------------------------------------
// Column index min/max checking
// ---------------------------------------------------------------------------

/// Check if a filter could possibly match given the column index min/max.
/// Returns true if the filter might match, false if the part can be skipped.
///
/// This is called per-granule (from marks) and per-part (from column index).
/// The key correctness invariant: returning `false` means the granule/part
/// DEFINITELY has no matching rows. Returning `true` is always safe (may
/// have false positives).
fn index_may_match(
    filter: &FilterExpr,
    col_name: &str,
    min_val: &[u8; 16],
    max_val: &[u8; 16],
    storage_type: StorageType,
) -> bool {
    match filter {
        // Compound filters: recurse into children
        FilterExpr::And(a, b) => {
            // AND: if either child definitely can't match, the whole AND can't match
            index_may_match(a, col_name, min_val, max_val, storage_type)
                && index_may_match(b, col_name, min_val, max_val, storage_type)
        }
        FilterExpr::Or(a, b) => {
            // OR: can only skip if BOTH children definitely can't match
            index_may_match(a, col_name, min_val, max_val, storage_type)
                || index_may_match(b, col_name, min_val, max_val, storage_type)
        }
        FilterExpr::Not(inner) => {
            // NOT: we can prune if the inner filter DEFINITELY matches all rows
            // in the range (meaning NOT would match none). This only works when
            // all values in the range are identical and equal the filter target.
            // For most cases, conservatively return true.
            index_may_match_not(inner, col_name, min_val, max_val, storage_type)
        }
        FilterExpr::Ip(ip) => {
            let relevant = match &ip.direction {
                IpDirection::Src => col_name == "sourceIPv4Address",
                IpDirection::Dst => col_name == "destinationIPv4Address",
                // For `Any` direction (src OR dst), a single column's mark
                // cannot exclude the granule — the value may be in the other
                // column. Always return true (may match) for Any.
                IpDirection::Any => return true,
                IpDirection::Named(col) => col_name == col.as_str(),
            };
            if !relevant {
                return true; // not this column
            }
            if storage_type != StorageType::U32 && storage_type != StorageType::U128 {
                return true;
            }
            // U128 columns (IPv6): conservatively assume may match for now.
            // Min/max pruning for 128-bit values requires careful comparison.
            if storage_type == StorageType::U128 {
                return true;
            }
            let col_min = u32::from_le_bytes(min_val[..4].try_into().unwrap());
            let col_max = u32::from_le_bytes(max_val[..4].try_into().unwrap());
            match &ip.value {
                IpValue::Addr(addr) => {
                    if let Ok(a) = addr.parse::<std::net::Ipv4Addr>() {
                        let v = u32::from(a);
                        let in_range = v >= col_min && v <= col_max;
                        if ip.negated {
                            // Negated: "NOT this addr". Can skip only if ALL
                            // values in the granule equal this addr (min == max == v).
                            !(col_min == v && col_max == v)
                        } else {
                            in_range
                        }
                    } else {
                        true
                    }
                }
                IpValue::Cidr(cidr) => {
                    // Check if CIDR range overlaps with [col_min, col_max]
                    let parts: Vec<&str> = cidr.split('/').collect();
                    if parts.len() != 2 {
                        return true;
                    }
                    let base: u32 = match parts[0].parse::<std::net::Ipv4Addr>() {
                        Ok(a) => u32::from(a),
                        Err(_) => return true,
                    };
                    let prefix_len: u32 = match parts[1].parse() {
                        Ok(p) if p <= 32 => p,
                        _ => return true,
                    };
                    if prefix_len == 0 {
                        return true;
                    }
                    let mask = u32::MAX << (32 - prefix_len);
                    let cidr_min = base & mask;
                    let cidr_max = cidr_min | !mask;
                    let overlaps = col_min <= cidr_max && col_max >= cidr_min;
                    if ip.negated {
                        // Negated CIDR: can skip only if ALL values are inside the CIDR
                        // (meaning NOT would match nothing). This requires the entire
                        // granule range [col_min, col_max] to be within [cidr_min, cidr_max].
                        let fully_contained = col_min >= cidr_min && col_max <= cidr_max;
                        !fully_contained
                    } else {
                        overlaps
                    }
                }
                IpValue::List(addrs) => {
                    // Can prune if no address in the list falls within [col_min, col_max]
                    if ip.negated {
                        return true; // negated list: can't easily prune
                    }
                    addrs.iter().any(|addr| {
                        if let Ok(a) = addr.parse::<std::net::Ipv4Addr>() {
                            let v = u32::from(a);
                            v >= col_min && v <= col_max
                        } else {
                            true // unparseable: assume may match
                        }
                    })
                }
                IpValue::Wildcard(_) => true, // wildcard: can't easily prune from min/max
            }
        }
        FilterExpr::Port(port) => {
            let relevant = match port.direction {
                PortDirection::Src => col_name == "sourceTransportPort",
                PortDirection::Dst => col_name == "destinationTransportPort",
                // Same as IP: `port` (Any) = OR semantics, cannot prune by
                // a single column's mark range.
                PortDirection::Any => return true,
            };
            if !relevant {
                return true;
            }
            if storage_type != StorageType::U16 {
                return true;
            }
            let col_min = u16::from_le_bytes(min_val[..2].try_into().unwrap());
            let col_max = u16::from_le_bytes(max_val[..2].try_into().unwrap());
            match &port.value {
                PortValue::Single(p) => {
                    let in_range = *p >= col_min && *p <= col_max;
                    if port.negated {
                        !(*p == col_min && *p == col_max)
                    } else {
                        in_range
                    }
                }
                PortValue::Range(lo, hi) => {
                    let overlaps = col_min <= *hi && col_max >= *lo;
                    if port.negated {
                        let fully_contained = col_min >= *lo && col_max <= *hi;
                        !fully_contained
                    } else {
                        overlaps
                    }
                }
                PortValue::List(items) => {
                    if port.negated {
                        return true;
                    }
                    items
                        .iter()
                        .any(|item| port_value_overlaps_range(item, col_min, col_max))
                }
                PortValue::Named(name) => {
                    if let Some(p) = resolve_named_port(name) {
                        let in_range = p >= col_min && p <= col_max;
                        if port.negated {
                            !(p == col_min && p == col_max)
                        } else {
                            in_range
                        }
                    } else {
                        true
                    }
                }
                PortValue::OpenRange(lo) => {
                    if port.negated {
                        // NOT port >= lo  =>  port < lo. Can match if col_min < lo.
                        col_min < *lo
                    } else {
                        col_max >= *lo
                    }
                }
            }
        }
        FilterExpr::Proto(proto) => {
            if col_name != "protocolIdentifier" || storage_type != StorageType::U8 {
                return true;
            }
            let col_min = min_val[0];
            let col_max = max_val[0];
            match &proto.value {
                ProtoValue::Named(name) => {
                    if let Some(n) = resolve_proto(name) {
                        let in_range = n >= col_min && n <= col_max;
                        if proto.negated {
                            !(n == col_min && n == col_max)
                        } else {
                            in_range
                        }
                    } else {
                        true
                    }
                }
                ProtoValue::Number(n) => {
                    let in_range = *n >= col_min && *n <= col_max;
                    if proto.negated {
                        !(*n == col_min && *n == col_max)
                    } else {
                        in_range
                    }
                }
                ProtoValue::List(items) => {
                    if proto.negated {
                        return true;
                    }
                    items.iter().any(|item| match item {
                        ProtoValue::Named(name) => {
                            resolve_proto(name).is_none_or(|n| n >= col_min && n <= col_max)
                        }
                        ProtoValue::Number(n) => *n >= col_min && *n <= col_max,
                        ProtoValue::List(_) => true,
                    })
                }
            }
        }
        FilterExpr::Numeric(num) => {
            let resolved = resolve_field_name(&num.field);
            if col_name != resolved {
                return true;
            }
            index_may_match_numeric(
                num.op,
                resolve_numeric_target(&num.value),
                min_val,
                max_val,
                storage_type,
            )
        }
        FilterExpr::Field(ff) => {
            let resolved = resolve_field_name(&ff.field);
            if col_name != resolved {
                return true;
            }
            let target = match &ff.value {
                AstFieldValue::Integer(n) => *n,
                AstFieldValue::String(_) | AstFieldValue::List(_) => return true,
            };
            index_may_match_numeric(ff.op, target, min_val, max_val, storage_type)
        }
        FilterExpr::StringFilter(_) => true, // string min/max not stored in a comparable way
    }
}

/// Helper for numeric min/max range checks shared by Numeric and Field filters.
fn index_may_match_numeric(
    op: CompareOp,
    target: u64,
    min_val: &[u8; 16],
    max_val: &[u8; 16],
    storage_type: StorageType,
) -> bool {
    let (col_min, col_max) = match storage_type {
        StorageType::U32 => (
            u32::from_le_bytes(min_val[..4].try_into().unwrap()) as u64,
            u32::from_le_bytes(max_val[..4].try_into().unwrap()) as u64,
        ),
        StorageType::U64 => (
            u64::from_le_bytes(min_val[..8].try_into().unwrap()),
            u64::from_le_bytes(max_val[..8].try_into().unwrap()),
        ),
        StorageType::U16 => (
            u16::from_le_bytes(min_val[..2].try_into().unwrap()) as u64,
            u16::from_le_bytes(max_val[..2].try_into().unwrap()) as u64,
        ),
        StorageType::U8 => (min_val[0] as u64, max_val[0] as u64),
        _ => return true,
    };
    match op {
        CompareOp::Eq => target >= col_min && target <= col_max,
        CompareOp::Ne => !(col_min == target && col_max == target),
        CompareOp::Gt => col_max > target,
        CompareOp::Ge => col_max >= target,
        CompareOp::Lt => col_min < target,
        CompareOp::Le => col_min <= target,
    }
}

/// For NOT(inner), check if we can definitively skip a granule.
///
/// We can skip a granule for NOT(inner) only if inner DEFINITELY matches
/// every row in the granule. This happens when:
/// - inner is an exact equality check and min == max == target
fn index_may_match_not(
    inner: &FilterExpr,
    col_name: &str,
    min_val: &[u8; 16],
    max_val: &[u8; 16],
    storage_type: StorageType,
) -> bool {
    // Delegate to the inner filter and check if it matches the entire range.
    // For equality: if min == max == target, inner matches all rows, so NOT matches none.
    match inner {
        FilterExpr::Numeric(num) => {
            let resolved = resolve_field_name(&num.field);
            if col_name != resolved {
                return true;
            }
            if num.op != CompareOp::Eq {
                return true;
            }
            let target = resolve_numeric_target(&num.value);
            let (col_min, col_max) = match storage_type {
                StorageType::U32 => (
                    u32::from_le_bytes(min_val[..4].try_into().unwrap()) as u64,
                    u32::from_le_bytes(max_val[..4].try_into().unwrap()) as u64,
                ),
                StorageType::U64 => (
                    u64::from_le_bytes(min_val[..8].try_into().unwrap()),
                    u64::from_le_bytes(max_val[..8].try_into().unwrap()),
                ),
                StorageType::U16 => (
                    u16::from_le_bytes(min_val[..2].try_into().unwrap()) as u64,
                    u16::from_le_bytes(max_val[..2].try_into().unwrap()) as u64,
                ),
                StorageType::U8 => (min_val[0] as u64, max_val[0] as u64),
                _ => return true,
            };
            // If every value in the granule equals target, NOT(==target) matches nothing
            !(col_min == target && col_max == target)
        }
        _ => true, // conservative: can't determine if inner matches all rows
    }
}

/// Check if a port value overlaps with a [col_min, col_max] range.
fn port_value_overlaps_range(pv: &PortValue, col_min: u16, col_max: u16) -> bool {
    match pv {
        PortValue::Single(p) => *p >= col_min && *p <= col_max,
        PortValue::Range(lo, hi) => col_min <= *hi && col_max >= *lo,
        PortValue::List(items) => items
            .iter()
            .any(|item| port_value_overlaps_range(item, col_min, col_max)),
        PortValue::Named(name) => {
            resolve_named_port(name).is_none_or(|p| p >= col_min && p <= col_max)
        }
        PortValue::OpenRange(lo) => col_max >= *lo,
    }
}

// ---------------------------------------------------------------------------
// Bloom filter value extraction
// ---------------------------------------------------------------------------

/// Extract bytes for bloom filter lookup from a filter expression.
///
/// For AND semantics: if any bloom says "definitely not present", we can skip
/// the granule. For OR: we can only skip if ALL branches say "not present".
/// For NOT: bloom filters cannot help (not-present doesn't mean present).
///
/// Returns `(column_name, value_bytes)` pairs for point-query bloom lookups.
pub fn bloom_lookup_bytes(filter: &FilterExpr) -> Vec<(String, Vec<u8>)> {
    let mut result = Vec::new();
    match filter {
        FilterExpr::Ip(ip) if !ip.negated => {
            if let IpValue::Addr(addr) = &ip.value {
                if let Ok(a) = addr.parse::<std::net::Ipv4Addr>() {
                    let bytes = u32::from(a).to_le_bytes().to_vec();
                    match &ip.direction {
                        IpDirection::Src => {
                            result.push(("sourceIPv4Address".to_string(), bytes));
                        }
                        IpDirection::Dst => {
                            result.push(("destinationIPv4Address".to_string(), bytes));
                        }
                        IpDirection::Named(col) => {
                            result.push((col.clone(), bytes));
                        }
                        // `ip` (Any) means src OR dst — cannot use bloom AND
                        // semantics. A granule that has the value in src but
                        // not in dst would be incorrectly skipped. Skip bloom
                        // for `Any` direction; row-level filter handles it.
                        IpDirection::Any => {}
                    }
                } else if let Ok(v6) = addr.parse::<std::net::Ipv6Addr>() {
                    // IPv6: encode as 16 bytes (two u64 LE halves, matching storage)
                    let pair = ipv6_to_pair(v6);
                    let mut bytes = Vec::with_capacity(16);
                    bytes.extend_from_slice(&pair[0].to_le_bytes());
                    bytes.extend_from_slice(&pair[1].to_le_bytes());
                    match &ip.direction {
                        IpDirection::Src => {
                            result.push(("sourceIPv6Address".to_string(), bytes));
                        }
                        IpDirection::Dst => {
                            result.push(("destinationIPv6Address".to_string(), bytes));
                        }
                        IpDirection::Named(col) => {
                            result.push((col.clone(), bytes));
                        }
                        IpDirection::Any => {}
                    }
                }
            }
        }
        FilterExpr::Port(port) if !port.negated => {
            if let PortValue::Single(p) = &port.value {
                let bytes = p.to_le_bytes().to_vec();
                match port.direction {
                    PortDirection::Src => {
                        result.push(("sourceTransportPort".to_string(), bytes));
                    }
                    PortDirection::Dst => {
                        result.push(("destinationTransportPort".to_string(), bytes));
                    }
                    // Same as IP: `port` (Any) = src OR dst, bloom is AND.
                    PortDirection::Any => {}
                }
            }
        }
        FilterExpr::Proto(proto) if !proto.negated => {
            if let ProtoValue::Named(name) = &proto.value {
                if let Some(num) = resolve_proto(name) {
                    result.push(("protocolIdentifier".to_string(), vec![num]));
                }
            } else if let ProtoValue::Number(n) = &proto.value {
                result.push(("protocolIdentifier".to_string(), vec![*n]));
            }
        }
        // Numeric equality: exact match on numeric columns (ports, counters, etc.)
        FilterExpr::Numeric(num) if num.op == CompareOp::Eq => {
            let col_name = resolve_field_name(&num.field).to_string();
            let target = resolve_numeric_target(&num.value);
            // Bloom filters store values in their native LE encoding.
            // We emit 8 bytes (u64 LE) — the bloom was built from the column's
            // native width, but the hash function operates on byte slices so we
            // must match the encoding used during insert. The insert uses the
            // column's native width, so we need to match that. Since we don't
            // know the storage type here, emit all plausible widths and let
            // the caller match against the column's bloom file which exists
            // only for the correct width.
            // Actually, we can just emit the target in the native sizes that
            // the bloom was built with. The bloom_lookup consumer matches by
            // column name, so we emit multiple candidate byte widths.
            // For simplicity and correctness, we emit all four possible widths.
            // The bloom file for the column was built with the column's actual
            // storage type, so only the matching width will produce a valid
            // bloom lookup. Non-matching widths will just fail to skip
            // (false positive) which is safe.
            result.push((col_name, target.to_le_bytes().to_vec()));
        }
        // Field equality: same as numeric but on generic IPFIX IE fields
        FilterExpr::Field(ff) if ff.op == CompareOp::Eq => {
            if let AstFieldValue::Integer(n) = &ff.value {
                let col_name = resolve_field_name(&ff.field).to_string();
                result.push((col_name, n.to_le_bytes().to_vec()));
            }
        }
        FilterExpr::And(a, b) => {
            // AND: bloom lookups from both branches; if ANY bloom says "not
            // present", the granule can be skipped (correct under AND).
            result.extend(bloom_lookup_bytes(a));
            result.extend(bloom_lookup_bytes(b));
        }
        FilterExpr::Not(inner) => {
            // NOT: bloom filters cannot help skip granules for negated queries.
            // A bloom saying "not present" for the inner value means the
            // granule doesn't contain that value, which means the NOT *would*
            // match — so we can't skip. Drop through.
            let _ = inner;
        }
        _ => {}
    }
    result
}

/// Extract bloom lookup values for list-style filters (IP list, port list).
///
/// For list/IN filters, a granule can be skipped only if NONE of the list
/// values are present in the bloom. Returns `(column_name, Vec<value_bytes>)`
/// where all values must be absent to skip.
pub fn bloom_lookup_list_bytes(filter: &FilterExpr) -> Vec<(String, Vec<Vec<u8>>)> {
    let mut result = Vec::new();
    match filter {
        FilterExpr::Ip(ip) if !ip.negated => {
            if let IpValue::List(addrs) = &ip.value {
                let bytes_list: Vec<Vec<u8>> = addrs
                    .iter()
                    .filter_map(|a| {
                        a.parse::<std::net::Ipv4Addr>()
                            .ok()
                            .map(|addr| u32::from(addr).to_le_bytes().to_vec())
                    })
                    .collect();
                if !bytes_list.is_empty() {
                    match &ip.direction {
                        IpDirection::Src => {
                            result.push(("sourceIPv4Address".to_string(), bytes_list));
                        }
                        IpDirection::Dst => {
                            result.push(("destinationIPv4Address".to_string(), bytes_list));
                        }
                        IpDirection::Named(col) => {
                            result.push((col.clone(), bytes_list));
                        }
                        IpDirection::Any => {} // OR semantics, can't use bloom
                    }
                }
            }
        }
        FilterExpr::And(a, b) => {
            result.extend(bloom_lookup_list_bytes(a));
            result.extend(bloom_lookup_list_bytes(b));
        }
        _ => {}
    }
    result
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
struct AggAccumulator {
    sum: f64,
    count: u64,
    min: Option<f64>,
    max: Option<f64>,
    first: Option<f64>,
    last: Option<f64>,
    /// For uniq: collect distinct u64 values (bounded)
    distinct: Option<Vec<u64>>,
}

impl AggAccumulator {
    fn accumulate(&mut self, val: f64, raw_u64: u64, func: AggFunc) {
        self.count += 1;
        self.sum += val;
        self.min = Some(self.min.map_or(val, |m: f64| m.min(val)));
        self.max = Some(self.max.map_or(val, |m: f64| m.max(val)));
        if self.first.is_none() {
            self.first = Some(val);
        }
        self.last = Some(val);
        if func == AggFunc::Uniq {
            let distinct = self.distinct.get_or_insert_with(Vec::new);
            if distinct.len() < 100_000 && !distinct.contains(&raw_u64) {
                distinct.push(raw_u64);
            }
        }
    }

    fn result(&self, func: AggFunc) -> serde_json::Value {
        match func {
            AggFunc::Sum => json_f64(self.sum),
            AggFunc::Avg => {
                if self.count > 0 {
                    json_f64(self.sum / self.count as f64)
                } else {
                    serde_json::Value::Null
                }
            }
            AggFunc::Min => self.min.map_or(serde_json::Value::Null, json_f64),
            AggFunc::Max => self.max.map_or(serde_json::Value::Null, json_f64),
            AggFunc::Count => serde_json::Value::Number(serde_json::Number::from(self.count)),
            AggFunc::Uniq => {
                let n = self.distinct.as_ref().map_or(0, Vec::len);
                serde_json::Value::Number(serde_json::Number::from(n as u64))
            }
            AggFunc::First => self.first.map_or(serde_json::Value::Null, json_f64),
            AggFunc::Last => self.last.map_or(serde_json::Value::Null, json_f64),
            AggFunc::Rate => {
                if self.count > 0 {
                    json_f64(self.sum / self.count as f64)
                } else {
                    serde_json::Value::Null
                }
            }
            AggFunc::Stddev | AggFunc::P50 | AggFunc::P95 | AggFunc::P99 => {
                // Not implemented yet — return sum as placeholder
                json_f64(self.sum)
            }
        }
    }
}

fn json_f64(v: f64) -> serde_json::Value {
    serde_json::Number::from_f64(v)
        .map(serde_json::Value::Number)
        .unwrap_or(serde_json::Value::Null)
}

// ---------------------------------------------------------------------------
// Part deduplication: skip gen-0 parts subsumed by higher-gen parts
// ---------------------------------------------------------------------------

/// Remove parts whose time range is fully covered by a higher-generation part.
///
/// After a merge, a higher-generation part contains all data from the
/// lower-generation parts it was built from.  If both are visible at query
/// time we would double-count rows.  This function marks the lower-gen part
/// as subsumed and removes it from the list, recording skip steps in the
/// execution plan.
fn deduplicate_parts(parts: &mut Vec<PartEntry>, plan: &mut ExecutionPlan) {
    // Generation-based dedup is intentionally disabled.
    //
    // The merge process already handles concurrency correctly:
    // 1. The merged output part carries a `.merging` marker until all source
    //    parts are deleted.
    // 2. The executor skips parts with `.merging` markers (see process_part).
    // 3. After merge commits, source parts are deleted — no duplicates remain.
    //
    // Time-range-based dedup was previously used here but caused data loss:
    // during the current hour, the merge coordinator merges same-generation
    // parts in small batches (chunks of 4). This creates parts at different
    // generations that share the same time range but contain DISJOINT data.
    // Skipping lower-gen parts based on time containment by higher-gen parts
    // silently dropped rows that were never merged into the higher-gen part.
    let _ = (parts, plan);
}

// ---------------------------------------------------------------------------
// Query Executor implementation
// ---------------------------------------------------------------------------

impl QueryExecutor {
    /// Create a new executor for the given storage directory.
    pub fn new(storage_dir: &Path, granule_size: usize) -> Self {
        Self {
            table_base: storage_dir.join("flows"),
            granule_size,
            cache: Arc::new(StorageCache::default()),
            part_locks: crate::part_locks::PartLocks::new(),
        }
    }

    pub fn with_cache(
        storage_dir: &Path,
        granule_size: usize,
        cache: Arc<StorageCache>,
        part_locks: crate::part_locks::PartLocks,
    ) -> Self {
        Self {
            table_base: storage_dir.join("flows"),
            granule_size,
            cache,
            part_locks,
        }
    }

    /// Execute a parsed query and return results with the execution plan.
    ///
    /// `offset` and `limit` control pagination at the executor level.
    /// `pinned_time`: if `Some((start, end))`, use these absolute bounds
    /// instead of resolving the query's time range from `now()`. This
    /// prevents the time window from shifting during infinite scroll.
    ///
    /// For non-aggregate queries, the executor processes parts in reverse time
    /// order and stops once `offset + limit` matching rows are collected.
    /// For aggregate queries, all matching rows must be processed.
    pub fn execute(
        &self,
        query: &Query,
        offset: u64,
        limit: u64,
        pinned_time: Option<(u64, u64)>,
        pinned_columns: Option<Vec<String>>,
        cursor_row_id: Option<[u64; 2]>,
    ) -> std::io::Result<QueryResult> {
        let mut plan = ExecutionPlan {
            steps: Vec::new(),
            parts_total: 0,
            parts_skipped_by_time: 0,
            parts_skipped_by_index: 0,
            granules_total: 0,
            granules_skipped_by_bloom: 0,
            granules_skipped_by_marks: 0,
            columns_to_read: Vec::new(),
            bytes_read: 0,
        };

        // Step 1: Convert time range to unix bounds (or use pinned bounds)
        let time_prune_start = Instant::now();
        let (time_start, time_end) =
            pinned_time.unwrap_or_else(|| time_range_to_bounds(&query.time_range));
        debug!(time_start, time_end, "Query time range");

        // Step 2: Discover parts
        let table = Table::open(self.table_base.parent().unwrap_or(Path::new(".")), "flows")?;
        let all_parts = table.list_all_parts()?;
        let parts_total = all_parts.len();
        let filtered_parts = table.list_parts(Some(time_start), Some(time_end))?;
        let parts_after_time = filtered_parts.len();
        let time_prune_us = u64::try_from(time_prune_start.elapsed().as_micros()).ok();

        plan.parts_total = parts_total;
        plan.parts_skipped_by_time = parts_total.saturating_sub(parts_after_time);
        plan.steps.push(PlanStep::TimeRangePrune {
            start: time_start,
            end: time_end,
            parts_before: parts_total,
            parts_after: parts_after_time,
            duration_us: time_prune_us,
        });

        // Step 3: Determine columns to read
        let referenced_cols = collect_referenced_columns(query);
        let has_select_stage = query.stages.iter().any(|s| matches!(s, Stage::Select(_)));
        let needs_all_columns = !has_select_stage
            || query
                .stages
                .iter()
                .any(|s| matches!(s, Stage::Select(SelectExpr::All | SelectExpr::AllExcept(_))));
        plan.columns_to_read = referenced_cols.clone();

        // Step 4: Extract filters and pre-resolve constants
        let ast_filters: Vec<&FilterExpr> = query
            .stages
            .iter()
            .filter_map(|s| {
                if let Stage::Filter(f) = s {
                    Some(f)
                } else {
                    None
                }
            })
            .collect();

        let mut resolved_filters: Vec<ResolvedFilter> =
            ast_filters.iter().map(|f| resolve_filter(f)).collect();

        // Inject row-level time range filter: flowcusExportTime >= start AND <= end.
        // Part-level pruning only skips parts whose entire range is outside the
        // query window. Within a part, rows outside the window must be filtered.
        resolved_filters.push(ResolvedFilter::And(
            Box::new(ResolvedFilter::Field(ResolvedFieldFilter {
                col_name: "flowcusExportTime".into(),
                op: CompareOp::Ge,
                target: time_start,
            })),
            Box::new(ResolvedFilter::Field(ResolvedFieldFilter {
                col_name: "flowcusExportTime".into(),
                op: CompareOp::Le,
                target: time_end,
            })),
        ));

        // Inject cursor filter if provided — rows must have flowcusRowId > cursor.
        if let Some([hi, lo]) = cursor_row_id {
            resolved_filters.push(ResolvedFilter::RowIdCursor { hi, lo });
        }

        // Step 5: Deduplicate parts (skip lower-gen parts subsumed by higher-gen)
        let mut filtered_parts = filtered_parts;
        deduplicate_parts(&mut filtered_parts, &mut plan);

        // Compute total rows in range from part metadata (no column reads).
        let total_rows_in_range: u64 = {
            let mut sum = 0u64;
            for pe in &filtered_parts {
                if pe.path.join(".merging").exists() {
                    continue;
                }
                let meta_path = pe.path.join("meta.bin");
                if let Ok((meta, _)) = self.cache.get_meta(&meta_path) {
                    sum += meta.row_count;
                }
            }
            sum
        };

        // Step 6: Determine if this is an aggregate query
        let is_aggregate = query.stages.iter().any(|s| {
            matches!(
                s,
                Stage::Aggregate(AggExpr::GroupBy { .. })
                    | Stage::Aggregate(AggExpr::TopN { .. })
                    | Stage::Aggregate(AggExpr::Sort { .. })
            )
        });
        let has_explicit_limit = query
            .stages
            .iter()
            .any(|s| matches!(s, Stage::Aggregate(AggExpr::Limit(_))));

        // For non-aggregate queries, sort parts in reverse time order (newest first)
        // so we can early-terminate once we have enough rows.
        if !is_aggregate {
            filtered_parts.sort_by(|a, b| {
                b.time_max
                    .cmp(&a.time_max)
                    .then(b.time_min.cmp(&a.time_min))
            });
        }

        // Determine the needed row count for early termination.
        // For non-aggregate queries: we need offset + limit rows.
        // For aggregate queries: we need all rows.
        let needed_rows = if is_aggregate {
            u64::MAX
        } else {
            offset.saturating_add(limit)
        };

        // Determine which output columns we need (include flowcusExportTime for time range filter)
        let mut filter_col_names = collect_filter_column_names(&ast_filters);
        if !filter_col_names.contains(&"flowcusExportTime".to_string()) {
            filter_col_names.push("flowcusExportTime".to_string());
        }
        // When cursor is present, ensure flowcusRowId is loaded for filtering.
        if cursor_row_id.is_some() && !filter_col_names.contains(&"flowcusRowId".to_string()) {
            filter_col_names.push("flowcusRowId".to_string());
        }

        // Step 7: Build unified column list across all parts in the time range.
        // This must happen before processing so that every part's rows use the
        // same column ordering — otherwise rows from parts with different schemas
        // would be misaligned when concatenated.
        // When pinned_columns is provided (paginated queries), use it as the base
        // to ensure columns never disappear across pages due to background merges.
        // User-selected columns (from a Select stage) are always included so they
        // remain stable even if no part in the current time range contains them.
        let mut all_schema_columns: Vec<String> = pinned_columns.unwrap_or_default();

        // Seed with user-selected columns so they're always in schema_columns.
        for stage in &query.stages {
            if let Stage::Select(SelectExpr::Fields(fields)) = stage {
                for f in fields {
                    let col_name = f.alias.clone().unwrap_or_else(|| match &f.expr {
                        SelectFieldExpr::Field(name) => resolve_field_name(name).to_string(),
                        SelectFieldExpr::BinaryOp { left, op, right } => {
                            let op_str = match op {
                                flowcus_query::ast::ArithOp::Add => "+",
                                flowcus_query::ast::ArithOp::Sub => "-",
                                flowcus_query::ast::ArithOp::Mul => "*",
                                flowcus_query::ast::ArithOp::Div => "/",
                            };
                            format!("{left} {op_str} {right}")
                        }
                    });
                    if !all_schema_columns.contains(&col_name) {
                        all_schema_columns.push(col_name);
                    }
                }
            }
        }

        for pe in &filtered_parts {
            if pe.path.join(".merging").exists() {
                continue;
            }
            if let Ok(cols) = part::list_columns(&pe.path) {
                for col in cols {
                    if !all_schema_columns.contains(&col) {
                        all_schema_columns.push(col);
                    }
                }
            }
        }

        // Step 8: Process parts with columnar filtering
        let mut all_rows: Vec<Vec<serde_json::Value>> = Vec::new();
        let mut total_rows_scanned: u64 = 0;
        let mut total_matching_rows: u64 = 0;
        let mut parts_skipped_by_index: usize = 0;
        let mut parts_skipped_by_merge: usize = 0;
        // For aggregate queries, we accumulate column buffers + matching indices
        // instead of building JSON rows eagerly
        let mut agg_matching_indices: Vec<(usize, u32)> = Vec::new(); // (part_idx, row_idx)
        let mut agg_part_columns: Vec<HashMap<String, ColumnBuffer>> = Vec::new();

        // ================================================================
        // TopN/Sort fast path: read only the sort field column, use a heap
        // to find top-N rows, then decode remaining columns only for those.
        // ================================================================
        let topn_start = Instant::now();
        if let Some(result) = self.try_topn_fast_path(
            query,
            &filtered_parts,
            &ast_filters,
            &resolved_filters,
            &filter_col_names,
            &referenced_cols,
            needs_all_columns,
            &mut plan,
            parts_total,
            parts_after_time,
            time_start,
            time_end,
            total_rows_in_range,
            topn_start,
        ) {
            return result;
        }

        // Helper: process one part, handling errors inline.
        // Acquires a read lock so merges can't delete the part while we read it.
        let process_one_part = |part_entry: &PartEntry,
                                local_plan: &mut ExecutionPlan,
                                remaining_rows: Option<usize>|
         -> std::io::Result<ProcessPartResult> {
            if part_entry.path.join(".merging").exists() {
                local_plan.steps.push(PlanStep::PartSkippedMerging {
                    path: part_entry.path.display().to_string(),
                });
                return Ok(ProcessPartResult::SkippedMerge);
            }

            // Acquire read lock — if a merge is deleting this part, skip it.
            let locked_result = self.part_locks.with_read(&part_entry.path, || {
                self.process_part_columnar(
                    part_entry,
                    &ast_filters,
                    &resolved_filters,
                    &referenced_cols,
                    &filter_col_names,
                    needs_all_columns,
                    is_aggregate,
                    &all_schema_columns,
                    remaining_rows,
                    local_plan,
                )
            });

            // Lock timeout → merge is deleting this part, skip it
            let Some(inner_result) = locked_result else {
                local_plan.steps.push(PlanStep::PartSkippedMerging {
                    path: part_entry.path.display().to_string(),
                });
                return Ok(ProcessPartResult::SkippedMerge);
            };

            match inner_result {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    let subsumed = filtered_parts.iter().any(|other| {
                        other.generation > part_entry.generation
                            && other.time_min <= part_entry.time_min
                            && other.time_max >= part_entry.time_max
                    });
                    if subsumed {
                        debug!(error = %e, part = %part_entry.path.display(),
                                "Part not found but subsumed by higher-gen part, skipping");
                    } else {
                        tracing::warn!(error = %e, part = %part_entry.path.display(),
                                "Part not found — data may be temporarily unavailable during merge");
                    }
                    local_plan.steps.push(PlanStep::PartSkippedMerge {
                        path: part_entry.path.display().to_string(),
                    });
                    Ok(ProcessPartResult::SkippedMerge)
                }
                Err(e) => {
                    debug!(error = %e, part = %part_entry.path.display(), "Error processing part, skipping");
                    Ok(ProcessPartResult::SkippedMerge)
                }
                ok => ok,
            }
        };

        // Macro-like helper to merge a part result into accumulators.
        // Inlined to avoid borrow checker issues with closures.
        macro_rules! merge_part_result {
            ($result:expr, $local_plan:expr) => {
                plan.steps.extend($local_plan.steps);
                plan.bytes_read += $local_plan.bytes_read;
                plan.granules_skipped_by_bloom += $local_plan.granules_skipped_by_bloom;
                plan.granules_skipped_by_marks += $local_plan.granules_skipped_by_marks;
                match $result {
                    ProcessPartResult::Skipped => {
                        parts_skipped_by_index += 1;
                    }
                    ProcessPartResult::SkippedMerge => {
                        parts_skipped_by_merge += 1;
                    }
                    ProcessPartResult::Rows {
                        rows,
                        rows_scanned,
                        total_matching,
                    } => {
                        total_rows_scanned += rows_scanned;
                        total_matching_rows += total_matching;
                        all_rows.extend(rows);
                    }
                    ProcessPartResult::ColumnarData {
                        columns: part_cols,
                        matching_indices,
                        rows_scanned,
                        ..
                    } => {
                        total_rows_scanned += rows_scanned;
                        total_matching_rows += matching_indices.len() as u64;
                        let part_idx = agg_part_columns.len();
                        agg_part_columns.push(part_cols);
                        for idx in matching_indices {
                            agg_matching_indices.push((part_idx, idx));
                        }
                    }
                }
            };
        }

        // Collect per-part timing steps separately so they always appear
        // right after the ParallelScan header in the plan.
        let mut part_scan_steps: Vec<PlanStep> = Vec::new();

        if is_aggregate {
            // ================================================================
            // Aggregate queries: parallel scan — must process ALL parts.
            // ================================================================
            struct ParPartResult {
                result: ProcessPartResult,
                local_plan: ExecutionPlan,
                part_path: String,
                duration_us: Option<u64>,
            }

            let parallel_start = Instant::now();

            let par_results: Vec<ParPartResult> = filtered_parts
                .par_iter()
                .map(|part_entry| {
                    let part_start = Instant::now();
                    let mut local_plan = ExecutionPlan::default();
                    let result = process_one_part(part_entry, &mut local_plan, None)
                        .unwrap_or(ProcessPartResult::SkippedMerge);
                    let dur = u64::try_from(part_start.elapsed().as_micros()).ok();
                    let path = part_entry
                        .path
                        .file_name()
                        .map(|n| n.to_string_lossy().into_owned())
                        .unwrap_or_default();
                    ParPartResult {
                        result,
                        local_plan,
                        part_path: path,
                        duration_us: dur,
                    }
                })
                .collect();

            let parallel_us = u64::try_from(parallel_start.elapsed().as_micros()).ok();
            plan.steps.push(PlanStep::ParallelScan {
                parts_count: par_results.len(),
                duration_us: parallel_us,
            });

            for par in &par_results {
                let (rows_scanned, rows_matched) = match &par.result {
                    ProcessPartResult::Rows {
                        total_matching,
                        rows_scanned,
                        ..
                    } => (*rows_scanned, *total_matching),
                    ProcessPartResult::ColumnarData {
                        matching_indices,
                        rows_scanned,
                        ..
                    } => (*rows_scanned, matching_indices.len() as u64),
                    _ => (0, 0),
                };
                part_scan_steps.push(PlanStep::PartScan {
                    part: par.part_path.clone(),
                    rows_scanned,
                    rows_matched,
                    bytes_read: par.local_plan.bytes_read,
                    duration_us: par.duration_us,
                });
            }

            plan.steps.extend(part_scan_steps);

            for par in par_results {
                merge_part_result!(par.result, par.local_plan);
            }

            // Guard: cap aggregate memory to prevent OOM on huge time ranges.
            // 10M matching indices ≈ ~120 MB; beyond this is likely a mistake.
            if agg_matching_indices.len() > AGG_MAX_MATCHING_ROWS {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "Aggregate query matched {} rows, exceeding limit of {AGG_MAX_MATCHING_ROWS}. \
                         Narrow the time range or add filters.",
                        agg_matching_indices.len()
                    ),
                ));
            }
        } else {
            // ================================================================
            // Non-aggregate queries: sequential scan with early exit.
            // Stops as soon as we have enough rows for offset + limit.
            // ================================================================
            let scan_start = Instant::now();
            let mut parts_scanned_count = 0usize;

            for part_entry in &filtered_parts {
                let part_start = Instant::now();
                let mut local_plan = ExecutionPlan::default();
                let remaining = (needed_rows as usize).saturating_sub(all_rows.len());
                let result = process_one_part(part_entry, &mut local_plan, Some(remaining));
                let part_dur = u64::try_from(part_start.elapsed().as_micros()).ok();
                let path = part_entry
                    .path
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default();

                if let Ok(result) = result {
                    let (rows_scanned, rows_matched) = match &result {
                        ProcessPartResult::Rows {
                            rows, rows_scanned, ..
                        } => (*rows_scanned, rows.len() as u64),
                        ProcessPartResult::ColumnarData {
                            matching_indices,
                            rows_scanned,
                            ..
                        } => (*rows_scanned, matching_indices.len() as u64),
                        _ => (0, 0),
                    };
                    part_scan_steps.push(PlanStep::PartScan {
                        part: path,
                        rows_scanned,
                        rows_matched,
                        bytes_read: local_plan.bytes_read,
                        duration_us: part_dur,
                    });
                    parts_scanned_count += 1;
                    merge_part_result!(result, local_plan);
                }

                if all_rows.len() as u64 >= needed_rows {
                    break;
                }
            }

            let scan_us = u64::try_from(scan_start.elapsed().as_micros()).ok();
            plan.steps.push(PlanStep::ParallelScan {
                parts_count: parts_scanned_count,
                duration_us: scan_us,
            });
            plan.steps.extend(part_scan_steps);
        }

        plan.parts_skipped_by_index = parts_skipped_by_index;

        // Step 8: Determine output columns
        let output_columns = self.determine_output_columns_from_schema(query, &all_schema_columns);

        // Step 9: Apply aggregation or finalize results
        let agg_start = Instant::now();
        let (result_columns, result_rows) = if is_aggregate {
            self.apply_aggregation_columnar(
                query,
                &output_columns,
                &all_rows,
                &agg_part_columns,
                &agg_matching_indices,
                &all_schema_columns,
            )
        } else {
            // For non-aggregate: rows are already Vec<Vec<Value>>
            // Apply default sort by flowcusExportTime desc if no explicit sort
            if !has_explicit_limit
                && !query
                    .stages
                    .iter()
                    .any(|s| matches!(s, Stage::Aggregate(AggExpr::Sort { .. })))
            {
                // Find flowcusExportTime column index in output
                let time_col_idx = all_schema_columns
                    .iter()
                    .position(|c| c == "flowcusExportTime");
                if let Some(idx) = time_col_idx {
                    all_rows.sort_by(|a, b| {
                        let va = a.get(idx).and_then(|v| v.as_u64()).unwrap_or(0);
                        let vb = b.get(idx).and_then(|v| v.as_u64()).unwrap_or(0);
                        vb.cmp(&va) // descending
                    });
                }
            }

            // Apply explicit query limit if present
            let query_limit = query.stages.iter().find_map(|s| {
                if let Stage::Aggregate(AggExpr::Limit(n)) = s {
                    Some(*n)
                } else {
                    None
                }
            });
            if let Some(lim) = query_limit {
                all_rows.truncate(lim as usize);
                total_matching_rows = total_matching_rows.min(lim);
            }

            // Map rows from unified column order to output column order.
            // Skip the remap when columns already match (common case).
            if output_columns == all_schema_columns {
                (output_columns.clone(), all_rows)
            } else {
                let mapped_rows =
                    self.map_rows_to_output(&all_rows, &all_schema_columns, &output_columns);
                (output_columns.clone(), mapped_rows)
            }
        };

        let agg_us = u64::try_from(agg_start.elapsed().as_micros()).ok();

        // Push aggregate step if this was an aggregate query
        if is_aggregate {
            plan.steps.push(PlanStep::Aggregate {
                function: "aggregation".to_string(),
                groups: result_rows.len(),
                duration_us: agg_us,
            });
        }

        // Step 10: Apply pagination (offset/limit) to the result
        let total_result_rows = result_rows.len() as u64;
        let paginated_rows = if !is_aggregate {
            let start = (offset as usize).min(result_rows.len());
            let end = (start + limit as usize).min(result_rows.len());
            result_rows[start..end].to_vec()
        } else {
            total_matching_rows = total_result_rows;
            result_rows
        };

        let total_parts_skipped =
            (plan.parts_skipped_by_time + parts_skipped_by_index + parts_skipped_by_merge) as u64;
        let total_parts_scanned = parts_after_time
            .saturating_sub(parts_skipped_by_index)
            .saturating_sub(parts_skipped_by_merge) as u64;

        // Append storage cache statistics
        let (cache_hits, cache_misses) = self.cache.stats();
        plan.steps.push(PlanStep::CacheStats {
            hits: cache_hits,
            misses: cache_misses,
        });

        // Extract next_cursor from the last row's flowcusRowId column.
        let next_cursor = extract_row_cursor(&paginated_rows, &result_columns);

        Ok(QueryResult {
            columns: result_columns,
            rows: paginated_rows,
            plan,
            rows_scanned: total_rows_scanned,
            parts_scanned: total_parts_scanned,
            parts_skipped: total_parts_skipped,
            total_matching_rows,
            total_rows_in_range,
            time_start,
            time_end,
            schema_columns: all_schema_columns,
            next_cursor,
        })
    }

    /// Look up a single row by its flowcusRowId.
    ///
    /// Uses UUIDv7 timestamp to narrow the time range, then bloom filter on
    /// flowcusRowId to find the containing granule, then linear scan for exact match.
    /// Returns all columns for the matched row.
    pub fn execute_single_row(&self, row_id: [u64; 2]) -> std::io::Result<Option<SingleRowResult>> {
        let ts_ms = crate::uuid7::extract_timestamp_ms(row_id);

        // Search ±1 hour around the embedded timestamp.
        let hour_ms = 3_600_000;
        let time_start = ts_ms.saturating_sub(hour_ms);
        let time_end = ts_ms.saturating_add(hour_ms);

        let table =
            crate::table::Table::open(self.table_base.parent().unwrap_or(Path::new(".")), "flows")?;
        let parts = table.list_parts(Some(time_start), Some(time_end))?;

        let _target_hex = format!("{:016x}{:016x}", row_id[0], row_id[1]);

        for pe in &parts {
            let part_dir = &pe.path;
            // NOTE: we intentionally do NOT skip parts with a `.merging`
            // marker here. The marker is a crash-recovery journal created
            // *after* the part data is fully written and fsync'd, so the
            // data is safe to read. Skipping these parts causes a race
            // where row-by-id returns 404: source parts are deleted while
            // the new merged part is still hidden behind `.merging`.

            // Check if this part has flowcusRowId column.
            let schema_path = part_dir.join("schema.bin");
            let schema = match part::read_schema_bin(&schema_path) {
                Ok(s) => s,
                Err(_) => continue,
            };

            let rowid_col = schema.columns.iter().find(|c| c.name == "flowcusRowId");
            if rowid_col.is_none() {
                continue;
            }

            // Try bloom filter to skip this part quickly.
            let bloom_path = part_dir.join("columns").join("flowcusRowId.bloom");
            if bloom_path.exists() {
                if let Ok((_bits, blooms, _cached)) = self.cache.get_rowid_blooms(&bloom_path) {
                    let mut hash_bytes = [0u8; 16];
                    hash_bytes[..8].copy_from_slice(&row_id[0].to_le_bytes());
                    hash_bytes[8..].copy_from_slice(&row_id[1].to_le_bytes());
                    let may_contain = blooms.iter().any(|b| b.may_contain(&hash_bytes));
                    if !may_contain {
                        continue;
                    }
                }
            }

            // Decode flowcusRowId column and find the row.
            let col_path = part_dir.join("columns").join("flowcusRowId.col");
            if !col_path.exists() {
                continue;
            }

            let rowid_buf = crate::decode::decode_column(&col_path, StorageType::U128)?;
            let _row_count = rowid_buf.row_count();

            let matched_row = match &rowid_buf {
                ColumnBuffer::U128(v) => v.iter().position(|r| *r == row_id),
                _ => None,
            };

            if let Some(row_idx) = matched_row {
                // Found! Decode all columns for this row.
                let mut values = HashMap::new();
                let mut col_names = Vec::new();

                for col_def in &schema.columns {
                    col_names.push(col_def.name.clone());
                    let col_path = part_dir
                        .join("columns")
                        .join(format!("{}.col", col_def.name));
                    if !col_path.exists() {
                        values.insert(col_def.name.clone(), serde_json::Value::Null);
                        continue;
                    }
                    let buf = crate::decode::decode_column(&col_path, col_def.storage_type)?;
                    let val = column_to_json(&buf, row_idx, &col_def.name);
                    values.insert(col_def.name.clone(), val);
                }

                return Ok(Some(SingleRowResult {
                    columns: col_names,
                    values,
                }));
            }
        }

        Ok(None)
    }

    /// Map rows from part-schema column ordering to output column ordering.
    fn map_rows_to_output(
        &self,
        rows: &[Vec<serde_json::Value>],
        schema_columns: &[String],
        output_columns: &[String],
    ) -> Vec<Vec<serde_json::Value>> {
        // Build index mapping: output_col -> schema_col index
        let index_map: Vec<Option<usize>> = output_columns
            .iter()
            .map(|out_col| schema_columns.iter().position(|s| s == out_col))
            .collect();

        rows.iter()
            .map(|row| {
                index_map
                    .iter()
                    .map(|idx| {
                        idx.and_then(|i| row.get(i))
                            .cloned()
                            .unwrap_or(serde_json::Value::Null)
                    })
                    .collect()
            })
            .collect()
    }

    /// Determine output columns from the query and discovered schema columns.
    fn determine_output_columns_from_schema(
        &self,
        query: &Query,
        schema_columns: &[String],
    ) -> Vec<String> {
        for stage in &query.stages {
            if let Stage::Select(sel) = stage {
                match sel {
                    SelectExpr::Fields(fields) => {
                        return fields
                            .iter()
                            .map(|f| {
                                f.alias.clone().unwrap_or_else(|| match &f.expr {
                                    SelectFieldExpr::Field(name) => {
                                        resolve_field_name(name).to_string()
                                    }
                                    SelectFieldExpr::BinaryOp { left, op, right } => {
                                        let op_str = match op {
                                            flowcus_query::ast::ArithOp::Add => "+",
                                            flowcus_query::ast::ArithOp::Sub => "-",
                                            flowcus_query::ast::ArithOp::Mul => "*",
                                            flowcus_query::ast::ArithOp::Div => "/",
                                        };
                                        format!("{left} {op_str} {right}")
                                    }
                                })
                            })
                            .collect();
                    }
                    SelectExpr::All => {
                        let mut cols = schema_columns.to_vec();
                        cols.sort();
                        return cols;
                    }
                    SelectExpr::AllExcept(excluded) => {
                        let mut cols: Vec<String> = schema_columns
                            .iter()
                            .filter(|c| !excluded.contains(c))
                            .cloned()
                            .collect();
                        cols.sort();
                        return cols;
                    }
                }
            }
        }

        // Default (no select stage): return all schema columns sorted
        let mut cols = schema_columns.to_vec();
        cols.sort();
        cols
    }

    /// Apply aggregation for aggregate queries, operating on columnar data
    /// when possible.
    fn apply_aggregation_columnar(
        &self,
        query: &Query,
        output_columns: &[String],
        json_rows: &[Vec<serde_json::Value>],
        agg_part_columns: &[HashMap<String, ColumnBuffer>],
        agg_matching_indices: &[(usize, u32)],
        schema_columns: &[String],
    ) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
        // If we have json_rows (from non-columnar path), convert to HashMap for
        // backward compat with the existing aggregation code
        if !json_rows.is_empty() {
            let hash_rows: Vec<HashMap<String, serde_json::Value>> = json_rows
                .iter()
                .map(|row| {
                    schema_columns
                        .iter()
                        .zip(row.iter())
                        .map(|(name, val)| (name.clone(), val.clone()))
                        .collect()
                })
                .collect();
            return self.apply_aggregation_hashmap(query, output_columns, &hash_rows);
        }

        // For columnar aggregate data, reconstruct HashMap rows from the column
        // buffers. This is the path for aggregate queries that used ColumnarData.
        if !agg_matching_indices.is_empty() {
            let hash_rows: Vec<HashMap<String, serde_json::Value>> = agg_matching_indices
                .iter()
                .map(|(part_idx, row_idx)| {
                    let cols = &agg_part_columns[*part_idx];
                    let mut row_map = HashMap::new();
                    for (col_name, buf) in cols {
                        row_map.insert(
                            col_name.clone(),
                            column_to_json(buf, *row_idx as usize, col_name),
                        );
                    }
                    row_map
                })
                .collect();
            return self.apply_aggregation_hashmap(query, output_columns, &hash_rows);
        }

        // No data
        (output_columns.to_vec(), Vec::new())
    }

    /// Apply aggregation using HashMap-based rows (shared logic).
    fn apply_aggregation_hashmap(
        &self,
        query: &Query,
        output_columns: &[String],
        rows: &[HashMap<String, serde_json::Value>],
    ) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
        let mut agg_stage = None;
        let mut limit = None;

        for stage in &query.stages {
            match stage {
                Stage::Aggregate(agg) => match agg {
                    AggExpr::Limit(n) => limit = Some(*n),
                    _ => agg_stage = Some(agg),
                },
                _ => {}
            }
        }

        if let Some(agg) = agg_stage {
            match agg {
                AggExpr::GroupBy { keys, functions } => {
                    self.execute_group_by(keys, functions, rows, limit)
                }
                AggExpr::TopN { n, by, bottom } => {
                    self.execute_top_n(*n, by, *bottom, rows, output_columns)
                }
                AggExpr::Sort { by, descending } => {
                    self.execute_sort(by, *descending, rows, output_columns, limit)
                }
                AggExpr::Limit(n) => {
                    let limited: Vec<_> = rows.iter().take(*n as usize).collect();
                    let result_rows = limited
                        .iter()
                        .map(|row| {
                            output_columns
                                .iter()
                                .map(|c| row.get(c).cloned().unwrap_or(serde_json::Value::Null))
                                .collect()
                        })
                        .collect();
                    (output_columns.to_vec(), result_rows)
                }
            }
        } else {
            let result_rows: Vec<Vec<serde_json::Value>> = if let Some(lim) = limit {
                rows.iter()
                    .take(lim as usize)
                    .map(|row| {
                        output_columns
                            .iter()
                            .map(|c| row.get(c).cloned().unwrap_or(serde_json::Value::Null))
                            .collect()
                    })
                    .collect()
            } else {
                rows.iter()
                    .map(|row| {
                        output_columns
                            .iter()
                            .map(|c| row.get(c).cloned().unwrap_or(serde_json::Value::Null))
                            .collect()
                    })
                    .collect()
            };
            (output_columns.to_vec(), result_rows)
        }
    }

    fn execute_group_by(
        &self,
        keys: &[GroupByKey],
        functions: &[AggCall],
        rows: &[HashMap<String, serde_json::Value>],
        limit: Option<u64>,
    ) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
        let mut groups: HashMap<String, (Vec<serde_json::Value>, Vec<AggAccumulator>)> =
            HashMap::new();

        for row in rows {
            // Build group key
            let mut key_parts = Vec::new();
            let mut key_values = Vec::new();
            for gk in keys {
                let (col_name, val) = match gk {
                    GroupByKey::Field(name) => {
                        let resolved = resolve_field_name(name);
                        let val = row
                            .get(resolved)
                            .cloned()
                            .unwrap_or(serde_json::Value::Null);
                        (resolved.to_string(), val)
                    }
                    GroupByKey::Subnet { field, prefix_len } => {
                        let resolved = resolve_field_name(field);
                        let val = row
                            .get(resolved)
                            .cloned()
                            .unwrap_or(serde_json::Value::Null);
                        // Apply subnet mask
                        let masked = if let Some(s) = val.as_str() {
                            if let Ok(ip) = s.parse::<std::net::Ipv4Addr>() {
                                let bits = u32::from(ip);
                                let mask = if *prefix_len >= 32 {
                                    u32::MAX
                                } else {
                                    u32::MAX << (32 - prefix_len)
                                };
                                let masked_ip = std::net::Ipv4Addr::from(bits & mask);
                                serde_json::Value::String(format!("{masked_ip}/{prefix_len}"))
                            } else {
                                val
                            }
                        } else {
                            val
                        };
                        (format!("{resolved}/{prefix_len}"), masked)
                    }
                    GroupByKey::TimeBucket(dur) => {
                        let secs = duration_to_secs(dur);
                        let val = row
                            .get("flowcusExportTime")
                            .cloned()
                            .unwrap_or(serde_json::Value::Null);
                        let bucketed = if let Some(n) = val.as_u64() {
                            let bucket = (n / secs) * secs;
                            serde_json::Value::Number(serde_json::Number::from(bucket))
                        } else {
                            val
                        };
                        ("time_bucket".to_string(), bucketed)
                    }
                };
                key_parts.push(format!("{val}"));
                key_values.push((col_name, val));
            }

            let key = key_parts.join("|");
            let entry = groups.entry(key).or_insert_with(|| {
                let accumulators = functions
                    .iter()
                    .map(|_| AggAccumulator::default())
                    .collect();
                let vals = key_values.iter().map(|(_, v)| v.clone()).collect();
                (vals, accumulators)
            });

            // Accumulate
            for (i, func) in functions.iter().enumerate() {
                let field_name = func
                    .field
                    .as_ref()
                    .map(|f| resolve_field_name(f).to_string());
                let val = if let Some(ref f) = field_name {
                    row.get(f.as_str())
                        .and_then(|v| v.as_f64().or_else(|| v.as_u64().map(|n| n as f64)))
                        .unwrap_or(0.0)
                } else {
                    1.0 // for count()
                };
                let raw = if let Some(ref f) = field_name {
                    row.get(f.as_str()).and_then(|v| v.as_u64()).unwrap_or(0)
                } else {
                    0
                };
                entry.1[i].accumulate(val, raw, func.func);
            }
        }

        // Build output columns
        let mut col_names = Vec::new();
        for gk in keys {
            match gk {
                GroupByKey::Field(name) => {
                    col_names.push(resolve_field_name(name).to_string());
                }
                GroupByKey::Subnet { field, prefix_len } => {
                    col_names.push(format!("{}/{prefix_len}", resolve_field_name(field)));
                }
                GroupByKey::TimeBucket(_) => {
                    col_names.push("time_bucket".to_string());
                }
            }
        }
        for func in functions {
            let name = match (&func.func, &func.field) {
                (f, Some(field)) => format!("{f:?}({field})").to_lowercase(),
                (f, None) => format!("{f:?}()").to_lowercase(),
            };
            col_names.push(name);
        }

        let mut result_rows: Vec<Vec<serde_json::Value>> = groups
            .into_values()
            .map(|(key_vals, accumulators)| {
                let mut row = key_vals;
                for (i, func) in functions.iter().enumerate() {
                    row.push(accumulators[i].result(func.func));
                }
                row
            })
            .collect();

        // Sort by first aggregate descending by default
        if !functions.is_empty() {
            let agg_col_idx = keys.len();
            result_rows.sort_by(|a, b| {
                let va = a.get(agg_col_idx).and_then(|v| v.as_f64()).unwrap_or(0.0);
                let vb = b.get(agg_col_idx).and_then(|v| v.as_f64()).unwrap_or(0.0);
                vb.partial_cmp(&va).unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        if let Some(lim) = limit {
            result_rows.truncate(lim as usize);
        }

        (col_names, result_rows)
    }

    fn execute_top_n(
        &self,
        n: u64,
        by: &AggCall,
        bottom: bool,
        rows: &[HashMap<String, serde_json::Value>],
        output_columns: &[String],
    ) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
        let field_name = by.field.as_ref().map(|f| resolve_field_name(f).to_string());

        let mut scored: Vec<(f64, &HashMap<String, serde_json::Value>)> = rows
            .iter()
            .map(|row| {
                let val = if let Some(ref f) = field_name {
                    row.get(f.as_str())
                        .and_then(|v| v.as_f64().or_else(|| v.as_u64().map(|n| n as f64)))
                        .unwrap_or(0.0)
                } else {
                    1.0
                };
                (val, row)
            })
            .collect();

        if bottom {
            scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        } else {
            scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        }

        scored.truncate(n as usize);

        let result_rows = scored
            .iter()
            .map(|(_, row)| {
                output_columns
                    .iter()
                    .map(|c| row.get(c).cloned().unwrap_or(serde_json::Value::Null))
                    .collect()
            })
            .collect();

        (output_columns.to_vec(), result_rows)
    }

    fn execute_sort(
        &self,
        by: &AggCall,
        descending: bool,
        rows: &[HashMap<String, serde_json::Value>],
        output_columns: &[String],
        limit: Option<u64>,
    ) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
        let field_name = by.field.as_ref().map(|f| resolve_field_name(f).to_string());

        let mut sorted: Vec<&HashMap<String, serde_json::Value>> = rows.iter().collect();
        sorted.sort_by(|a, b| {
            let va = if let Some(ref f) = field_name {
                a.get(f.as_str())
                    .and_then(|v| v.as_f64().or_else(|| v.as_u64().map(|n| n as f64)))
                    .unwrap_or(0.0)
            } else {
                0.0
            };
            let vb = if let Some(ref f) = field_name {
                b.get(f.as_str())
                    .and_then(|v| v.as_f64().or_else(|| v.as_u64().map(|n| n as f64)))
                    .unwrap_or(0.0)
            } else {
                0.0
            };
            if descending {
                vb.partial_cmp(&va).unwrap_or(std::cmp::Ordering::Equal)
            } else {
                va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
            }
        });

        if let Some(lim) = limit {
            sorted.truncate(lim as usize);
        }

        let result_rows = sorted
            .iter()
            .map(|row| {
                output_columns
                    .iter()
                    .map(|c| row.get(c).cloned().unwrap_or(serde_json::Value::Null))
                    .collect()
            })
            .collect();

        (output_columns.to_vec(), result_rows)
    }

    /// TopN/Sort fast path: instead of decoding all columns for all rows,
    /// read only the sort field column, use a binary heap to identify the
    /// top-N row indices, then decode remaining columns only for those rows.
    ///
    /// This reduces memory from O(rows * columns) to O(rows * 1) + O(N * columns),
    /// which is the difference between 10GB RSS and a few MB for large datasets.
    ///
    /// Handles both TopN and Sort stages. For Sort, N = limit or a default cap.
    /// Also works WITH filters: applies filter first, then scans only matching
    /// indices for the sort field.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn try_topn_fast_path(
        &self,
        query: &Query,
        filtered_parts: &[PartEntry],
        ast_filters: &[&FilterExpr],
        resolved_filters: &[ResolvedFilter],
        filter_col_names: &[String],
        referenced_cols: &[String],
        needs_all_columns: bool,
        plan: &mut ExecutionPlan,
        _parts_total: usize,
        _parts_after_time: usize,
        time_start: u64,
        time_end: u64,
        total_rows_in_range: u64,
        step_start: Instant,
    ) -> Option<std::io::Result<QueryResult>> {
        // Extract TopN or Sort stage
        let (n, sort_field, bottom) = self.extract_topn_params(query)?;

        debug!(
            n,
            sort_field,
            bottom,
            parts = filtered_parts.len(),
            "TopN fast path activated"
        );

        // Use a scored-row heap to track top-N across all parts.
        // For top-N (descending), we want a min-heap so we can evict the smallest.
        // For bottom-N (ascending), we want a max-heap so we can evict the largest.
        let mut heap: BinaryHeap<ScoredRow> = BinaryHeap::new();
        let mut total_rows_scanned: u64 = 0;
        let mut parts_scanned: u64 = 0;
        let mut parts_skipped_merge: usize = 0;
        let mut parts_skipped_index: usize = 0;

        // We need to track which parts + row indices made it into the heap,
        // so we can go back and decode remaining columns for just those rows.
        // Store part paths so we can re-read columns later.
        let mut part_paths: Vec<PathBuf> = Vec::new();
        // Map from part index in our vec -> schema info
        let mut part_schemas: Vec<(HashMap<String, StorageType>, Vec<String>)> = Vec::new();

        for part_entry in filtered_parts {
            if part_entry.path.join(".merging").exists() {
                parts_skipped_merge += 1;
                plan.steps.push(PlanStep::PartSkippedMerging {
                    path: part_entry.path.display().to_string(),
                });
                continue;
            }

            // Read metadata
            let meta = match self
                .cache
                .get_meta(&part_entry.path.join("meta.bin"))
                .map(|(m, _)| m)
            {
                Ok(m) => m,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    parts_skipped_merge += 1;
                    plan.steps.push(PlanStep::PartSkippedMerge {
                        path: part_entry.path.display().to_string(),
                    });
                    continue;
                }
                Err(e) => return Some(Err(e)),
            };
            let row_count = meta.row_count as usize;

            // Check for incomplete parts
            match part::list_columns(&part_entry.path) {
                Ok(cols) if cols.is_empty() => continue,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                _ => {}
            }

            // Read schema for column types
            let schema = match part::read_schema_bin(&part_entry.path.join("schema.bin")) {
                Ok(s) => s,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    parts_skipped_merge += 1;
                    continue;
                }
                Err(_) => crate::schema::Schema {
                    columns: Vec::new(),
                    duration_source: None,
                },
            };

            let col_types: HashMap<String, StorageType> = schema
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.storage_type))
                .collect();

            // Column index min/max pruning
            let index_path = part_entry.path.join("column_index.bin");
            if index_path.exists() && !ast_filters.is_empty() {
                if let Ok(col_index) = self.cache.get_column_index(&index_path).map(|(e, _)| e) {
                    let mut skip = false;
                    for (i, idx_entry) in col_index.iter().enumerate() {
                        if let Some(col_def) = schema.columns.get(i) {
                            for filter in ast_filters {
                                if !index_may_match(
                                    filter,
                                    &col_def.name,
                                    &idx_entry.min_value,
                                    &idx_entry.max_value,
                                    col_def.storage_type,
                                ) {
                                    skip = true;
                                    break;
                                }
                            }
                            if skip {
                                break;
                            }
                        }
                    }
                    if skip {
                        parts_skipped_index += 1;
                        continue;
                    }
                }
            }

            let available_columns = match part::list_columns(&part_entry.path) {
                Ok(c) => c,
                Err(_) => continue,
            };

            // Phase 1: Decode filter columns and build matching indices
            let matching_indices = if resolved_filters.is_empty() {
                // No filters: all rows match
                None // sentinel for "all rows"
            } else {
                let filter_cols_available: Vec<String> = filter_col_names
                    .iter()
                    .filter(|c| available_columns.contains(c))
                    .cloned()
                    .collect();

                let mut decoded_filter: HashMap<String, ColumnBuffer> = HashMap::new();
                for col_name in &filter_cols_available {
                    let col_path = part_entry
                        .path
                        .join("columns")
                        .join(format!("{col_name}.col"));
                    if !col_path.exists() {
                        continue;
                    }
                    let st = col_types.get(col_name).copied().unwrap_or(StorageType::U32);
                    match decode::decode_column(&col_path, st) {
                        Ok(buf) => {
                            decoded_filter.insert(col_name.clone(), buf);
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            return Some(Err(e));
                        }
                        Err(_) => {}
                    }
                }

                let granule_size = self.granule_size.max(1);
                let mut indices = Vec::new();
                for row_idx in 0..row_count {
                    let _ = granule_size; // granule mask not applied in fast path for simplicity
                    let passes = resolved_filters
                        .iter()
                        .all(|f| evaluate_resolved_filter(f, row_idx, &decoded_filter));
                    if passes {
                        indices.push(row_idx as u32);
                    }
                }
                Some(indices)
            };

            // Phase 2: Decode ONLY the sort field column
            let sort_col_path = part_entry
                .path
                .join("columns")
                .join(format!("{sort_field}.col"));
            if !sort_col_path.exists() {
                // Sort field not in this part — skip it
                total_rows_scanned += row_count as u64;
                parts_scanned += 1;
                continue;
            }

            let sort_st = col_types
                .get(&sort_field)
                .copied()
                .unwrap_or(StorageType::U64);
            let sort_buf = match decode::decode_column(&sort_col_path, sort_st) {
                Ok(buf) => buf,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    parts_skipped_merge += 1;
                    continue;
                }
                Err(e) => {
                    debug!(error = %e, "Failed to decode sort column");
                    continue;
                }
            };

            let part_idx = part_paths.len();
            part_paths.push(part_entry.path.clone());
            let col_names: Vec<String> = available_columns.clone();
            part_schemas.push((col_types.clone(), col_names));

            // Phase 3: Scan sort values, maintain top-N heap
            let rows_to_scan: Box<dyn Iterator<Item = u32>> = match &matching_indices {
                Some(indices) => Box::new(indices.iter().copied()),
                None => Box::new(0..row_count as u32),
            };

            for row_idx in rows_to_scan {
                let value = column_get_u64(&sort_buf, row_idx as usize);
                let scored = ScoredRow {
                    part_idx,
                    row_idx,
                    value,
                    bottom,
                };

                if heap.len() < n {
                    heap.push(scored);
                } else if let Some(worst) = heap.peek() {
                    let dominated = if bottom {
                        // For bottom-N (ascending), max-heap: evict if new value is smaller
                        value < worst.value
                    } else {
                        // For top-N (descending), min-heap via Reverse ordering: evict if new value is larger
                        value > worst.value
                    };
                    if dominated {
                        heap.pop();
                        heap.push(scored);
                    }
                }
            }

            total_rows_scanned += row_count as u64;
            parts_scanned += 1;
        }

        // Phase 4: Extract top-N results and decode remaining columns
        let mut top_rows: Vec<ScoredRow> = heap.into_vec();
        // Sort in final order: descending for top, ascending for bottom
        if bottom {
            top_rows.sort_by(|a, b| a.value.cmp(&b.value));
        } else {
            top_rows.sort_by(|a, b| b.value.cmp(&a.value));
        }

        plan.steps.push(PlanStep::TopNFastPath {
            sort_field: sort_field.clone(),
            n,
            parts_scanned: parts_scanned as usize,
            rows_scanned: total_rows_scanned,
            duration_us: u64::try_from(step_start.elapsed().as_micros()).ok(),
        });
        plan.parts_skipped_by_index = parts_skipped_index;

        // Determine output columns — seed with user-selected columns for stability.
        let mut all_schema_columns: Vec<String> = Vec::new();
        for stage in &query.stages {
            if let Stage::Select(SelectExpr::Fields(fields)) = stage {
                for f in fields {
                    let col_name = f.alias.clone().unwrap_or_else(|| match &f.expr {
                        SelectFieldExpr::Field(name) => resolve_field_name(name).to_string(),
                        SelectFieldExpr::BinaryOp { left, op, right } => {
                            let op_str = match op {
                                flowcus_query::ast::ArithOp::Add => "+",
                                flowcus_query::ast::ArithOp::Sub => "-",
                                flowcus_query::ast::ArithOp::Mul => "*",
                                flowcus_query::ast::ArithOp::Div => "/",
                            };
                            format!("{left} {op_str} {right}")
                        }
                    });
                    if !all_schema_columns.contains(&col_name) {
                        all_schema_columns.push(col_name);
                    }
                }
            }
        }
        for (_, col_names) in &part_schemas {
            for col in col_names {
                if !all_schema_columns.contains(col) {
                    all_schema_columns.push(col.clone());
                }
            }
        }
        let output_columns = self.determine_output_columns_from_schema(query, &all_schema_columns);

        // Group top rows by part index to batch column reads
        let mut rows_by_part: HashMap<usize, Vec<(usize, u32)>> = HashMap::new();
        for (result_idx, scored) in top_rows.iter().enumerate() {
            rows_by_part
                .entry(scored.part_idx)
                .or_default()
                .push((result_idx, scored.row_idx));
        }

        // Decode output columns only for the top-N rows
        let mut result_rows: Vec<Vec<serde_json::Value>> = vec![Vec::new(); top_rows.len()];

        for (part_idx, row_indices) in &rows_by_part {
            let part_path = &part_paths[*part_idx];
            let (col_types, available) = &part_schemas[*part_idx];

            // Determine which columns to read for this part
            let cols_to_read: Vec<String> = if needs_all_columns {
                available.clone()
            } else {
                referenced_cols
                    .iter()
                    .filter(|c| available.contains(c))
                    .cloned()
                    .collect()
            };

            // Also include any output columns not in referenced_cols
            let mut all_needed: Vec<String> = cols_to_read;
            for col in &output_columns {
                if available.contains(col) && !all_needed.contains(col) {
                    all_needed.push(col.clone());
                }
            }

            // Decode needed columns for this part
            let mut decoded: HashMap<String, ColumnBuffer> = HashMap::new();
            for col_name in &all_needed {
                let col_path = part_path.join("columns").join(format!("{col_name}.col"));
                if !col_path.exists() {
                    continue;
                }
                let st = col_types.get(col_name).copied().unwrap_or(StorageType::U32);
                match decode::decode_column(&col_path, st) {
                    Ok(buf) => {
                        decoded.insert(col_name.clone(), buf);
                    }
                    Err(_) => {}
                }
            }

            // Build result rows for these indices
            for (result_idx, row_idx) in row_indices {
                let row: Vec<serde_json::Value> = output_columns
                    .iter()
                    .map(|name| {
                        decoded
                            .get(name)
                            .map(|buf| column_to_json(buf, *row_idx as usize, name))
                            .unwrap_or(serde_json::Value::Null)
                    })
                    .collect();
                result_rows[*result_idx] = row;
            }
        }

        let total_parts_skipped =
            (plan.parts_skipped_by_time + parts_skipped_index + parts_skipped_merge) as u64;

        let next_cursor = extract_row_cursor(&result_rows, &output_columns);

        Some(Ok(QueryResult {
            columns: output_columns,
            rows: result_rows,
            plan: std::mem::take(plan),
            rows_scanned: total_rows_scanned,
            parts_scanned,
            parts_skipped: total_parts_skipped,
            total_matching_rows: n as u64,
            total_rows_in_range,
            time_start,
            time_end,
            schema_columns: all_schema_columns,
            next_cursor,
        }))
    }

    /// Extract TopN/Sort parameters from the query stages.
    /// Returns (n, sort_field_name, is_bottom) or None if not a TopN/Sort query.
    fn extract_topn_params(&self, query: &Query) -> Option<(usize, String, bool)> {
        let mut limit_val: Option<u64> = None;

        for stage in &query.stages {
            if let Stage::Aggregate(AggExpr::Limit(n)) = stage {
                limit_val = Some(*n);
            }
        }

        for stage in &query.stages {
            match stage {
                Stage::Aggregate(AggExpr::TopN { n, by, bottom }) => {
                    let field = by
                        .field
                        .as_ref()
                        .map(|f| resolve_field_name(f).to_string())?;
                    return Some((*n as usize, field, *bottom));
                }
                Stage::Aggregate(AggExpr::Sort { by, descending }) => {
                    let field = by
                        .field
                        .as_ref()
                        .map(|f| resolve_field_name(f).to_string())?;
                    // For Sort, use the explicit limit or a default cap
                    let n = limit_val.unwrap_or(10_000) as usize;
                    return Some((n, field, !*descending));
                }
                _ => {}
            }
        }
        None
    }

    /// Build a time histogram using granule marks and bloom filters.
    ///
    /// Reads `flowcusExportTime.mrk` per part for precise per-granule time
    /// ranges and row counts. When filters are present, checks `.bloom` files
    /// to estimate which granules might match (upper-bound estimate).
    ///
    /// Optimized path:
    /// - Parts are processed in parallel via rayon
    /// - Bucket distribution uses binary search (O(log n) per granule)
    /// - Parts fitting a single bucket skip granule-level work entirely
    ///
    /// Calls `on_progress` periodically with partial results.
    pub fn histogram_from_metadata(
        &self,
        time_start: u64,
        time_end: u64,
        bucket_ms: u64,
        bloom_lookups: &[(String, Vec<u8>)],
        mut on_progress: impl FnMut(&HistogramResult),
    ) -> std::io::Result<HistogramResult> {
        let table = Table::open(self.table_base.parent().unwrap_or(Path::new(".")), "flows")?;
        let filtered_parts = { table.list_parts(Some(time_start), Some(time_end))? };

        // Build aligned bucket starts (just timestamps, counts computed separately).
        let aligned_start = (time_start / bucket_ms) * bucket_ms;
        let num_buckets = time_end.saturating_sub(aligned_start).div_ceil(bucket_ms) as usize;
        let bucket_starts: Vec<u64> = (0..num_buckets)
            .map(|i| aligned_start + (i as u64) * bucket_ms)
            .collect();

        // Process parts in parallel — each produces a local counts vector.
        let part_results: Vec<(Vec<u64>, u64)> = filtered_parts
            .par_iter()
            .filter_map(|pe| {
                if pe.path.join(".merging").exists() {
                    return None;
                }

                // Part-level column index pruning: skip parts where bloom
                // filters definitively exclude the part. Check if the part
                // has bloom files and if ALL bloom lookups miss for ALL
                // granules in the part, the part has zero matching rows.
                // This is done inside histogram_process_part via the bloom mask.

                let (counts, rows) =
                    self.histogram_process_part(pe, &bucket_starts, bucket_ms, bloom_lookups);
                if rows > 0 { Some((counts, rows)) } else { None }
            })
            .collect();

        // Merge parallel results into final buckets.
        let mut counts = vec![0u64; num_buckets];
        let mut last_emitted_rows: u64 = 0;
        let mut parts_since_emit: usize = 0;

        for (part_counts, _part_rows) in &part_results {
            for (i, &c) in part_counts.iter().enumerate() {
                counts[i] += c;
            }
            parts_since_emit += 1;

            let running_total: u64 = counts.iter().sum();
            if parts_since_emit >= 5
                || running_total > last_emitted_rows + last_emitted_rows / 50 + 100
            {
                let result = HistogramResult {
                    buckets: bucket_starts
                        .iter()
                        .zip(counts.iter())
                        .map(|(&ts, &c)| (ts, c))
                        .collect(),
                    total_rows: running_total,
                    time_start,
                    time_end,
                };
                on_progress(&result);
                last_emitted_rows = running_total;
                parts_since_emit = 0;
            }
        }

        let buckets: Vec<(u64, u64)> = bucket_starts
            .iter()
            .zip(counts.iter())
            .map(|(&ts, &c)| (ts, c))
            .collect();

        // Use sum of bucket counts as total — these are proportionally
        // distributed to only cover the query time range, unlike raw
        // part row counts which may extend beyond the window.
        let total_rows: u64 = buckets.iter().map(|(_, c)| c).sum();

        Ok(HistogramResult {
            buckets,
            total_rows,
            time_start,
            time_end,
        })
    }

    /// Filtered histogram: reads filter columns + export time, evaluates
    /// filters row-by-row, and directly buckets matching rows.
    ///
    /// Much faster than a full `execute()` because:
    /// - No output columns decoded (only filter columns + export time)
    /// - No JSON conversion or aggregation framework
    /// - Bloom and mark skipping applied per-granule
    /// - Parallel part processing via rayon
    pub fn histogram_filtered(
        &self,
        query: &Query,
        time_start: u64,
        time_end: u64,
        bucket_ms: u64,
        mut on_progress: impl FnMut(&HistogramResult),
    ) -> std::io::Result<HistogramResult> {
        let table = Table::open(self.table_base.parent().unwrap_or(Path::new(".")), "flows")?;
        let filtered_parts = table.list_parts(Some(time_start), Some(time_end))?;

        // Extract filter expressions from query stages.
        let ast_filters: Vec<&FilterExpr> = query
            .stages
            .iter()
            .filter_map(|s| {
                if let Stage::Filter(f) = s {
                    Some(f)
                } else {
                    None
                }
            })
            .collect();

        // Pre-resolve filters once (avoids repeated parsing per-row).
        let resolved_filters: Vec<ResolvedFilter> =
            ast_filters.iter().map(|f| resolve_filter(f)).collect();

        // Extract bloom and mark lookup data.
        let bloom_lookups: Vec<(String, Vec<u8>)> = ast_filters
            .iter()
            .flat_map(|f| bloom_lookup_bytes(f))
            .collect();
        let bloom_list_lookups: Vec<(String, Vec<Vec<u8>>)> = ast_filters
            .iter()
            .flat_map(|f| bloom_lookup_list_bytes(f))
            .collect();

        // Filter column names.
        let filter_col_names = collect_filter_column_names(&ast_filters);

        let aligned_start = (time_start / bucket_ms) * bucket_ms;
        let num_buckets = time_end.saturating_sub(aligned_start).div_ceil(bucket_ms) as usize;
        let bucket_starts: Vec<u64> = (0..num_buckets)
            .map(|i| aligned_start + (i as u64) * bucket_ms)
            .collect();

        // Process parts in parallel.
        let part_results: Vec<(Vec<u64>, u64)> = filtered_parts
            .par_iter()
            .filter_map(|pe| {
                if pe.path.join(".merging").exists() {
                    return None;
                }
                let result = self.histogram_filtered_part(
                    pe,
                    &bucket_starts,
                    bucket_ms,
                    &ast_filters,
                    &resolved_filters,
                    &bloom_lookups,
                    &bloom_list_lookups,
                    &filter_col_names,
                );
                match result {
                    Ok((counts, rows)) if rows > 0 => Some((counts, rows)),
                    _ => None,
                }
            })
            .collect();

        // Merge.
        let mut counts = vec![0u64; num_buckets];
        let mut total_rows: u64 = 0;
        let mut last_emitted_rows: u64 = 0;
        let mut parts_since_emit: usize = 0;

        for (part_counts, part_rows) in &part_results {
            for (i, &c) in part_counts.iter().enumerate() {
                counts[i] += c;
            }
            total_rows += part_rows;
            parts_since_emit += 1;
            if parts_since_emit >= 5
                || total_rows > last_emitted_rows + last_emitted_rows / 50 + 100
            {
                let result = HistogramResult {
                    buckets: bucket_starts
                        .iter()
                        .zip(counts.iter())
                        .map(|(&ts, &c)| (ts, c))
                        .collect(),
                    total_rows,
                    time_start,
                    time_end,
                };
                on_progress(&result);
                last_emitted_rows = total_rows;
                parts_since_emit = 0;
            }
        }

        Ok(HistogramResult {
            buckets: bucket_starts
                .iter()
                .zip(counts.iter())
                .map(|(&ts, &c)| (ts, c))
                .collect(),
            total_rows,
            time_start,
            time_end,
        })
    }

    /// Process a single part for the filtered histogram.
    /// Reads only filter columns + flowcusExportTime, applies filters,
    /// and directly buckets matching rows by their export time.
    #[allow(clippy::too_many_arguments)]
    fn histogram_filtered_part(
        &self,
        pe: &PartEntry,
        bucket_starts: &[u64],
        bucket_ms: u64,
        ast_filters: &[&FilterExpr],
        resolved_filters: &[ResolvedFilter],
        bloom_lookups: &[(String, Vec<u8>)],
        bloom_list_lookups: &[(String, Vec<Vec<u8>>)],
        filter_col_names: &[String],
    ) -> std::io::Result<(Vec<u64>, u64)> {
        let num_buckets = bucket_starts.len();
        let mut counts = vec![0u64; num_buckets];
        let part_dir = &pe.path;

        // Read metadata.
        let meta = self
            .cache
            .get_meta(&part_dir.join("meta.bin"))
            .map(|(m, _)| m)?;
        let row_count = meta.row_count as usize;
        if row_count == 0 {
            return Ok((counts, 0));
        }

        // Read schema for column types.
        let schema = match part::read_schema_bin(&part_dir.join("schema.bin")) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(e),
            Err(_) => crate::schema::Schema {
                columns: Vec::new(),
                duration_source: None,
            },
        };
        let col_types: HashMap<String, StorageType> = schema
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.storage_type))
            .collect();

        // Column index min/max pruning.
        let index_path = part_dir.join("column_index.bin");
        if index_path.exists() {
            if let Ok((col_index, _)) = self.cache.get_column_index(&index_path) {
                for (i, idx_entry) in col_index.iter().enumerate() {
                    if let Some(col_def) = schema.columns.get(i) {
                        for filter in ast_filters {
                            if !index_may_match(
                                filter,
                                &col_def.name,
                                &idx_entry.min_value,
                                &idx_entry.max_value,
                                col_def.storage_type,
                            ) {
                                return Ok((counts, 0));
                            }
                        }
                    }
                }
            }
        }

        // Build granule mask from bloom + mark checks.
        let granule_size = self.granule_size.max(1);
        let mut granule_mask: Option<Vec<bool>> = None;

        // Bloom filter checks.
        for (col_name, value_bytes) in bloom_lookups {
            let bloom_path = part_dir.join("columns").join(format!("{col_name}.bloom"));
            if let Ok((_bits, blooms, _)) = self.cache.get_blooms_for_column(&bloom_path, col_name)
            {
                let mask = granule_mask.get_or_insert_with(|| vec![true; blooms.len()]);
                let col_st = col_types.get(col_name).copied().unwrap_or(StorageType::U32);
                let effective_bytes: &[u8] = match col_st.element_size() {
                    Some(n) if n < value_bytes.len() => &value_bytes[..n],
                    _ => value_bytes,
                };
                for (gi, bloom) in blooms.iter().enumerate() {
                    if gi < mask.len() && mask[gi] && !bloom.may_contain(effective_bytes) {
                        mask[gi] = false;
                    }
                }
            }
        }

        // List bloom checks.
        for (col_name, values_list) in bloom_list_lookups {
            let bloom_path = part_dir.join("columns").join(format!("{col_name}.bloom"));
            if let Ok((_bits, blooms, _)) = self.cache.get_blooms_for_column(&bloom_path, col_name)
            {
                let mask = granule_mask.get_or_insert_with(|| vec![true; blooms.len()]);
                for (gi, bloom) in blooms.iter().enumerate() {
                    if gi < mask.len() && mask[gi] {
                        if !values_list.iter().any(|v| bloom.may_contain(v)) {
                            mask[gi] = false;
                        }
                    }
                }
            }
        }

        // Mark-based range pruning on filter columns.
        for col_name in filter_col_names {
            let marks_path = part_dir.join("columns").join(format!("{col_name}.mrk"));
            if let Ok((_gs, marks, _)) = self.cache.get_marks_for_column(&marks_path, col_name) {
                let st = col_types.get(col_name).copied().unwrap_or(StorageType::U32);
                let mask = granule_mask.get_or_insert_with(|| vec![true; marks.len()]);
                for (gi, mark) in marks.iter().enumerate() {
                    if gi < mask.len() && mask[gi] {
                        for filter in ast_filters {
                            if !index_may_match(
                                filter,
                                col_name,
                                &mark.min_value,
                                &mark.max_value,
                                st,
                            ) {
                                mask[gi] = false;
                                break;
                            }
                        }
                    }
                }
            }
        }

        // If all granules are masked out, skip this part entirely.
        if let Some(ref mask) = granule_mask {
            if mask.iter().all(|&m| !m) {
                return Ok((counts, 0));
            }
        }

        // Decode filter columns.
        let available_columns = part::list_columns(part_dir)?;
        let mut decoded: HashMap<String, ColumnBuffer> = HashMap::new();
        for col_name in filter_col_names {
            if available_columns.contains(col_name) {
                let col_path = part_dir.join("columns").join(format!("{col_name}.col"));
                if let Ok(buf) = decode::decode_column(
                    &col_path,
                    col_types.get(col_name).copied().unwrap_or(StorageType::U32),
                ) {
                    decoded.insert(col_name.clone(), buf);
                }
            }
        }

        // Decode flowcusExportTime column for bucketing (now U64 milliseconds).
        let export_time_col = if !decoded.contains_key("flowcusExportTime") {
            let col_path = part_dir.join("columns/flowcusExportTime.col");
            if col_path.exists() {
                decode::decode_column(&col_path, StorageType::U64).ok()
            } else {
                None
            }
        } else {
            decoded.get("flowcusExportTime").cloned()
        };

        let time_values = match &export_time_col {
            Some(ColumnBuffer::U64(v)) => v.as_slice(),
            _ => {
                // No export time column — fall back to metadata distribution.
                let (meta_counts, meta_rows) =
                    self.histogram_process_part(pe, bucket_starts, bucket_ms, &[]);
                return Ok((meta_counts, meta_rows));
            }
        };

        // Evaluate filters row-by-row, with granule skipping.
        let mut total_matching: u64 = 0;
        let process_row = |row_idx: usize| -> bool {
            resolved_filters.is_empty()
                || resolved_filters
                    .iter()
                    .all(|f| evaluate_resolved_filter(f, row_idx, &decoded))
        };

        // Bounds for skipping rows outside the bucket window — bucket_idx
        // clamps out-of-range timestamps which would inflate boundary buckets.
        let win_start = bucket_starts[0];
        let win_end = bucket_starts[num_buckets - 1] + bucket_ms;

        if let Some(ref mask) = granule_mask {
            for (granule_idx, &active) in mask.iter().enumerate() {
                if !active {
                    continue;
                }
                let row_start = granule_idx * granule_size;
                let row_end = ((granule_idx + 1) * granule_size).min(row_count);
                for row_idx in row_start..row_end {
                    if process_row(row_idx) {
                        if let Some(&ts) = time_values.get(row_idx) {
                            if ts >= win_start && ts < win_end {
                                let bi = bucket_idx(bucket_starts, bucket_ms, ts);
                                if let Some(c) = counts.get_mut(bi) {
                                    *c += 1;
                                }
                                total_matching += 1;
                            }
                        }
                    }
                }
            }
        } else {
            for row_idx in 0..row_count {
                if process_row(row_idx) {
                    if let Some(&ts) = time_values.get(row_idx) {
                        if ts >= win_start && ts < win_end {
                            let bi = bucket_idx(bucket_starts, bucket_ms, ts);
                            if let Some(c) = counts.get_mut(bi) {
                                *c += 1;
                            }
                            total_matching += 1;
                        }
                    }
                }
            }
        }

        Ok((counts, total_matching))
    }

    /// Process a single part for the histogram. Returns (per-bucket counts, total rows).
    fn histogram_process_part(
        &self,
        pe: &PartEntry,
        bucket_starts: &[u64],
        bucket_ms: u64,
        bloom_lookups: &[(String, Vec<u8>)],
    ) -> (Vec<u64>, u64) {
        let num_buckets = bucket_starts.len();
        let mut counts = vec![0u64; num_buckets];
        let mut total_rows: u64 = 0;

        // Fast path: if the entire part fits within a single bucket AND no
        // bloom filters are active, skip granule-level work entirely.
        // Guard: only when the part is genuinely inside the bucket window —
        // bucket_idx clamps out-of-range timestamps, which would incorrectly
        // dump all rows into a boundary bucket.
        let window_end = bucket_starts[num_buckets - 1] + bucket_ms;
        if bucket_ms > 0
            && bloom_lookups.is_empty()
            && pe.time_min >= bucket_starts[0]
            && pe.time_max < window_end
        {
            let part_first_bucket = bucket_idx(bucket_starts, bucket_ms, pe.time_min);
            let part_last_bucket = bucket_idx(bucket_starts, bucket_ms, pe.time_max);
            if part_first_bucket == part_last_bucket {
                let meta_path = pe.path.join("meta.bin");
                if let Ok((meta, _)) = self.cache.get_meta(&meta_path) {
                    if meta.row_count > 0 {
                        if let Some(c) = counts.get_mut(part_first_bucket) {
                            *c += meta.row_count;
                        }
                        return (counts, meta.row_count);
                    }
                }
                return (counts, 0);
            }
        }

        let mrk_path = pe.path.join("columns/flowcusExportTime.mrk");
        let marks_result = self.cache.get_marks(&mrk_path);

        match marks_result {
            Ok((_granule_size, ref marks, _cached)) if !marks.is_empty() => {
                // Build per-granule bloom mask if bloom lookups are requested.
                let bloom_mask: Option<Vec<bool>> = if bloom_lookups.is_empty() {
                    None
                } else {
                    let mut mask = vec![true; marks.len()];
                    for (col_name, value_bytes) in bloom_lookups {
                        let bloom_path = pe.path.join(format!("columns/{col_name}.bloom"));
                        let blooms = match self.cache.get_blooms_for_column(&bloom_path, col_name) {
                            Ok((_bits, blooms, _cached)) => blooms,
                            Err(_) => continue,
                        };
                        for (i, masked) in mask.iter_mut().enumerate() {
                            if *masked {
                                if let Some(bloom) = blooms.get(i) {
                                    if !bloom.may_contain(value_bytes) {
                                        *masked = false;
                                    }
                                }
                            }
                        }
                    }
                    Some(mask)
                };

                for (gi, mark) in marks.iter().enumerate() {
                    if let Some(ref mask) = bloom_mask {
                        if !mask[gi] {
                            continue;
                        }
                    }
                    if mark.row_count == 0 {
                        continue;
                    }

                    let g_min = u64::from_le_bytes(mark.min_value[..8].try_into().unwrap());
                    let g_max = u64::from_le_bytes(mark.max_value[..8].try_into().unwrap());
                    let row_count = u64::from(mark.row_count);
                    total_rows += row_count;

                    distribute_rows_to_buckets(
                        &mut counts,
                        bucket_starts,
                        g_min,
                        g_max,
                        row_count,
                        bucket_ms,
                    );
                }
            }
            _ => {
                let meta_path = pe.path.join("meta.bin");
                let meta = match self.cache.get_meta(&meta_path) {
                    Ok((m, _)) => m,
                    Err(_) => return (counts, 0),
                };
                if meta.row_count == 0 {
                    return (counts, 0);
                }
                total_rows += meta.row_count;
                distribute_rows_to_buckets(
                    &mut counts,
                    bucket_starts,
                    meta.time_min,
                    meta.time_max,
                    meta.row_count,
                    bucket_ms,
                );
            }
        }

        (counts, total_rows)
    }
}

/// Find the bucket index for a timestamp using direct arithmetic.
#[inline]
fn bucket_idx(bucket_starts: &[u64], bucket_ms: u64, ts: u64) -> usize {
    if bucket_starts.is_empty() || bucket_ms == 0 {
        return 0;
    }
    // Direct arithmetic — buckets are uniformly spaced.
    let first = bucket_starts[0];
    if ts < first {
        return 0;
    }
    let idx = ((ts - first) / bucket_ms) as usize;
    idx.min(bucket_starts.len() - 1)
}

/// Distribute `row_count` rows from time range `[t_min, t_max]` into
/// histogram bucket counts using O(1) bucket lookup + overlap iteration.
fn distribute_rows_to_buckets(
    counts: &mut [u64],
    bucket_starts: &[u64],
    t_min: u64,
    t_max: u64,
    row_count: u64,
    bucket_ms: u64,
) {
    if counts.is_empty() || bucket_ms == 0 {
        return;
    }

    if t_min == t_max {
        // Point timestamp — direct index, but skip if outside the window
        // (bucket_idx clamps, which would inflate boundary buckets).
        let first_start = bucket_starts[0];
        let last_end = bucket_starts[bucket_starts.len() - 1] + bucket_ms;
        if t_min >= first_start && t_min < last_end {
            let idx = bucket_idx(bucket_starts, bucket_ms, t_min);
            if let Some(c) = counts.get_mut(idx) {
                *c += row_count;
            }
        }
        return;
    }

    let span = t_max.saturating_sub(t_min).max(1);

    // Find the range of buckets that overlap [t_min, t_max].
    let first = bucket_idx(bucket_starts, bucket_ms, t_min);
    let last = bucket_idx(bucket_starts, bucket_ms, t_max);

    for i in first..=last {
        let b_start = bucket_starts[i];
        let b_end = b_start + bucket_ms;

        let overlap_start = t_min.max(b_start);
        let overlap_end = t_max.min(b_end);

        if overlap_start >= overlap_end {
            continue;
        }

        let overlap = overlap_end - overlap_start;
        let rows = (row_count * overlap + span / 2) / span;
        counts[i] += rows;
    }
}

/// A row scored by its sort-field value, used in the TopN binary heap.
///
/// Ordering is reversed based on the `bottom` flag:
/// - For top-N (descending result): min-heap, so the smallest value is at the top
///   and gets evicted when a larger value is found.
/// - For bottom-N (ascending result): max-heap, so the largest value is at the top
///   and gets evicted when a smaller value is found.
struct ScoredRow {
    part_idx: usize,
    row_idx: u32,
    value: u64,
    bottom: bool,
}

impl PartialEq for ScoredRow {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Eq for ScoredRow {}

impl PartialOrd for ScoredRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.bottom {
            // Bottom-N: max-heap (largest at top, evicted first)
            self.value.cmp(&other.value)
        } else {
            // Top-N: min-heap (smallest at top, evicted first)
            other.value.cmp(&self.value)
        }
    }
}

// ---------------------------------------------------------------------------
// Part processing
// ---------------------------------------------------------------------------

enum ProcessPartResult {
    Skipped,
    /// Skipped due to merge race (not found, .merging marker, etc.)
    SkippedMerge,
    /// Non-aggregate: rows in unified column order. `total_matching` is the
    /// full count of filter-matching rows in the part (may exceed `rows.len()`
    /// when a row budget caps materialization for early termination).
    Rows {
        rows: Vec<Vec<serde_json::Value>>,
        rows_scanned: u64,
        total_matching: u64,
    },
    /// Aggregate: keep column buffers + matching row indices to avoid
    /// premature JSON conversion. The aggregation engine operates on
    /// native typed columns for efficiency.
    ColumnarData {
        columns: HashMap<String, ColumnBuffer>,
        matching_indices: Vec<u32>,
        rows_scanned: u64,
    },
}

impl QueryExecutor {
    /// Process a single part using columnar filtering.
    ///
    /// Phase 1: Read metadata, check bloom/marks, decode ONLY filter columns
    /// Phase 2: Build matching-row indices using pre-resolved filters
    /// Phase 3: Decode remaining output columns, build Vec<Vec<Value>> directly
    ///
    /// This avoids the old approach of building `HashMap<String, Value>` per row
    /// which was the primary bottleneck.
    #[allow(clippy::too_many_arguments)]
    fn process_part_columnar(
        &self,
        part_entry: &PartEntry,
        ast_filters: &[&FilterExpr],
        resolved_filters: &[ResolvedFilter],
        referenced_cols: &[String],
        filter_col_names: &[String],
        needs_all_columns: bool,
        is_aggregate: bool,
        unified_columns: &[String],
        remaining_rows: Option<usize>,
        plan: &mut ExecutionPlan,
    ) -> std::io::Result<ProcessPartResult> {
        let part_dir = &part_entry.path;

        // Read metadata — NotFound propagates to caller for merge-race handling
        let meta = self
            .cache
            .get_meta(&part_dir.join("meta.bin"))
            .map(|(m, _)| m)?;
        let row_count = meta.row_count as usize;

        // Skip incomplete parts (directory created but columns not yet written)
        match part::list_columns(part_dir) {
            Ok(cols) if cols.is_empty() => {
                plan.steps.push(PlanStep::PartSkippedIncomplete {
                    path: part_dir.display().to_string(),
                });
                return Ok(ProcessPartResult::Skipped);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                plan.steps.push(PlanStep::PartSkippedIncomplete {
                    path: part_dir.display().to_string(),
                });
                return Ok(ProcessPartResult::Skipped);
            }
            _ => {} // has columns or other error — proceed
        }

        // Read schema to know column types
        let schema = match part::read_schema_bin(&part_dir.join("schema.bin")) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(e),
            Err(_) => crate::schema::Schema {
                columns: Vec::new(),
                duration_source: None,
            },
        };

        // Build column name -> storage type map
        let col_types: HashMap<String, StorageType> = schema
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.storage_type))
            .collect();

        // Read column index for min/max pruning
        let index_path = part_dir.join("column_index.bin");
        let col_index = if index_path.exists() {
            match self.cache.get_column_index(&index_path).map(|(e, _)| e) {
                Ok(idx) => idx,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(e),
                Err(_) => Vec::new(),
            }
        } else {
            Vec::new()
        };

        // Check column index min/max against filter predicates
        if !col_index.is_empty() && !ast_filters.is_empty() {
            let idx_start = Instant::now();
            let mut skipped_by_index = false;
            let mut skip_column = String::new();
            let mut skip_predicate = String::new();
            for (i, idx_entry) in col_index.iter().enumerate() {
                if let Some(col_def) = schema.columns.get(i) {
                    for filter in ast_filters {
                        if !index_may_match(
                            filter,
                            &col_def.name,
                            &idx_entry.min_value,
                            &idx_entry.max_value,
                            col_def.storage_type,
                        ) {
                            skipped_by_index = true;
                            skip_column = col_def.name.clone();
                            skip_predicate = format!("{filter:?}").chars().take(80).collect();
                            break;
                        }
                    }
                    if skipped_by_index {
                        break;
                    }
                }
            }
            let idx_dur = u64::try_from(idx_start.elapsed().as_micros()).ok();
            plan.steps.push(PlanStep::ColumnIndexFilter {
                column: if skipped_by_index {
                    skip_column
                } else {
                    "all".to_string()
                },
                predicate: if skipped_by_index {
                    skip_predicate
                } else {
                    "pass".to_string()
                },
                parts_skipped: usize::from(skipped_by_index),
                duration_us: idx_dur,
            });
            if skipped_by_index {
                return Ok(ProcessPartResult::Skipped);
            }
        }

        // Determine available columns in this part
        let available_columns = part::list_columns(part_dir)?;
        if available_columns.is_empty() {
            return Ok(ProcessPartResult::Skipped);
        }

        // Bloom filter check for equality predicates
        let bloom_lookups = ast_filters
            .iter()
            .flat_map(|f| bloom_lookup_bytes(f))
            .collect::<Vec<_>>();

        // List-style bloom lookups: for IN/list filters, skip granule only
        // if NONE of the list values are present in the bloom.
        let bloom_list_lookups = ast_filters
            .iter()
            .flat_map(|f| bloom_lookup_list_bytes(f))
            .collect::<Vec<_>>();

        let mut granule_mask: Option<Vec<bool>> = None;
        let num_granules = (row_count + self.granule_size - 1) / self.granule_size.max(1);
        plan.granules_total += num_granules;

        // Point-query bloom lookups: skip granule if bloom says "definitely not present"
        for (col_name, value_bytes) in &bloom_lookups {
            let bloom_step_start = Instant::now();
            let bloom_path = part_dir.join("columns").join(format!("{col_name}.bloom"));
            let bloom_result = self.cache.get_blooms_for_column(&bloom_path, col_name);
            if let Err(e) = &bloom_result {
                if e.kind() == std::io::ErrorKind::NotFound {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Part merged away: {}", bloom_path.display()),
                    ));
                }
            }
            if let Ok((_bits, blooms, _cached)) = bloom_result {
                let mask = granule_mask.get_or_insert_with(|| vec![true; blooms.len()]);
                let mut skipped = 0usize;

                // For Numeric/Field bloom lookups, value_bytes is 8 bytes (u64 LE).
                // But the bloom was built with the column's native width. Try
                // matching with the column's actual storage type width.
                let col_st = col_types.get(col_name).copied().unwrap_or(StorageType::U32);
                let effective_bytes: &[u8] = match col_st.element_size() {
                    Some(n) if n < value_bytes.len() => &value_bytes[..n],
                    _ => value_bytes,
                };

                for (gi, bloom) in blooms.iter().enumerate() {
                    if gi < mask.len() && mask[gi] {
                        if !bloom.may_contain(effective_bytes) {
                            mask[gi] = false;
                            skipped += 1;
                        }
                    }
                }
                plan.granules_skipped_by_bloom += skipped;
                plan.steps.push(PlanStep::BloomFilter {
                    column: col_name.clone(),
                    value: format!("{effective_bytes:?}").chars().take(40).collect(),
                    granules_skipped: skipped,
                    duration_us: u64::try_from(bloom_step_start.elapsed().as_micros()).ok(),
                });
            }
        }

        // List bloom lookups: skip granule only if NONE of the list values present
        for (col_name, values_list) in &bloom_list_lookups {
            let bloom_step_start = Instant::now();
            let bloom_path = part_dir.join("columns").join(format!("{col_name}.bloom"));
            let bloom_result = self.cache.get_blooms_for_column(&bloom_path, col_name);
            if let Err(e) = &bloom_result {
                if e.kind() == std::io::ErrorKind::NotFound {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Part merged away: {}", bloom_path.display()),
                    ));
                }
            }
            if let Ok((_bits, blooms, _cached)) = bloom_result {
                let mask = granule_mask.get_or_insert_with(|| vec![true; blooms.len()]);
                let mut skipped = 0usize;
                for (gi, bloom) in blooms.iter().enumerate() {
                    if gi < mask.len() && mask[gi] {
                        // Skip only if NO value in the list may be present
                        let any_present = values_list.iter().any(|v| bloom.may_contain(v));
                        if !any_present {
                            mask[gi] = false;
                            skipped += 1;
                        }
                    }
                }
                plan.granules_skipped_by_bloom += skipped;
                plan.steps.push(PlanStep::BloomFilter {
                    column: col_name.clone(),
                    value: format!("list[{}]", values_list.len()),
                    granules_skipped: skipped,
                    duration_us: u64::try_from(bloom_step_start.elapsed().as_micros()).ok(),
                });
            }
        }

        // Mark-based seeking for range predicates.
        // Only check marks for columns that are referenced by filters — marks
        // contain min/max values which are only useful for pruning against
        // filter predicates. Checking marks for output-only columns wastes I/O.
        let mark_check_cols: Vec<String> = filter_col_names
            .iter()
            .filter(|c| available_columns.contains(c))
            .cloned()
            .collect();

        for col_name in &mark_check_cols {
            let mark_step_start = Instant::now();
            let marks_path = part_dir.join("columns").join(format!("{col_name}.mrk"));
            let marks_result = self.cache.get_marks_for_column(&marks_path, col_name);
            if let Err(e) = &marks_result {
                if e.kind() == std::io::ErrorKind::NotFound {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Part merged away: {}", marks_path.display()),
                    ));
                }
            }
            if let Ok((_gs, marks, _cached)) = marks_result {
                let st = col_types.get(col_name).copied().unwrap_or(StorageType::U32);
                let mut mark_skipped = 0usize;

                for (gi, mark) in marks.iter().enumerate() {
                    let mask = granule_mask.get_or_insert_with(|| vec![true; marks.len()]);
                    if gi < mask.len() && mask[gi] {
                        let mut can_skip = false;
                        for filter in ast_filters {
                            if !index_may_match(
                                filter,
                                col_name,
                                &mark.min_value,
                                &mark.max_value,
                                st,
                            ) {
                                can_skip = true;
                                break;
                            }
                        }
                        if can_skip {
                            mask[gi] = false;
                            mark_skipped += 1;
                        }
                    }
                }

                plan.granules_skipped_by_marks += mark_skipped;
                plan.steps.push(PlanStep::MarkSeek {
                    column: col_name.clone(),
                    granule_range: (0, num_granules),
                    duration_us: u64::try_from(mark_step_start.elapsed().as_micros()).ok(),
                });
            }
        }

        // ---------------------------------------------------------------
        // Phase 1: Decode ONLY filter columns
        // ---------------------------------------------------------------
        let filter_cols_available: Vec<String> = filter_col_names
            .iter()
            .filter(|c| available_columns.contains(c))
            .cloned()
            .collect();

        // Parallel column decode for filter columns
        let filter_col_results: Vec<_> = filter_cols_available
            .par_iter()
            .filter_map(|col_name| {
                let col_read_start = Instant::now();
                let col_path = part_dir.join("columns").join(format!("{col_name}.col"));
                if !col_path.exists() {
                    return None;
                }
                let st = col_types.get(col_name).copied().unwrap_or(StorageType::U32);
                match decode::decode_column(&col_path, st) {
                    Ok(buf) => {
                        let bytes = buf.mem_size() as u64;
                        let dur = u64::try_from(col_read_start.elapsed().as_micros()).ok();
                        Some(Ok((col_name.clone(), buf, bytes, dur)))
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Some(Err(e)),
                    Err(e) => {
                        trace!(error = %e, column = col_name, "Failed to decode column");
                        None
                    }
                }
            })
            .collect();

        let mut decoded: HashMap<String, ColumnBuffer> = HashMap::new();
        for result in filter_col_results {
            match result {
                Ok((name, buf, bytes, dur)) => {
                    plan.bytes_read += bytes;
                    plan.steps.push(PlanStep::ColumnRead {
                        column: name.clone(),
                        bytes,
                        duration_us: dur,
                    });
                    decoded.insert(name, buf);
                }
                Err(e) => return Err(e),
            }
        }

        // ---------------------------------------------------------------
        // Phase 2: Build matching row indices using pre-resolved filters
        // ---------------------------------------------------------------
        // Uses granule-level skipping: when a granule is masked out by bloom
        // or mark checks, we jump past the entire granule's row range instead
        // of checking each row individually.
        let filter_start = Instant::now();
        let mut matching_indices: Vec<u32> = Vec::new();
        let granule_size = self.granule_size.max(1);

        if let Some(ref mask) = granule_mask {
            // Granule mask exists: iterate by granule, skip entire masked-out
            // granules in O(1) instead of checking each row.
            for (granule_idx, &active) in mask.iter().enumerate() {
                if !active {
                    continue;
                }
                let row_start = granule_idx * granule_size;
                let row_end = ((granule_idx + 1) * granule_size).min(row_count);
                for row_idx in row_start..row_end {
                    let passes = resolved_filters.is_empty()
                        || resolved_filters
                            .iter()
                            .all(|f| evaluate_resolved_filter(f, row_idx, &decoded));
                    if passes {
                        matching_indices.push(row_idx as u32);
                    }
                }
            }
            // Handle any rows beyond the mask (shouldn't happen, but be safe)
            let mask_covers = mask.len() * granule_size;
            if mask_covers < row_count {
                for row_idx in mask_covers..row_count {
                    let passes = resolved_filters.is_empty()
                        || resolved_filters
                            .iter()
                            .all(|f| evaluate_resolved_filter(f, row_idx, &decoded));
                    if passes {
                        matching_indices.push(row_idx as u32);
                    }
                }
            }
        } else {
            // No granule mask: scan all rows
            for row_idx in 0..row_count {
                let passes = resolved_filters.is_empty()
                    || resolved_filters
                        .iter()
                        .all(|f| evaluate_resolved_filter(f, row_idx, &decoded));
                if passes {
                    matching_indices.push(row_idx as u32);
                }
            }
        }

        let filter_us = u64::try_from(filter_start.elapsed().as_micros()).ok();
        if !resolved_filters.is_empty() {
            plan.steps.push(PlanStep::FilterApply {
                expression: format!("{} filter(s) on part", resolved_filters.len()),
                rows_before: row_count,
                rows_after: matching_indices.len(),
                duration_us: filter_us,
            });
        }

        if matching_indices.is_empty() {
            return Ok(ProcessPartResult::Skipped);
        }

        // ---------------------------------------------------------------
        // Phase 3: Decode output columns and build result rows
        // ---------------------------------------------------------------

        // Determine which columns to output
        let output_col_names: Vec<String> = if needs_all_columns {
            available_columns.clone()
        } else {
            referenced_cols
                .iter()
                .filter(|c| available_columns.contains(c))
                .cloned()
                .collect()
        };

        // Parallel decode of remaining output columns not already decoded
        let remaining_cols: Vec<String> = output_col_names
            .iter()
            .filter(|c| !decoded.contains_key(*c))
            .cloned()
            .collect();

        let output_col_results: Vec<_> = remaining_cols
            .par_iter()
            .filter_map(|col_name| {
                let col_read_start = Instant::now();
                let col_path = part_dir.join("columns").join(format!("{col_name}.col"));
                if !col_path.exists() {
                    return None;
                }
                let st = col_types.get(col_name).copied().unwrap_or(StorageType::U32);
                match decode::decode_column(&col_path, st) {
                    Ok(buf) => {
                        let bytes = buf.mem_size() as u64;
                        let dur = u64::try_from(col_read_start.elapsed().as_micros()).ok();
                        Some(Ok((col_name.clone(), buf, bytes, dur)))
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Some(Err(e)),
                    Err(e) => {
                        trace!(error = %e, column = col_name, "Failed to decode column");
                        None
                    }
                }
            })
            .collect();

        for result in output_col_results {
            match result {
                Ok((name, buf, bytes, dur)) => {
                    plan.bytes_read += bytes;
                    plan.steps.push(PlanStep::ColumnRead {
                        column: name.clone(),
                        bytes,
                        duration_us: dur,
                    });
                    decoded.insert(name, buf);
                }
                Err(e) => return Err(e),
            }
        }

        // For aggregate queries, return columnar data to avoid premature
        // JSON materialization. The aggregation engine will consume it directly.
        if is_aggregate {
            return Ok(ProcessPartResult::ColumnarData {
                columns: decoded,
                matching_indices,
                rows_scanned: row_count as u64,
            });
        }

        // Build Vec<Vec<Value>> in unified column order so rows from different
        // parts are aligned. Columns not present in this part produce Null.
        // Only materialize up to `remaining_rows` to avoid wasting resources,
        // but report `total_matching` for accurate pagination.
        let total_matching = matching_indices.len() as u64;
        let row_budget = remaining_rows.unwrap_or(usize::MAX);
        let materialize_count = matching_indices.len().min(row_budget);
        let rows: Vec<Vec<serde_json::Value>> = matching_indices[..materialize_count]
            .iter()
            .map(|&idx| {
                unified_columns
                    .iter()
                    .map(|name| match decoded.get(name) {
                        Some(buf) => column_to_json(buf, idx as usize, name),
                        None => serde_json::Value::Null,
                    })
                    .collect()
            })
            .collect();

        Ok(ProcessPartResult::Rows {
            rows,
            rows_scanned: row_count as u64,
            total_matching,
        })
    }
}

/// Extract the flowcusRowId from the last row of results for cursor pagination.
fn extract_row_cursor(rows: &[Vec<serde_json::Value>], columns: &[String]) -> Option<String> {
    let last_row = rows.last()?;
    let idx = columns.iter().position(|c| c == "flowcusRowId")?;
    match last_row.get(idx)? {
        serde_json::Value::String(s) if !s.is_empty() => Some(s.clone()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn make_part(generation: u32, time_min: u64, time_max: u64, seq: u32) -> PartEntry {
        PartEntry {
            path: PathBuf::from(format!(
                "/tmp/test/1_{generation:05}_{time_min}_{time_max}_{seq:06}"
            )),
            format_version: 1,
            generation,
            time_min,
            time_max,
            seq,
        }
    }

    fn empty_plan() -> ExecutionPlan {
        ExecutionPlan {
            steps: Vec::new(),
            parts_total: 0,
            parts_skipped_by_time: 0,
            parts_skipped_by_index: 0,
            granules_total: 0,
            granules_skipped_by_bloom: 0,
            granules_skipped_by_marks: 0,
            columns_to_read: Vec::new(),
            bytes_read: 0,
        }
    }

    #[test]
    fn test_deduplicate_removes_subsumed_parts() {
        // Dedup is now disabled — all parts are kept regardless of generation.
        // The merge process itself is responsible for deleting source parts
        // after creating the merged output.
        let mut parts = vec![
            make_part(0, 1000, 2000, 1),
            make_part(0, 2000, 3000, 2),
            make_part(1, 1000, 3000, 3),
        ];
        let mut plan = empty_plan();
        deduplicate_parts(&mut parts, &mut plan);

        assert_eq!(parts.len(), 3);
        assert!(plan.steps.is_empty());
    }

    #[test]
    fn test_deduplicate_keeps_non_overlapping() {
        // gen-0 part NOT covered by gen-1 -> both kept
        let mut parts = vec![
            make_part(0, 5000, 6000, 1), // outside gen-1's range
            make_part(1, 1000, 3000, 2),
        ];
        let mut plan = empty_plan();
        deduplicate_parts(&mut parts, &mut plan);

        assert_eq!(parts.len(), 2);
        assert!(plan.steps.is_empty());
    }

    #[test]
    fn test_deduplicate_partial_overlap_kept() {
        // gen-0 part partially overlaps gen-1 -> both kept
        let mut parts = vec![
            make_part(0, 2500, 4000, 1), // extends beyond gen-1's max
            make_part(1, 1000, 3000, 2),
        ];
        let mut plan = empty_plan();
        deduplicate_parts(&mut parts, &mut plan);

        assert_eq!(parts.len(), 2);
        assert!(plan.steps.is_empty());
    }

    #[test]
    fn test_deduplicate_same_generation_not_removed() {
        // Two gen-0 parts with overlapping ranges — neither should be removed
        let mut parts = vec![make_part(0, 1000, 2000, 1), make_part(0, 1000, 2000, 2)];
        let mut plan = empty_plan();
        deduplicate_parts(&mut parts, &mut plan);

        assert_eq!(parts.len(), 2);
    }

    #[test]
    fn test_deduplicate_exact_time_match() {
        // gen-1 has exact same time range as gen-0 -> both kept.
        // Dedup is disabled: the merge process handles cleanup by deleting
        // source parts after commit. If both still exist, they contain
        // disjoint data and must both be read.
        let mut parts = vec![make_part(0, 1000, 2000, 1), make_part(1, 1000, 2000, 2)];
        let mut plan = empty_plan();
        deduplicate_parts(&mut parts, &mut plan);

        assert_eq!(parts.len(), 2);
    }

    #[test]
    fn test_empty_part_skipped() {
        // Create a temporary part directory with meta.bin but no columns
        let tmp = std::env::temp_dir().join("flowcus_test_empty_part_skipped");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let part_dir = tmp.join("00000_1000_2000_000001");
        std::fs::create_dir_all(part_dir.join("columns")).unwrap();

        // Write a minimal meta.bin (256 bytes) with valid CRC
        let mut meta = vec![0u8; 256];
        meta[0..4].copy_from_slice(b"FMTA");
        meta[4] = 1; // version
        meta[8..16].copy_from_slice(&100u64.to_le_bytes()); // row_count
        meta[16..20].copy_from_slice(&0u32.to_le_bytes()); // generation
        meta[20..28].copy_from_slice(&1000u64.to_le_bytes()); // time_min (ms)
        meta[28..36].copy_from_slice(&2000u64.to_le_bytes()); // time_max (ms)
        // CRC32-C of bytes 0..88 stored at bytes 88..92
        let crc = crate::crc::crc32c(&meta[..88]);
        meta[88..92].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(part_dir.join("meta.bin"), &meta).unwrap();

        // No column files in columns/ — part should be skipped as incomplete
        let executor = QueryExecutor::new(&tmp, 8192);
        let part_entry = PartEntry {
            path: part_dir.clone(),
            format_version: 1,
            generation: 0,
            time_min: 1000,
            time_max: 2000,
            seq: 1,
        };

        let mut plan = empty_plan();
        let ast_filters: Vec<&FilterExpr> = vec![];
        let resolved_filters: Vec<ResolvedFilter> = vec![];
        let result = executor.process_part_columnar(
            &part_entry,
            &ast_filters,
            &resolved_filters,
            &[],
            &[],
            false,
            false,
            &[],
            None,
            &mut plan,
        );
        assert!(matches!(result, Ok(ProcessPartResult::Skipped)));
        assert!(
            plan.steps
                .iter()
                .any(|s| matches!(s, PlanStep::PartSkippedIncomplete { .. }))
        );
    }

    #[test]
    fn test_resolved_ip_filter_exact() {
        let filter = resolve_ip_filter(&IpFilter {
            direction: IpDirection::Src,
            negated: false,
            value: IpValue::Addr("10.0.0.1".to_string()),
        });
        let mut columns = HashMap::new();
        let mut col = ColumnBuffer::new(StorageType::U32);
        // Push 10.0.0.1 as u32
        let ip_val = u32::from(std::net::Ipv4Addr::new(10, 0, 0, 1));
        if let ColumnBuffer::U32(ref mut v) = col {
            v.push(ip_val);
            v.push(ip_val + 1); // 10.0.0.2
        }
        columns.insert("sourceIPv4Address".to_string(), col);

        let resolved = ResolvedFilter::Ip(filter);
        assert!(evaluate_resolved_filter(&resolved, 0, &columns));
        assert!(!evaluate_resolved_filter(&resolved, 1, &columns));
    }

    #[test]
    fn test_resolved_cidr_filter() {
        let filter = resolve_ip_filter(&IpFilter {
            direction: IpDirection::Src,
            negated: false,
            value: IpValue::Cidr("10.0.0.0/8".to_string()),
        });
        let mut columns = HashMap::new();
        let mut col = ColumnBuffer::new(StorageType::U32);
        if let ColumnBuffer::U32(ref mut v) = col {
            v.push(u32::from(std::net::Ipv4Addr::new(10, 1, 2, 3)));
            v.push(u32::from(std::net::Ipv4Addr::new(192, 168, 1, 1)));
        }
        columns.insert("sourceIPv4Address".to_string(), col);

        let resolved = ResolvedFilter::Ip(filter);
        assert!(evaluate_resolved_filter(&resolved, 0, &columns));
        assert!(!evaluate_resolved_filter(&resolved, 1, &columns));
    }

    #[test]
    fn test_resolved_port_filter() {
        let filter = resolve_port_filter(&PortFilter {
            direction: PortDirection::Dst,
            negated: false,
            value: PortValue::Range(80, 443),
        });
        let mut columns = HashMap::new();
        let mut col = ColumnBuffer::new(StorageType::U16);
        if let ColumnBuffer::U16(ref mut v) = col {
            v.push(80);
            v.push(443);
            v.push(8080);
        }
        columns.insert("destinationTransportPort".to_string(), col);

        let resolved = ResolvedFilter::Port(filter);
        assert!(evaluate_resolved_filter(&resolved, 0, &columns));
        assert!(evaluate_resolved_filter(&resolved, 1, &columns));
        assert!(!evaluate_resolved_filter(&resolved, 2, &columns));
    }

    #[test]
    fn test_resolved_proto_filter() {
        let filter = resolve_proto_filter(&ProtoFilter {
            negated: false,
            value: ProtoValue::Named("tcp".to_string()),
        });
        let mut columns = HashMap::new();
        let mut col = ColumnBuffer::new(StorageType::U8);
        if let ColumnBuffer::U8(ref mut v) = col {
            v.push(6); // TCP
            v.push(17); // UDP
        }
        columns.insert("protocolIdentifier".to_string(), col);

        let resolved = ResolvedFilter::Proto(filter);
        assert!(evaluate_resolved_filter(&resolved, 0, &columns));
        assert!(!evaluate_resolved_filter(&resolved, 1, &columns));
    }

    #[test]
    fn test_resolved_numeric_filter() {
        let filter = resolve_numeric_filter(&NumericFilter {
            field: "bytes".to_string(),
            op: CompareOp::Gt,
            value: NumericValue::WithSuffix(1, "k".to_string()),
        });
        let mut columns = HashMap::new();
        let mut col = ColumnBuffer::new(StorageType::U64);
        if let ColumnBuffer::U64(ref mut v) = col {
            v.push(500); // < 1000
            v.push(2000); // > 1000
        }
        columns.insert("octetDeltaCount".to_string(), col);

        let resolved = ResolvedFilter::Numeric(filter);
        assert!(!evaluate_resolved_filter(&resolved, 0, &columns));
        assert!(evaluate_resolved_filter(&resolved, 1, &columns));
    }

    // ---------------------------------------------------------------
    // index_may_match tests — compound filters & new filter types
    // ---------------------------------------------------------------

    fn min_max_u32(min: u32, max: u32) -> ([u8; 16], [u8; 16]) {
        let mut min_val = [0u8; 16];
        let mut max_val = [0u8; 16];
        min_val[..4].copy_from_slice(&min.to_le_bytes());
        max_val[..4].copy_from_slice(&max.to_le_bytes());
        (min_val, max_val)
    }

    fn min_max_u16(min: u16, max: u16) -> ([u8; 16], [u8; 16]) {
        let mut min_val = [0u8; 16];
        let mut max_val = [0u8; 16];
        min_val[..2].copy_from_slice(&min.to_le_bytes());
        max_val[..2].copy_from_slice(&max.to_le_bytes());
        (min_val, max_val)
    }

    fn min_max_u64(min: u64, max: u64) -> ([u8; 16], [u8; 16]) {
        let mut min_val = [0u8; 16];
        let mut max_val = [0u8; 16];
        min_val[..8].copy_from_slice(&min.to_le_bytes());
        max_val[..8].copy_from_slice(&max.to_le_bytes());
        (min_val, max_val)
    }

    #[test]
    fn index_may_match_and_filter() {
        // AND(port=80, proto=tcp) on port column with range [443, 8080]
        // port=80 is NOT in [443, 8080], so AND should return false
        let filter = FilterExpr::And(
            Box::new(FilterExpr::Port(PortFilter {
                direction: PortDirection::Dst,
                negated: false,
                value: PortValue::Single(80),
            })),
            Box::new(FilterExpr::Proto(ProtoFilter {
                negated: false,
                value: ProtoValue::Named("tcp".to_string()),
            })),
        );
        let (min_val, max_val) = min_max_u16(443, 8080);
        assert!(!index_may_match(
            &filter,
            "destinationTransportPort",
            &min_val,
            &max_val,
            StorageType::U16
        ));
    }

    #[test]
    fn index_may_match_or_filter() {
        // OR(port=80, port=443) on column with range [100, 200]
        // port=80 is not in range, port=443 is not in range -> OR is false
        let filter = FilterExpr::Or(
            Box::new(FilterExpr::Port(PortFilter {
                direction: PortDirection::Dst,
                negated: false,
                value: PortValue::Single(80),
            })),
            Box::new(FilterExpr::Port(PortFilter {
                direction: PortDirection::Dst,
                negated: false,
                value: PortValue::Single(443),
            })),
        );
        let (min_val, max_val) = min_max_u16(100, 200);
        assert!(!index_may_match(
            &filter,
            "destinationTransportPort",
            &min_val,
            &max_val,
            StorageType::U16
        ));

        // But if one is in range, OR should return true
        let (min_val2, max_val2) = min_max_u16(70, 90);
        assert!(index_may_match(
            &filter,
            "destinationTransportPort",
            &min_val2,
            &max_val2,
            StorageType::U16
        ));
    }

    #[test]
    fn index_may_match_field_filter() {
        // Field filter on bgpSourceAsNumber = 65000
        let filter = FilterExpr::Field(FieldFilter {
            field: "bgpSourceAsNumber".to_string(),
            op: CompareOp::Eq,
            value: AstFieldValue::Integer(65000),
        });
        // Range [60000, 64000] doesn't contain 65000
        let (min_val, max_val) = min_max_u32(60000, 64000);
        assert!(!index_may_match(
            &filter,
            "bgpSourceAsNumber",
            &min_val,
            &max_val,
            StorageType::U32
        ));

        // Range [60000, 70000] contains 65000
        let (min_val2, max_val2) = min_max_u32(60000, 70000);
        assert!(index_may_match(
            &filter,
            "bgpSourceAsNumber",
            &min_val2,
            &max_val2,
            StorageType::U32
        ));
    }

    #[test]
    fn index_may_match_negated_ip_exact() {
        // NOT ip=10.0.0.5 — can skip only when ALL values equal 10.0.0.5
        let filter = FilterExpr::Ip(IpFilter {
            direction: IpDirection::Src,
            negated: true,
            value: IpValue::Addr("10.0.0.5".to_string()),
        });
        let target = u32::from(std::net::Ipv4Addr::new(10, 0, 0, 5));
        // All values are 10.0.0.5 -> NOT matches nothing -> can skip
        let (min_val, max_val) = min_max_u32(target, target);
        assert!(!index_may_match(
            &filter,
            "sourceIPv4Address",
            &min_val,
            &max_val,
            StorageType::U32
        ));

        // Range includes other values -> NOT might match -> can't skip
        let (min_val2, max_val2) = min_max_u32(target - 1, target + 1);
        assert!(index_may_match(
            &filter,
            "sourceIPv4Address",
            &min_val2,
            &max_val2,
            StorageType::U32
        ));
    }

    #[test]
    fn index_may_match_negated_cidr() {
        // NOT src in 10.0.0.0/24 — can skip if entire range is inside CIDR
        let filter = FilterExpr::Ip(IpFilter {
            direction: IpDirection::Src,
            negated: true,
            value: IpValue::Cidr("10.0.0.0/24".to_string()),
        });
        let cidr_min = u32::from(std::net::Ipv4Addr::new(10, 0, 0, 0));
        let cidr_max = u32::from(std::net::Ipv4Addr::new(10, 0, 0, 255));
        // Entire range is inside CIDR -> NOT matches nothing -> can skip
        let (min_val, max_val) = min_max_u32(cidr_min + 5, cidr_max - 5);
        assert!(!index_may_match(
            &filter,
            "sourceIPv4Address",
            &min_val,
            &max_val,
            StorageType::U32
        ));

        // Range extends outside CIDR -> some rows match NOT -> can't skip
        let outside = u32::from(std::net::Ipv4Addr::new(10, 0, 1, 5));
        let (min_val2, max_val2) = min_max_u32(cidr_min + 5, outside);
        assert!(index_may_match(
            &filter,
            "sourceIPv4Address",
            &min_val2,
            &max_val2,
            StorageType::U32
        ));
    }

    #[test]
    fn index_may_match_ip_list() {
        // src in (10.0.0.1, 10.0.0.5) on range [10.0.0.10, 10.0.0.20]
        let filter = FilterExpr::Ip(IpFilter {
            direction: IpDirection::Src,
            negated: false,
            value: IpValue::List(vec!["10.0.0.1".to_string(), "10.0.0.5".to_string()]),
        });
        let r_min = u32::from(std::net::Ipv4Addr::new(10, 0, 0, 10));
        let r_max = u32::from(std::net::Ipv4Addr::new(10, 0, 0, 20));
        let (min_val, max_val) = min_max_u32(r_min, r_max);
        // Neither 10.0.0.1 nor 10.0.0.5 is in [10.0.0.10, 10.0.0.20]
        assert!(!index_may_match(
            &filter,
            "sourceIPv4Address",
            &min_val,
            &max_val,
            StorageType::U32
        ));

        // Range [10.0.0.1, 10.0.0.10] contains 10.0.0.1
        let r_min2 = u32::from(std::net::Ipv4Addr::new(10, 0, 0, 1));
        let (min_val2, max_val2) = min_max_u32(r_min2, r_max);
        assert!(index_may_match(
            &filter,
            "sourceIPv4Address",
            &min_val2,
            &max_val2,
            StorageType::U32
        ));
    }

    #[test]
    fn index_may_match_proto() {
        // proto = tcp (6) on range [10, 20] — can skip (6 not in range)
        let filter = FilterExpr::Proto(ProtoFilter {
            negated: false,
            value: ProtoValue::Named("tcp".to_string()),
        });
        let mut min_val = [0u8; 16];
        let mut max_val = [0u8; 16];
        min_val[0] = 10;
        max_val[0] = 20;
        assert!(!index_may_match(
            &filter,
            "protocolIdentifier",
            &min_val,
            &max_val,
            StorageType::U8
        ));

        // Range [1, 10] includes 6
        min_val[0] = 1;
        max_val[0] = 10;
        assert!(index_may_match(
            &filter,
            "protocolIdentifier",
            &min_val,
            &max_val,
            StorageType::U8
        ));
    }

    #[test]
    fn index_may_match_ne_numeric() {
        // bytes != 5000 — can skip only when min == max == 5000
        let filter = FilterExpr::Numeric(NumericFilter {
            field: "bytes".to_string(),
            op: CompareOp::Ne,
            value: NumericValue::Integer(5000),
        });
        // All values equal 5000 -> != matches nothing -> can skip
        let (min_val, max_val) = min_max_u64(5000, 5000);
        assert!(!index_may_match(
            &filter,
            "octetDeltaCount",
            &min_val,
            &max_val,
            StorageType::U64
        ));

        // Range has other values -> can't skip
        let (min_val2, max_val2) = min_max_u64(4000, 6000);
        assert!(index_may_match(
            &filter,
            "octetDeltaCount",
            &min_val2,
            &max_val2,
            StorageType::U64
        ));
    }

    // ---------------------------------------------------------------
    // bloom_lookup_bytes tests — new filter types
    // ---------------------------------------------------------------

    #[test]
    fn bloom_lookup_numeric_eq() {
        let filter = FilterExpr::Numeric(NumericFilter {
            field: "bytes".to_string(),
            op: CompareOp::Eq,
            value: NumericValue::Integer(42),
        });
        let lookups = bloom_lookup_bytes(&filter);
        assert_eq!(lookups.len(), 1);
        assert_eq!(lookups[0].0, "octetDeltaCount");
        assert_eq!(lookups[0].1, 42u64.to_le_bytes().to_vec());
    }

    #[test]
    fn bloom_lookup_numeric_ne_excluded() {
        // != should NOT generate bloom lookups (bloom can't help with NOT)
        let filter = FilterExpr::Numeric(NumericFilter {
            field: "bytes".to_string(),
            op: CompareOp::Ne,
            value: NumericValue::Integer(42),
        });
        let lookups = bloom_lookup_bytes(&filter);
        assert!(lookups.is_empty());
    }

    #[test]
    fn bloom_lookup_field_eq() {
        let filter = FilterExpr::Field(FieldFilter {
            field: "bgpSourceAsNumber".to_string(),
            op: CompareOp::Eq,
            value: AstFieldValue::Integer(65000),
        });
        let lookups = bloom_lookup_bytes(&filter);
        assert_eq!(lookups.len(), 1);
        assert_eq!(lookups[0].0, "bgpSourceAsNumber");
    }

    #[test]
    fn bloom_lookup_negated_ip_excluded() {
        // Negated IP should NOT generate bloom lookups
        let filter = FilterExpr::Ip(IpFilter {
            direction: IpDirection::Src,
            negated: true,
            value: IpValue::Addr("10.0.0.1".to_string()),
        });
        let lookups = bloom_lookup_bytes(&filter);
        assert!(lookups.is_empty());
    }

    #[test]
    fn bloom_lookup_list_ip() {
        let filter = FilterExpr::Ip(IpFilter {
            direction: IpDirection::Src,
            negated: false,
            value: IpValue::List(vec!["10.0.0.1".to_string(), "10.0.0.2".to_string()]),
        });
        // Point lookups should be empty (list is not a point query)
        let point_lookups = bloom_lookup_bytes(&filter);
        assert!(point_lookups.is_empty());
        // List lookups should have an entry
        let list_lookups = bloom_lookup_list_bytes(&filter);
        assert_eq!(list_lookups.len(), 1);
        assert_eq!(list_lookups[0].0, "sourceIPv4Address");
        assert_eq!(list_lookups[0].1.len(), 2);
    }

    #[test]
    fn bloom_lookup_and_composition() {
        // AND(src=10.0.0.1, dport=80) should extract bloom lookups from both
        let filter = FilterExpr::And(
            Box::new(FilterExpr::Ip(IpFilter {
                direction: IpDirection::Src,
                negated: false,
                value: IpValue::Addr("10.0.0.1".to_string()),
            })),
            Box::new(FilterExpr::Port(PortFilter {
                direction: PortDirection::Dst,
                negated: false,
                value: PortValue::Single(80),
            })),
        );
        let lookups = bloom_lookup_bytes(&filter);
        assert_eq!(lookups.len(), 2);
    }

    // ── Histogram bucketing tests ────────────────────────────────

    #[test]
    fn distribute_rows_partial_overlap_before_window() {
        let bucket_starts = vec![1000, 2000, 3000];
        let bucket_ms = 1000;
        let mut counts = vec![0u64; 3];
        // Part spans 500-1500, window starts at 1000. 50% overlap with bucket 0.
        distribute_rows_to_buckets(&mut counts, &bucket_starts, 500, 1500, 1000, bucket_ms);
        assert_eq!(counts[0], 500);
        assert_eq!(counts[1], 0);
        assert_eq!(counts[2], 0);
    }

    #[test]
    fn distribute_rows_entirely_before_window() {
        let bucket_starts = vec![1000, 2000, 3000];
        let bucket_ms = 1000;
        let mut counts = vec![0u64; 3];
        // Part spans 200-800, entirely before window start 1000.
        distribute_rows_to_buckets(&mut counts, &bucket_starts, 200, 800, 5000, bucket_ms);
        assert_eq!(counts, vec![0, 0, 0]);
    }

    #[test]
    fn distribute_rows_entirely_after_window() {
        let bucket_starts = vec![1000, 2000, 3000];
        let bucket_ms = 1000;
        let mut counts = vec![0u64; 3];
        // Part spans 5000-6000, entirely after window end 4000.
        distribute_rows_to_buckets(&mut counts, &bucket_starts, 5000, 6000, 5000, bucket_ms);
        assert_eq!(counts, vec![0, 0, 0]);
    }

    #[test]
    fn distribute_rows_point_before_window() {
        let bucket_starts = vec![1000, 2000, 3000];
        let bucket_ms = 1000;
        let mut counts = vec![0u64; 3];
        // Point timestamp at 500, before window start.
        distribute_rows_to_buckets(&mut counts, &bucket_starts, 500, 500, 100, bucket_ms);
        assert_eq!(counts, vec![0, 0, 0]);
    }

    #[test]
    fn distribute_rows_point_after_window() {
        let bucket_starts = vec![1000, 2000, 3000];
        let bucket_ms = 1000;
        let mut counts = vec![0u64; 3];
        // Point timestamp at 5000, after window end.
        distribute_rows_to_buckets(&mut counts, &bucket_starts, 5000, 5000, 100, bucket_ms);
        assert_eq!(counts, vec![0, 0, 0]);
    }

    #[test]
    fn distribute_rows_point_inside_window() {
        let bucket_starts = vec![1000, 2000, 3000];
        let bucket_ms = 1000;
        let mut counts = vec![0u64; 3];
        // Point timestamp at 2500, inside bucket 1.
        distribute_rows_to_buckets(&mut counts, &bucket_starts, 2500, 2500, 100, bucket_ms);
        assert_eq!(counts, vec![0, 100, 0]);
    }

    #[test]
    fn distribute_rows_spanning_full_window() {
        let bucket_starts = vec![1000, 2000, 3000];
        let bucket_ms = 1000;
        let mut counts = vec![0u64; 3];
        // Part spans 0-5000, window is 1000-4000 (3 buckets).
        distribute_rows_to_buckets(&mut counts, &bucket_starts, 0, 5000, 5000, bucket_ms);
        // Each bucket covers 1000ms out of 5000ms total span → 1000 rows each.
        assert_eq!(counts[0], 1000);
        assert_eq!(counts[1], 1000);
        assert_eq!(counts[2], 1000);
    }

    #[test]
    fn bucket_idx_clamps_to_boundaries() {
        let bucket_starts = vec![1000, 2000, 3000];
        let bucket_ms = 1000;
        assert_eq!(bucket_idx(&bucket_starts, bucket_ms, 500), 0); // before window
        assert_eq!(bucket_idx(&bucket_starts, bucket_ms, 1000), 0); // first bucket start
        assert_eq!(bucket_idx(&bucket_starts, bucket_ms, 1500), 0); // mid bucket 0
        assert_eq!(bucket_idx(&bucket_starts, bucket_ms, 2000), 1); // bucket 1 start
        assert_eq!(bucket_idx(&bucket_starts, bucket_ms, 3500), 2); // mid bucket 2
        assert_eq!(bucket_idx(&bucket_starts, bucket_ms, 5000), 2); // after window, clamped
    }

    // -----------------------------------------------------------------------
    // Named IP direction (non-src/dst columns like postNATSourceIPv4Address)
    // -----------------------------------------------------------------------

    #[test]
    fn test_named_ipv4_filter_eq() {
        let filter = resolve_ip_filter(&IpFilter {
            direction: IpDirection::Named("postNATSourceIPv4Address".to_string()),
            negated: false,
            value: IpValue::Addr("91.150.183.53".to_string()),
        });
        let mut columns = HashMap::new();
        let mut col = ColumnBuffer::new(StorageType::U32);
        let target = u32::from(std::net::Ipv4Addr::new(91, 150, 183, 53));
        let other = u32::from(std::net::Ipv4Addr::new(10, 0, 0, 1));
        if let ColumnBuffer::U32(ref mut v) = col {
            v.push(target);
            v.push(other);
            v.push(0); // 0.0.0.0
        }
        columns.insert("postNATSourceIPv4Address".to_string(), col);

        let resolved = ResolvedFilter::Ip(filter);
        assert!(evaluate_resolved_filter(&resolved, 0, &columns)); // exact match
        assert!(!evaluate_resolved_filter(&resolved, 1, &columns)); // different IP
        assert!(!evaluate_resolved_filter(&resolved, 2, &columns)); // 0.0.0.0
    }

    #[test]
    fn test_named_ipv4_filter_ne() {
        let filter = resolve_ip_filter(&IpFilter {
            direction: IpDirection::Named("postNATSourceIPv4Address".to_string()),
            negated: true,
            value: IpValue::Addr("0.0.0.0".to_string()),
        });
        let mut columns = HashMap::new();
        let mut col = ColumnBuffer::new(StorageType::U32);
        if let ColumnBuffer::U32(ref mut v) = col {
            v.push(u32::from(std::net::Ipv4Addr::new(91, 150, 183, 53)));
            v.push(0); // 0.0.0.0
        }
        columns.insert("postNATSourceIPv4Address".to_string(), col);

        let resolved = ResolvedFilter::Ip(filter);
        assert!(evaluate_resolved_filter(&resolved, 0, &columns)); // non-zero → passes ne
        assert!(!evaluate_resolved_filter(&resolved, 1, &columns)); // zero → fails ne
    }

    #[test]
    fn test_named_ipv4_cidr_filter() {
        let filter = resolve_ip_filter(&IpFilter {
            direction: IpDirection::Named("postNATDestinationIPv4Address".to_string()),
            negated: false,
            value: IpValue::Cidr("10.0.0.0/8".to_string()),
        });
        let mut columns = HashMap::new();
        let mut col = ColumnBuffer::new(StorageType::U32);
        if let ColumnBuffer::U32(ref mut v) = col {
            v.push(u32::from(std::net::Ipv4Addr::new(10, 1, 2, 3)));
            v.push(u32::from(std::net::Ipv4Addr::new(192, 168, 1, 1)));
        }
        columns.insert("postNATDestinationIPv4Address".to_string(), col);

        let resolved = ResolvedFilter::Ip(filter);
        assert!(evaluate_resolved_filter(&resolved, 0, &columns));
        assert!(!evaluate_resolved_filter(&resolved, 1, &columns));
    }

    // -----------------------------------------------------------------------
    // IPv6 filter tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_ipv6_filter_eq() {
        let filter = resolve_ip_filter(&IpFilter {
            direction: IpDirection::Named("sourceIPv6Address".to_string()),
            negated: false,
            value: IpValue::Addr("2001:db8::1".to_string()),
        });
        let target_addr: std::net::Ipv6Addr = "2001:db8::1".parse().unwrap();
        let other_addr: std::net::Ipv6Addr = "fe80::1".parse().unwrap();
        let target_pair = ipv6_to_pair(target_addr);
        let other_pair = ipv6_to_pair(other_addr);

        let mut columns = HashMap::new();
        let mut col = ColumnBuffer::new(StorageType::U128);
        if let ColumnBuffer::U128(ref mut v) = col {
            v.push(target_pair);
            v.push(other_pair);
        }
        columns.insert("sourceIPv6Address".to_string(), col);

        let resolved = ResolvedFilter::Ip(filter);
        assert!(evaluate_resolved_filter(&resolved, 0, &columns));
        assert!(!evaluate_resolved_filter(&resolved, 1, &columns));
    }

    #[test]
    fn test_ipv6_cidr_filter() {
        let filter = resolve_ip_filter(&IpFilter {
            direction: IpDirection::Named("sourceIPv6Address".to_string()),
            negated: false,
            value: IpValue::Cidr("2001:db8::/32".to_string()),
        });
        let inside: std::net::Ipv6Addr = "2001:db8::abcd".parse().unwrap();
        let outside: std::net::Ipv6Addr = "fe80::1".parse().unwrap();

        let mut columns = HashMap::new();
        let mut col = ColumnBuffer::new(StorageType::U128);
        if let ColumnBuffer::U128(ref mut v) = col {
            v.push(ipv6_to_pair(inside));
            v.push(ipv6_to_pair(outside));
        }
        columns.insert("sourceIPv6Address".to_string(), col);

        let resolved = ResolvedFilter::Ip(filter);
        assert!(evaluate_resolved_filter(&resolved, 0, &columns));
        assert!(!evaluate_resolved_filter(&resolved, 1, &columns));
    }

    #[test]
    fn test_ipv6_ne_filter() {
        let filter = resolve_ip_filter(&IpFilter {
            direction: IpDirection::Named("sourceIPv6Address".to_string()),
            negated: true,
            value: IpValue::Addr("::".to_string()),
        });
        let nonzero: std::net::Ipv6Addr = "2001:db8::1".parse().unwrap();
        let zero: std::net::Ipv6Addr = "::".parse().unwrap();

        let mut columns = HashMap::new();
        let mut col = ColumnBuffer::new(StorageType::U128);
        if let ColumnBuffer::U128(ref mut v) = col {
            v.push(ipv6_to_pair(nonzero));
            v.push(ipv6_to_pair(zero));
        }
        columns.insert("sourceIPv6Address".to_string(), col);

        let resolved = ResolvedFilter::Ip(filter);
        assert!(evaluate_resolved_filter(&resolved, 0, &columns));
        assert!(!evaluate_resolved_filter(&resolved, 1, &columns));
    }

    // -----------------------------------------------------------------------
    // MAC address string filter
    // -----------------------------------------------------------------------

    #[test]
    fn test_mac_address_string_eq() {
        let filter = resolve_string_filter(&StringFilterExpr {
            field: "sourceMacAddress".to_string(),
            op: StringOp::Eq,
            value: "aa:bb:cc:dd:ee:ff".to_string(),
        });
        let mut columns = HashMap::new();
        let mut col = ColumnBuffer::new(StorageType::Mac);
        if let ColumnBuffer::Mac(ref mut v) = col {
            v.push([0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff]);
            v.push([0x11, 0x22, 0x33, 0x44, 0x55, 0x66]);
        }
        columns.insert("sourceMacAddress".to_string(), col);

        let resolved = ResolvedFilter::StringFilter(filter);
        assert!(evaluate_resolved_filter(&resolved, 0, &columns));
        assert!(!evaluate_resolved_filter(&resolved, 1, &columns));
    }

    #[test]
    fn test_mac_address_string_ne() {
        let filter = resolve_string_filter(&StringFilterExpr {
            field: "sourceMacAddress".to_string(),
            op: StringOp::Ne,
            value: "00:00:00:00:00:00".to_string(),
        });
        let mut columns = HashMap::new();
        let mut col = ColumnBuffer::new(StorageType::Mac);
        if let ColumnBuffer::Mac(ref mut v) = col {
            v.push([0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff]);
            v.push([0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        }
        columns.insert("sourceMacAddress".to_string(), col);

        let resolved = ResolvedFilter::StringFilter(filter);
        assert!(evaluate_resolved_filter(&resolved, 0, &columns));
        assert!(!evaluate_resolved_filter(&resolved, 1, &columns));
    }

    #[test]
    fn test_mac_address_case_insensitive() {
        let filter = resolve_string_filter(&StringFilterExpr {
            field: "sourceMacAddress".to_string(),
            op: StringOp::Eq,
            value: "AA:BB:CC:DD:EE:FF".to_string(),
        });
        let mut columns = HashMap::new();
        let mut col = ColumnBuffer::new(StorageType::Mac);
        if let ColumnBuffer::Mac(ref mut v) = col {
            v.push([0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff]);
        }
        columns.insert("sourceMacAddress".to_string(), col);

        let resolved = ResolvedFilter::StringFilter(filter);
        assert!(evaluate_resolved_filter(&resolved, 0, &columns));
    }

    // -----------------------------------------------------------------------
    // IPv6 prefix mask helper
    // -----------------------------------------------------------------------

    #[test]
    fn test_ipv6_prefix_mask() {
        assert_eq!(ipv6_prefix_mask(0), [0, 0]);
        assert_eq!(ipv6_prefix_mask(64), [u64::MAX, 0]);
        assert_eq!(ipv6_prefix_mask(128), [u64::MAX, u64::MAX]);
        assert_eq!(ipv6_prefix_mask(32), [0xFFFF_FFFF_0000_0000, 0]);
        assert_eq!(ipv6_prefix_mask(96), [u64::MAX, 0xFFFF_FFFF_0000_0000]);
    }

    // -----------------------------------------------------------------------
    // Regression: existing src/dst IP filters still work
    // -----------------------------------------------------------------------

    #[test]
    fn test_src_dst_ip_filters_still_work() {
        // Src direction
        let src_filter = resolve_ip_filter(&IpFilter {
            direction: IpDirection::Src,
            negated: false,
            value: IpValue::Addr("10.0.0.1".to_string()),
        });
        let mut columns = HashMap::new();
        let mut col = ColumnBuffer::new(StorageType::U32);
        if let ColumnBuffer::U32(ref mut v) = col {
            v.push(u32::from(std::net::Ipv4Addr::new(10, 0, 0, 1)));
            v.push(u32::from(std::net::Ipv4Addr::new(10, 0, 0, 2)));
        }
        columns.insert("sourceIPv4Address".to_string(), col);

        let resolved = ResolvedFilter::Ip(src_filter);
        assert!(evaluate_resolved_filter(&resolved, 0, &columns));
        assert!(!evaluate_resolved_filter(&resolved, 1, &columns));

        // Dst direction
        let dst_filter = resolve_ip_filter(&IpFilter {
            direction: IpDirection::Dst,
            negated: false,
            value: IpValue::Addr("192.168.1.1".to_string()),
        });
        let mut dst_col = ColumnBuffer::new(StorageType::U32);
        if let ColumnBuffer::U32(ref mut v) = dst_col {
            v.push(u32::from(std::net::Ipv4Addr::new(192, 168, 1, 1)));
            v.push(u32::from(std::net::Ipv4Addr::new(10, 0, 0, 1)));
        }
        columns.insert("destinationIPv4Address".to_string(), dst_col);

        let resolved = ResolvedFilter::Ip(dst_filter);
        assert!(evaluate_resolved_filter(&resolved, 0, &columns));
        assert!(!evaluate_resolved_filter(&resolved, 1, &columns));
    }
}
