//! Query execution engine: reads columnar storage and returns actual results.
//!
//! Execution pipeline:
//! 1. Convert time range to unix second bounds
//! 2. Discover parts via `Table::list_parts` with time pruning
//! 3. For each part: read column index, check bloom/marks, decode columns, filter
//! 4. Aggregate if needed (group-by, top-N)
//! 5. Return results with execution plan

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::Serialize;
use tracing::{debug, trace};

use flowcus_query::ast::{
    AggCall, AggExpr, AggFunc, CompareOp, Duration as FqlDuration, DurationUnit, FieldFilter,
    FieldValue as AstFieldValue, FilterExpr, GroupByKey, IpDirection, IpFilter, IpValue,
    NumericFilter, NumericValue, PortDirection, PortFilter, PortValue, ProtoFilter, ProtoValue,
    Query, SelectExpr, SelectFieldExpr, Stage, StringFilterExpr, StringOp, TimeRange,
};

use crate::column::ColumnBuffer;
use crate::decode;
use crate::granule;
use crate::part;
use crate::schema::StorageType;
use crate::table::{PartEntry, Table};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// The query executor: reads parts from disk and evaluates queries.
pub struct QueryExecutor {
    table_base: PathBuf,
    granule_size: usize,
}

/// Execution plan with statistics about what happened during the query.
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionPlan {
    pub steps: Vec<PlanStep>,
    pub parts_total: usize,
    pub parts_skipped_by_time: usize,
    pub parts_skipped_by_index: usize,
    pub granules_total: usize,
    pub granules_skipped_by_bloom: usize,
    pub granules_skipped_by_marks: usize,
    pub columns_to_read: Vec<String>,
}

/// A single step in the execution plan.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum PlanStep {
    TimeRangePrune {
        start: u32,
        end: u32,
        parts_before: usize,
        parts_after: usize,
    },
    ColumnIndexFilter {
        column: String,
        predicate: String,
        parts_skipped: usize,
    },
    BloomFilter {
        column: String,
        value: String,
        granules_skipped: usize,
    },
    MarkSeek {
        column: String,
        granule_range: (usize, usize),
    },
    ColumnRead {
        column: String,
        bytes: u64,
    },
    FilterApply {
        expression: String,
        rows_before: usize,
        rows_after: usize,
    },
    Aggregate {
        function: String,
        groups: usize,
    },
    PartSkippedMerge {
        path: String,
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
}

// ---------------------------------------------------------------------------
// Field name resolution
// ---------------------------------------------------------------------------

/// Resolve short aliases to canonical IPFIX field names.
fn resolve_field_name(name: &str) -> &str {
    match name {
        "src" => "sourceIPv4Address",
        "dst" => "destinationIPv4Address",
        "sport" => "sourceTransportPort",
        "dport" => "destinationTransportPort",
        "proto" => "protocolIdentifier",
        "bytes" => "octetDeltaCount",
        "packets" => "packetDeltaCount",
        "tos" => "ipClassOfService",
        "nexthop" => "ipNextHopIPv4Address",
        "vlan" => "vlanId",
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

fn now_unix() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32
}

/// Convert a `TimeRange` to (start_unix_secs, end_unix_secs).
fn time_range_to_bounds(tr: &TimeRange) -> (u32, u32) {
    let now = now_unix();
    match tr {
        TimeRange::Relative { duration, offset } => {
            let dur = duration_to_secs(duration) as u32;
            let off = offset
                .as_ref()
                .map(|o| duration_to_secs(o) as u32)
                .unwrap_or(0);
            let end = now.saturating_sub(off);
            let start = end.saturating_sub(dur);
            (start, end)
        }
        TimeRange::Absolute { start, end } => {
            let s = parse_datetime(start).unwrap_or(now.saturating_sub(3600));
            let e = parse_datetime(end).unwrap_or(now);
            (s, e)
        }
        TimeRange::PointInTime { datetime, window } => {
            let t = parse_datetime(datetime).unwrap_or(now);
            match window {
                flowcus_query::ast::PointWindow::PlusMinus(d) => {
                    let w = duration_to_secs(d) as u32;
                    (t.saturating_sub(w), t.saturating_add(w))
                }
                flowcus_query::ast::PointWindow::Plus(d) => {
                    let w = duration_to_secs(d) as u32;
                    (t, t.saturating_add(w))
                }
                flowcus_query::ast::PointWindow::Minus(d) => {
                    let w = duration_to_secs(d) as u32;
                    (t.saturating_sub(w), t)
                }
            }
        }
        TimeRange::Recurring { base, .. } => time_range_to_bounds(base),
        TimeRange::Combined(ranges) => {
            if ranges.is_empty() {
                (now.saturating_sub(3600), now)
            } else {
                let mut min_start = u32::MAX;
                let mut max_end = 0u32;
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
fn parse_datetime(s: &str) -> Option<u32> {
    // Try "YYYY-MM-DD" or "YYYY-MM-DDTHH:MM:SS" or "YYYY-MM-DDTHH:MM"
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

    let (hour, minute, second) = if parts.len() > 1 {
        let time_parts: Vec<u32> = parts[1].split(':').filter_map(|p| p.parse().ok()).collect();
        (
            *time_parts.first().unwrap_or(&0),
            *time_parts.get(1).unwrap_or(&0),
            *time_parts.get(2).unwrap_or(&0),
        )
    } else {
        (0, 0, 0)
    };

    // Convert to unix timestamp (simplified, no leap seconds)
    let days = ymd_to_days(year, month, day)?;
    let secs = days * 86400 + hour * 3600 + minute * 60 + second;
    Some(secs)
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

fn collect_filter_columns(f: &FilterExpr, out: &mut Vec<String>) {
    match f {
        FilterExpr::And(a, b) | FilterExpr::Or(a, b) => {
            collect_filter_columns(a, out);
            collect_filter_columns(b, out);
        }
        FilterExpr::Not(inner) => collect_filter_columns(inner, out),
        FilterExpr::Ip(ip) => match ip.direction {
            IpDirection::Src => out.push("sourceIPv4Address".to_string()),
            IpDirection::Dst => out.push("destinationIPv4Address".to_string()),
            IpDirection::Any => {
                out.push("sourceIPv4Address".to_string());
                out.push("destinationIPv4Address".to_string());
            }
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

fn _column_get_u128(buf: &ColumnBuffer, row: usize) -> [u64; 2] {
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
// Filter evaluation
// ---------------------------------------------------------------------------

/// Evaluate a filter expression against a row from decoded column buffers.
fn evaluate_filter(
    filter: &FilterExpr,
    row: usize,
    columns: &HashMap<String, ColumnBuffer>,
) -> bool {
    match filter {
        FilterExpr::And(a, b) => {
            evaluate_filter(a, row, columns) && evaluate_filter(b, row, columns)
        }
        FilterExpr::Or(a, b) => {
            evaluate_filter(a, row, columns) || evaluate_filter(b, row, columns)
        }
        FilterExpr::Not(inner) => !evaluate_filter(inner, row, columns),
        FilterExpr::Ip(ip) => evaluate_ip_filter(ip, row, columns),
        FilterExpr::Port(port) => evaluate_port_filter(port, row, columns),
        FilterExpr::Proto(proto) => evaluate_proto_filter(proto, row, columns),
        FilterExpr::Numeric(num) => evaluate_numeric_filter(num, row, columns),
        FilterExpr::StringFilter(sf) => evaluate_string_filter(sf, row, columns),
        FilterExpr::Field(ff) => evaluate_field_filter(ff, row, columns),
    }
}

fn evaluate_ip_filter(ip: &IpFilter, row: usize, columns: &HashMap<String, ColumnBuffer>) -> bool {
    let check = |col_name: &str| -> bool {
        let buf = match columns.get(col_name) {
            Some(b) => b,
            None => return false,
        };
        let val = column_get_u32(buf, row);
        let matched = match &ip.value {
            IpValue::Addr(addr) => addr
                .parse::<std::net::Ipv4Addr>()
                .map(|a| u32::from(a) == val)
                .unwrap_or(false),
            IpValue::Cidr(cidr) => match_cidr(val, cidr),
            IpValue::List(addrs) => addrs.iter().any(|a| {
                a.parse::<std::net::Ipv4Addr>()
                    .map(|parsed| u32::from(parsed) == val)
                    .unwrap_or(false)
            }),
            IpValue::Wildcard(pattern) => match_ip_wildcard(val, pattern),
        };
        if ip.negated { !matched } else { matched }
    };

    match ip.direction {
        IpDirection::Src => check("sourceIPv4Address"),
        IpDirection::Dst => check("destinationIPv4Address"),
        IpDirection::Any => check("sourceIPv4Address") || check("destinationIPv4Address"),
    }
}

fn match_cidr(val: u32, cidr: &str) -> bool {
    let parts: Vec<&str> = cidr.split('/').collect();
    if parts.len() != 2 {
        return false;
    }
    let base: u32 = match parts[0].parse::<std::net::Ipv4Addr>() {
        Ok(a) => u32::from(a),
        Err(_) => return false,
    };
    let prefix_len: u32 = match parts[1].parse() {
        Ok(p) if p <= 32 => p,
        _ => return false,
    };
    if prefix_len == 0 {
        return true;
    }
    let mask = u32::MAX << (32 - prefix_len);
    (val & mask) == (base & mask)
}

fn match_ip_wildcard(val: u32, pattern: &str) -> bool {
    let ip = std::net::Ipv4Addr::from(val);
    let octets: Vec<&str> = pattern.split('.').collect();
    let ip_octets = ip.octets();
    if octets.len() != 4 {
        return false;
    }
    for (i, oct) in octets.iter().enumerate() {
        if *oct == "*" {
            continue;
        }
        if let Ok(v) = oct.parse::<u8>() {
            if ip_octets[i] != v {
                return false;
            }
        } else {
            return false;
        }
    }
    true
}

fn evaluate_port_filter(
    port: &PortFilter,
    row: usize,
    columns: &HashMap<String, ColumnBuffer>,
) -> bool {
    let check = |col_name: &str| -> bool {
        let buf = match columns.get(col_name) {
            Some(b) => b,
            None => return false,
        };
        let val = column_get_u16(buf, row);
        let matched = match_port_value(val, &port.value);
        if port.negated { !matched } else { matched }
    };

    match port.direction {
        PortDirection::Src => check("sourceTransportPort"),
        PortDirection::Dst => check("destinationTransportPort"),
        PortDirection::Any => check("sourceTransportPort") || check("destinationTransportPort"),
    }
}

fn match_port_value(val: u16, pv: &PortValue) -> bool {
    match pv {
        PortValue::Single(p) => val == *p,
        PortValue::Range(lo, hi) => val >= *lo && val <= *hi,
        PortValue::List(items) => items.iter().any(|item| match_port_value(val, item)),
        PortValue::Named(name) => resolve_named_port(name).map(|p| val == p).unwrap_or(false),
        PortValue::OpenRange(lo) => val >= *lo,
    }
}

fn evaluate_proto_filter(
    proto: &ProtoFilter,
    row: usize,
    columns: &HashMap<String, ColumnBuffer>,
) -> bool {
    let buf = match columns.get("protocolIdentifier") {
        Some(b) => b,
        None => return false,
    };
    let val = column_get_u8(buf, row);
    let matched = match_proto_value(val, &proto.value);
    if proto.negated { !matched } else { matched }
}

fn match_proto_value(val: u8, pv: &ProtoValue) -> bool {
    match pv {
        ProtoValue::Named(name) => resolve_proto(name).map(|p| val == p).unwrap_or(false),
        ProtoValue::Number(n) => val == *n,
        ProtoValue::List(items) => items.iter().any(|item| match_proto_value(val, item)),
    }
}

fn evaluate_numeric_filter(
    num: &NumericFilter,
    row: usize,
    columns: &HashMap<String, ColumnBuffer>,
) -> bool {
    let col_name = resolve_field_name(&num.field);
    let buf = match columns.get(col_name) {
        Some(b) => b,
        None => return false,
    };
    let val = column_get_u64(buf, row);
    let target = match &num.value {
        NumericValue::Integer(n) => *n,
        NumericValue::WithSuffix(n, suffix) => {
            let multiplier = match suffix.to_lowercase().as_str() {
                "k" => 1_000,
                "m" => 1_000_000,
                "g" => 1_000_000_000,
                "t" => 1_000_000_000_000,
                _ => 1,
            };
            n * multiplier
        }
    };

    match num.op {
        CompareOp::Eq => val == target,
        CompareOp::Ne => val != target,
        CompareOp::Gt => val > target,
        CompareOp::Ge => val >= target,
        CompareOp::Lt => val < target,
        CompareOp::Le => val <= target,
    }
}

fn evaluate_string_filter(
    sf: &StringFilterExpr,
    row: usize,
    columns: &HashMap<String, ColumnBuffer>,
) -> bool {
    let col_name = resolve_field_name(&sf.field);
    let buf = match columns.get(col_name) {
        Some(b) => b,
        None => return false,
    };
    let val_bytes = column_get_varlen(buf, row);
    let val = String::from_utf8_lossy(val_bytes);

    match sf.op {
        StringOp::Eq => val == sf.value,
        StringOp::Ne => val != sf.value,
        StringOp::Regex | StringOp::NotRegex => {
            // Simple substring/glob match without regex dependency
            let matched = val.contains(&sf.value);
            if sf.op == StringOp::NotRegex {
                !matched
            } else {
                matched
            }
        }
        StringOp::In | StringOp::NotIn => {
            let items: Vec<&str> = sf.value.split(',').map(|s| s.trim()).collect();
            let matched = items.iter().any(|item| val.as_ref() == *item);
            if sf.op == StringOp::NotIn {
                !matched
            } else {
                matched
            }
        }
    }
}

fn evaluate_field_filter(
    ff: &FieldFilter,
    row: usize,
    columns: &HashMap<String, ColumnBuffer>,
) -> bool {
    let col_name = resolve_field_name(&ff.field);
    let buf = match columns.get(col_name) {
        Some(b) => b,
        None => return false,
    };
    let val = column_get_u64(buf, row);
    let target = match &ff.value {
        AstFieldValue::Integer(n) => *n,
        AstFieldValue::String(_) => return false, // can't compare string to numeric
        AstFieldValue::List(_) => return false,
    };

    match ff.op {
        CompareOp::Eq => val == target,
        CompareOp::Ne => val != target,
        CompareOp::Gt => val > target,
        CompareOp::Ge => val >= target,
        CompareOp::Lt => val < target,
        CompareOp::Le => val <= target,
    }
}

// ---------------------------------------------------------------------------
// Column index min/max checking
// ---------------------------------------------------------------------------

/// Check if a filter could possibly match given the column index min/max.
/// Returns true if the filter might match, false if the part can be skipped.
fn index_may_match(
    filter: &FilterExpr,
    col_name: &str,
    min_val: &[u8; 16],
    max_val: &[u8; 16],
    storage_type: StorageType,
) -> bool {
    match filter {
        FilterExpr::Ip(ip) => {
            let relevant = match ip.direction {
                IpDirection::Src => col_name == "sourceIPv4Address",
                IpDirection::Dst => col_name == "destinationIPv4Address",
                IpDirection::Any => {
                    col_name == "sourceIPv4Address" || col_name == "destinationIPv4Address"
                }
            };
            if !relevant {
                return true; // not this column
            }
            if storage_type != StorageType::U32 {
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
                            !in_range || col_min != col_max
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
                        !overlaps || col_min != col_max
                    } else {
                        overlaps
                    }
                }
                _ => true, // list, wildcard: can't easily prune
            }
        }
        FilterExpr::Port(port) => {
            let relevant = match port.direction {
                PortDirection::Src => col_name == "sourceTransportPort",
                PortDirection::Dst => col_name == "destinationTransportPort",
                PortDirection::Any => {
                    col_name == "sourceTransportPort" || col_name == "destinationTransportPort"
                }
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
                        !in_range || col_min != col_max
                    } else {
                        in_range
                    }
                }
                PortValue::Range(lo, hi) => {
                    let overlaps = col_min <= *hi && col_max >= *lo;
                    if port.negated {
                        !overlaps || col_min != col_max
                    } else {
                        overlaps
                    }
                }
                _ => true,
            }
        }
        FilterExpr::Numeric(num) => {
            let resolved = resolve_field_name(&num.field);
            if col_name != resolved {
                return true;
            }
            let target = match &num.value {
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
            };
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
            match num.op {
                CompareOp::Eq => target >= col_min && target <= col_max,
                CompareOp::Ne => col_min != col_max || col_min != target,
                CompareOp::Gt => col_max > target,
                CompareOp::Ge => col_max >= target,
                CompareOp::Lt => col_min < target,
                CompareOp::Le => col_min <= target,
            }
        }
        _ => true, // complex filters: can't prune from index
    }
}

// ---------------------------------------------------------------------------
// Bloom filter value extraction
// ---------------------------------------------------------------------------

/// Extract bytes for bloom filter lookup from a filter expression.
fn bloom_lookup_bytes(filter: &FilterExpr) -> Vec<(String, Vec<u8>)> {
    let mut result = Vec::new();
    match filter {
        FilterExpr::Ip(ip) if !ip.negated => {
            if let IpValue::Addr(addr) = &ip.value {
                if let Ok(a) = addr.parse::<std::net::Ipv4Addr>() {
                    let bytes = u32::from(a).to_le_bytes().to_vec();
                    match ip.direction {
                        IpDirection::Src => {
                            result.push(("sourceIPv4Address".to_string(), bytes));
                        }
                        IpDirection::Dst => {
                            result.push(("destinationIPv4Address".to_string(), bytes));
                        }
                        IpDirection::Any => {
                            result.push(("sourceIPv4Address".to_string(), bytes.clone()));
                            result.push(("destinationIPv4Address".to_string(), bytes));
                        }
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
                    PortDirection::Any => {
                        result.push(("sourceTransportPort".to_string(), bytes.clone()));
                        result.push(("destinationTransportPort".to_string(), bytes));
                    }
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
        FilterExpr::And(a, b) => {
            result.extend(bloom_lookup_bytes(a));
            result.extend(bloom_lookup_bytes(b));
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
                // rate = sum / count (simplified)
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
// Query Executor implementation
// ---------------------------------------------------------------------------

impl QueryExecutor {
    /// Create a new executor for the given storage directory.
    pub fn new(storage_dir: &Path, granule_size: usize) -> Self {
        Self {
            table_base: storage_dir.join("flows"),
            granule_size,
        }
    }

    /// Execute a parsed query and return results with the execution plan.
    pub fn execute(&self, query: &Query) -> std::io::Result<QueryResult> {
        let mut plan = ExecutionPlan {
            steps: Vec::new(),
            parts_total: 0,
            parts_skipped_by_time: 0,
            parts_skipped_by_index: 0,
            granules_total: 0,
            granules_skipped_by_bloom: 0,
            granules_skipped_by_marks: 0,
            columns_to_read: Vec::new(),
        };

        // Step 1: Convert time range to unix bounds
        let (time_start, time_end) = time_range_to_bounds(&query.time_range);
        debug!(time_start, time_end, "Query time range");

        // Step 2: Discover parts
        let table = Table::open(self.table_base.parent().unwrap_or(Path::new(".")), "flows")?;
        let all_parts = table.list_all_parts()?;
        let parts_total = all_parts.len();
        let filtered_parts = table.list_parts(Some(time_start), Some(time_end))?;
        let parts_after_time = filtered_parts.len();

        plan.parts_total = parts_total;
        plan.parts_skipped_by_time = parts_total.saturating_sub(parts_after_time);
        plan.steps.push(PlanStep::TimeRangePrune {
            start: time_start,
            end: time_end,
            parts_before: parts_total,
            parts_after: parts_after_time,
        });

        // Step 3: Determine columns to read
        let referenced_cols = collect_referenced_columns(query);
        let needs_all_columns = query
            .stages
            .iter()
            .any(|s| matches!(s, Stage::Select(SelectExpr::All | SelectExpr::AllExcept(_))));
        plan.columns_to_read = referenced_cols.clone();

        // Step 4: Extract filters
        let filters: Vec<&FilterExpr> = query
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

        // Step 5: Process parts
        let mut all_rows: Vec<HashMap<String, serde_json::Value>> = Vec::new();
        let mut total_rows_scanned: u64 = 0;
        let mut parts_skipped_by_index: usize = 0;
        let mut parts_skipped_by_merge: usize = 0;

        for part_entry in &filtered_parts {
            let part_result = self.process_part(
                part_entry,
                &filters,
                &referenced_cols,
                needs_all_columns,
                &mut plan,
            );

            match part_result {
                Ok(ProcessPartResult::Skipped) => {
                    parts_skipped_by_index += 1;
                }
                Ok(ProcessPartResult::Rows { rows, rows_scanned }) => {
                    total_rows_scanned += rows_scanned;
                    all_rows.extend(rows);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    debug!(
                        error = %e,
                        part = %part_entry.path.display(),
                        "Part not found (likely merged away), skipping"
                    );
                    parts_skipped_by_merge += 1;
                    plan.steps.push(PlanStep::PartSkippedMerge {
                        path: part_entry.path.display().to_string(),
                    });
                }
                Err(e) => {
                    debug!(error = %e, part = %part_entry.path.display(), "Error processing part, skipping");
                }
            }
        }

        plan.parts_skipped_by_index = parts_skipped_by_index;

        // Step 6: Determine output columns
        let output_columns = self.determine_output_columns(query, &all_rows);

        // Step 7: Check if query has an aggregate stage or explicit limit
        let has_aggregate = query.stages.iter().any(|s| {
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

        // Step 7b: Apply default sort + limit for raw (non-aggregate) queries
        if !has_aggregate && !has_explicit_limit {
            // Find flowcusExportTime column index to sort by time descending
            let time_col = "flowcusExportTime";
            let has_time_col = all_rows
                .first()
                .map(|r| r.contains_key(time_col))
                .unwrap_or(false);
            if has_time_col {
                all_rows.sort_by(|a, b| {
                    let va = a.get(time_col).and_then(|v| v.as_u64()).unwrap_or(0);
                    let vb = b.get(time_col).and_then(|v| v.as_u64()).unwrap_or(0);
                    vb.cmp(&va) // descending
                });
            }
        }

        // Step 8: Apply aggregation if present
        let (result_columns, result_rows) =
            self.apply_aggregation(query, &output_columns, &all_rows);

        plan.steps.push(PlanStep::FilterApply {
            expression: format!("{} filter(s)", filters.len()),
            rows_before: total_rows_scanned as usize,
            rows_after: all_rows.len(),
        });

        let total_parts_skipped =
            (plan.parts_skipped_by_time + parts_skipped_by_index + parts_skipped_by_merge) as u64;
        let total_parts_scanned = parts_after_time
            .saturating_sub(parts_skipped_by_index)
            .saturating_sub(parts_skipped_by_merge) as u64;

        Ok(QueryResult {
            columns: result_columns,
            rows: result_rows,
            plan,
            rows_scanned: total_rows_scanned,
            parts_scanned: total_parts_scanned,
            parts_skipped: total_parts_skipped,
        })
    }

    fn determine_output_columns(
        &self,
        query: &Query,
        rows: &[HashMap<String, serde_json::Value>],
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
                        // Return all columns we have
                        if let Some(first) = rows.first() {
                            let mut cols: Vec<String> = first.keys().cloned().collect();
                            cols.sort();
                            return cols;
                        }
                    }
                    SelectExpr::AllExcept(excluded) => {
                        if let Some(first) = rows.first() {
                            let mut cols: Vec<String> = first
                                .keys()
                                .filter(|k| !excluded.contains(k))
                                .cloned()
                                .collect();
                            cols.sort();
                            return cols;
                        }
                    }
                }
            }
        }

        // Default: return all columns
        if let Some(first) = rows.first() {
            let mut cols: Vec<String> = first.keys().cloned().collect();
            cols.sort();
            cols
        } else {
            Vec::new()
        }
    }

    fn apply_aggregation(
        &self,
        query: &Query,
        output_columns: &[String],
        rows: &[HashMap<String, serde_json::Value>],
    ) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
        // Find aggregation and limit stages
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
                    // Just limit
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
            // Default limit: 100 rows for non-aggregate queries without explicit limit
            let lim = limit.unwrap_or(100) as usize;
            let result_rows = rows
                .iter()
                .take(lim)
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
}

// ---------------------------------------------------------------------------
// Part processing
// ---------------------------------------------------------------------------

enum ProcessPartResult {
    Skipped,
    Rows {
        rows: Vec<HashMap<String, serde_json::Value>>,
        rows_scanned: u64,
    },
}

impl QueryExecutor {
    /// Process a single part: read column index, check filters, decode, evaluate.
    ///
    /// If any I/O operation fails with `NotFound`, we propagate it so the caller
    /// can treat it as a merge-race skip (the part was merged away between
    /// `list_parts` and now).
    fn process_part(
        &self,
        part_entry: &PartEntry,
        filters: &[&FilterExpr],
        referenced_cols: &[String],
        needs_all_columns: bool,
        plan: &mut ExecutionPlan,
    ) -> std::io::Result<ProcessPartResult> {
        let part_dir = &part_entry.path;

        // Read metadata — NotFound propagates to caller for merge-race handling
        let meta = part::read_meta_bin(&part_dir.join("meta.bin"))?;
        let row_count = meta.row_count as usize;

        // Read schema to know column types
        let schema = match part::read_schema_bin(&part_dir.join("schema.bin")) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(e),
            Err(_) => crate::schema::Schema {
                columns: Vec::new(),
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
            match part::read_column_index(&index_path) {
                Ok(idx) => idx,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(e),
                Err(_) => Vec::new(),
            }
        } else {
            Vec::new()
        };

        // Check column index min/max against filter predicates
        if !col_index.is_empty() && !filters.is_empty() {
            for (i, idx_entry) in col_index.iter().enumerate() {
                if let Some(col_def) = schema.columns.get(i) {
                    for filter in filters {
                        if !index_may_match(
                            filter,
                            &col_def.name,
                            &idx_entry.min_value,
                            &idx_entry.max_value,
                            col_def.storage_type,
                        ) {
                            plan.steps.push(PlanStep::ColumnIndexFilter {
                                column: col_def.name.clone(),
                                predicate: format!("{filter:?}").chars().take(80).collect(),
                                parts_skipped: 1,
                            });
                            return Ok(ProcessPartResult::Skipped);
                        }
                    }
                }
            }
        }

        // Determine which columns to actually read
        // list_columns does readdir — NotFound propagates for merge-race handling
        let columns_to_read: Vec<String> = if needs_all_columns {
            part::list_columns(part_dir)?
        } else {
            let available = part::list_columns(part_dir)?;
            referenced_cols
                .iter()
                .filter(|c| available.contains(c))
                .cloned()
                .collect()
        };

        if columns_to_read.is_empty() {
            return Ok(ProcessPartResult::Skipped);
        }

        // Bloom filter check for equality predicates
        let bloom_lookups = filters
            .iter()
            .flat_map(|f| bloom_lookup_bytes(f))
            .collect::<Vec<_>>();

        // For each bloom lookup, check if any granule might contain the value
        let mut granule_mask: Option<Vec<bool>> = None;
        let num_granules = (row_count + self.granule_size - 1) / self.granule_size.max(1);
        plan.granules_total += num_granules;

        for (col_name, value_bytes) in &bloom_lookups {
            let bloom_path = part_dir.join("columns").join(format!("{col_name}.bloom"));
            let bloom_result = granule::read_blooms(&bloom_path);
            if let Err(e) = &bloom_result {
                if e.kind() == std::io::ErrorKind::NotFound {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Part merged away: {}", bloom_path.display()),
                    ));
                }
            }
            if let Ok((_bits, blooms)) = bloom_result {
                let mask = granule_mask.get_or_insert_with(|| vec![true; blooms.len()]);
                let mut skipped = 0usize;
                for (gi, bloom) in blooms.iter().enumerate() {
                    if gi < mask.len() && mask[gi] {
                        if !bloom.may_contain(value_bytes) {
                            mask[gi] = false;
                            skipped += 1;
                        }
                    }
                }
                if skipped > 0 {
                    plan.granules_skipped_by_bloom += skipped;
                    plan.steps.push(PlanStep::BloomFilter {
                        column: col_name.clone(),
                        value: format!("{value_bytes:?}").chars().take(40).collect(),
                        granules_skipped: skipped,
                    });
                }
            }
        }

        // Mark-based seeking for range predicates
        for col_name in &columns_to_read {
            let marks_path = part_dir.join("columns").join(format!("{col_name}.mrk"));
            let marks_result = granule::read_marks(&marks_path);
            if let Err(e) = &marks_result {
                if e.kind() == std::io::ErrorKind::NotFound {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Part merged away: {}", marks_path.display()),
                    ));
                }
            }
            if let Ok((_gs, marks)) = marks_result {
                let st = col_types.get(col_name).copied().unwrap_or(StorageType::U32);
                let mut mark_skipped = 0usize;

                for (gi, mark) in marks.iter().enumerate() {
                    let mask = granule_mask.get_or_insert_with(|| vec![true; marks.len()]);
                    if gi < mask.len() && mask[gi] {
                        // Check mark min/max against filters
                        let mut can_skip = false;
                        for filter in filters {
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

                if mark_skipped > 0 {
                    plan.granules_skipped_by_marks += mark_skipped;
                    plan.steps.push(PlanStep::MarkSeek {
                        column: col_name.clone(),
                        granule_range: (0, num_granules),
                    });
                }
            }
        }

        // Decode columns
        let mut decoded: HashMap<String, ColumnBuffer> = HashMap::new();
        for col_name in &columns_to_read {
            let col_path = part_dir.join("columns").join(format!("{col_name}.col"));
            if !col_path.exists() {
                continue;
            }
            let st = col_types.get(col_name).copied().unwrap_or(StorageType::U32);
            match decode::decode_column(&col_path, st) {
                Ok(buf) => {
                    let bytes = buf.mem_size() as u64;
                    plan.steps.push(PlanStep::ColumnRead {
                        column: col_name.clone(),
                        bytes,
                    });
                    decoded.insert(col_name.clone(), buf);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Err(e);
                }
                Err(e) => {
                    trace!(error = %e, column = col_name, "Failed to decode column");
                }
            }
        }

        // Apply filters row by row, respecting granule mask
        let mut matching_rows: Vec<HashMap<String, serde_json::Value>> = Vec::new();

        for row_idx in 0..row_count {
            // Check granule mask
            if let Some(ref mask) = granule_mask {
                let granule_idx = row_idx / self.granule_size.max(1);
                if granule_idx < mask.len() && !mask[granule_idx] {
                    continue;
                }
            }

            // Evaluate filters
            let passes = filters.is_empty()
                || filters
                    .iter()
                    .all(|f| evaluate_filter(f, row_idx, &decoded));

            if passes {
                let mut row_map = HashMap::new();
                for (col_name, buf) in &decoded {
                    row_map.insert(col_name.clone(), column_to_json(buf, row_idx, col_name));
                }
                matching_rows.push(row_map);
            }
        }

        Ok(ProcessPartResult::Rows {
            rows: matching_rows,
            rows_scanned: row_count as u64,
        })
    }
}
