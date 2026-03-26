//! Structured query types for JSON-based query construction.
//!
//! These types provide a serde-deserializable alternative to the FQL text
//! syntax. The frontend sends structured JSON that deserializes into
//! [`StructuredQuery`], which is then converted to the canonical
//! [`crate::ast::Query`] via [`StructuredQuery::to_ast`].

use serde::{Deserialize, Serialize};

use crate::ast;

// ---------------------------------------------------------------------------
// Top-level query
// ---------------------------------------------------------------------------

/// A structured (JSON) query that can be converted to the FQL AST.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredQuery {
    /// Required time range for the query.
    pub time_range: StructuredTimeRange,
    /// Zero or more filter predicates.
    #[serde(default)]
    pub filters: Vec<Filter>,
    /// How to combine multiple filters (AND / OR). Defaults to AND.
    #[serde(default)]
    pub logic: FilterLogic,
    /// Optional column projection. `None` means select all.
    pub columns: Option<Vec<String>>,
    /// Optional aggregation / top-N / sort / limit.
    pub aggregate: Option<StructuredAggregate>,
    /// Optional sort (independent of aggregation sort).
    pub sort: Option<StructuredSort>,
}

// ---------------------------------------------------------------------------
// Time range
// ---------------------------------------------------------------------------

/// Time range specification: either relative ("1h") or absolute timestamps.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StructuredTimeRange {
    /// Relative duration, e.g. `"1h"`, `"30m"`, `"7d"`.
    Relative { duration: String },
    /// Absolute start/end timestamps as ISO-8601 strings.
    Absolute { start: String, end: String },
}

// ---------------------------------------------------------------------------
// Filters
// ---------------------------------------------------------------------------

/// How to combine multiple filters.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterLogic {
    /// All filters must match.
    #[default]
    And,
    /// Any filter may match.
    Or,
}

/// A single filter predicate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    /// Field name or alias (e.g. `"src"`, `"sourceTransportPort"`).
    pub field: String,
    /// Comparison operator.
    pub op: FilterOp,
    /// Value to compare against. Type depends on the operator.
    pub value: serde_json::Value,
    /// If true, negate the filter.
    #[serde(default)]
    pub negated: bool,
}

/// Filter comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOp {
    Eq,
    Ne,
    In,
    NotIn,
    Gt,
    Ge,
    Lt,
    Le,
    Between,
    Cidr,
    Wildcard,
    IpRange,
    Regex,
    NotRegex,
    Contains,
    NotContains,
    StartsWith,
    EndsWith,
    PortRange,
    Named,
    Prefix,
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

/// Aggregation stage definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StructuredAggregate {
    /// Top-N / Bottom-N query.
    TopN {
        n: u64,
        by: AggCallDef,
        bottom: Option<bool>,
    },
    /// Group-by with aggregation functions.
    GroupBy {
        keys: Vec<GroupByKeyDef>,
        functions: Vec<AggCallDef>,
    },
    /// Sort by an aggregation result.
    Sort {
        by: AggCallDef,
        descending: Option<bool>,
    },
    /// Limit the number of result rows.
    Limit { n: u64 },
}

/// Definition of an aggregation function call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggCallDef {
    /// Function name: `"sum"`, `"avg"`, `"count"`, etc.
    pub func: String,
    /// Field to aggregate over. `None` for `count()`.
    pub field: Option<String>,
}

/// Group-by key definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GroupByKeyDef {
    /// Group by a plain field.
    Field { name: String },
    /// Group by subnet prefix (e.g. `src /24`).
    Subnet { field: String, prefix_len: u8 },
    /// Group by time bucket (e.g. `"5m"`).
    TimeBucket { duration: String },
}

// ---------------------------------------------------------------------------
// Sort
// ---------------------------------------------------------------------------

/// Sort specification (outside of aggregation).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredSort {
    /// Field or expression to sort by.
    pub field: String,
    /// `"asc"` or `"desc"`. Defaults to `"desc"`.
    pub dir: Option<String>,
}

// ---------------------------------------------------------------------------
// Conversion to AST
// ---------------------------------------------------------------------------

impl StructuredQuery {
    /// Convert this structured query into the canonical FQL AST.
    ///
    /// # Errors
    ///
    /// Returns an error string if any field cannot be mapped to a valid AST
    /// node (e.g. unknown duration unit, invalid filter value).
    pub fn to_ast(&self) -> Result<ast::Query, String> {
        let time_range = convert_time_range(&self.time_range)?;

        let mut stages: Vec<ast::Stage> = Vec::new();

        // Filter stage
        if !self.filters.is_empty() {
            let filter_expr = build_filter_expr(&self.filters, &self.logic)?;
            stages.push(ast::Stage::Filter(filter_expr));
        }

        // Select stage
        if let Some(ref cols) = self.columns {
            let fields = cols
                .iter()
                .map(|name| ast::SelectField {
                    expr: ast::SelectFieldExpr::Field(name.clone()),
                    alias: None,
                })
                .collect();
            stages.push(ast::Stage::Select(ast::SelectExpr::Fields(fields)));
        }

        // Aggregate stage
        if let Some(ref agg) = self.aggregate {
            stages.push(ast::Stage::Aggregate(convert_aggregate(agg)?));
        }

        // Sort stage (standalone, not inside aggregate)
        if let Some(ref sort) = self.sort {
            let agg_call = ast::AggCall {
                func: ast::AggFunc::Count,
                field: Some(sort.field.clone()),
            };
            let descending = sort
                .dir
                .as_deref()
                .map_or(true, |d| !d.eq_ignore_ascii_case("asc"));
            stages.push(ast::Stage::Aggregate(ast::AggExpr::Sort {
                by: agg_call,
                descending,
            }));
        }

        Ok(ast::Query { time_range, stages })
    }
}

// ---------------------------------------------------------------------------
// Time range conversion
// ---------------------------------------------------------------------------

fn convert_time_range(tr: &StructuredTimeRange) -> Result<ast::TimeRange, String> {
    match tr {
        StructuredTimeRange::Relative { duration } => {
            let dur = parse_duration_string(duration)?;
            Ok(ast::TimeRange::Relative {
                duration: dur,
                offset: None,
            })
        }
        StructuredTimeRange::Absolute { start, end } => Ok(ast::TimeRange::Absolute {
            start: start.clone(),
            end: end.clone(),
        }),
    }
}

/// Parse a compound duration string like `"1h30m"`, `"5m"`, `"7d"` into
/// an AST [`Duration`](ast::Duration).
///
/// Supported units: `s` (seconds), `m` (minutes), `h` (hours), `d` (days),
/// `w` (weeks), `M` (months).
pub fn parse_duration_string(s: &str) -> Result<ast::Duration, String> {
    let mut parts = Vec::new();
    let mut remaining = s.trim();

    if remaining.is_empty() {
        return Err("empty duration string".to_string());
    }

    while !remaining.is_empty() {
        // Parse the numeric prefix
        let digit_end = remaining
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(remaining.len());

        if digit_end == 0 {
            return Err(format!(
                "expected digit in duration string at: {remaining:?}"
            ));
        }

        let value: u64 = remaining[..digit_end]
            .parse()
            .map_err(|e| format!("invalid duration number: {e}"))?;

        remaining = &remaining[digit_end..];

        // Parse the unit suffix
        if remaining.is_empty() {
            return Err(format!("missing unit after value {value} in duration"));
        }

        let (unit, consumed) = match remaining.as_bytes()[0] {
            b's' => (ast::DurationUnit::Seconds, 1),
            b'm' => (ast::DurationUnit::Minutes, 1),
            b'h' => (ast::DurationUnit::Hours, 1),
            b'd' => (ast::DurationUnit::Days, 1),
            b'w' => (ast::DurationUnit::Weeks, 1),
            b'M' => (ast::DurationUnit::Months, 1),
            other => {
                return Err(format!(
                    "unknown duration unit '{}' in {s:?}",
                    other as char
                ));
            }
        };

        parts.push(ast::DurationPart { value, unit });
        remaining = &remaining[consumed..];
    }

    Ok(ast::Duration { parts })
}

// ---------------------------------------------------------------------------
// Filter conversion
// ---------------------------------------------------------------------------

/// Explicit list of fields that should be treated as IP addresses.
const IP_FIELDS: &[&str] = &[
    // Standard src/dst
    "sourceIPv4Address",
    "destinationIPv4Address",
    "sourceIPv6Address",
    "destinationIPv6Address",
    // System columns
    "flowcusExporterIPv4",
    // Next-hop
    "ipNextHopIPv4Address",
    "ipNextHopIPv6Address",
    "bgpNextHopIPv4Address",
    "bgpNextHopIPv6Address",
    // NAT
    "postNATSourceIPv4Address",
    "postNATDestinationIPv4Address",
    "postNATSourceIPv6Address",
    "postNATDestinationIPv6Address",
    // Prefixes
    "sourceIPv4Prefix",
    "destinationIPv4Prefix",
    // MPLS
    "mplsTopLabelIPv4Address",
    "mplsTopLabelIPv6Address",
    // Collector / exporter
    "collectorIPv4Address",
    "collectorIPv6Address",
    "exporterIPv4Address",
    "exporterIPv6Address",
    // Vendor: Juniper
    "juniperNatSrcAddress",
    "juniperNatDstAddress",
    // Vendor: VMware
    "vmwareTenantSourceIPv4",
    "vmwareTenantDestIPv4",
    // Vendor: Barracuda
    "barracudaBindIPv4Address",
    // Vendor: Huawei
    "huaweiNatSourceAddress",
    "huaweiNatDestAddress",
    // Aliases
    "src",
    "dst",
    "nexthop",
    "nexthop6",
    "bgp_nexthop",
    "exporter",
];

/// Fields that should be treated as ports.
const PORT_FIELDS: &[&str] = &[
    "sourceTransportPort",
    "destinationTransportPort",
    "sport",
    "dport",
    "port",
];

/// Fields that should be treated as protocol identifiers.
const PROTO_FIELDS: &[&str] = &["protocolIdentifier", "proto"];

fn is_ip_field(field: &str) -> bool {
    IP_FIELDS.contains(&field)
        || field.contains("IPv4Address")
        || field.contains("IPv6Address")
        || field.contains("Ipv4Address")
        || field.contains("Ipv6Address")
        || field.contains("IPv4Prefix")
        || field.contains("IPv6Prefix")
}

fn is_port_field(field: &str) -> bool {
    PORT_FIELDS.contains(&field)
}

fn is_proto_field(field: &str) -> bool {
    PROTO_FIELDS.contains(&field)
}

/// Determine `IpDirection` from a field name.
fn ip_direction(field: &str) -> ast::IpDirection {
    match field {
        "sourceIPv4Address" | "sourceIPv6Address" | "src" => ast::IpDirection::Src,
        "destinationIPv4Address" | "destinationIPv6Address" | "dst" => ast::IpDirection::Dst,
        _ => ast::IpDirection::Named(field.to_string()),
    }
}

/// Determine `PortDirection` from a field name.
fn port_direction(field: &str) -> ast::PortDirection {
    match field {
        "sourceTransportPort" | "sport" => ast::PortDirection::Src,
        "destinationTransportPort" | "dport" => ast::PortDirection::Dst,
        _ => ast::PortDirection::Any,
    }
}

/// Whether a `FilterOp` is a string-oriented operator.
fn is_string_op(op: &FilterOp) -> bool {
    matches!(
        op,
        FilterOp::Regex
            | FilterOp::NotRegex
            | FilterOp::Contains
            | FilterOp::NotContains
            | FilterOp::StartsWith
            | FilterOp::EndsWith
    )
}

/// Whether a `FilterOp` is an IP-oriented operator.
fn is_ip_op(op: &FilterOp) -> bool {
    matches!(op, FilterOp::Cidr | FilterOp::Wildcard | FilterOp::IpRange)
}

fn build_filter_expr(filters: &[Filter], logic: &FilterLogic) -> Result<ast::FilterExpr, String> {
    if filters.is_empty() {
        return Err("no filters provided".to_string());
    }

    let exprs: Vec<ast::FilterExpr> = filters
        .iter()
        .map(convert_single_filter)
        .collect::<Result<Vec<_>, _>>()?;

    let combined = match logic {
        FilterLogic::And => exprs
            .into_iter()
            .reduce(|a, b| ast::FilterExpr::And(Box::new(a), Box::new(b)))
            .expect("non-empty"),
        FilterLogic::Or => exprs
            .into_iter()
            .reduce(|a, b| ast::FilterExpr::Or(Box::new(a), Box::new(b)))
            .expect("non-empty"),
    };

    Ok(combined)
}

fn convert_single_filter(filter: &Filter) -> Result<ast::FilterExpr, String> {
    let expr = if is_ip_field(&filter.field) && !is_string_op(&filter.op) {
        convert_ip_filter(filter)?
    } else if is_port_field(&filter.field) {
        convert_port_filter(filter)?
    } else if is_proto_field(&filter.field) {
        convert_proto_filter(filter)?
    } else if is_string_op(&filter.op) {
        convert_string_filter(filter)?
    } else if is_ip_op(&filter.op) {
        // Op indicates IP semantics even if field isn't in the known list
        convert_ip_filter(filter)?
    } else {
        convert_numeric_or_field_filter(filter)?
    };

    if filter.negated {
        Ok(ast::FilterExpr::Not(Box::new(expr)))
    } else {
        Ok(expr)
    }
}

// ---------------------------------------------------------------------------
// IP filter helpers
// ---------------------------------------------------------------------------

fn convert_ip_filter(filter: &Filter) -> Result<ast::FilterExpr, String> {
    let direction = ip_direction(&filter.field);
    let negated = matches!(filter.op, FilterOp::Ne | FilterOp::NotIn);

    let value = match filter.op {
        FilterOp::Eq | FilterOp::Ne => {
            let addr = json_as_string(&filter.value, "ip address")?;
            ast::IpValue::Addr(addr)
        }
        FilterOp::Cidr => {
            let cidr = json_as_string(&filter.value, "CIDR notation")?;
            ast::IpValue::Cidr(cidr)
        }
        FilterOp::Wildcard => {
            let pattern = json_as_string(&filter.value, "wildcard pattern")?;
            ast::IpValue::Wildcard(pattern)
        }
        FilterOp::In | FilterOp::NotIn => {
            let list = json_as_string_list(&filter.value, "IP list")?;
            ast::IpValue::List(list)
        }
        FilterOp::IpRange => {
            // Expect value like "10.0.0.1-10.0.0.255" — map to CIDR or list
            let range = json_as_string(&filter.value, "IP range")?;
            ast::IpValue::Cidr(range)
        }
        other => {
            return Err(format!(
                "unsupported operator {other:?} for IP field {:?}",
                filter.field
            ));
        }
    };

    Ok(ast::FilterExpr::Ip(ast::IpFilter {
        direction,
        negated,
        value,
    }))
}

// ---------------------------------------------------------------------------
// Port filter helpers
// ---------------------------------------------------------------------------

fn convert_port_filter(filter: &Filter) -> Result<ast::FilterExpr, String> {
    let direction = port_direction(&filter.field);
    let negated = matches!(filter.op, FilterOp::Ne | FilterOp::NotIn);

    let value = match filter.op {
        FilterOp::Eq | FilterOp::Ne => {
            let port = json_as_u16(&filter.value, "port number")?;
            ast::PortValue::Single(port)
        }
        FilterOp::Gt | FilterOp::Ge => {
            let port = json_as_u16(&filter.value, "port number")?;
            let start = if matches!(filter.op, FilterOp::Gt) {
                port.checked_add(1)
                    .ok_or_else(|| "port overflow".to_string())?
            } else {
                port
            };
            ast::PortValue::OpenRange(start)
        }
        FilterOp::Lt | FilterOp::Le => {
            let port = json_as_u16(&filter.value, "port number")?;
            let end = if matches!(filter.op, FilterOp::Lt) {
                port.saturating_sub(1)
            } else {
                port
            };
            ast::PortValue::Range(0, end)
        }
        FilterOp::PortRange | FilterOp::Between => {
            let (lo, hi) = json_as_u16_range(&filter.value, "port range")?;
            ast::PortValue::Range(lo, hi)
        }
        FilterOp::In | FilterOp::NotIn => {
            let nums = json_as_u16_list(&filter.value, "port list")?;
            let items: Vec<ast::PortValue> = nums.into_iter().map(ast::PortValue::Single).collect();
            ast::PortValue::List(items)
        }
        FilterOp::Named => {
            let name = json_as_string(&filter.value, "named port")?;
            ast::PortValue::Named(name)
        }
        other => {
            return Err(format!(
                "unsupported operator {other:?} for port field {:?}",
                filter.field
            ));
        }
    };

    Ok(ast::FilterExpr::Port(ast::PortFilter {
        direction,
        negated,
        value,
    }))
}

// ---------------------------------------------------------------------------
// Protocol filter helpers
// ---------------------------------------------------------------------------

fn convert_proto_filter(filter: &Filter) -> Result<ast::FilterExpr, String> {
    let negated = matches!(filter.op, FilterOp::Ne | FilterOp::NotIn);

    let value = match filter.op {
        FilterOp::Eq | FilterOp::Ne => match &filter.value {
            serde_json::Value::Number(n) => {
                let num = n
                    .as_u64()
                    .and_then(|v| u8::try_from(v).ok())
                    .ok_or_else(|| "protocol number must be 0-255".to_string())?;
                ast::ProtoValue::Number(num)
            }
            serde_json::Value::String(s) => ast::ProtoValue::Named(s.clone()),
            _ => return Err("protocol value must be a number or string".to_string()),
        },
        FilterOp::Named => {
            let name = json_as_string(&filter.value, "protocol name")?;
            ast::ProtoValue::Named(name)
        }
        FilterOp::In | FilterOp::NotIn => {
            let arr = filter
                .value
                .as_array()
                .ok_or("'in' operator requires an array value")?;
            let items: Result<Vec<ast::ProtoValue>, String> = arr
                .iter()
                .map(|v| match v {
                    serde_json::Value::Number(n) => {
                        let num = n
                            .as_u64()
                            .and_then(|val| u8::try_from(val).ok())
                            .ok_or_else(|| "protocol number must be 0-255".to_string())?;
                        Ok(ast::ProtoValue::Number(num))
                    }
                    serde_json::Value::String(s) => Ok(ast::ProtoValue::Named(s.clone())),
                    _ => Err("protocol list items must be numbers or strings".to_string()),
                })
                .collect();
            ast::ProtoValue::List(items?)
        }
        other => return Err(format!("unsupported operator {other:?} for protocol field")),
    };

    Ok(ast::FilterExpr::Proto(ast::ProtoFilter { negated, value }))
}

// ---------------------------------------------------------------------------
// String filter helpers
// ---------------------------------------------------------------------------

fn convert_string_filter(filter: &Filter) -> Result<ast::FilterExpr, String> {
    let s = json_as_string(&filter.value, "string value")?;

    let op = match filter.op {
        FilterOp::Eq => ast::StringOp::Eq,
        FilterOp::Ne => ast::StringOp::Ne,
        FilterOp::Regex => ast::StringOp::Regex,
        FilterOp::NotRegex => ast::StringOp::NotRegex,
        FilterOp::In => ast::StringOp::In,
        FilterOp::NotIn => ast::StringOp::NotIn,
        // Map contains/starts_with/ends_with to regex patterns
        FilterOp::Contains => ast::StringOp::Regex,
        FilterOp::NotContains => ast::StringOp::NotRegex,
        FilterOp::StartsWith => ast::StringOp::Regex,
        FilterOp::EndsWith => ast::StringOp::Regex,
        other => {
            return Err(format!(
                "unsupported operator {other:?} for string field {:?}",
                filter.field
            ));
        }
    };

    // Convert contains/starts_with/ends_with to regex patterns
    let value = match filter.op {
        FilterOp::Contains | FilterOp::NotContains => regex_escape(&s),
        FilterOp::StartsWith => format!("^{}", regex_escape(&s)),
        FilterOp::EndsWith => format!("{}$", regex_escape(&s)),
        _ => s,
    };

    Ok(ast::FilterExpr::StringFilter(ast::StringFilterExpr {
        field: filter.field.clone(),
        op,
        value,
    }))
}

/// Minimal regex escaping for literal string matching.
fn regex_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for c in s.chars() {
        if matches!(
            c,
            '.' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '\\' | '^' | '$' | '|'
        ) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

// ---------------------------------------------------------------------------
// Numeric / field filter helpers
// ---------------------------------------------------------------------------

fn convert_numeric_or_field_filter(filter: &Filter) -> Result<ast::FilterExpr, String> {
    match filter.op {
        FilterOp::Eq | FilterOp::Ne | FilterOp::Gt | FilterOp::Ge | FilterOp::Lt | FilterOp::Le => {
            let compare_op = match filter.op {
                FilterOp::Eq => ast::CompareOp::Eq,
                FilterOp::Ne => ast::CompareOp::Ne,
                FilterOp::Gt => ast::CompareOp::Gt,
                FilterOp::Ge => ast::CompareOp::Ge,
                FilterOp::Lt => ast::CompareOp::Lt,
                FilterOp::Le => ast::CompareOp::Le,
                _ => unreachable!(),
            };

            // Try as numeric first, fall back to string eq/ne
            if let Some(n) = filter.value.as_u64() {
                Ok(ast::FilterExpr::Numeric(ast::NumericFilter {
                    field: filter.field.clone(),
                    op: compare_op,
                    value: ast::NumericValue::Integer(n),
                }))
            } else if let Some(s) = filter.value.as_str() {
                // Could be a suffixed value like "1M" or a string comparison
                if let Some(numeric) = parse_suffixed_number(s) {
                    Ok(ast::FilterExpr::Numeric(ast::NumericFilter {
                        field: filter.field.clone(),
                        op: compare_op,
                        value: numeric,
                    }))
                } else {
                    let string_op = match compare_op {
                        ast::CompareOp::Eq => ast::StringOp::Eq,
                        ast::CompareOp::Ne => ast::StringOp::Ne,
                        _ => {
                            return Err(format!(
                                "string field {:?} does not support operator {:?}",
                                filter.field, filter.op
                            ));
                        }
                    };
                    Ok(ast::FilterExpr::StringFilter(ast::StringFilterExpr {
                        field: filter.field.clone(),
                        op: string_op,
                        value: s.to_string(),
                    }))
                }
            } else {
                Err(format!(
                    "filter value for {:?} must be a number or string, got: {}",
                    filter.field, filter.value
                ))
            }
        }
        FilterOp::Between => {
            // Between requires an array of two values; emit Ge + Le combined with And
            let arr = filter
                .value
                .as_array()
                .ok_or("'between' operator requires an array [low, high]")?;
            if arr.len() != 2 {
                return Err("'between' requires exactly 2 values".to_string());
            }
            let lo = arr[0]
                .as_u64()
                .ok_or("'between' low bound must be a number")?;
            let hi = arr[1]
                .as_u64()
                .ok_or("'between' high bound must be a number")?;

            let ge = ast::FilterExpr::Numeric(ast::NumericFilter {
                field: filter.field.clone(),
                op: ast::CompareOp::Ge,
                value: ast::NumericValue::Integer(lo),
            });
            let le = ast::FilterExpr::Numeric(ast::NumericFilter {
                field: filter.field.clone(),
                op: ast::CompareOp::Le,
                value: ast::NumericValue::Integer(hi),
            });
            Ok(ast::FilterExpr::And(Box::new(ge), Box::new(le)))
        }
        FilterOp::In | FilterOp::NotIn => {
            // For numeric In/NotIn, use a string-based In filter on the field
            let list = json_as_string_list(&filter.value, "value list")?;
            let op = if matches!(filter.op, FilterOp::In) {
                ast::StringOp::In
            } else {
                ast::StringOp::NotIn
            };
            Ok(ast::FilterExpr::StringFilter(ast::StringFilterExpr {
                field: filter.field.clone(),
                op,
                value: list.join(","),
            }))
        }
        other => Err(format!(
            "unsupported operator {other:?} for field {:?}",
            filter.field
        )),
    }
}

/// Try to parse a suffixed numeric value like `"1M"`, `"500K"`, `"10G"`.
fn parse_suffixed_number(s: &str) -> Option<ast::NumericValue> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let digit_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    if digit_end == 0 || digit_end == s.len() {
        // All digits or starts with non-digit — try plain parse
        return s.parse::<u64>().ok().map(ast::NumericValue::Integer);
    }

    let value: u64 = s[..digit_end].parse().ok()?;
    let suffix = &s[digit_end..];
    Some(ast::NumericValue::WithSuffix(value, suffix.to_string()))
}

// ---------------------------------------------------------------------------
// Aggregate conversion
// ---------------------------------------------------------------------------

fn convert_aggregate(agg: &StructuredAggregate) -> Result<ast::AggExpr, String> {
    match agg {
        StructuredAggregate::TopN { n, by, bottom } => Ok(ast::AggExpr::TopN {
            n: *n,
            by: convert_agg_call(by)?,
            bottom: bottom.unwrap_or(false),
        }),
        StructuredAggregate::GroupBy { keys, functions } => {
            let ast_keys: Result<Vec<ast::GroupByKey>, String> =
                keys.iter().map(convert_group_by_key).collect();
            let ast_funcs: Result<Vec<ast::AggCall>, String> =
                functions.iter().map(convert_agg_call).collect();
            Ok(ast::AggExpr::GroupBy {
                keys: ast_keys?,
                functions: ast_funcs?,
            })
        }
        StructuredAggregate::Sort { by, descending } => Ok(ast::AggExpr::Sort {
            by: convert_agg_call(by)?,
            descending: descending.unwrap_or(true),
        }),
        StructuredAggregate::Limit { n } => Ok(ast::AggExpr::Limit(*n)),
    }
}

fn convert_agg_call(def: &AggCallDef) -> Result<ast::AggCall, String> {
    let func = parse_agg_func(&def.func)?;
    Ok(ast::AggCall {
        func,
        field: def.field.clone(),
    })
}

fn parse_agg_func(s: &str) -> Result<ast::AggFunc, String> {
    match s.to_lowercase().as_str() {
        "sum" => Ok(ast::AggFunc::Sum),
        "avg" => Ok(ast::AggFunc::Avg),
        "min" => Ok(ast::AggFunc::Min),
        "max" => Ok(ast::AggFunc::Max),
        "count" => Ok(ast::AggFunc::Count),
        "uniq" => Ok(ast::AggFunc::Uniq),
        "p50" => Ok(ast::AggFunc::P50),
        "p95" => Ok(ast::AggFunc::P95),
        "p99" => Ok(ast::AggFunc::P99),
        "stddev" => Ok(ast::AggFunc::Stddev),
        "rate" => Ok(ast::AggFunc::Rate),
        "first" => Ok(ast::AggFunc::First),
        "last" => Ok(ast::AggFunc::Last),
        other => Err(format!("unknown aggregation function: {other:?}")),
    }
}

fn convert_group_by_key(key: &GroupByKeyDef) -> Result<ast::GroupByKey, String> {
    match key {
        GroupByKeyDef::Field { name } => Ok(ast::GroupByKey::Field(name.clone())),
        GroupByKeyDef::Subnet { field, prefix_len } => Ok(ast::GroupByKey::Subnet {
            field: field.clone(),
            prefix_len: *prefix_len,
        }),
        GroupByKeyDef::TimeBucket { duration } => {
            let dur = parse_duration_string(duration)?;
            Ok(ast::GroupByKey::TimeBucket(dur))
        }
    }
}

// ---------------------------------------------------------------------------
// JSON value extraction helpers
// ---------------------------------------------------------------------------

fn json_as_string(v: &serde_json::Value, context: &str) -> Result<String, String> {
    match v {
        serde_json::Value::String(s) => Ok(s.clone()),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        _ => Err(format!("expected string for {context}, got: {v}")),
    }
}

fn json_as_string_list(v: &serde_json::Value, context: &str) -> Result<Vec<String>, String> {
    match v {
        serde_json::Value::Array(arr) => arr
            .iter()
            .map(|item| json_as_string(item, context))
            .collect(),
        _ => Err(format!("expected array for {context}, got: {v}")),
    }
}

fn json_as_u16(v: &serde_json::Value, context: &str) -> Result<u16, String> {
    match v {
        serde_json::Value::Number(n) => n
            .as_u64()
            .and_then(|val| u16::try_from(val).ok())
            .ok_or_else(|| format!("{context}: value out of u16 range")),
        serde_json::Value::String(s) => s.parse::<u16>().map_err(|e| format!("{context}: {e}")),
        _ => Err(format!("expected number for {context}, got: {v}")),
    }
}

fn json_as_u16_list(v: &serde_json::Value, context: &str) -> Result<Vec<u16>, String> {
    match v {
        serde_json::Value::Array(arr) => {
            arr.iter().map(|item| json_as_u16(item, context)).collect()
        }
        _ => Err(format!("expected array for {context}, got: {v}")),
    }
}

fn json_as_u16_range(v: &serde_json::Value, context: &str) -> Result<(u16, u16), String> {
    match v {
        serde_json::Value::Array(arr) if arr.len() == 2 => {
            let lo = json_as_u16(&arr[0], context)?;
            let hi = json_as_u16(&arr[1], context)?;
            Ok((lo, hi))
        }
        serde_json::Value::String(s) => {
            // Parse "80-90" format
            let parts: Vec<&str> = s.splitn(2, '-').collect();
            if parts.len() == 2 {
                let lo: u16 = parts[0]
                    .trim()
                    .parse()
                    .map_err(|e| format!("{context}: {e}"))?;
                let hi: u16 = parts[1]
                    .trim()
                    .parse()
                    .map_err(|e| format!("{context}: {e}"))?;
                Ok((lo, hi))
            } else {
                Err(format!("{context}: expected 'low-high' format"))
            }
        }
        _ => Err(format!(
            "expected array [low, high] or string 'low-high' for {context}, got: {v}"
        )),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_simple() {
        let d = parse_duration_string("1h").unwrap();
        assert_eq!(d.parts.len(), 1);
        assert_eq!(d.parts[0].value, 1);
        assert_eq!(d.parts[0].unit, ast::DurationUnit::Hours);
    }

    #[test]
    fn parse_duration_compound() {
        let d = parse_duration_string("1h30m").unwrap();
        assert_eq!(d.parts.len(), 2);
        assert_eq!(d.parts[0].value, 1);
        assert_eq!(d.parts[0].unit, ast::DurationUnit::Hours);
        assert_eq!(d.parts[1].value, 30);
        assert_eq!(d.parts[1].unit, ast::DurationUnit::Minutes);
    }

    #[test]
    fn parse_duration_all_units() {
        let d = parse_duration_string("7d12h30m45s").unwrap();
        assert_eq!(d.parts.len(), 4);
        assert_eq!(d.parts[0].unit, ast::DurationUnit::Days);
        assert_eq!(d.parts[1].unit, ast::DurationUnit::Hours);
        assert_eq!(d.parts[2].unit, ast::DurationUnit::Minutes);
        assert_eq!(d.parts[3].unit, ast::DurationUnit::Seconds);
    }

    #[test]
    fn parse_duration_errors() {
        assert!(parse_duration_string("").is_err());
        assert!(parse_duration_string("h").is_err());
        assert!(parse_duration_string("5").is_err());
        assert!(parse_duration_string("1x").is_err());
    }

    #[test]
    fn to_ast_relative_time() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![],
            logic: FilterLogic::default(),
            columns: None,
            aggregate: None,
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        assert!(matches!(q.time_range, ast::TimeRange::Relative { .. }));
        assert!(q.stages.is_empty());
    }

    #[test]
    fn to_ast_absolute_time() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Absolute {
                start: "2024-03-15T08:00:00Z".to_string(),
                end: "2024-03-15T17:00:00Z".to_string(),
            },
            filters: vec![],
            logic: FilterLogic::default(),
            columns: None,
            aggregate: None,
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        assert!(matches!(q.time_range, ast::TimeRange::Absolute { .. }));
    }

    #[test]
    fn to_ast_ip_filter_cidr() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![Filter {
                field: "src".to_string(),
                op: FilterOp::Cidr,
                value: serde_json::json!("10.0.0.0/8"),
                negated: false,
            }],
            logic: FilterLogic::And,
            columns: None,
            aggregate: None,
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        assert_eq!(q.stages.len(), 1);
        match &q.stages[0] {
            ast::Stage::Filter(ast::FilterExpr::Ip(ip)) => {
                assert_eq!(ip.direction, ast::IpDirection::Src);
                assert!(!ip.negated);
                assert!(matches!(&ip.value, ast::IpValue::Cidr(c) if c == "10.0.0.0/8"));
            }
            other => panic!("expected IP filter, got: {other:?}"),
        }
    }

    #[test]
    fn to_ast_ip_filter_eq() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "5m".to_string(),
            },
            filters: vec![Filter {
                field: "destinationIPv4Address".to_string(),
                op: FilterOp::Eq,
                value: serde_json::json!("192.168.1.1"),
                negated: false,
            }],
            logic: FilterLogic::And,
            columns: None,
            aggregate: None,
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        match &q.stages[0] {
            ast::Stage::Filter(ast::FilterExpr::Ip(ip)) => {
                assert_eq!(ip.direction, ast::IpDirection::Dst);
                assert!(matches!(&ip.value, ast::IpValue::Addr(a) if a == "192.168.1.1"));
            }
            other => panic!("expected IP filter, got: {other:?}"),
        }
    }

    #[test]
    fn to_ast_port_filter() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![Filter {
                field: "dport".to_string(),
                op: FilterOp::Eq,
                value: serde_json::json!(443),
                negated: false,
            }],
            logic: FilterLogic::And,
            columns: None,
            aggregate: None,
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        match &q.stages[0] {
            ast::Stage::Filter(ast::FilterExpr::Port(p)) => {
                assert_eq!(p.direction, ast::PortDirection::Dst);
                assert!(!p.negated);
                assert!(matches!(p.value, ast::PortValue::Single(443)));
            }
            other => panic!("expected port filter, got: {other:?}"),
        }
    }

    #[test]
    fn to_ast_port_range() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![Filter {
                field: "sport".to_string(),
                op: FilterOp::PortRange,
                value: serde_json::json!([1024, 65535]),
                negated: false,
            }],
            logic: FilterLogic::And,
            columns: None,
            aggregate: None,
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        match &q.stages[0] {
            ast::Stage::Filter(ast::FilterExpr::Port(p)) => {
                assert_eq!(p.direction, ast::PortDirection::Src);
                assert!(matches!(p.value, ast::PortValue::Range(1024, 65535)));
            }
            other => panic!("expected port filter, got: {other:?}"),
        }
    }

    #[test]
    fn to_ast_proto_filter_named() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![Filter {
                field: "proto".to_string(),
                op: FilterOp::Eq,
                value: serde_json::json!("tcp"),
                negated: false,
            }],
            logic: FilterLogic::And,
            columns: None,
            aggregate: None,
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        match &q.stages[0] {
            ast::Stage::Filter(ast::FilterExpr::Proto(p)) => {
                assert!(!p.negated);
                assert!(matches!(&p.value, ast::ProtoValue::Named(n) if n == "tcp"));
            }
            other => panic!("expected proto filter, got: {other:?}"),
        }
    }

    #[test]
    fn to_ast_proto_filter_number() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![Filter {
                field: "protocolIdentifier".to_string(),
                op: FilterOp::Eq,
                value: serde_json::json!(6),
                negated: false,
            }],
            logic: FilterLogic::And,
            columns: None,
            aggregate: None,
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        match &q.stages[0] {
            ast::Stage::Filter(ast::FilterExpr::Proto(p)) => {
                assert!(matches!(p.value, ast::ProtoValue::Number(6)));
            }
            other => panic!("expected proto filter, got: {other:?}"),
        }
    }

    #[test]
    fn to_ast_numeric_filter() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![Filter {
                field: "octetDeltaCount".to_string(),
                op: FilterOp::Gt,
                value: serde_json::json!(1000),
                negated: false,
            }],
            logic: FilterLogic::And,
            columns: None,
            aggregate: None,
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        match &q.stages[0] {
            ast::Stage::Filter(ast::FilterExpr::Numeric(n)) => {
                assert_eq!(n.field, "octetDeltaCount");
                assert_eq!(n.op, ast::CompareOp::Gt);
                assert!(matches!(n.value, ast::NumericValue::Integer(1000)));
            }
            other => panic!("expected numeric filter, got: {other:?}"),
        }
    }

    #[test]
    fn to_ast_string_filter_contains() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![Filter {
                field: "applicationName".to_string(),
                op: FilterOp::Contains,
                value: serde_json::json!("http"),
                negated: false,
            }],
            logic: FilterLogic::And,
            columns: None,
            aggregate: None,
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        match &q.stages[0] {
            ast::Stage::Filter(ast::FilterExpr::StringFilter(sf)) => {
                assert_eq!(sf.field, "applicationName");
                assert_eq!(sf.op, ast::StringOp::Regex);
                assert_eq!(sf.value, "http");
            }
            other => panic!("expected string filter, got: {other:?}"),
        }
    }

    #[test]
    fn to_ast_negated_filter() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![Filter {
                field: "src".to_string(),
                op: FilterOp::Cidr,
                value: serde_json::json!("10.0.0.0/8"),
                negated: true,
            }],
            logic: FilterLogic::And,
            columns: None,
            aggregate: None,
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        assert!(matches!(
            &q.stages[0],
            ast::Stage::Filter(ast::FilterExpr::Not(_))
        ));
    }

    #[test]
    fn to_ast_multiple_filters_and() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![
                Filter {
                    field: "src".to_string(),
                    op: FilterOp::Cidr,
                    value: serde_json::json!("10.0.0.0/8"),
                    negated: false,
                },
                Filter {
                    field: "dport".to_string(),
                    op: FilterOp::Eq,
                    value: serde_json::json!(80),
                    negated: false,
                },
            ],
            logic: FilterLogic::And,
            columns: None,
            aggregate: None,
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        assert!(matches!(
            &q.stages[0],
            ast::Stage::Filter(ast::FilterExpr::And(_, _))
        ));
    }

    #[test]
    fn to_ast_multiple_filters_or() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![
                Filter {
                    field: "dport".to_string(),
                    op: FilterOp::Eq,
                    value: serde_json::json!(80),
                    negated: false,
                },
                Filter {
                    field: "dport".to_string(),
                    op: FilterOp::Eq,
                    value: serde_json::json!(443),
                    negated: false,
                },
            ],
            logic: FilterLogic::Or,
            columns: None,
            aggregate: None,
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        assert!(matches!(
            &q.stages[0],
            ast::Stage::Filter(ast::FilterExpr::Or(_, _))
        ));
    }

    #[test]
    fn to_ast_column_projection() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![],
            logic: FilterLogic::default(),
            columns: Some(vec![
                "src".to_string(),
                "dst".to_string(),
                "bytes".to_string(),
            ]),
            aggregate: None,
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        assert_eq!(q.stages.len(), 1);
        match &q.stages[0] {
            ast::Stage::Select(ast::SelectExpr::Fields(fields)) => {
                assert_eq!(fields.len(), 3);
                assert!(matches!(&fields[0].expr, ast::SelectFieldExpr::Field(f) if f == "src"));
            }
            other => panic!("expected select, got: {other:?}"),
        }
    }

    #[test]
    fn to_ast_top_n() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![],
            logic: FilterLogic::default(),
            columns: None,
            aggregate: Some(StructuredAggregate::TopN {
                n: 10,
                by: AggCallDef {
                    func: "sum".to_string(),
                    field: Some("octetDeltaCount".to_string()),
                },
                bottom: None,
            }),
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        match &q.stages[0] {
            ast::Stage::Aggregate(ast::AggExpr::TopN { n, by, bottom }) => {
                assert_eq!(*n, 10);
                assert_eq!(by.func, ast::AggFunc::Sum);
                assert_eq!(by.field.as_deref(), Some("octetDeltaCount"));
                assert!(!bottom);
            }
            other => panic!("expected top-n, got: {other:?}"),
        }
    }

    #[test]
    fn to_ast_group_by() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![],
            logic: FilterLogic::default(),
            columns: None,
            aggregate: Some(StructuredAggregate::GroupBy {
                keys: vec![
                    GroupByKeyDef::Field {
                        name: "src".to_string(),
                    },
                    GroupByKeyDef::TimeBucket {
                        duration: "5m".to_string(),
                    },
                ],
                functions: vec![AggCallDef {
                    func: "sum".to_string(),
                    field: Some("octetDeltaCount".to_string()),
                }],
            }),
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        match &q.stages[0] {
            ast::Stage::Aggregate(ast::AggExpr::GroupBy { keys, functions }) => {
                assert_eq!(keys.len(), 2);
                assert!(matches!(&keys[0], ast::GroupByKey::Field(f) if f == "src"));
                assert!(matches!(&keys[1], ast::GroupByKey::TimeBucket(_)));
                assert_eq!(functions.len(), 1);
                assert_eq!(functions[0].func, ast::AggFunc::Sum);
            }
            other => panic!("expected group-by, got: {other:?}"),
        }
    }

    #[test]
    fn to_ast_limit() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![],
            logic: FilterLogic::default(),
            columns: None,
            aggregate: Some(StructuredAggregate::Limit { n: 50 }),
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        assert!(matches!(
            &q.stages[0],
            ast::Stage::Aggregate(ast::AggExpr::Limit(50))
        ));
    }

    #[test]
    fn to_ast_between_numeric() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![Filter {
                field: "octetDeltaCount".to_string(),
                op: FilterOp::Between,
                value: serde_json::json!([100, 5000]),
                negated: false,
            }],
            logic: FilterLogic::And,
            columns: None,
            aggregate: None,
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        // Between produces And(Ge, Le)
        assert!(matches!(
            &q.stages[0],
            ast::Stage::Filter(ast::FilterExpr::And(_, _))
        ));
    }

    #[test]
    fn to_ast_full_pipeline() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![Filter {
                field: "src".to_string(),
                op: FilterOp::Cidr,
                value: serde_json::json!("10.0.0.0/8"),
                negated: false,
            }],
            logic: FilterLogic::And,
            columns: Some(vec!["src".to_string(), "dst".to_string()]),
            aggregate: Some(StructuredAggregate::TopN {
                n: 10,
                by: AggCallDef {
                    func: "count".to_string(),
                    field: None,
                },
                bottom: None,
            }),
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        // filter + select + aggregate = 3 stages
        assert_eq!(q.stages.len(), 3);
        assert!(matches!(&q.stages[0], ast::Stage::Filter(_)));
        assert!(matches!(&q.stages[1], ast::Stage::Select(_)));
        assert!(matches!(&q.stages[2], ast::Stage::Aggregate(_)));
    }

    #[test]
    fn serde_roundtrip() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![Filter {
                field: "src".to_string(),
                op: FilterOp::Cidr,
                value: serde_json::json!("10.0.0.0/8"),
                negated: false,
            }],
            logic: FilterLogic::And,
            columns: Some(vec!["src".to_string()]),
            aggregate: None,
            sort: None,
        };
        let json = serde_json::to_string(&sq).unwrap();
        let deserialized: StructuredQuery = serde_json::from_str(&json).unwrap();
        let ast1 = sq.to_ast().unwrap();
        let ast2 = deserialized.to_ast().unwrap();
        // Both should produce the same serialized AST
        let s1 = serde_json::to_string(&ast1).unwrap();
        let s2 = serde_json::to_string(&ast2).unwrap();
        assert_eq!(s1, s2);
    }

    // -----------------------------------------------------------------------
    // IP field detection and Named direction routing
    // -----------------------------------------------------------------------

    #[test]
    fn is_ip_field_detects_nat_fields() {
        assert!(is_ip_field("postNATSourceIPv4Address"));
        assert!(is_ip_field("postNATDestinationIPv4Address"));
        assert!(is_ip_field("postNATSourceIPv6Address"));
        assert!(is_ip_field("postNATDestinationIPv6Address"));
    }

    #[test]
    fn is_ip_field_detects_vendor_fields() {
        assert!(is_ip_field("juniperNatSrcAddress"));
        assert!(is_ip_field("vmwareTenantSourceIPv4"));
        assert!(is_ip_field("huaweiNatSourceAddress"));
        assert!(is_ip_field("barracudaBindIPv4Address"));
    }

    #[test]
    fn is_ip_field_suffix_fallback() {
        // Unknown field with IPv4Address suffix should be detected
        assert!(is_ip_field("someVendorIPv4Address"));
        assert!(is_ip_field("customIPv6Address"));
        assert!(!is_ip_field("octetDeltaCount"));
        assert!(!is_ip_field("sourceTransportPort"));
    }

    #[test]
    fn ip_direction_named_for_non_standard() {
        assert_eq!(
            ip_direction("postNATSourceIPv4Address"),
            ast::IpDirection::Named("postNATSourceIPv4Address".to_string())
        );
        assert_eq!(
            ip_direction("ipNextHopIPv4Address"),
            ast::IpDirection::Named("ipNextHopIPv4Address".to_string())
        );
        // Standard src/dst still use Src/Dst
        assert_eq!(ip_direction("sourceIPv4Address"), ast::IpDirection::Src);
        assert_eq!(
            ip_direction("destinationIPv4Address"),
            ast::IpDirection::Dst
        );
        assert_eq!(ip_direction("src"), ast::IpDirection::Src);
        assert_eq!(ip_direction("dst"), ast::IpDirection::Dst);
    }

    #[test]
    fn to_ast_nat_ip_filter_routes_to_ip_filter() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![Filter {
                field: "postNATSourceIPv4Address".to_string(),
                op: FilterOp::Eq,
                value: serde_json::json!("91.150.183.53"),
                negated: false,
            }],
            logic: FilterLogic::And,
            columns: None,
            aggregate: None,
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        match &q.stages[0] {
            ast::Stage::Filter(ast::FilterExpr::Ip(ip)) => {
                assert_eq!(
                    ip.direction,
                    ast::IpDirection::Named("postNATSourceIPv4Address".to_string())
                );
                assert!(!ip.negated);
                assert!(matches!(&ip.value, ast::IpValue::Addr(a) if a == "91.150.183.53"));
            }
            other => panic!("expected IP filter with Named direction, got: {other:?}"),
        }
    }

    #[test]
    fn to_ast_nat_ipv6_filter() {
        let sq = StructuredQuery {
            time_range: StructuredTimeRange::Relative {
                duration: "1h".to_string(),
            },
            filters: vec![Filter {
                field: "postNATSourceIPv6Address".to_string(),
                op: FilterOp::Ne,
                value: serde_json::json!("::"),
                negated: false,
            }],
            logic: FilterLogic::And,
            columns: None,
            aggregate: None,
            sort: None,
        };
        let q = sq.to_ast().unwrap();
        match &q.stages[0] {
            ast::Stage::Filter(ast::FilterExpr::Ip(ip)) => {
                assert_eq!(
                    ip.direction,
                    ast::IpDirection::Named("postNATSourceIPv6Address".to_string())
                );
                assert!(ip.negated);
            }
            other => panic!("expected IP filter, got: {other:?}"),
        }
    }
}
