use std::collections::HashMap;
use std::convert::Infallible;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use axum::{
    Router,
    extract::{Query as AxumQuery, State},
    http::StatusCode,
    response::{
        IntoResponse, Json,
        sse::{Event, Sse},
    },
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::state::{AppState, CachedQueryStats, CachedResult};

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

/// Incoming query request: either a structured JSON query or an FQL text query.
///
/// Serde's untagged enum tries `Structured` first (requires `time_range` object),
/// then falls back to `Fql` (requires `query` string).
#[derive(Deserialize)]
#[serde(untagged)]
pub enum QueryRequest {
    Structured(StructuredQueryRequest),
    Fql(FqlQueryRequest),
}

/// JSON-based structured query from the frontend query builder.
#[derive(Deserialize)]
pub struct StructuredQueryRequest {
    pub time_range: flowcus_query::structured::StructuredTimeRange,
    #[serde(default)]
    pub filters: Vec<flowcus_query::structured::Filter>,
    #[serde(default)]
    pub logic: flowcus_query::structured::FilterLogic,
    pub columns: Option<Vec<String>>,
    pub aggregate: Option<flowcus_query::structured::StructuredAggregate>,
    pub sort: Option<flowcus_query::structured::StructuredSort>,
    pub offset: Option<u64>,
    pub limit: Option<u64>,
    /// Pin the time range for paginated queries.
    pub time_start: Option<u32>,
    pub time_end: Option<u32>,
}

/// FQL text query (the original path).
#[derive(Deserialize)]
pub struct FqlQueryRequest {
    pub query: String,
    pub offset: Option<u64>,
    pub limit: Option<u64>,
    /// Pin the time range for paginated queries. When set, the executor
    /// uses these absolute bounds instead of re-resolving `last Xm` relative
    /// to `now()`. Prevents the time window from shifting during infinite scroll.
    pub time_start: Option<u32>,
    pub time_end: Option<u32>,
}

#[derive(Serialize)]
pub struct QueryResponse {
    pub parsed: serde_json::Value,
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub stats: QueryStats,
    pub explain: Vec<serde_json::Value>,
    pub pagination: Pagination,
    pub time_range: TimeRangeBounds,
}

#[derive(Serialize, Clone)]
pub struct TimeRangeBounds {
    pub start: u32,
    pub end: u32,
}

#[derive(Serialize, Clone)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
}

#[derive(Serialize)]
pub struct Pagination {
    pub offset: u64,
    pub limit: u64,
    pub total: u64,
    pub has_more: bool,
}

#[derive(Serialize)]
pub struct QueryStats {
    pub parse_time_us: u64,
    pub execution_time_us: u64,
    pub rows_scanned: u64,
    pub rows_returned: u64,
    pub total_rows: u64,
    pub parts_scanned: u64,
    pub parts_skipped: u64,
    pub bytes_read: u64,
    pub cached: bool,
}

#[derive(Serialize)]
pub struct QueryError {
    pub error: String,
    pub position: Option<usize>,
    pub suggestion: Option<String>,
}

#[derive(Deserialize)]
pub struct CompletionsParams {
    pub prefix: Option<String>,
    pub position: Option<usize>,
}

#[derive(Serialize)]
pub struct CompletionItem {
    pub label: String,
    pub kind: CompletionKind,
    pub detail: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionKind {
    Keyword,
    Field,
    Function,
    NamedPort,
}

#[derive(Serialize)]
pub struct FieldInfo {
    pub name: String,
    pub data_type: String,
    pub description: String,
    pub element_id: u16,
    pub enterprise_id: u32,
}

// ---------------------------------------------------------------------------
// Schema types
// ---------------------------------------------------------------------------

/// Response for `GET /api/query/schema`: describes available filter types
/// and field metadata for the structured query builder.
#[derive(Serialize)]
pub struct SchemaResponse {
    pub filter_types: HashMap<String, Vec<String>>,
    pub fields: Vec<SchemaField>,
    pub op_hints: HashMap<String, String>,
}

/// A single field with its semantic filter type and data type.
#[derive(Serialize)]
pub struct SchemaField {
    pub name: String,
    pub filter_type: String,
    pub data_type: String,
    pub description: String,
    pub semantic_hint: String,
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/query", post(execute_query))
        .route("/query/completions", get(completions))
        .route("/query/fields", get(fields))
        .route("/query/schema", get(schema))
}

/// SSE routes that must NOT be wrapped in compression (compression buffers
/// the body, defeating streaming).
pub fn sse_routes() -> Router<AppState> {
    Router::new().route("/stats/histogram", post(stats_histogram))
}

// ---------------------------------------------------------------------------
// POST /api/query
// ---------------------------------------------------------------------------

/// Parsed query with metadata needed for execution.
struct ParsedRequest {
    query: flowcus_query::Query,
    parsed_json: serde_json::Value,
    cache_key: u64,
    offset: u64,
    limit: u64,
    pinned_time: Option<(u32, u32)>,
}

/// Parse the incoming request (FQL or structured) into a canonical AST.
fn parse_request(req: QueryRequest) -> Result<ParsedRequest, (QueryError, std::time::Duration)> {
    let start = Instant::now();

    match req {
        QueryRequest::Fql(fql) => {
            let offset = fql.offset.unwrap_or(0);
            let limit = fql.limit.unwrap_or(100).min(10_000);
            let pinned_time = match (fql.time_start, fql.time_end) {
                (Some(s), Some(e)) => Some((s, e)),
                _ => None,
            };
            let cache_key = crate::state::QueryCache::cache_key(&fql.query, offset, limit);

            match flowcus_query::parse(&fql.query) {
                Ok(query) => {
                    let parsed_json = serde_json::to_value(&query).unwrap_or_default();
                    Ok(ParsedRequest {
                        query,
                        parsed_json,
                        cache_key,
                        offset,
                        limit,
                        pinned_time,
                    })
                }
                Err(err) => {
                    let (position, suggestion) = parse_error_details(&err);
                    Err((
                        QueryError {
                            error: err.to_string(),
                            position,
                            suggestion,
                        },
                        start.elapsed(),
                    ))
                }
            }
        }
        QueryRequest::Structured(s) => {
            let offset = s.offset.unwrap_or(0);
            let limit = s.limit.unwrap_or(100).min(10_000);
            let pinned_time = match (s.time_start, s.time_end) {
                (Some(start), Some(end)) => Some((start, end)),
                _ => None,
            };

            // Build the structured query and convert to AST
            let structured = flowcus_query::structured::StructuredQuery {
                time_range: s.time_range,
                filters: s.filters,
                logic: s.logic,
                columns: s.columns,
                aggregate: s.aggregate,
                sort: s.sort,
            };

            // Cache key: hash the serialized structured query
            #[allow(clippy::collection_is_never_read)]
            let cache_key = {
                let canonical = serde_json::to_string(&structured).unwrap_or_default();
                let mut hasher = DefaultHasher::new();
                canonical.hash(&mut hasher);
                offset.hash(&mut hasher);
                limit.hash(&mut hasher);
                hasher.finish()
            };

            match structured.to_ast() {
                Ok(query) => {
                    let parsed_json = serde_json::to_value(&query).unwrap_or_default();
                    Ok(ParsedRequest {
                        query,
                        parsed_json,
                        cache_key,
                        offset,
                        limit,
                        pinned_time,
                    })
                }
                Err(err) => Err((
                    QueryError {
                        error: err,
                        position: None,
                        suggestion: None,
                    },
                    start.elapsed(),
                )),
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn execute_query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    let start = Instant::now();

    state
        .metrics()
        .query_requests
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let parsed = match parse_request(req) {
        Ok(p) => p,
        Err((error, _duration)) => {
            warn!(error = %error.error, "Query parse failed");
            state
                .metrics()
                .query_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::to_value(error).unwrap_or_default()),
            )
                .into_response();
        }
    };

    let parse_time = start.elapsed();
    let ParsedRequest {
        query,
        parsed_json,
        cache_key,
        offset: req_offset,
        limit: req_limit,
        pinned_time,
    } = parsed;

    // Combine flush + merge counters for cache invalidation.
    let flush_count = state
        .metrics()
        .writer_parts_flushed
        .load(std::sync::atomic::Ordering::Acquire);
    let merge_count = state
        .metrics()
        .merge_completed
        .load(std::sync::atomic::Ordering::Acquire);
    let current_flush_count = flush_count.wrapping_add(merge_count);

    if let Some(cached) = state.query_cache().get(cache_key, current_flush_count) {
        debug!("Query cache hit");
        state
            .metrics()
            .query_cache_hits
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let columns = resolve_column_types(&cached.columns);
        let response = QueryResponse {
            parsed: parsed_json,
            columns,
            rows: cached.rows,
            stats: QueryStats {
                parse_time_us: u64::try_from(parse_time.as_micros()).unwrap_or(u64::MAX),
                execution_time_us: 0,
                rows_scanned: cached.stats.rows_scanned,
                rows_returned: cached.stats.rows_returned,
                total_rows: cached.stats.total_rows,
                parts_scanned: cached.stats.parts_scanned,
                parts_skipped: cached.stats.parts_skipped,
                bytes_read: cached.stats.bytes_read,
                cached: true,
            },
            explain: cached.explain,
            pagination: Pagination {
                offset: req_offset,
                limit: req_limit,
                total: cached.total,
                has_more: req_offset + req_limit < cached.total,
            },
            time_range: cached.time_range,
        };
        return (
            StatusCode::OK,
            Json(serde_json::to_value(response).unwrap_or_default()),
        )
            .into_response();
    }

    state
        .metrics()
        .query_cache_misses
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let exec_start = Instant::now();

    let storage_dir = state.storage_dir().to_string();
    let granule_size = state.granule_size();
    let query_clone = query.clone();

    let storage_cache = state.storage_cache().clone();
    let exec_result = tokio::task::spawn_blocking(move || {
        let executor = flowcus_storage::executor::QueryExecutor::with_cache(
            Path::new(&storage_dir),
            granule_size,
            storage_cache,
        );
        executor.execute(&query_clone, req_offset, req_limit, pinned_time)
    })
    .await;

    let exec_time = exec_start.elapsed();

    match exec_result {
        Ok(Ok(result)) => {
            let explain: Vec<serde_json::Value> = result
                .plan
                .steps
                .iter()
                .filter_map(|step| serde_json::to_value(step).ok())
                .collect();

            let total = result.total_matching_rows;
            let rows_returned = result.rows.len() as u64;
            let columns = resolve_column_types(&result.columns);

            // Record query metrics
            {
                use std::sync::atomic::Ordering::Relaxed;
                let m = state.metrics();
                m.query_rows_scanned.fetch_add(result.rows_scanned, Relaxed);
                m.query_rows_returned.fetch_add(rows_returned, Relaxed);
                m.query_parts_scanned
                    .fetch_add(result.parts_scanned, Relaxed);
                m.query_parts_skipped
                    .fetch_add(result.parts_skipped, Relaxed);
                m.query_duration_us.fetch_add(
                    u64::try_from(exec_time.as_micros()).unwrap_or(u64::MAX),
                    Relaxed,
                );
            }

            let time_range = TimeRangeBounds {
                start: result.time_start,
                end: result.time_end,
            };

            let cache_entry = CachedResult {
                columns: result.columns.clone(),
                rows: result.rows.clone(),
                stats: CachedQueryStats {
                    rows_scanned: result.rows_scanned,
                    rows_returned,
                    total_rows: result.total_rows_in_range,
                    parts_scanned: result.parts_scanned,
                    parts_skipped: result.parts_skipped,
                    bytes_read: result.plan.bytes_read,
                },
                total,
                explain: explain.clone(),
                flush_count: current_flush_count,
                created_at: Instant::now(),
                time_range: time_range.clone(),
            };
            state.query_cache().put(cache_key, cache_entry);

            debug!(
                rows_scanned = result.rows_scanned,
                rows_returned,
                parts_scanned = result.parts_scanned,
                parts_skipped = result.parts_skipped,
                exec_time_us = u64::try_from(exec_time.as_micros()).unwrap_or(u64::MAX),
                bytes_read = result.plan.bytes_read,
                "Query executed"
            );

            let response = QueryResponse {
                parsed: parsed_json,
                columns,
                rows: result.rows.clone(),
                stats: QueryStats {
                    parse_time_us: u64::try_from(parse_time.as_micros()).unwrap_or(u64::MAX),
                    execution_time_us: u64::try_from(exec_time.as_micros()).unwrap_or(u64::MAX),
                    rows_scanned: result.rows_scanned,
                    rows_returned,
                    total_rows: result.total_rows_in_range,
                    parts_scanned: result.parts_scanned,
                    parts_skipped: result.parts_skipped,
                    bytes_read: result.plan.bytes_read,
                    cached: false,
                },
                explain,
                pagination: Pagination {
                    offset: req_offset,
                    limit: req_limit,
                    total,
                    has_more: req_offset + req_limit < total,
                },
                time_range,
            };

            (
                StatusCode::OK,
                Json(serde_json::to_value(response).unwrap_or_default()),
            )
                .into_response()
        }
        Ok(Err(io_err)) => {
            warn!(error = %io_err, "Query execution failed (storage I/O)");
            state
                .metrics()
                .query_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let response = QueryResponse {
                parsed: parsed_json,
                columns: resolve_column_types(&extract_columns(&query)),
                rows: Vec::new(),
                stats: QueryStats {
                    parse_time_us: u64::try_from(parse_time.as_micros()).unwrap_or(u64::MAX),
                    execution_time_us: u64::try_from(exec_time.as_micros()).unwrap_or(u64::MAX),
                    rows_scanned: 0,
                    rows_returned: 0,
                    total_rows: 0,
                    parts_scanned: 0,
                    parts_skipped: 0,
                    bytes_read: 0,
                    cached: false,
                },
                explain: vec![serde_json::json!({
                    "type": "Error",
                    "message": io_err.to_string()
                })],
                pagination: Pagination {
                    offset: 0,
                    limit: 0,
                    total: 0,
                    has_more: false,
                },
                time_range: TimeRangeBounds { start: 0, end: 0 },
            };

            (
                StatusCode::OK,
                Json(serde_json::to_value(response).unwrap_or_default()),
            )
                .into_response()
        }
        Err(join_err) => {
            warn!(error = %join_err, "Query execution task panicked");
            state
                .metrics()
                .query_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let response = QueryError {
                error: format!("Execution failed: {join_err}"),
                position: None,
                suggestion: None,
            };
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::to_value(response).unwrap_or_default()),
            )
                .into_response()
        }
    }
}

/// Extract column names from the parsed query's select stage (if any).
fn extract_columns(query: &flowcus_query::Query) -> Vec<String> {
    use flowcus_query::ast::{SelectExpr, SelectFieldExpr, Stage};

    for stage in &query.stages {
        if let Stage::Select(sel) = stage {
            match sel {
                SelectExpr::Fields(fields) => {
                    return fields
                        .iter()
                        .map(|f| {
                            f.alias.clone().unwrap_or_else(|| match &f.expr {
                                SelectFieldExpr::Field(name) => name.clone(),
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
                SelectExpr::All => return vec!["*".to_string()],
                SelectExpr::AllExcept(excluded) => {
                    return vec![format!("* except ({})", excluded.join(", "))];
                }
            }
        }
    }

    Vec::new()
}

/// Extract position and generate a suggestion from a parse error.
fn parse_error_details(err: &flowcus_query::parser::ParseError) -> (Option<usize>, Option<String>) {
    use flowcus_query::parser::ParseError;

    let position = match err {
        ParseError::UnexpectedEof { pos }
        | ParseError::InvalidNumber { pos, .. }
        | ParseError::InvalidDuration { pos, .. }
        | ParseError::General { pos, .. }
        | ParseError::UnexpectedToken { pos, .. } => Some(*pos),
    };

    let suggestion = match err {
        ParseError::UnexpectedEof { .. } => {
            Some("Query appears incomplete. Did you forget a filter or stage?".to_string())
        }
        ParseError::UnexpectedToken { token, .. } => suggest_for_token(token),
        _ => None,
    };

    (position, suggestion)
}

/// Suggest corrections for common mistyped tokens.
fn suggest_for_token(token: &str) -> Option<String> {
    let lower = token.to_lowercase();
    let suggestions: &[(&[&str], &str)] = &[
        (
            &["where", "filter", "having"],
            "Use a filter expression like: src 10.0.0.0/8",
        ),
        (
            &["group", "groupby"],
            "Use: group by <field> | <aggregation>",
        ),
        (&["order", "orderby"], "Use: sort <aggregation> desc"),
        (&["sel", "fields"], "Use: select <field1>, <field2>"),
    ];

    for (variants, suggestion) in suggestions {
        if variants.contains(&lower.as_str()) {
            return Some((*suggestion).to_string());
        }
    }

    None
}

// ---------------------------------------------------------------------------
/// Resolve column names to `ColumnInfo` with type information.
/// Uses the IPFIX IE registry for known fields and system column definitions.
fn resolve_column_types(names: &[String]) -> Vec<ColumnInfo> {
    names
        .iter()
        .map(|name| {
            let col_type = resolve_column_type(name);
            ColumnInfo {
                name: name.clone(),
                col_type,
            }
        })
        .collect()
}

fn resolve_column_type(name: &str) -> String {
    // System columns
    match name {
        "flowcusExporterIPv4" => return "ipv4".to_string(),
        "flowcusExporterPort" => return "uint16".to_string(),
        "flowcusExportTime" | "flowcusObservationDomainId" => return "uint32".to_string(),
        _ => {}
    }

    // Aggregation result columns (e.g. "sum(bytes)", "count()")
    if name.contains('(') {
        return "number".to_string();
    }

    // Time bucket column
    if name == "time_bucket" {
        return "uint32".to_string();
    }

    // Look up in IPFIX IE registry
    for ie in flowcus_ipfix::ie::all() {
        if ie.name == name {
            return format!("{:?}", ie.data_type).to_lowercase();
        }
    }

    "unknown".to_string()
}

// ---------------------------------------------------------------------------
// GET /api/query/schema
// ---------------------------------------------------------------------------

/// Returns filter type metadata and field classifications for the
/// structured query builder UI.
async fn schema() -> Json<SchemaResponse> {
    let filter_types = build_filter_types();
    let fields = build_schema_fields();
    let op_hints = build_op_hints();
    Json(SchemaResponse {
        filter_types,
        fields,
        op_hints,
    })
}

fn build_filter_types() -> HashMap<String, Vec<String>> {
    let mut m = HashMap::new();
    m.insert(
        "ipv4".to_string(),
        vec!["eq", "ne", "cidr", "wildcard", "ip_range", "in", "not_in"]
            .into_iter()
            .map(String::from)
            .collect(),
    );
    m.insert(
        "ipv6".to_string(),
        vec!["eq", "ne", "cidr", "in", "not_in"]
            .into_iter()
            .map(String::from)
            .collect(),
    );
    m.insert(
        "port".to_string(),
        vec![
            "eq",
            "ne",
            "gt",
            "ge",
            "lt",
            "le",
            "port_range",
            "in",
            "not_in",
            "named",
        ]
        .into_iter()
        .map(String::from)
        .collect(),
    );
    m.insert(
        "protocol".to_string(),
        vec!["eq", "ne", "in", "not_in", "named"]
            .into_iter()
            .map(String::from)
            .collect(),
    );
    m.insert(
        "numeric".to_string(),
        vec![
            "eq", "ne", "gt", "ge", "lt", "le", "between", "in", "not_in",
        ]
        .into_iter()
        .map(String::from)
        .collect(),
    );
    m.insert(
        "string".to_string(),
        vec![
            "eq",
            "ne",
            "regex",
            "not_regex",
            "contains",
            "not_contains",
            "starts_with",
            "ends_with",
            "in",
            "not_in",
        ]
        .into_iter()
        .map(String::from)
        .collect(),
    );
    m.insert(
        "mac".to_string(),
        vec!["eq", "ne", "prefix", "in", "not_in"]
            .into_iter()
            .map(String::from)
            .collect(),
    );
    m.insert("boolean".to_string(), vec!["eq".to_string()]);
    m
}

fn build_op_hints() -> HashMap<String, String> {
    [
        ("eq", "Exact match"),
        ("ne", "Not equal"),
        ("gt", "Greater than"),
        ("ge", "Greater or equal"),
        ("lt", "Less than"),
        ("le", "Less or equal"),
        ("in", "Match any in list"),
        ("not_in", "Match none in list"),
        ("between", "Within range (min-max)"),
        ("cidr", "Subnet match (e.g. /24)"),
        ("wildcard", "Wildcard pattern (e.g. 10.*.*.1)"),
        ("ip_range", "IP range (e.g. .1-.255)"),
        ("regex", "Regular expression"),
        ("not_regex", "Not matching regex"),
        ("contains", "Contains substring"),
        ("not_contains", "Does not contain"),
        ("starts_with", "Starts with prefix"),
        ("ends_with", "Ends with suffix"),
        ("port_range", "Port range (e.g. 1024-65535)"),
        ("named", "Well-known name"),
        ("prefix", "MAC vendor prefix"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

#[allow(clippy::too_many_lines)]
fn build_schema_fields() -> Vec<SchemaField> {
    let mut fields = Vec::new();

    // IPFIX IEs
    for ie in flowcus_ipfix::ie::all() {
        let filter_type = classify_ie_field(ie.name, ie.data_type);
        let semantic_hint = classify_semantic_hint(ie.name, ie.data_type);
        fields.push(SchemaField {
            name: ie.name.to_string(),
            filter_type,
            data_type: format!("{:?}", ie.data_type),
            description: ie.description.to_string(),
            semantic_hint,
        });
    }

    // System columns
    let system = [
        (
            "flowcusExporterIPv4",
            "ipv4",
            "Ipv4Address",
            "IPv4 address of the IPFIX exporter",
            "",
        ),
        (
            "flowcusExporterPort",
            "port",
            "Unsigned16",
            "UDP/TCP port of the IPFIX exporter",
            "",
        ),
        (
            "flowcusExportTime",
            "numeric",
            "Unsigned32",
            "Export timestamp (Unix epoch)",
            "timestamp_s",
        ),
        (
            "flowcusObservationDomainId",
            "numeric",
            "Unsigned32",
            "Observation Domain ID",
            "",
        ),
    ];
    for (name, ft, dt, desc, hint) in &system {
        fields.push(SchemaField {
            name: (*name).to_string(),
            filter_type: (*ft).to_string(),
            data_type: (*dt).to_string(),
            description: (*desc).to_string(),
            semantic_hint: (*hint).to_string(),
        });
    }

    // Aliases — short names familiar to network engineers
    let aliases: &[(&str, &str, &str, &str, &str)] = &[
        // Core 5-tuple
        (
            "src",
            "ipv4",
            "Ipv4Address",
            "Source IP (sourceIPv4Address)",
            "",
        ),
        (
            "dst",
            "ipv4",
            "Ipv4Address",
            "Destination IP (destinationIPv4Address)",
            "",
        ),
        (
            "sport",
            "port",
            "Unsigned16",
            "Source port (sourceTransportPort)",
            "",
        ),
        (
            "dport",
            "port",
            "Unsigned16",
            "Destination port (destinationTransportPort)",
            "",
        ),
        (
            "port",
            "port",
            "Unsigned16",
            "Any transport port (src or dst)",
            "",
        ),
        (
            "proto",
            "protocol",
            "Unsigned8",
            "IP protocol (protocolIdentifier)",
            "",
        ),
        // Counters
        (
            "bytes",
            "numeric",
            "Unsigned64",
            "Byte count (octetDeltaCount)",
            "counter",
        ),
        (
            "packets",
            "numeric",
            "Unsigned64",
            "Packet count (packetDeltaCount)",
            "counter",
        ),
        // QoS / classification
        (
            "tos",
            "numeric",
            "Unsigned8",
            "Type of Service (ipClassOfService)",
            "dscp",
        ),
        (
            "dscp",
            "numeric",
            "Unsigned8",
            "DSCP value (ipClassOfService)",
            "dscp",
        ),
        (
            "flags",
            "numeric",
            "Unsigned16",
            "TCP flags (tcpControlBits)",
            "tcp_flags",
        ),
        (
            "tcpflags",
            "numeric",
            "Unsigned16",
            "TCP flags (tcpControlBits)",
            "tcp_flags",
        ),
        // Routing
        (
            "nexthop",
            "ipv4",
            "Ipv4Address",
            "Next-hop IPv4 (ipNextHopIPv4Address)",
            "",
        ),
        (
            "nexthop6",
            "ipv6",
            "Ipv6Address",
            "Next-hop IPv6 (ipNextHopIPv6Address)",
            "",
        ),
        (
            "bgp_nexthop",
            "ipv4",
            "Ipv4Address",
            "BGP next-hop (bgpNextHopIPv4Address)",
            "",
        ),
        (
            "src_as",
            "numeric",
            "Unsigned32",
            "Source AS number (bgpSourceAsNumber)",
            "asn",
        ),
        (
            "dst_as",
            "numeric",
            "Unsigned32",
            "Destination AS number (bgpDestinationAsNumber)",
            "asn",
        ),
        (
            "srcas",
            "numeric",
            "Unsigned32",
            "Source AS number (bgpSourceAsNumber)",
            "asn",
        ),
        (
            "dstas",
            "numeric",
            "Unsigned32",
            "Destination AS number (bgpDestinationAsNumber)",
            "asn",
        ),
        // L2
        ("vlan", "numeric", "Unsigned16", "VLAN ID (vlanId)", ""),
        (
            "src_mac",
            "mac",
            "MacAddress",
            "Source MAC (sourceMacAddress)",
            "",
        ),
        (
            "dst_mac",
            "mac",
            "MacAddress",
            "Destination MAC (destinationMacAddress)",
            "",
        ),
        (
            "srcmac",
            "mac",
            "MacAddress",
            "Source MAC (sourceMacAddress)",
            "",
        ),
        (
            "dstmac",
            "mac",
            "MacAddress",
            "Destination MAC (destinationMacAddress)",
            "",
        ),
        // Interfaces
        (
            "ingress",
            "numeric",
            "Unsigned32",
            "Ingress interface index (ingressInterface)",
            "interface",
        ),
        (
            "egress",
            "numeric",
            "Unsigned32",
            "Egress interface index (egressInterface)",
            "interface",
        ),
        (
            "in_if",
            "numeric",
            "Unsigned32",
            "Ingress interface (ingressInterface)",
            "interface",
        ),
        (
            "out_if",
            "numeric",
            "Unsigned32",
            "Egress interface (egressInterface)",
            "interface",
        ),
        // Timing
        (
            "duration",
            "numeric",
            "Unsigned32",
            "Flow duration ms (flowDurationMilliseconds)",
            "duration_ms",
        ),
        (
            "start",
            "numeric",
            "Unsigned64",
            "Flow start time (flowStartMilliseconds)",
            "timestamp_ms",
        ),
        (
            "end",
            "numeric",
            "Unsigned64",
            "Flow end time (flowEndMilliseconds)",
            "timestamp_ms",
        ),
        // ICMP
        (
            "icmp_type",
            "numeric",
            "Unsigned16",
            "ICMP type/code (icmpTypeCodeIPv4)",
            "icmp_type",
        ),
        (
            "icmp6_type",
            "numeric",
            "Unsigned16",
            "ICMPv6 type/code (icmpTypeCodeIPv6)",
            "icmp_type",
        ),
        // Exporter metadata
        (
            "exporter",
            "ipv4",
            "Ipv4Address",
            "Exporter IP (flowcusExporterIPv4)",
            "",
        ),
        (
            "domain_id",
            "numeric",
            "Unsigned32",
            "Observation domain (flowcusObservationDomainId)",
            "",
        ),
        // Application
        (
            "app",
            "string",
            "String",
            "Application name (applicationName)",
            "",
        ),
    ];
    for (name, ft, dt, desc, hint) in aliases {
        fields.push(SchemaField {
            name: (*name).to_string(),
            filter_type: (*ft).to_string(),
            data_type: (*dt).to_string(),
            description: (*desc).to_string(),
            semantic_hint: (*hint).to_string(),
        });
    }

    fields.sort_by(|a, b| a.name.cmp(&b.name));
    fields
}

/// Classify an IPFIX IE into a semantic filter type based on its name and data type.
fn classify_ie_field(name: &str, data_type: flowcus_ipfix::protocol::DataType) -> String {
    use flowcus_ipfix::protocol::DataType;

    // Name-based classification (higher priority)
    let lower = name.to_lowercase();

    // IPv4 fields
    if lower.contains("ipv4") || lower.contains("ipv4address") {
        return "ipv4".to_string();
    }
    // IPv6 fields
    if lower.contains("ipv6") || lower.contains("ipv6address") {
        return "ipv6".to_string();
    }
    // Port fields
    if lower.contains("port")
        && (lower.contains("transport")
            || lower.contains("source")
            || lower.contains("destination"))
    {
        return "port".to_string();
    }
    // Protocol
    if name == "protocolIdentifier" {
        return "protocol".to_string();
    }

    // Data-type-based classification
    match data_type {
        DataType::Ipv4Address => "ipv4".to_string(),
        DataType::Ipv6Address => "ipv6".to_string(),
        DataType::MacAddress => "mac".to_string(),
        DataType::String | DataType::OctetArray => "string".to_string(),
        DataType::Boolean => "boolean".to_string(),
        DataType::Unsigned8
        | DataType::Unsigned16
        | DataType::Unsigned32
        | DataType::Unsigned64
        | DataType::Signed8
        | DataType::Signed16
        | DataType::Signed32
        | DataType::Signed64
        | DataType::Float32
        | DataType::Float64
        | DataType::DateTimeSeconds
        | DataType::DateTimeMilliseconds
        | DataType::DateTimeMicroseconds
        | DataType::DateTimeNanoseconds => "numeric".to_string(),
    }
}

/// Classify an IPFIX IE into a semantic hint for the frontend query builder.
fn classify_semantic_hint(name: &str, data_type: flowcus_ipfix::protocol::DataType) -> String {
    use flowcus_ipfix::protocol::DataType;

    let lower = name.to_lowercase();

    // Name-based classification (higher priority)
    if lower.contains("duration") {
        return "duration_ms".to_string();
    }
    if name == "tcpControlBits" {
        return "tcp_flags".to_string();
    }
    if name.starts_with("icmpTypeCode") {
        return "icmp_type".to_string();
    }
    if name == "ipClassOfService" {
        return "dscp".to_string();
    }
    if lower.contains("asnumber") {
        return "asn".to_string();
    }
    if lower.contains("interface") && (lower.contains("ingress") || lower.contains("egress")) {
        return "interface".to_string();
    }
    if name == "octetDeltaCount" || name == "packetDeltaCount" {
        return "counter".to_string();
    }

    // Data-type-based classification
    match data_type {
        DataType::DateTimeSeconds => "timestamp_s".to_string(),
        DataType::DateTimeMilliseconds
        | DataType::DateTimeMicroseconds
        | DataType::DateTimeNanoseconds => "timestamp_ms".to_string(),
        _ => String::new(),
    }
}

// ---------------------------------------------------------------------------
// GET /api/query/completions
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
async fn completions(AxumQuery(params): AxumQuery<CompletionsParams>) -> Json<Vec<CompletionItem>> {
    let prefix = params.prefix.unwrap_or_default().to_lowercase();
    let mut items = Vec::new();

    // FQL keywords
    let keywords = [
        "last", "select", "group", "by", "top", "bottom", "sort", "limit", "asc", "desc", "and",
        "or", "not", "in", "offset", "at", "daily", "weekly", "every", "except",
    ];
    for kw in &keywords {
        if prefix.is_empty() || kw.starts_with(&prefix) {
            items.push(CompletionItem {
                label: (*kw).to_string(),
                kind: CompletionKind::Keyword,
                detail: None,
            });
        }
    }

    // Aggregation functions
    let functions = [
        ("sum", "Sum of values"),
        ("avg", "Average of values"),
        ("min", "Minimum value"),
        ("max", "Maximum value"),
        ("count", "Count of records"),
        ("uniq", "Count of unique values"),
        ("p50", "50th percentile"),
        ("p95", "95th percentile"),
        ("p99", "99th percentile"),
        ("stddev", "Standard deviation"),
        ("rate", "Rate per second"),
        ("first", "First value"),
        ("last", "Last value"),
    ];
    for (name, detail) in &functions {
        if prefix.is_empty() || name.starts_with(&prefix) {
            items.push(CompletionItem {
                label: (*name).to_string(),
                kind: CompletionKind::Function,
                detail: Some((*detail).to_string()),
            });
        }
    }

    // Short aliases familiar to network engineers
    let aliases = [
        ("src", "Source IP"),
        ("dst", "Destination IP"),
        ("sport", "Source port"),
        ("dport", "Destination port"),
        ("port", "Any port"),
        ("proto", "IP protocol"),
        ("bytes", "Byte count"),
        ("packets", "Packet count"),
        ("tos", "Type of Service"),
        ("dscp", "DSCP value"),
        ("flags", "TCP flags"),
        ("tcpflags", "TCP flags"),
        ("nexthop", "Next-hop IPv4"),
        ("nexthop6", "Next-hop IPv6"),
        ("bgp_nexthop", "BGP next-hop"),
        ("src_as", "Source AS"),
        ("dst_as", "Destination AS"),
        ("srcas", "Source AS"),
        ("dstas", "Destination AS"),
        ("vlan", "VLAN ID"),
        ("src_mac", "Source MAC"),
        ("dst_mac", "Destination MAC"),
        ("srcmac", "Source MAC"),
        ("dstmac", "Destination MAC"),
        ("ingress", "Ingress interface"),
        ("egress", "Egress interface"),
        ("in_if", "Ingress interface"),
        ("out_if", "Egress interface"),
        ("duration", "Flow duration"),
        ("start", "Flow start"),
        ("end", "Flow end"),
        ("icmp_type", "ICMP type/code"),
        ("icmp6_type", "ICMPv6 type/code"),
        ("exporter", "Exporter IP"),
        ("domain_id", "Observation domain"),
        ("app", "Application name"),
    ];
    for (alias, detail) in &aliases {
        if prefix.is_empty() || alias.starts_with(&prefix) {
            items.push(CompletionItem {
                label: (*alias).to_string(),
                kind: CompletionKind::Field,
                detail: Some((*detail).to_string()),
            });
        }
    }

    // Named ports
    let named_ports = [
        ("http", "TCP/80"),
        ("https", "TCP/443"),
        ("dns", "UDP/53, TCP/53"),
        ("ssh", "TCP/22"),
        ("ftp", "TCP/21"),
        ("smtp", "TCP/25"),
        ("ntp", "UDP/123"),
        ("snmp", "UDP/161"),
        ("bgp", "TCP/179"),
        ("ldap", "TCP/389"),
        ("rdp", "TCP/3389"),
        ("mysql", "TCP/3306"),
        ("postgres", "TCP/5432"),
    ];
    for (name, detail) in &named_ports {
        if prefix.is_empty() || name.starts_with(&prefix) {
            items.push(CompletionItem {
                label: (*name).to_string(),
                kind: CompletionKind::NamedPort,
                detail: Some((*detail).to_string()),
            });
        }
    }

    // IPFIX IE field names (filtered by prefix if provided)
    for ie in flowcus_ipfix::ie::all() {
        let lower_name = ie.name.to_lowercase();
        if prefix.is_empty() || lower_name.starts_with(&prefix) || ie.name.starts_with(&prefix) {
            items.push(CompletionItem {
                label: ie.name.to_string(),
                kind: CompletionKind::Field,
                detail: Some(ie.description.to_string()),
            });
        }
    }

    Json(items)
}

// ---------------------------------------------------------------------------
// GET /api/query/fields
// ---------------------------------------------------------------------------

async fn fields() -> Json<Vec<FieldInfo>> {
    let mut result: Vec<FieldInfo> = Vec::new();

    // All IPFIX IEs from the registry (IANA + vendor)
    for ie in flowcus_ipfix::ie::all() {
        result.push(FieldInfo {
            name: ie.name.to_string(),
            data_type: format!("{:?}", ie.data_type),
            description: ie.description.to_string(),
            element_id: ie.element_id,
            enterprise_id: ie.enterprise_id,
        });
    }

    // System columns (flowcus-internal)
    let system_fields = [
        (
            "flowcusExporterIPv4",
            "Ipv4Address",
            "IPv4 address of the IPFIX exporter",
        ),
        (
            "flowcusExporterIPv6",
            "Ipv6Address",
            "IPv6 address of the IPFIX exporter",
        ),
        (
            "flowcusObservationDomain",
            "Unsigned32",
            "Observation domain ID from the exporter",
        ),
        (
            "flowcusReceiveTime",
            "DateTimeMilliseconds",
            "Timestamp when the flow was received by flowcus",
        ),
        (
            "flowcusPartition",
            "String",
            "Storage partition the flow belongs to",
        ),
    ];

    for (name, dtype, desc) in &system_fields {
        result.push(FieldInfo {
            name: (*name).to_string(),
            data_type: (*dtype).to_string(),
            description: (*desc).to_string(),
            element_id: 0,
            enterprise_id: 0,
        });
    }

    // Short aliases familiar to network engineers
    let aliases: &[(&str, &str, &str)] = &[
        ("src", "Ipv4Address", "Source IP"),
        ("dst", "Ipv4Address", "Destination IP"),
        ("sport", "Unsigned16", "Source port"),
        ("dport", "Unsigned16", "Destination port"),
        ("port", "Unsigned16", "Any port (src or dst)"),
        ("proto", "Unsigned8", "IP protocol"),
        ("bytes", "Unsigned64", "Byte count"),
        ("packets", "Unsigned64", "Packet count"),
        ("tos", "Unsigned8", "Type of Service"),
        ("dscp", "Unsigned8", "DSCP value"),
        ("flags", "Unsigned16", "TCP flags"),
        ("tcpflags", "Unsigned16", "TCP flags"),
        ("nexthop", "Ipv4Address", "Next-hop IPv4"),
        ("nexthop6", "Ipv6Address", "Next-hop IPv6"),
        ("bgp_nexthop", "Ipv4Address", "BGP next-hop"),
        ("src_as", "Unsigned32", "Source AS"),
        ("dst_as", "Unsigned32", "Destination AS"),
        ("srcas", "Unsigned32", "Source AS"),
        ("dstas", "Unsigned32", "Destination AS"),
        ("vlan", "Unsigned16", "VLAN ID"),
        ("src_mac", "MacAddress", "Source MAC"),
        ("dst_mac", "MacAddress", "Destination MAC"),
        ("srcmac", "MacAddress", "Source MAC"),
        ("dstmac", "MacAddress", "Destination MAC"),
        ("ingress", "Unsigned32", "Ingress interface"),
        ("egress", "Unsigned32", "Egress interface"),
        ("in_if", "Unsigned32", "Ingress interface"),
        ("out_if", "Unsigned32", "Egress interface"),
        ("duration", "Unsigned32", "Flow duration"),
        ("start", "Unsigned64", "Flow start time"),
        ("end", "Unsigned64", "Flow end time"),
        ("icmp_type", "Unsigned16", "ICMP type/code"),
        ("icmp6_type", "Unsigned16", "ICMPv6 type/code"),
        ("exporter", "Ipv4Address", "Exporter IP"),
        ("domain_id", "Unsigned32", "Observation domain"),
        ("app", "String", "Application name"),
    ];

    for (name, dtype, desc) in aliases {
        result.push(FieldInfo {
            name: (*name).to_string(),
            data_type: (*dtype).to_string(),
            description: (*desc).to_string(),
            element_id: 0,
            enterprise_id: 0,
        });
    }

    // Sort by name for a stable, user-friendly ordering
    result.sort_by(|a, b| a.name.cmp(&b.name));

    Json(result)
}

// ---------------------------------------------------------------------------
// POST /api/stats/histogram
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct HistogramRequest {
    pub time_range: flowcus_query::structured::StructuredTimeRange,
    #[serde(default)]
    pub filters: Vec<flowcus_query::structured::Filter>,
    #[serde(default)]
    pub logic: flowcus_query::structured::FilterLogic,
    /// Target number of buckets (default 60).
    pub buckets: Option<u32>,
}

#[derive(Serialize, Clone)]
pub struct HistogramResponse {
    pub buckets: Vec<HistogramBucket>,
    pub total_rows: u64,
    pub time_range: TimeRangeBounds,
    pub bucket_seconds: u32,
    pub done: bool,
}

#[derive(Serialize, Clone)]
pub struct HistogramBucket {
    pub timestamp: u32,
    pub count: u64,
}

/// Wrapper that turns a `tokio::sync::mpsc::Receiver` into a `futures_core::Stream`.
struct ReceiverStream<T> {
    rx: mpsc::Receiver<T>,
}

impl<T> ReceiverStream<T> {
    const fn new(rx: mpsc::Receiver<T>) -> Self {
        Self { rx }
    }
}

impl<T> futures_core::Stream for ReceiverStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

#[allow(clippy::too_many_lines)]
async fn stats_histogram(
    State(state): State<AppState>,
    Json(req): Json<HistogramRequest>,
) -> Sse<impl futures_core::Stream<Item = Result<Event, Infallible>>> {
    let storage_dir = state.storage_dir().to_string();
    let granule_size = state.granule_size();
    let storage_cache = state.storage_cache().clone();
    let has_filters = !req.filters.is_empty();

    // Build a time-range-only query to resolve bounds
    let time_range_sq = flowcus_query::structured::StructuredQuery {
        time_range: req.time_range.clone(),
        filters: vec![],
        logic: flowcus_query::structured::FilterLogic::default(),
        columns: None,
        aggregate: None,
        sort: None,
    };

    let time_query = match time_range_sq.to_ast() {
        Ok(q) => q,
        Err(e) => {
            let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(1);
            let error_json = serde_json::json!({"error": e, "done": true}).to_string();
            let _ = tx
                .send(Ok(Event::default().event("progress").data(error_json)))
                .await;
            return Sse::new(ReceiverStream::new(rx));
        }
    };

    let target_buckets = req.buckets.unwrap_or(60);

    // Extract bloom lookups from filters for the metadata path
    let bloom_lookups: Vec<(String, Vec<u8>)> = if has_filters {
        let filter_sq = flowcus_query::structured::StructuredQuery {
            time_range: req.time_range.clone(),
            filters: req.filters.clone(),
            logic: req.logic.clone(),
            columns: None,
            aggregate: None,
            sort: None,
        };

        if let Ok(filter_query) = filter_sq.to_ast() {
            filter_query
                .stages
                .iter()
                .filter_map(|stage| {
                    if let flowcus_query::ast::Stage::Filter(expr) = stage {
                        Some(flowcus_storage::executor::bloom_lookup_bytes(expr))
                    } else {
                        None
                    }
                })
                .flatten()
                .collect()
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    // Build the full filtered query for the filtered path (group-by aggregation)
    let filtered_query = if has_filters {
        let bucket_dur_str = |secs: u32| -> String {
            if secs >= 86400 && secs % 86400 == 0 {
                format!("{}d", secs / 86400)
            } else if secs >= 3600 && secs % 3600 == 0 {
                format!("{}h", secs / 3600)
            } else if secs >= 60 && secs % 60 == 0 {
                format!("{}m", secs / 60)
            } else {
                format!("{secs}s")
            }
        };

        let (time_start, time_end) =
            flowcus_storage::executor::time_range_to_bounds(&time_query.time_range);
        let window_secs = time_end.saturating_sub(time_start);
        let bucket_secs = pick_bucket_duration(window_secs, target_buckets);

        let sq = flowcus_query::structured::StructuredQuery {
            time_range: req.time_range.clone(),
            filters: req.filters.clone(),
            logic: req.logic.clone(),
            columns: None,
            aggregate: Some(flowcus_query::structured::StructuredAggregate::GroupBy {
                keys: vec![flowcus_query::structured::GroupByKeyDef::TimeBucket {
                    duration: bucket_dur_str(bucket_secs),
                }],
                functions: vec![flowcus_query::structured::AggCallDef {
                    func: "count".to_string(),
                    field: None,
                }],
            }),
            sort: None,
        };

        sq.to_ast().ok().map(|q| (q, bucket_secs))
    } else {
        None
    };

    // Capacity 1: the blocking sender waits for each event to be consumed
    // by the SSE stream before sending the next, ensuring true streaming.
    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(1);

    tokio::task::spawn_blocking(move || {
        let executor = flowcus_storage::executor::QueryExecutor::with_cache(
            Path::new(&storage_dir),
            granule_size,
            storage_cache,
        );

        let (time_start, time_end) =
            flowcus_storage::executor::time_range_to_bounds(&time_query.time_range);
        let window_secs = time_end.saturating_sub(time_start);
        let bucket_secs = pick_bucket_duration(window_secs, target_buckets);

        if let Some((query, bucket_secs)) = filtered_query {
            // Filtered path: run actual query with group by time_bucket.
            // Aggregation must complete before we have counts, so send one final event.
            let qr = match executor.execute(&query, 0, 10_000, None) {
                Ok(r) => r,
                Err(e) => {
                    let err_json =
                        serde_json::json!({"error": e.to_string(), "done": true}).to_string();
                    let _ = tx.blocking_send(Ok(Event::default().event("progress").data(err_json)));
                    return;
                }
            };

            let mut buckets = Vec::new();
            for row in &qr.rows {
                if let (Some(ts), Some(count)) = (
                    row.first().and_then(serde_json::Value::as_u64),
                    row.get(1).and_then(serde_json::Value::as_u64),
                ) {
                    #[allow(clippy::cast_possible_truncation)]
                    buckets.push((ts as u32, count));
                }
            }
            buckets.sort_by_key(|(ts, _)| *ts);

            let aligned_start = (time_start / bucket_secs) * bucket_secs;
            let existing: HashMap<u32, u64> = buckets.iter().copied().collect();
            let mut filled = Vec::new();
            let mut t = aligned_start;
            while t < time_end {
                filled.push((t, existing.get(&t).copied().unwrap_or(0)));
                t += bucket_secs;
            }

            let total_rows: u64 = filled.iter().map(|(_, c)| c).sum();

            let response = HistogramResponse {
                buckets: filled
                    .into_iter()
                    .map(|(ts, count)| HistogramBucket {
                        timestamp: ts,
                        count,
                    })
                    .collect(),
                total_rows,
                time_range: TimeRangeBounds {
                    start: time_start,
                    end: time_end,
                },
                bucket_seconds: bucket_secs,
                done: true,
            };

            if let Ok(json) = serde_json::to_string(&response) {
                let _ = tx.blocking_send(Ok(Event::default().event("progress").data(json)));
            }
        } else {
            // Unfiltered path: fast metadata-only histogram with streaming progress.
            let tx_progress = tx.clone();
            let result = executor.histogram_from_metadata(
                time_start,
                time_end,
                bucket_secs,
                &bloom_lookups,
                |partial| {
                    let response = HistogramResponse {
                        buckets: partial
                            .buckets
                            .iter()
                            .map(|&(ts, count)| HistogramBucket {
                                timestamp: ts,
                                count,
                            })
                            .collect(),
                        total_rows: partial.total_rows,
                        time_range: TimeRangeBounds {
                            start: partial.time_start,
                            end: partial.time_end,
                        },
                        bucket_seconds: bucket_secs,
                        done: false,
                    };
                    if let Ok(json) = serde_json::to_string(&response) {
                        let _ = tx_progress
                            .blocking_send(Ok(Event::default().event("progress").data(json)));
                    }
                },
            );

            match result {
                Ok(hist) => {
                    let response = HistogramResponse {
                        buckets: hist
                            .buckets
                            .into_iter()
                            .map(|(ts, count)| HistogramBucket {
                                timestamp: ts,
                                count,
                            })
                            .collect(),
                        total_rows: hist.total_rows,
                        time_range: TimeRangeBounds {
                            start: hist.time_start,
                            end: hist.time_end,
                        },
                        bucket_seconds: bucket_secs,
                        done: true,
                    };
                    if let Ok(json) = serde_json::to_string(&response) {
                        let _ = tx.blocking_send(Ok(Event::default().event("progress").data(json)));
                    }
                }
                Err(e) => {
                    let err_json =
                        serde_json::json!({"error": e.to_string(), "done": true}).to_string();
                    let _ = tx.blocking_send(Ok(Event::default().event("progress").data(err_json)));
                }
            }
        }
    });

    Sse::new(ReceiverStream::new(rx))
}

/// Pick a nice bucket duration targeting the given number of buckets.
fn pick_bucket_duration(window_secs: u32, target_buckets: u32) -> u32 {
    const NICE_DURATIONS: &[u32] = &[
        1, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 21600, 43200, 86400,
    ];

    let target = window_secs / target_buckets.max(1);
    let mut best = NICE_DURATIONS[0];
    for &d in NICE_DURATIONS {
        if d >= target {
            best = d;
            break;
        }
        best = d;
    }
    best
}
