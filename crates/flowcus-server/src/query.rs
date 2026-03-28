use std::path::Path;
use std::time::Instant;

use axum::{
    Router,
    extract::{Query as AxumQuery, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};

use crate::state::AppState;

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct QueryRequest {
    pub query: String,
    pub offset: Option<u64>,
    pub limit: Option<u64>,
}

#[derive(Serialize)]
pub struct QueryResponse {
    pub parsed: serde_json::Value,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub stats: QueryStats,
    pub explain: Vec<serde_json::Value>,
    pub pagination: Pagination,
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
    pub parts_scanned: u64,
    pub parts_skipped: u64,
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
// Routes
// ---------------------------------------------------------------------------

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/query", post(execute_query))
        .route("/query/completions", get(completions))
        .route("/query/fields", get(fields))
}

// ---------------------------------------------------------------------------
// POST /api/query
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
async fn execute_query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    let start = Instant::now();
    let parse_result = flowcus_query::parse(&req.query);
    let parse_time = start.elapsed();

    match parse_result {
        Ok(query) => {
            let parsed = serde_json::to_value(&query).unwrap_or_default();
            let exec_start = Instant::now();

            // Create executor and run the query against storage
            let storage_dir = state.storage_dir().to_string();
            let granule_size = state.granule_size();
            let query_clone = query.clone();

            let exec_result = tokio::task::spawn_blocking(move || {
                let executor = flowcus_storage::executor::QueryExecutor::new(
                    Path::new(&storage_dir),
                    granule_size,
                );
                executor.execute(&query_clone)
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

                    // Pagination: apply offset/limit to result rows
                    let req_offset = req.offset.unwrap_or(0);
                    let req_limit = req.limit.unwrap_or(100).min(10_000);
                    let total = result.rows.len() as u64;
                    #[allow(clippy::cast_possible_truncation)]
                    let start = (req_offset as usize).min(result.rows.len());
                    #[allow(clippy::cast_possible_truncation)]
                    let end = (start + req_limit as usize).min(result.rows.len());
                    let paginated_rows: Vec<Vec<serde_json::Value>> =
                        result.rows[start..end].to_vec();
                    let rows_returned = paginated_rows.len() as u64;

                    let response = QueryResponse {
                        parsed,
                        columns: result.columns,
                        rows: paginated_rows,
                        stats: QueryStats {
                            parse_time_us: u64::try_from(parse_time.as_micros())
                                .unwrap_or(u64::MAX),
                            execution_time_us: u64::try_from(exec_time.as_micros())
                                .unwrap_or(u64::MAX),
                            rows_scanned: result.rows_scanned,
                            rows_returned,
                            parts_scanned: result.parts_scanned,
                            parts_skipped: result.parts_skipped,
                        },
                        explain,
                        pagination: Pagination {
                            offset: req_offset,
                            limit: req_limit,
                            total,
                            has_more: req_offset + req_limit < total,
                        },
                    };

                    (
                        StatusCode::OK,
                        Json(serde_json::to_value(response).unwrap_or_default()),
                    )
                        .into_response()
                }
                Ok(Err(io_err)) => {
                    // I/O error during execution — still return parsed AST with error
                    let response = QueryResponse {
                        parsed,
                        columns: extract_columns(&query),
                        rows: Vec::new(),
                        stats: QueryStats {
                            parse_time_us: u64::try_from(parse_time.as_micros())
                                .unwrap_or(u64::MAX),
                            execution_time_us: u64::try_from(exec_time.as_micros())
                                .unwrap_or(u64::MAX),
                            rows_scanned: 0,
                            rows_returned: 0,
                            parts_scanned: 0,
                            parts_skipped: 0,
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
                    };

                    (
                        StatusCode::OK,
                        Json(serde_json::to_value(response).unwrap_or_default()),
                    )
                        .into_response()
                }
                Err(join_err) => {
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
        Err(err) => {
            let (position, suggestion) = parse_error_details(&err);
            let response = QueryError {
                error: err.to_string(),
                position,
                suggestion,
            };
            (
                StatusCode::BAD_REQUEST,
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
// GET /api/query/completions
// ---------------------------------------------------------------------------

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

    // Short aliases for common fields
    let aliases = [
        ("src", "sourceIPv4Address / sourceIPv6Address"),
        ("dst", "destinationIPv4Address / destinationIPv6Address"),
        ("sport", "sourceTransportPort"),
        ("dport", "destinationTransportPort"),
        ("proto", "protocolIdentifier"),
        ("bytes", "octetDeltaCount"),
        ("packets", "packetDeltaCount"),
        ("tos", "ipClassOfService"),
        ("nexthop", "ipNextHopIPv4Address"),
        ("vlan", "vlanId"),
        ("asn", "bgpSourceAsNumber / bgpDestinationAsNumber"),
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

    // Short aliases
    let aliases = [
        (
            "src",
            "Ipv4Address",
            "Alias for sourceIPv4Address / sourceIPv6Address",
        ),
        (
            "dst",
            "Ipv4Address",
            "Alias for destinationIPv4Address / destinationIPv6Address",
        ),
        ("sport", "Unsigned16", "Alias for sourceTransportPort"),
        ("dport", "Unsigned16", "Alias for destinationTransportPort"),
        ("proto", "Unsigned8", "Alias for protocolIdentifier"),
        ("bytes", "Unsigned64", "Alias for octetDeltaCount"),
        ("packets", "Unsigned64", "Alias for packetDeltaCount"),
        ("tos", "Unsigned8", "Alias for ipClassOfService"),
        ("nexthop", "Ipv4Address", "Alias for ipNextHopIPv4Address"),
        ("vlan", "Unsigned16", "Alias for vlanId"),
    ];

    for (name, dtype, desc) in &aliases {
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
