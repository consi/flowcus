#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::redundant_closure_for_method_calls,
    clippy::needless_collect,
    clippy::doc_markdown
)]

use std::net::{Ipv4Addr, TcpListener};
use std::path::{Path, PathBuf};
use std::time::Duration;

use flowcus_core::AppConfig;
use flowcus_ipfix::protocol::*;
use flowcus_server::state::AppState;
use flowcus_storage::table::Table;
use flowcus_storage::writer::{StorageWriter, WriterConfig};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn spawn_server() -> u16 {
    let port = free_port();
    let mut config = AppConfig::default();
    config.server.port = port;
    config.server.host = "127.0.0.1".to_string();

    let metrics = flowcus_core::observability::Metrics::new();
    let state = AppState::new(config.clone(), metrics, PathBuf::from("test.settings"));
    let server_config = config.server.clone();
    tokio::spawn(async move {
        flowcus_server::serve(&server_config, state).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    port
}

// ---------------------------------------------------------------------------
// Histogram test helpers
// ---------------------------------------------------------------------------

fn histogram_test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("flowcus_histogram_e2e")
        .join(name);
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

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

fn make_message_with_time(records: Vec<DataRecord>, export_time: u32) -> IpfixMessage {
    IpfixMessage {
        header: MessageHeader {
            version: 0x000a,
            length: 0,
            export_time,
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

fn tiny_flush_writer(dir: &Path) -> StorageWriter {
    let table = Table::open(dir, "flows").unwrap();
    let config = WriterConfig {
        flush_bytes: 1,
        flush_interval_secs: 0,
        initial_row_capacity: 64,
        ..WriterConfig::default()
    };
    StorageWriter::new(table, config, None)
}

/// Spawn a server backed by a pre-populated storage directory.
async fn spawn_server_with_storage(storage_dir: &Path) -> u16 {
    let port = free_port();
    let mut config = AppConfig::default();
    config.server.port = port;
    config.server.host = "127.0.0.1".to_string();
    config.storage.dir = storage_dir.to_string_lossy().to_string();

    let metrics = flowcus_core::observability::Metrics::new();
    let state = AppState::new(config.clone(), metrics, PathBuf::from("test.settings"));
    let server_config = config.server.clone();
    tokio::spawn(async move {
        flowcus_server::serve(&server_config, state).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    port
}

/// Ingest `count` records at the given export_time and flush to disk.
fn ingest_records_at_time(dir: &Path, export_time: u32, count: usize) {
    let mut writer = tiny_flush_writer(dir);
    let records: Vec<DataRecord> = (0..count)
        .map(|i| {
            make_record(
                Ipv4Addr::new(10, 0, (i >> 8) as u8, i as u8),
                Ipv4Addr::new(192, 168, (i >> 8) as u8, i as u8),
                (i * 100) as u64,
            )
        })
        .collect();
    let msg = make_message_with_time(records, export_time);
    writer.ingest(&msg);
    writer.flush_all();
}

/// Parse all SSE `data:` payloads from a `text/event-stream` response body.
fn parse_sse_events(text: &str) -> Vec<serde_json::Value> {
    let mut events = Vec::new();
    for block in text.split("\n\n") {
        for line in block.split('\n') {
            let trimmed = line.trim_end_matches('\r');
            if let Some(rest) = trimmed.strip_prefix("data:") {
                let json_str = rest.strip_prefix(' ').unwrap_or(rest);
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(json_str) {
                    events.push(val);
                }
            }
        }
    }
    events
}

/// Extract the final (done=true) event from SSE events, or the last one.
fn final_event(events: &[serde_json::Value]) -> &serde_json::Value {
    events
        .iter()
        .rfind(|e| e.get("done").and_then(|d| d.as_bool()) == Some(true))
        .or_else(|| events.last())
        .expect("expected at least one SSE event")
}

/// POST to the histogram endpoint and return the raw response text.
async fn fetch_histogram_raw(
    client: &reqwest::Client,
    port: u16,
    time_range: serde_json::Value,
    filters: serde_json::Value,
) -> String {
    let resp = client
        .post(format!("http://127.0.0.1:{port}/api/stats/histogram"))
        .header("Accept", "text/event-stream")
        .json(&serde_json::json!({
            "time_range": time_range,
            "filters": filters,
            "logic": "and",
            "buckets": 60,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    resp.text().await.unwrap()
}

#[tokio::test]
async fn health_returns_ok() {
    let port = spawn_server().await;
    let body: serde_json::Value = reqwest::get(format!("http://127.0.0.1:{port}/api/health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn info_returns_structured_response() {
    let port = spawn_server().await;
    let body: serde_json::Value = reqwest::get(format!("http://127.0.0.1:{port}/api/info"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["name"], "flowcus");
    assert!(body["version"].is_string());
    assert!(body["storage"]["merge_workers"].is_number());
}

#[tokio::test]
async fn structured_query_returns_valid_response() {
    let port = spawn_server().await;
    let client = reqwest::Client::new();
    let body: serde_json::Value = client
        .post(format!("http://127.0.0.1:{port}/api/query"))
        .json(&serde_json::json!({
            "time_range": { "type": "relative", "duration": "1h" },
            "filters": [],
            "logic": "and"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert!(
        body.get("columns").is_some(),
        "response must have 'columns'"
    );
    assert!(body["columns"].is_array(), "columns must be an array");
    assert!(body.get("rows").is_some(), "response must have 'rows'");
    assert!(body["rows"].is_array(), "rows must be an array");
    assert!(body.get("stats").is_some(), "response must have 'stats'");
    assert!(body["stats"].is_object(), "stats must be an object");
    assert!(
        body.get("pagination").is_some(),
        "response must have 'pagination'"
    );
    assert!(
        body["pagination"].is_object(),
        "pagination must be an object"
    );
    assert!(
        body.get("time_range").is_some(),
        "response must have 'time_range'"
    );
    assert!(
        body["time_range"].is_object(),
        "time_range must be an object"
    );
}

#[tokio::test]
async fn fql_query_backward_compatible() {
    let port = spawn_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/api/query"))
        .json(&serde_json::json!({ "query": "last 1h" }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "FQL query should return 200");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body.get("columns").is_some(),
        "response must have 'columns'"
    );
    assert!(body["rows"].is_array(), "rows must be an array");
    assert!(body["stats"].is_object(), "stats must be an object");
    assert!(
        body["pagination"].is_object(),
        "pagination must be an object"
    );
    assert!(
        body["time_range"].is_object(),
        "time_range must be an object"
    );
}

#[tokio::test]
async fn query_schema_returns_fields() {
    let port = spawn_server().await;
    let body: serde_json::Value = reqwest::get(format!("http://127.0.0.1:{port}/api/query/schema"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    // filter_types must be an object with known keys
    let filter_types = body
        .get("filter_types")
        .expect("response must have 'filter_types'");
    assert!(filter_types.is_object(), "filter_types must be an object");
    let ft = filter_types.as_object().unwrap();
    for expected_key in &["ipv4", "port", "string", "numeric", "protocol", "mac"] {
        assert!(
            ft.contains_key(*expected_key),
            "filter_types missing key '{expected_key}'"
        );
    }

    // fields must be a non-empty array
    let fields = body.get("fields").expect("response must have 'fields'");
    assert!(fields.is_array(), "fields must be an array");
    assert!(
        !fields.as_array().unwrap().is_empty(),
        "fields must not be empty"
    );
}

#[tokio::test]
async fn structured_query_with_filters() {
    let port = spawn_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/api/query"))
        .json(&serde_json::json!({
            "time_range": { "type": "relative", "duration": "1h" },
            "filters": [
                { "field": "protocolIdentifier", "op": "eq", "value": 6, "negated": false }
            ]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        200,
        "structured query with filters should return 200"
    );
}

// ===========================================================================
// Histogram E2E tests
// ===========================================================================

#[tokio::test]
async fn histogram_returns_valid_sse_structure() {
    let dir = histogram_test_dir("valid_sse");
    // Single part: 10 records at t=1_700_000_100
    ingest_records_at_time(&dir, 1_700_000_100, 10);

    let port = spawn_server_with_storage(&dir).await;
    let client = reqwest::Client::new();
    let text = fetch_histogram_raw(
        &client,
        port,
        serde_json::json!({
            "type": "absolute",
            "start": "2023-11-14T22:14:00Z",
            "end": "2023-11-14T22:16:00Z"
        }),
        serde_json::json!([]),
    )
    .await;

    let events = parse_sse_events(&text);
    assert!(!events.is_empty(), "must receive at least one SSE event");

    let final_ev = final_event(&events);

    // Validate required fields
    assert!(
        final_ev.get("buckets").is_some(),
        "response must have 'buckets'"
    );
    assert!(final_ev["buckets"].is_array(), "buckets must be an array");
    assert!(
        final_ev.get("total_rows").is_some(),
        "response must have 'total_rows'"
    );
    assert!(
        final_ev.get("time_range").is_some(),
        "response must have 'time_range'"
    );
    assert!(
        final_ev["time_range"].get("start").is_some(),
        "time_range must have 'start'"
    );
    assert!(
        final_ev["time_range"].get("end").is_some(),
        "time_range must have 'end'"
    );
    assert!(
        final_ev.get("bucket_ms").is_some(),
        "response must have 'bucket_ms'"
    );
    assert!(final_ev.get("done").is_some(), "response must have 'done'");
    assert_eq!(
        final_ev["done"].as_bool(),
        Some(true),
        "final event must have done=true"
    );

    // Each bucket must have timestamp and count
    for bucket in final_ev["buckets"].as_array().unwrap() {
        assert!(
            bucket.get("timestamp").is_some(),
            "bucket must have 'timestamp'"
        );
        assert!(bucket.get("count").is_some(), "bucket must have 'count'");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn histogram_empty_range_returns_zero_total() {
    let dir = histogram_test_dir("empty_range");
    // Data at t=1_700_000_100, query a completely different range
    ingest_records_at_time(&dir, 1_700_000_100, 5);

    let port = spawn_server_with_storage(&dir).await;
    let client = reqwest::Client::new();
    let text = fetch_histogram_raw(
        &client,
        port,
        serde_json::json!({
            "type": "absolute",
            "start": "2020-01-01T00:00:00Z",
            "end": "2020-01-01T00:05:00Z"
        }),
        serde_json::json!([]),
    )
    .await;

    let events = parse_sse_events(&text);
    let final_ev = final_event(&events);

    let total = final_ev["total_rows"].as_u64().unwrap();
    assert_eq!(total, 0, "total_rows must be 0 for a range with no data");

    // All bucket counts must be 0
    let buckets = final_ev["buckets"].as_array().unwrap();
    assert!(!buckets.is_empty(), "must return buckets even with no data");
    for bucket in buckets {
        assert_eq!(
            bucket["count"].as_u64().unwrap(),
            0,
            "all bucket counts must be 0 when no data in range"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn histogram_total_rows_match_ingested() {
    let dir = histogram_test_dir("total_match");
    // Ingest exactly 50 records at a known time
    let export_time: u32 = 1_700_000_100;
    ingest_records_at_time(&dir, export_time, 50);

    let port = spawn_server_with_storage(&dir).await;
    let client = reqwest::Client::new();

    // Query a range that fully contains the export time
    let text = fetch_histogram_raw(
        &client,
        port,
        serde_json::json!({
            "type": "absolute",
            "start": "2023-11-14T22:00:00Z",
            "end": "2023-11-14T22:30:00Z"
        }),
        serde_json::json!([]),
    )
    .await;

    let events = parse_sse_events(&text);
    let final_ev = final_event(&events);

    let total = final_ev["total_rows"].as_u64().unwrap();
    assert_eq!(
        total, 50,
        "total_rows ({total}) must match the 50 ingested records"
    );

    // Sum of all bucket counts must also equal total_rows
    let bucket_sum: u64 = final_ev["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["count"].as_u64().unwrap())
        .sum();
    assert_eq!(
        bucket_sum, total,
        "sum of bucket counts ({bucket_sum}) must equal total_rows ({total})"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn histogram_buckets_cover_queried_range() {
    let dir = histogram_test_dir("bucket_coverage");
    ingest_records_at_time(&dir, 1_700_000_100, 10);

    let port = spawn_server_with_storage(&dir).await;
    let client = reqwest::Client::new();

    // Absolute range: 2023-11-14T22:00:00Z to 2023-11-14T23:00:00Z (1 hour)
    let text = fetch_histogram_raw(
        &client,
        port,
        serde_json::json!({
            "type": "absolute",
            "start": "2023-11-14T22:00:00Z",
            "end": "2023-11-14T23:00:00Z"
        }),
        serde_json::json!([]),
    )
    .await;

    let events = parse_sse_events(&text);
    let final_ev = final_event(&events);

    let buckets = final_ev["buckets"].as_array().unwrap();
    assert!(
        buckets.len() >= 2,
        "must have at least 2 buckets for a 1-hour range, got {}",
        buckets.len()
    );

    let bucket_ms = final_ev["bucket_ms"].as_u64().unwrap();
    assert!(bucket_ms > 0, "bucket_ms must be positive");

    let time_start = final_ev["time_range"]["start"].as_u64().unwrap();
    let time_end = final_ev["time_range"]["end"].as_u64().unwrap();

    // First bucket timestamp must be at or before the query start (aligned)
    let first_ts = buckets[0]["timestamp"].as_u64().unwrap();
    assert!(
        first_ts <= time_start,
        "first bucket ts ({first_ts}) must be <= query start ({time_start})"
    );

    // Last bucket must start before the query end
    let last_ts = buckets.last().unwrap()["timestamp"].as_u64().unwrap();
    assert!(
        last_ts < time_end,
        "last bucket ts ({last_ts}) must be < query end ({time_end})"
    );

    // Last bucket end (ts + bucket_ms) must reach or exceed the query end
    assert!(
        last_ts + bucket_ms >= time_end,
        "last bucket end ({}) must >= query end ({time_end})",
        last_ts + bucket_ms
    );

    // Buckets must be sorted and uniformly spaced
    for pair in buckets.windows(2) {
        let t0 = pair[0]["timestamp"].as_u64().unwrap();
        let t1 = pair[1]["timestamp"].as_u64().unwrap();
        assert_eq!(
            t1 - t0,
            bucket_ms,
            "buckets must be uniformly spaced by bucket_ms ({bucket_ms})"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn histogram_multi_part_aggregation() {
    let dir = histogram_test_dir("multi_part");

    // Write 3 parts at different times within the same hour
    // t=1_700_000_000 (2023-11-14T22:13:20Z): 20 records
    // t=1_700_000_060 (22:14:20): 30 records
    // t=1_700_000_120 (22:15:20): 50 records
    ingest_records_at_time(&dir, 1_700_000_000, 20);
    ingest_records_at_time(&dir, 1_700_000_060, 30);
    ingest_records_at_time(&dir, 1_700_000_120, 50);

    let port = spawn_server_with_storage(&dir).await;
    let client = reqwest::Client::new();

    let text = fetch_histogram_raw(
        &client,
        port,
        serde_json::json!({
            "type": "absolute",
            "start": "2023-11-14T22:13:00Z",
            "end": "2023-11-14T22:16:00Z"
        }),
        serde_json::json!([]),
    )
    .await;

    let events = parse_sse_events(&text);
    let final_ev = final_event(&events);

    let total = final_ev["total_rows"].as_u64().unwrap();
    assert_eq!(
        total, 100,
        "total_rows ({total}) must be 20+30+50=100 across 3 parts"
    );

    let bucket_sum: u64 = final_ev["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["count"].as_u64().unwrap())
        .sum();
    assert_eq!(
        bucket_sum, total,
        "sum of bucket counts ({bucket_sum}) must equal total_rows ({total})"
    );

    // At least some buckets must have non-zero counts
    let nonzero: Vec<_> = final_ev["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|b| b["count"].as_u64().unwrap() > 0)
        .collect();
    assert!(
        !nonzero.is_empty(),
        "some buckets must have non-zero counts"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn histogram_progressive_events_are_subsets_of_final() {
    let dir = histogram_test_dir("progressive");

    // Write enough parts to trigger progressive SSE events.
    // The metadata path emits progress every ~3 parts.
    for i in 0..12u32 {
        ingest_records_at_time(&dir, 1_700_000_000 + i * 10, 10);
    }

    let port = spawn_server_with_storage(&dir).await;
    let client = reqwest::Client::new();

    let text = fetch_histogram_raw(
        &client,
        port,
        serde_json::json!({
            "type": "absolute",
            "start": "2023-11-14T22:13:00Z",
            "end": "2023-11-14T22:16:00Z"
        }),
        serde_json::json!([]),
    )
    .await;

    let events = parse_sse_events(&text);
    let final_ev = final_event(&events);
    let final_total = final_ev["total_rows"].as_u64().unwrap();

    assert_eq!(
        final_total, 120,
        "final total_rows ({final_total}) must be 12*10=120"
    );

    // If we got multiple events, progressive totals must be monotonically
    // increasing and <= final total.
    if events.len() > 1 {
        let mut prev_total = 0u64;
        for ev in &events {
            let t = ev["total_rows"].as_u64().unwrap_or(0);
            assert!(
                t >= prev_total,
                "progressive total_rows must be monotonically increasing ({prev_total} -> {t})"
            );
            prev_total = t;
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn histogram_data_lands_in_correct_bucket() {
    let dir = histogram_test_dir("correct_bucket");
    // All data at exactly t=1_700_000_100 (2023-11-14T22:15:00Z)
    ingest_records_at_time(&dir, 1_700_000_100, 25);

    let port = spawn_server_with_storage(&dir).await;
    let client = reqwest::Client::new();

    // 5-minute window, should yield small bucket sizes
    let text = fetch_histogram_raw(
        &client,
        port,
        serde_json::json!({
            "type": "absolute",
            "start": "2023-11-14T22:13:00Z",
            "end": "2023-11-14T22:18:00Z"
        }),
        serde_json::json!([]),
    )
    .await;

    let events = parse_sse_events(&text);
    let final_ev = final_event(&events);

    let bucket_ms = final_ev["bucket_ms"].as_u64().unwrap();
    let buckets = final_ev["buckets"].as_array().unwrap();

    // Find which bucket(s) contain the export time (now in ms)
    let data_ts: u64 = 1_700_000_100_000;
    let containing: Vec<_> = buckets
        .iter()
        .filter(|b| {
            let ts = b["timestamp"].as_u64().unwrap();
            ts <= data_ts && data_ts < ts + bucket_ms
        })
        .collect();

    assert_eq!(
        containing.len(),
        1,
        "exactly one bucket must contain the data timestamp"
    );

    let data_bucket_count = containing[0]["count"].as_u64().unwrap();
    assert_eq!(
        data_bucket_count, 25,
        "the bucket containing the data must have count=25, got {data_bucket_count}"
    );

    // All other buckets must be 0
    let other_sum: u64 = buckets
        .iter()
        .filter(|b| {
            let ts = b["timestamp"].as_u64().unwrap();
            !(ts <= data_ts && data_ts < ts + bucket_ms)
        })
        .map(|b| b["count"].as_u64().unwrap())
        .sum();
    assert_eq!(
        other_sum, 0,
        "buckets not containing the data must have count=0, got sum={other_sum}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn histogram_no_double_count_at_bucket_boundary() {
    let dir = histogram_test_dir("boundary_no_double");
    // Pick a time that aligns to a common bucket boundary.
    // With 5-minute window and 60 target buckets, bucket_ms=5000.
    // Use t=1_700_000_100 which is divisible by 5, 10, etc.
    let boundary_ts: u32 = 1_700_000_100;
    ingest_records_at_time(&dir, boundary_ts, 40);

    let port = spawn_server_with_storage(&dir).await;
    let client = reqwest::Client::new();

    let text = fetch_histogram_raw(
        &client,
        port,
        serde_json::json!({
            "type": "absolute",
            "start": "2023-11-14T22:14:00Z",
            "end": "2023-11-14T22:19:00Z"
        }),
        serde_json::json!([]),
    )
    .await;

    let events = parse_sse_events(&text);
    let final_ev = final_event(&events);

    let total = final_ev["total_rows"].as_u64().unwrap();
    let bucket_sum: u64 = final_ev["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["count"].as_u64().unwrap())
        .sum();

    assert_eq!(total, 40, "total_rows must be exactly 40, got {total}");
    assert_eq!(
        bucket_sum, total,
        "bucket count sum ({bucket_sum}) must equal total_rows ({total}) — no double-counting"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn histogram_with_filter_reduces_count() {
    let dir = histogram_test_dir("with_filter");
    // Ingest 30 records with sourceIPv4Address 10.0.0.x
    ingest_records_at_time(&dir, 1_700_000_100, 30);

    let port = spawn_server_with_storage(&dir).await;
    let client = reqwest::Client::new();

    // Unfiltered: should get 30
    let text_unfiltered = fetch_histogram_raw(
        &client,
        port,
        serde_json::json!({
            "type": "absolute",
            "start": "2023-11-14T22:00:00Z",
            "end": "2023-11-14T23:00:00Z"
        }),
        serde_json::json!([]),
    )
    .await;

    let events_unfiltered = parse_sse_events(&text_unfiltered);
    let ev_unfiltered = final_event(&events_unfiltered);
    let total_unfiltered = ev_unfiltered["total_rows"].as_u64().unwrap();
    assert_eq!(total_unfiltered, 30);

    // Filtered by a specific source IP — the filtered path (group-by aggregation)
    // should return a subset. Our test data has sourceIPv4Address = 10.0.0.{0..29},
    // so filtering for 10.0.0.1 should match exactly 1 record.
    let text_filtered = fetch_histogram_raw(
        &client,
        port,
        serde_json::json!({
            "type": "absolute",
            "start": "2023-11-14T22:00:00Z",
            "end": "2023-11-14T23:00:00Z"
        }),
        serde_json::json!([
            { "field": "sourceIPv4Address", "op": "eq", "value": "10.0.0.1" }
        ]),
    )
    .await;

    let events_filtered = parse_sse_events(&text_filtered);
    let ev_filtered = final_event(&events_filtered);
    let total_filtered = ev_filtered["total_rows"].as_u64().unwrap();
    assert!(
        total_filtered < total_unfiltered,
        "filtered total ({total_filtered}) must be less than unfiltered ({total_unfiltered})"
    );
    assert_eq!(
        total_filtered, 1,
        "filtering for sourceIPv4Address=10.0.0.1 should match exactly 1 record, got {total_filtered}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn histogram_spread_across_multiple_buckets() {
    let dir = histogram_test_dir("multi_bucket_spread");

    // Ingest 4 batches spread across 10 minutes so they land in different buckets.
    // For a 10-minute window with 60 target buckets, bucket_ms=10000.
    ingest_records_at_time(&dir, 1_700_000_000, 10); // 22:13:20
    ingest_records_at_time(&dir, 1_700_000_120, 15); // 22:15:20
    ingest_records_at_time(&dir, 1_700_000_300, 20); // 22:18:20
    ingest_records_at_time(&dir, 1_700_000_540, 25); // 22:22:20

    let port = spawn_server_with_storage(&dir).await;
    let client = reqwest::Client::new();

    let text = fetch_histogram_raw(
        &client,
        port,
        serde_json::json!({
            "type": "absolute",
            "start": "2023-11-14T22:13:00Z",
            "end": "2023-11-14T22:23:00Z"
        }),
        serde_json::json!([]),
    )
    .await;

    let events = parse_sse_events(&text);
    let final_ev = final_event(&events);

    let total = final_ev["total_rows"].as_u64().unwrap();
    assert_eq!(total, 70, "total_rows ({total}) must be 10+15+20+25=70");

    // With data at 4 distinct times, we should have at least 4 non-zero buckets
    // (unless two times land in the same bucket at coarser granularity)
    let nonzero_count = final_ev["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|b| b["count"].as_u64().unwrap() > 0)
        .count();
    assert!(
        nonzero_count >= 3,
        "data spread across 10 minutes should produce at least 3 non-zero buckets, got {nonzero_count}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
