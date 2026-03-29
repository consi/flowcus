use axum::{
    Router,
    extract::State,
    http::header,
    response::{IntoResponse, Json},
    routing::get,
};
use serde_json::{Value, json};

use crate::state::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route("/health/stats", get(health_stats))
        .route("/info", get(info))
        .route("/interfaces", get(interfaces))
}

pub fn observability_routes() -> Router<AppState> {
    Router::new().route("/metrics", get(prometheus_metrics))
}

async fn prometheus_metrics(State(state): State<AppState>) -> impl IntoResponse {
    let body = state.metrics().to_prometheus();
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn health_stats(State(state): State<AppState>) -> Json<Value> {
    let metrics = state.metrics();
    let values: serde_json::Map<String, Value> = metrics
        .to_json_values()
        .into_iter()
        .map(|(k, v)| (k.to_string(), json!(v)))
        .collect();

    // Process memory from /proc
    let mut process_rss_bytes: i64 = 0;
    let mut process_threads: i64 = 0;
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(v) = line.strip_prefix("VmRSS:") {
                process_rss_bytes = v
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(0)
                    * 1024;
            }
            if let Some(v) = line.strip_prefix("Threads:") {
                process_threads = v.trim().parse().unwrap_or(0);
            }
        }
    }

    // Cache saturation
    let (cache_used, cache_max) = state.storage_cache().saturation();
    let (cache_hits, cache_misses) = state.storage_cache().stats();
    let cache_partitions: Vec<Value> = state
        .storage_cache()
        .partition_stats()
        .iter()
        .map(|(name, used, max)| json!({ "name": name, "used_bytes": used, "max_bytes": max }))
        .collect();

    // Storage stats: scan the table directory for live part counts and disk usage.
    // This is cheap (directory entries only, no file reads) and always accurate.
    let storage_dir = state.storage_dir().to_string();
    let (parts_total, parts_gen0, parts_merged, disk_bytes) =
        tokio::task::spawn_blocking(move || compute_storage_stats(&storage_dir))
            .await
            .unwrap_or((0, 0, 0, 0));

    Json(json!({
        "metrics": values,
        "process": {
            "rss_bytes": process_rss_bytes,
            "threads": process_threads,
        },
        "cache": {
            "used_bytes": cache_used,
            "max_bytes": cache_max,
            "hits": cache_hits,
            "misses": cache_misses,
            "partitions": cache_partitions,
        },
        "storage": {
            "parts_total": parts_total,
            "parts_gen0": parts_gen0,
            "parts_merged": parts_merged,
            "disk_bytes": disk_bytes,
        },
    }))
}

fn walk_part_tree(
    dir: &std::path::Path,
    depth: u8,
    total: &mut u64,
    gen0: &mut u64,
    merged: &mut u64,
    bytes: &mut u64,
) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(ft) = entry.file_type() else { continue };
        if ft.is_dir() {
            if depth < 4 {
                walk_part_tree(&entry.path(), depth + 1, total, gen0, merged, bytes);
            } else {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                *total += 1;
                if let Some(generation) = name_str
                    .split('_')
                    .nth(1)
                    .and_then(|s| s.parse::<u32>().ok())
                {
                    if generation == 0 {
                        *gen0 += 1;
                    } else {
                        *merged += 1;
                    }
                }
                sum_dir_bytes(&entry.path(), bytes);
            }
        }
    }
}

fn sum_dir_bytes(dir: &std::path::Path, bytes: &mut u64) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(ft) = entry.file_type() else { continue };
        if ft.is_file() {
            if let Ok(meta) = entry.metadata() {
                *bytes += meta.len();
            }
        } else if ft.is_dir() {
            sum_dir_bytes(&entry.path(), bytes);
        }
    }
}

/// Walk the storage directory tree and compute part counts + total disk bytes.
fn compute_storage_stats(storage_dir: &str) -> (u64, u64, u64, u64) {
    let table_dir = std::path::Path::new(storage_dir).join("flows");
    let mut parts_total: u64 = 0;
    let mut parts_gen0: u64 = 0;
    let mut parts_merged: u64 = 0;
    let mut disk_bytes: u64 = 0;
    walk_part_tree(
        &table_dir,
        0,
        &mut parts_total,
        &mut parts_gen0,
        &mut parts_merged,
        &mut disk_bytes,
    );
    (parts_total, parts_gen0, parts_merged, disk_bytes)
}

async fn interfaces(State(state): State<AppState>) -> Json<Value> {
    let entries = if let Some(session) = state.session_store() {
        let names = session.lock().await.get_interface_names();
        let mut entries: Vec<Value> = names
            .into_iter()
            .map(|((exporter, domain_id, index), name)| {
                json!({
                    "exporter": exporter.to_string(),
                    "domain_id": domain_id,
                    "index": index,
                    "name": name,
                })
            })
            .collect();
        entries.sort_by(|a, b| {
            let key = |v: &Value| {
                (
                    v["exporter"].as_str().unwrap_or("").to_string(),
                    v["domain_id"].as_u64().unwrap_or(0),
                    v["index"].as_u64().unwrap_or(0),
                )
            };
            key(a).cmp(&key(b))
        });
        entries
    } else {
        Vec::new()
    };

    Json(json!({ "interfaces": entries }))
}

async fn info(State(state): State<AppState>) -> Json<Value> {
    let config = state.config();
    Json(json!({
        "name": "flowcus",
        "version": env!("CARGO_PKG_VERSION"),
        "server": {
            "host": config.server.host,
            "port": config.server.port,
            "dev_mode": config.server.dev_mode,
        },
        "storage": {
            "merge_workers": config.storage.merge_workers,
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use flowcus_core::AppConfig;

    fn test_state() -> AppState {
        let config = AppConfig::default();
        let metrics = flowcus_core::observability::Metrics::new();
        AppState::new(config, metrics, std::path::PathBuf::from("test.settings"))
    }

    #[tokio::test]
    async fn health_returns_ok() {
        let app = routes().with_state(test_state());
        let req = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn info_returns_version() {
        let app = routes().with_state(test_state());
        let req = Request::builder().uri("/info").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
