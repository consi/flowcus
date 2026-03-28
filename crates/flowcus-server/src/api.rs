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
        .route("/info", get(info))
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
        AppState::new(config, metrics)
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
