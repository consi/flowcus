use std::net::TcpListener;
use std::time::Duration;

use flowcus_core::AppConfig;
use flowcus_server::state::AppState;
use flowcus_worker::WorkerPool;

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

    let pool = WorkerPool::new(&config.worker).unwrap();
    let handle = pool.handle();
    std::thread::spawn(move || pool.run_cpu_dispatch());

    let metrics = flowcus_core::observability::Metrics::new();
    let state = AppState::new(config.clone(), handle, metrics);
    let server_config = config.server.clone();
    tokio::spawn(async move {
        flowcus_server::serve(&server_config, state).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    port
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
    assert!(body["workers"]["cpu"].is_number());
}
