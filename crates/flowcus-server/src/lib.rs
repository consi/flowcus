pub mod api;
pub mod assets;
pub mod query;
pub mod state;

use std::net::SocketAddr;

use axum::Router;
use tokio::net::TcpListener;
use tower_http::{compression::CompressionLayer, cors::CorsLayer, trace::TraceLayer};
use tracing::info;

use flowcus_core::config::ServerConfig;

use crate::state::AppState;

/// Build the full application router.
pub fn build_router(state: AppState) -> Router {
    let api_routes = api::routes().merge(query::routes());
    let obs_routes = api::observability_routes();

    Router::new()
        .nest("/api", api_routes)
        .nest("/observability", obs_routes)
        .fallback(assets::static_handler)
        .layer(TraceLayer::new_for_http())
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        .with_state(state)
}

/// Start the server and listen for connections.
///
/// # Errors
/// Returns an error if the server fails to bind or encounters a runtime error.
pub async fn serve(config: &ServerConfig, state: AppState) -> flowcus_core::Result<()> {
    let addr = SocketAddr::new(
        config.host.parse().unwrap_or_else(|_| [0, 0, 0, 0].into()),
        config.port,
    );
    let router = build_router(state);

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| flowcus_core::Error::server(e.to_string()))?;

    info!(%addr, "Server listening");

    axum::serve(listener, router)
        .await
        .map_err(|e| flowcus_core::Error::server(e.to_string()))?;

    Ok(())
}
