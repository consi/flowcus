use axum::{
    extract::Request,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use rust_embed::Embed;

#[derive(Embed)]
#[folder = "../../frontend/dist/"]
struct FrontendAssets;

/// Serve embedded frontend assets, fall back to index.html for SPA routing.
///
/// Never cached: every response gets `no-store, no-cache, must-revalidate`.
/// The frontend is embedded in the binary, so serving it is free.
pub async fn static_handler(req: Request) -> Response {
    let path = req.uri().path().trim_start_matches('/');
    serve_embedded(path)
}

/// In dev mode, proxy all non-API requests to the Vite dev server on :5173.
///
/// This ensures the browser always gets fresh frontend code with HMR support.
/// Falls back to embedded assets if Vite is not running yet.
pub async fn dev_proxy_handler(req: Request) -> Response {
    let path_and_query = req
        .uri()
        .path_and_query()
        .map_or_else(|| "/".to_string(), |pq| pq.as_str().to_string());
    let path = req.uri().path().trim_start_matches('/').to_string();

    let vite_url = format!("http://localhost:5173{path_and_query}");

    let Ok(resp) = reqwest::get(&vite_url).await else {
        // Vite not ready yet — fall back to embedded assets
        return serve_embedded(&path);
    };

    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let body: Vec<u8> = resp.bytes().await.map(|b| b.to_vec()).unwrap_or_default();

    (status, [(header::CONTENT_TYPE, content_type)], body).into_response()
}

fn serve_embedded(path: &str) -> Response {
    if let Some(content) = FrontendAssets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        return no_cache_response(StatusCode::OK, mime.as_ref(), content.data.to_vec());
    }

    // SPA fallback: serve index.html for non-file routes
    if let Some(index) = FrontendAssets::get("index.html") {
        return no_cache_response(StatusCode::OK, "text/html", index.data.to_vec());
    }

    (StatusCode::NOT_FOUND, "Not found").into_response()
}

fn no_cache_response(status: StatusCode, content_type: &str, body: Vec<u8>) -> Response {
    (
        status,
        [
            (header::CONTENT_TYPE, content_type.to_string()),
            (
                header::CACHE_CONTROL,
                "no-store, no-cache, must-revalidate".to_string(),
            ),
            (header::PRAGMA, "no-cache".to_string()),
            (header::EXPIRES, "0".to_string()),
        ],
        body,
    )
        .into_response()
}
