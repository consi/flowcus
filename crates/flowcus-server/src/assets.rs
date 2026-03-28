use axum::{
    extract::Request,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use rust_embed::Embed;

#[derive(Embed)]
#[folder = "../../frontend/dist/"]
struct FrontendAssets;

/// Serve embedded frontend assets, falling back to index.html for SPA routing.
pub async fn static_handler(req: Request) -> Response {
    let path = req.uri().path().trim_start_matches('/');

    // Try the exact path first
    if let Some(content) = FrontendAssets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        return (
            StatusCode::OK,
            [(header::CONTENT_TYPE, mime.as_ref())],
            content.data.to_vec(),
        )
            .into_response();
    }

    // SPA fallback: serve index.html for non-file routes
    if let Some(index) = FrontendAssets::get("index.html") {
        return (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/html")],
            index.data.to_vec(),
        )
            .into_response();
    }

    (StatusCode::NOT_FOUND, "Not found").into_response()
}
