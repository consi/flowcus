use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
};
use serde_json::{Value, json};
use tracing::{info, warn};

use crate::state::AppState;

/// Settings API routes — mounted under `/api` alongside other API routes.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/settings", get(get_settings).put(put_settings))
        .route("/settings/schema", get(get_settings_schema))
        .route("/settings/defaults", get(get_settings_defaults))
        .route("/settings/validate", post(validate_settings))
        .route("/restart", post(restart_server))
}

/// Return the current configuration as JSON.
async fn get_settings(State(state): State<AppState>) -> Json<Value> {
    Json(flowcus_core::settings::config_to_json(state.config()))
}

/// Return the settings schema (field descriptors, control types, sections).
async fn get_settings_schema(State(state): State<AppState>) -> Json<Value> {
    let _ = &state; // accessed for consistency; schema is static
    let schema = flowcus_core::settings::schema();
    Json(serde_json::to_value(schema).unwrap_or_default())
}

/// Return the default configuration values.
async fn get_settings_defaults(State(state): State<AppState>) -> Json<Value> {
    let _ = &state;
    Json(flowcus_core::settings::defaults_json())
}

/// Validate a partial settings patch without saving.
async fn validate_settings(
    State(state): State<AppState>,
    Json(patch): Json<Value>,
) -> impl IntoResponse {
    let base = state.config();
    let merged = match flowcus_core::settings::apply_partial(base, &patch) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": e.to_string() })),
            );
        }
    };
    let result = flowcus_core::settings::validate(&merged);
    (
        StatusCode::OK,
        Json(serde_json::to_value(result).unwrap_or_default()),
    )
}

/// Apply a partial settings patch: validate, save to disk, and report which
/// fields require a restart to take effect.
async fn put_settings(
    State(state): State<AppState>,
    Json(patch): Json<Value>,
) -> impl IntoResponse {
    // Serialize settings writes so concurrent PUTs don't race on the file.
    let _guard = state.settings_lock().lock().await;

    // Load the latest config from disk (not from state — another PUT may have
    // written since this process started).
    let base = match flowcus_core::settings::load_or_create(state.settings_path()) {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "Failed to load settings file");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("failed to load settings: {e}") })),
            );
        }
    };

    // Apply the partial patch on top of the on-disk config.
    let merged = match flowcus_core::settings::apply_partial(&base, &patch) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": e.to_string() })),
            );
        }
    };

    // Validate — return 400 if there are errors.
    let validation = flowcus_core::settings::validate(&merged);
    if validation.has_errors() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::to_value(&validation).unwrap_or_default()),
        );
    }

    // Persist to disk.
    if let Err(e) = flowcus_core::settings::save(state.settings_path(), &merged) {
        warn!(error = %e, "Failed to save settings file");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("failed to save settings: {e}") })),
        );
    }

    info!("Settings saved to {}", state.settings_path().display());

    // Determine which fields changed that require a restart.
    let restart_required = flowcus_core::settings::changed_restart_fields(state.config(), &merged);

    (
        StatusCode::OK,
        Json(json!({
            "config": flowcus_core::settings::config_to_json(&merged),
            "restart_required": restart_required,
            "validation": validation,
        })),
    )
}

/// Signal the server to restart. The response is sent before the shutdown
/// signal fires so the client receives a clean 200.
async fn restart_server(State(state): State<AppState>) -> Json<Value> {
    info!("Restart requested via API");
    tokio::spawn(async move {
        // Small delay so the HTTP response can flush to the client.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        state.trigger_restart();
    });
    Json(json!({ "status": "restarting" }))
}
