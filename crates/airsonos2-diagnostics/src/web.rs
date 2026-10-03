//! Config web GUI. It edits the config file; changes apply after a restart.

use std::path::PathBuf;
use std::sync::Arc;

use airsonos2_core::Config;
use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::info;

const INDEX_HTML: &str = include_str!("../web/index.html");

pub struct ConfigUi {
    pub path: PathBuf,
    /// Set when another tool generates the file, such as the Home Assistant app.
    pub read_only: bool,
    /// The config the bridge started with. The file differs from it when a restart is due.
    pub running: Config,
    pub started_at_ms: u64,
    pub status: watch::Receiver<BridgeStatus>,
    /// Cancelled when the user asks for a restart. The service owner must exit.
    pub restart: CancellationToken,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum BridgeStatus {
    Starting,
    /// No speaker could start. The bridge retries and keeps the GUI available.
    Setup {
        error: String,
    },
    Running {
        speakers: Vec<Speaker>,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct Speaker {
    pub room: String,
    pub airplay_name: String,
    pub rtsp_port: u16,
}

#[derive(Serialize)]
struct ConfigView {
    version: &'static str,
    path: PathBuf,
    read_only: bool,
    restart_required: bool,
    started_at_ms: u64,
    status: BridgeStatus,
    /// Secrets are removed. `secrets` tells whether each one is set.
    config: Config,
    secrets: SecretState<bool>,
}

/// For a save, `None` keeps the stored secret and an empty string removes it.
#[derive(Default, Deserialize, Serialize)]
#[serde(default)]
struct SecretState<T> {
    home_assistant_token: T,
    rtsp_password: T,
}

#[derive(Deserialize)]
struct SaveRequest {
    /// Secret fields in here are ignored.
    config: Config,
    #[serde(default)]
    secrets: SecretState<Option<String>>,
}

/// A JSON body forces a CORS preflight, so other web pages cannot restart the bridge.
#[derive(Deserialize)]
struct RestartRequest {}

struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({ "error": self.1 });
        (self.0, Json(body)).into_response()
    }
}

fn internal(error: impl std::fmt::Display) -> ApiError {
    ApiError(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

pub fn router(ui: ConfigUi) -> Router {
    Router::new()
        .route("/", get(|| async { Html(INDEX_HTML) }))
        .route("/api/config", get(read_config).put(save_config))
        .route("/api/restart", post(restart))
        .with_state(Arc::new(ui))
}

async fn load_file(ui: &ConfigUi) -> Result<Config, ApiError> {
    let path = ui.path.clone();
    tokio::task::spawn_blocking(move || Config::from_path(path))
        .await
        .map_err(internal)?
        .map_err(internal)
}

fn view(ui: &ConfigUi, mut config: Config) -> ConfigView {
    // Compare before removing secrets, because secret changes also need a restart.
    let restart_required = config != ui.running;
    let is_set = |secret: Option<String>| secret.is_some_and(|secret| !secret.is_empty());
    let secrets = SecretState {
        home_assistant_token: is_set(config.home_assistant.token.take()),
        rtsp_password: is_set(config.airplay.rtsp_password.take()),
    };
    ConfigView {
        version: env!("CARGO_PKG_VERSION"),
        path: ui.path.clone(),
        read_only: ui.read_only,
        restart_required,
        started_at_ms: ui.started_at_ms,
        status: ui.status.borrow().clone(),
        config,
        secrets,
    }
}

async fn read_config(State(ui): State<Arc<ConfigUi>>) -> Result<Json<ConfigView>, ApiError> {
    let config = load_file(&ui).await?;
    Ok(Json(view(&ui, config)))
}

async fn save_config(
    State(ui): State<Arc<ConfigUi>>,
    request: Result<Json<SaveRequest>, JsonRejection>,
) -> Result<Json<ConfigView>, ApiError> {
    if ui.read_only {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "this config file is read-only; edit its source instead".to_owned(),
        ));
    }
    let Json(SaveRequest {
        mut config,
        secrets,
    }) = request.map_err(|rejection| ApiError(rejection.status(), rejection.body_text()))?;
    let stored = load_file(&ui).await?;
    let update = |change: Option<String>, stored: Option<String>| {
        change.map_or(stored, |value| (!value.is_empty()).then_some(value))
    };
    config.home_assistant.token = update(secrets.home_assistant_token, stored.home_assistant.token);
    config.airplay.rtsp_password = update(secrets.rtsp_password, stored.airplay.rtsp_password);
    config
        .validate()
        .map_err(|error| ApiError(StatusCode::UNPROCESSABLE_ENTITY, error.to_string()))?;

    let path = ui.path.clone();
    let written = config.clone();
    tokio::task::spawn_blocking(move || written.write_to_path(path))
        .await
        .map_err(internal)?
        .map_err(internal)?;
    info!(path = %ui.path.display(), "config saved from the web GUI");
    Ok(Json(view(&ui, config)))
}

async fn restart(
    State(ui): State<Arc<ConfigUi>>,
    request: Result<Json<RestartRequest>, JsonRejection>,
) -> Result<StatusCode, ApiError> {
    let Json(RestartRequest {}) =
        request.map_err(|rejection| ApiError(rejection.status(), rejection.body_text()))?;
    info!("restart requested from the web GUI");
    ui.restart.cancel();
    Ok(StatusCode::ACCEPTED)
}
