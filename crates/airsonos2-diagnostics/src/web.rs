//! Config web GUI. It edits the config file; changes apply after a restart.

use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use airsonos2_core::Config;
use airsonos2_homeassistant::HomeAssistantClient;
use airsonos2_sonos::discover_sonos_zones_from_sources;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Request, State};
use axum::http::uri::Authority;
use axum::http::{StatusCode, header};
use axum::middleware::Next;
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
    /// Where this page is served now. A saved change moves it after the restart.
    listener: String,
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

/// The page's unsaved state. Save writes it; discover scans with it.
#[derive(Deserialize)]
struct DraftRequest {
    /// Secret fields in here are ignored.
    config: Config,
    #[serde(default)]
    secrets: SecretState<Option<String>>,
}

/// Speakers that the draft settings can reach, whether or not they are bridged.
#[derive(Serialize)]
struct Discovery {
    sonos: Found<SonosRoom>,
    /// `None` when no Home Assistant URL is set.
    home_assistant: Option<Found<HomeAssistantPlayer>>,
}

#[derive(Serialize)]
struct Found<T> {
    items: Vec<T>,
    error: Option<String>,
}

impl<T> Found<T> {
    fn from_result<E: std::fmt::Display>(result: Result<Vec<T>, E>) -> Self {
        match result {
            Ok(items) => Self { items, error: None },
            Err(error) => Self {
                items: Vec::new(),
                error: Some(error.to_string()),
            },
        }
    }
}

#[derive(Serialize)]
struct SonosRoom {
    room: String,
    ip: IpAddr,
    model: String,
}

/// Only players that support `play_media`, because AirSonos2 cannot use the others.
#[derive(Serialize)]
struct HomeAssistantPlayer {
    entity_id: String,
    name: String,
    state: String,
    unsupported: Vec<&'static str>,
}

/// A JSON body forces a CORS preflight, so other web pages cannot restart the bridge.
#[derive(Deserialize)]
struct RestartRequest {}

#[derive(Debug)]
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
    let ui = Arc::new(ui);
    Router::new()
        .route("/", get(|| async { Html(INDEX_HTML) }))
        .route("/api/config", get(read_config).put(save_config))
        .route("/api/discover", post(discover))
        .route("/api/restart", post(restart))
        .route("/api/instance", get(instance))
        .layer(axum::middleware::from_fn_with_state(ui.clone(), check_host))
        .with_state(ui)
}

/// Blocks DNS rebinding. A page on another domain can make that domain resolve to this
/// host and then call the GUI as its own origin, which CORS allows. Its requests still
/// carry that domain in `Host`.
async fn check_host(State(ui): State<Arc<ConfigUi>>, request: Request, next: Next) -> Response {
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|host| host.to_str().ok())
        .or_else(|| request.uri().authority().map(Authority::as_str));
    if host.is_some_and(|host| host_allowed(host, &ui.running.diagnostics.allowed_hosts)) {
        return next.run(request).await;
    }
    ApiError(
        StatusCode::FORBIDDEN,
        format!(
            "host {:?} is not allowed; add it to diagnostics.allowed_hosts",
            host.unwrap_or_default()
        ),
    )
    .into_response()
}

/// IP addresses, single-label names and local suffixes cannot point at a public
/// attacker's domain. Other names must be listed in `allowed_hosts`.
fn host_allowed(host: &str, allowed_hosts: &[String]) -> bool {
    const LOCAL_SUFFIXES: [&str; 5] = [".local", ".lan", ".home.arpa", ".internal", ".localhost"];
    let Ok(authority) = host.parse::<Authority>() else {
        return false;
    };
    let name = authority
        .host()
        .trim_start_matches('[')
        .trim_end_matches(']');
    if name.parse::<IpAddr>().is_ok() {
        return true;
    }
    let name = name.trim_end_matches('.').to_ascii_lowercase();
    !name.contains('.')
        || LOCAL_SUFFIXES.iter().any(|suffix| name.ends_with(suffix))
        || allowed_hosts
            .iter()
            .any(|allowed| allowed.trim_end_matches('.').eq_ignore_ascii_case(&name))
}

/// Identifies this process to a page that waits for a restart on a moved listener. It
/// reveals only the start time, so any origin may read it.
async fn instance(State(ui): State<Arc<ConfigUi>>) -> impl IntoResponse {
    (
        [(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")],
        Json(serde_json::json!({ "started_at_ms": ui.started_at_ms })),
    )
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
        listener: ui.running.diagnostics.metrics_addr.clone(),
        status: ui.status.borrow().clone(),
        config,
        secrets,
    }
}

async fn read_config(State(ui): State<Arc<ConfigUi>>) -> Result<Json<ConfigView>, ApiError> {
    let config = load_file(&ui).await?;
    Ok(Json(view(&ui, config)))
}

struct Draft {
    config: Config,
    /// The Home Assistant URL changed and no token came with it. That URL must not
    /// receive a stored token, including `SUPERVISOR_TOKEN`, because anyone who can
    /// reach the GUI could point it at their own server.
    new_ha_url_without_token: bool,
}

/// The draft config, with secrets taken from the request or else from the file.
async fn draft_config(
    ui: &ConfigUi,
    request: Result<Json<DraftRequest>, JsonRejection>,
) -> Result<Draft, ApiError> {
    let Json(DraftRequest {
        mut config,
        secrets,
    }) = request.map_err(|rejection| ApiError(rejection.status(), rejection.body_text()))?;
    let stored = load_file(ui).await?;
    let update = |change: Option<String>, stored: Option<String>| {
        change.map_or(stored, |value| (!value.is_empty()).then_some(value))
    };
    let url_changed = config.home_assistant.url != stored.home_assistant.url;
    let token_sent = secrets
        .home_assistant_token
        .as_ref()
        .is_some_and(|token| !token.is_empty());
    let stored_token = (!url_changed)
        .then_some(stored.home_assistant.token)
        .flatten();
    config.home_assistant.token = update(secrets.home_assistant_token, stored_token);
    config.airplay.rtsp_password = update(secrets.rtsp_password, stored.airplay.rtsp_password);
    Ok(Draft {
        config,
        new_ha_url_without_token: url_changed && !token_sent,
    })
}

async fn save_config(
    State(ui): State<Arc<ConfigUi>>,
    request: Result<Json<DraftRequest>, JsonRejection>,
) -> Result<Json<ConfigView>, ApiError> {
    if ui.read_only {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "this config file is read-only; edit its source instead".to_owned(),
        ));
    }
    let draft = draft_config(&ui, request).await?;
    // Without a token, the bridge would send `SUPERVISOR_TOKEN` to the new URL.
    if draft.new_ha_url_without_token && draft.config.home_assistant.url.is_some() {
        return Err(ApiError(
            StatusCode::UNPROCESSABLE_ENTITY,
            "home_assistant.token: enter the access token for the new Home Assistant URL"
                .to_owned(),
        ));
    }
    let config = draft.config;
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

async fn discover(
    State(ui): State<Arc<ConfigUi>>,
    request: Result<Json<DraftRequest>, JsonRejection>,
) -> Result<Json<Discovery>, ApiError> {
    let Draft {
        config,
        new_ha_url_without_token,
    } = draft_config(&ui, request).await?;
    let sonos = async {
        let zones = discover_sonos_zones_from_sources(
            Duration::from_secs(3),
            &config.sonos.static_ips,
            config.sonos.auto_discover,
        )
        .await?;
        let mut rooms: Vec<SonosRoom> = zones
            .into_iter()
            .filter(|zone| zone.is_visible_room)
            .map(|zone| SonosRoom {
                room: zone.room_name,
                ip: zone.ip,
                model: zone.model,
            })
            .collect();
        rooms.sort_by(|a, b| a.room.cmp(&b.room));
        Ok::<_, airsonos2_sonos::DiscoveryError>(rooms)
    };
    let home_assistant = async {
        config.home_assistant.url.as_ref()?;
        if new_ha_url_without_token {
            return Some(Found {
                items: Vec::new(),
                error: Some("enter the access token for the new Home Assistant URL".to_owned()),
            });
        }
        let players = match HomeAssistantClient::from_config(&config.home_assistant) {
            Ok(client) => client.media_players().await,
            Err(error) => Err(error),
        };
        Some(Found::from_result(players.map(|players| {
            let mut players: Vec<HomeAssistantPlayer> = players
                .into_iter()
                .filter(|player| player.supports_play_media())
                .map(|player| HomeAssistantPlayer {
                    unsupported: player.unsupported_services(),
                    entity_id: player.entity_id,
                    name: player.name,
                    state: player.state,
                })
                .collect();
            players.sort_by(|a, b| a.name.cmp(&b.name));
            players
        })))
    };
    let (sonos, home_assistant) = tokio::join!(sonos, home_assistant);
    Ok(Json(Discovery {
        sonos: Found::from_result(sonos),
        home_assistant,
    }))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_local_hosts_and_listed_names_reach_the_gui() {
        let allowed = ["airsonos.example.com".to_owned()];
        for host in [
            "192.168.1.5:9100",
            "[::1]:9100",
            "localhost:9100",
            "airsonos",
            "airsonos.lan:9100",
            "Speaker.LOCAL.",
            "airsonos.example.com:9100",
        ] {
            assert!(host_allowed(host, &allowed), "{host} should be allowed");
        }
        for host in [
            "rebind.attacker.example:9100",
            "lan.attacker.example",
            "",
            "a b",
        ] {
            assert!(!host_allowed(host, &allowed), "{host} should be rejected");
        }
    }

    /// Anyone who can reach the GUI can edit the draft, so a changed URL must not
    /// receive the stored token.
    #[tokio::test]
    async fn stored_token_stays_with_its_home_assistant_url() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let mut stored = Config::default();
        stored.home_assistant.url = Some("http://ha.local:8123".parse().expect("url"));
        stored.home_assistant.token = Some("stored".to_owned());
        stored.write_to_path(&path).expect("write config");
        let ui = Arc::new(ConfigUi {
            path,
            read_only: false,
            running: stored.clone(),
            started_at_ms: 0,
            status: watch::channel(BridgeStatus::Starting).1,
            restart: CancellationToken::new(),
        });
        let request = |url: &str, token: Option<&str>| {
            let mut config = stored.clone();
            config.home_assistant.url = Some(url.parse().expect("url"));
            config.home_assistant.token = None;
            let secrets = SecretState {
                home_assistant_token: token.map(str::to_owned),
                rtsp_password: None,
            };
            Ok(Json(DraftRequest { config, secrets }))
        };
        let draft = |url: &str, token: Option<&str>| draft_config(&ui, request(url, token));

        let same = draft("http://ha.local:8123", None).await.expect("draft");
        assert_eq!(same.config.home_assistant.token.as_deref(), Some("stored"));
        assert!(!same.new_ha_url_without_token);

        let moved = draft("http://attacker.example", None).await.expect("draft");
        assert_eq!(moved.config.home_assistant.token, None);
        assert!(moved.new_ha_url_without_token);

        let moved = draft("http://ha2.local:8123", Some("new"))
            .await
            .expect("draft");
        assert_eq!(moved.config.home_assistant.token.as_deref(), Some("new"));
        assert!(!moved.new_ha_url_without_token);

        // A saved URL without a token would get `SUPERVISOR_TOKEN` after the restart.
        let saved = save_config(State(ui.clone()), request("http://attacker.example", None)).await;
        assert_eq!(
            saved.err().map(|error| error.0),
            Some(StatusCode::UNPROCESSABLE_ENTITY)
        );
    }
}
