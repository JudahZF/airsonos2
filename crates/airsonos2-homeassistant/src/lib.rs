//! Home Assistant REST client for `media_player` entities.
//!
//! Music Assistant players are `media_player` entities in Home Assistant, so this client
//! drives them too.

use std::fmt;
use std::time::Duration;

use airsonos2_core::HomeAssistantConfig;
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;
use url::Url;

/// Token the Supervisor gives to Home Assistant apps that set `homeassistant_api: true`.
pub const SUPERVISOR_TOKEN_ENV: &str = "SUPERVISOR_TOKEN";

const MEDIA_PLAYER_PREFIX: &str = "media_player.";
/// `MediaPlayerEntityFeature.PLAY_MEDIA`.
const FEATURE_PLAY_MEDIA: u32 = 512;
/// Home Assistant answers a service call only after the service finishes. Cast and Music
/// Assistant players can take several seconds to start a stream.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq)]
pub struct MediaPlayer {
    pub entity_id: String,
    pub name: String,
    pub state: String,
    pub supported_features: u32,
}

impl MediaPlayer {
    pub fn supports_play_media(&self) -> bool {
        self.supported_features & FEATURE_PLAY_MEDIA != 0
    }
}

fn is_media_player(entity_id: &str) -> bool {
    entity_id
        .strip_prefix(MEDIA_PLAYER_PREFIX)
        .is_some_and(|object_id| !object_id.is_empty())
}

/// Returns the entity id without its domain, such as `kitchen` for `media_player.kitchen`.
pub fn object_id(entity_id: &str) -> &str {
    entity_id
        .split_once('.')
        .map_or(entity_id, |(_, object_id)| object_id)
}

#[derive(Clone)]
pub struct HomeAssistantClient {
    api_url: Url,
    token: String,
    http: reqwest::Client,
}

impl fmt::Debug for HomeAssistantClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HomeAssistantClient")
            .field("api_url", &self.api_url)
            .finish_non_exhaustive()
    }
}

impl HomeAssistantClient {
    pub fn new(base_url: &Url, token: impl Into<String>) -> Result<Self, HomeAssistantError> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()?;
        Ok(Self {
            api_url: api_url(base_url)?,
            token: token.into(),
            http,
        })
    }

    /// Builds a client from config. Without `token`, it uses `SUPERVISOR_TOKEN`, so the
    /// Home Assistant app never writes a token to disk.
    pub fn from_config(config: &HomeAssistantConfig) -> Result<Self, HomeAssistantError> {
        let url = config.url.as_ref().ok_or(HomeAssistantError::MissingUrl)?;
        let token = config
            .token
            .clone()
            .filter(|token| !token.is_empty())
            .or_else(|| std::env::var(SUPERVISOR_TOKEN_ENV).ok())
            .filter(|token| !token.is_empty())
            .ok_or(HomeAssistantError::MissingToken)?;
        Self::new(url, token)
    }

    pub async fn media_players(&self) -> Result<Vec<MediaPlayer>, HomeAssistantError> {
        let body = self.get("states").await?;
        Ok(parse_media_players(&body)?)
    }

    /// Reads `volume_level` (0.0 to 1.0). Home Assistant hides it while a player is off.
    pub async fn volume_level(&self, entity_id: &str) -> Result<f32, HomeAssistantError> {
        let body = self.get(&format!("states/{entity_id}")).await?;
        let state: EntityState<MediaPlayerAttributes> = serde_json::from_str(&body)?;
        state
            .attributes
            .volume_level
            .ok_or_else(|| HomeAssistantError::MissingVolumeLevel {
                entity_id: entity_id.to_owned(),
            })
    }

    pub async fn play_media(&self, entity_id: &str, url: &str) -> Result<(), HomeAssistantError> {
        self.call_media_player(
            "play_media",
            json!({
                "entity_id": entity_id,
                "media_content_id": url,
                "media_content_type": "music",
            }),
        )
        .await
    }

    pub async fn stop(&self, entity_id: &str) -> Result<(), HomeAssistantError> {
        self.call_media_player("media_stop", json!({ "entity_id": entity_id }))
            .await
    }

    pub async fn set_volume(
        &self,
        entity_id: &str,
        volume_level: f32,
    ) -> Result<(), HomeAssistantError> {
        self.call_media_player(
            "volume_set",
            json!({ "entity_id": entity_id, "volume_level": volume_level }),
        )
        .await
    }

    async fn get(&self, path: &str) -> Result<String, HomeAssistantError> {
        let response = self
            .http
            .get(self.api_url.join(path)?)
            .bearer_auth(&self.token)
            .send()
            .await?
            .error_for_status()?;
        Ok(response.text().await?)
    }

    async fn call_media_player(
        &self,
        service: &str,
        data: serde_json::Value,
    ) -> Result<(), HomeAssistantError> {
        self.http
            .post(
                self.api_url
                    .join(&format!("services/media_player/{service}"))?,
            )
            .bearer_auth(&self.token)
            .json(&data)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum HomeAssistantError {
    #[error("home_assistant.url is not set")]
    MissingUrl,
    #[error("no Home Assistant token; set home_assistant.token or SUPERVISOR_TOKEN")]
    MissingToken,
    #[error("failed to build Home Assistant URL: {0}")]
    Url(#[from] url::ParseError),
    #[error("Home Assistant request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("failed to parse Home Assistant response: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{entity_id} does not report a volume level; it may be off")]
    MissingVolumeLevel { entity_id: String },
}

impl HomeAssistantError {
    /// Matches `SonosClientError::is_retryable`: network failures and server-side errors
    /// retry. A bad token, unknown entity, or bad service data does not.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Http(error) => {
                error.is_timeout()
                    || error.is_connect()
                    || error.is_body()
                    || error.status().is_some_and(|status| {
                        status.is_server_error()
                            || status == reqwest::StatusCode::REQUEST_TIMEOUT
                            || status == reqwest::StatusCode::TOO_MANY_REQUESTS
                    })
            }
            _ => false,
        }
    }

    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Http(error) if error.is_timeout())
    }
}

#[derive(Deserialize)]
struct EntityState<A> {
    entity_id: String,
    state: String,
    attributes: A,
}

#[derive(Deserialize)]
struct MediaPlayerAttributes {
    friendly_name: Option<String>,
    volume_level: Option<f32>,
    #[serde(default)]
    supported_features: u32,
}

/// Joins `api/` onto the base URL, keeping a path prefix such as the Supervisor's `/core`.
fn api_url(base_url: &Url) -> Result<Url, url::ParseError> {
    let mut base_url = base_url.clone();
    if !base_url.path().ends_with('/') {
        base_url.set_path(&format!("{}/", base_url.path()));
    }
    base_url.join("api/")
}

/// Parses `GET /api/states`. Attributes of other domains stay untyped, so an odd entity
/// elsewhere cannot break the player list.
fn parse_media_players(body: &str) -> Result<Vec<MediaPlayer>, serde_json::Error> {
    let states: Vec<EntityState<serde_json::Value>> = serde_json::from_str(body)?;
    states
        .into_iter()
        .filter(|state| is_media_player(&state.entity_id))
        .map(|state| {
            let attributes: MediaPlayerAttributes = serde_json::from_value(state.attributes)?;
            let name = attributes
                .friendly_name
                .unwrap_or_else(|| object_id(&state.entity_id).to_owned());
            Ok(MediaPlayer {
                entity_id: state.entity_id,
                name,
                state: state.state,
                supported_features: attributes.supported_features,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_url_keeps_supervisor_path_prefix() {
        let supervisor = Url::parse("http://supervisor/core").expect("url");
        let direct = Url::parse("http://homeassistant.local:8123/").expect("url");

        assert_eq!(
            api_url(&supervisor).expect("api url").as_str(),
            "http://supervisor/core/api/"
        );
        assert_eq!(
            api_url(&direct).expect("api url").as_str(),
            "http://homeassistant.local:8123/api/"
        );
    }

    #[test]
    fn parses_only_media_players_from_states() {
        let body = r#"[
            {"entity_id": "media_player.kitchen", "state": "idle",
             "attributes": {"friendly_name": "Kitchen", "volume_level": 0.4, "supported_features": 152463}},
            {"entity_id": "media_player.tv", "state": "off", "attributes": {}},
            {"entity_id": "sensor.odd", "state": "1", "attributes": {"volume_level": "loud"}}
        ]"#;

        let players = parse_media_players(body).expect("players");

        assert_eq!(players.len(), 2);
        assert_eq!(players[0].name, "Kitchen");
        assert!(players[0].supports_play_media());
        assert_eq!(players[1].name, "tv");
        assert!(!players[1].supports_play_media());
    }
}
