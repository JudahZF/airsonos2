use std::net::IpAddr;

use airsonos2_core::{SonosZone, ZoneId};
use airsonos2_homeassistant::{HomeAssistantClient, HomeAssistantError};
use airsonos2_sonos::{SonosClient, SonosClientError};
use thiserror::Error;
use url::Url;

/// A device that plays the HTTP stream of one virtual AirPlay endpoint.
#[derive(Clone, Debug)]
pub(crate) enum Renderer {
    Sonos {
        zone: SonosZone,
        client: SonosClient,
    },
    /// A Home Assistant `media_player` entity, including Music Assistant players. `id` is
    /// the entity id.
    HomeAssistant {
        id: ZoneId,
        name: String,
        client: HomeAssistantClient,
    },
}

impl Renderer {
    pub(crate) fn id(&self) -> &ZoneId {
        match self {
            Self::Sonos { zone, .. } => &zone.id,
            Self::HomeAssistant { id, .. } => id,
        }
    }

    pub(crate) fn name(&self) -> &str {
        match self {
            Self::Sonos { zone, .. } => &zone.room_name,
            Self::HomeAssistant { name, .. } => name,
        }
    }

    /// The renderer's own IP, when known. Home Assistant does not expose one.
    pub(crate) fn ip(&self) -> Option<IpAddr> {
        match self {
            Self::Sonos { zone, .. } => Some(zone.ip),
            Self::HomeAssistant { .. } => None,
        }
    }

    /// Starts playback. Sonos plays the URI loaded during prepare; Home Assistant has no
    /// load step, so it receives `url` here.
    pub(crate) async fn play(&self, url: &Url) -> Result<(), RendererError> {
        match self {
            Self::Sonos { client, .. } => client.play().await?,
            Self::HomeAssistant { id, client, .. } => {
                client.play_media(id.as_str(), url.as_str()).await?;
            }
        }
        Ok(())
    }

    /// Leaves a native Sonos group, which also stops this player. Home Assistant
    /// players never join one, so they only stop.
    pub(crate) async fn leave_group(&self) -> Result<(), RendererError> {
        match self {
            Self::Sonos { client, .. } => client.become_coordinator_of_standalone_group().await?,
            Self::HomeAssistant { .. } => self.stop().await?,
        }
        Ok(())
    }

    pub(crate) async fn stop(&self) -> Result<(), RendererError> {
        match self {
            Self::Sonos { client, .. } => client.stop().await?,
            Self::HomeAssistant { id, client, .. } => client.stop(id.as_str()).await?,
        }
        Ok(())
    }

    /// Sets volume on the 0-100 scale.
    pub(crate) async fn set_volume(&self, percent: u8) -> Result<(), RendererError> {
        match self {
            Self::Sonos { client, .. } => client.set_volume(percent).await?,
            Self::HomeAssistant { id, client, .. } => {
                client
                    .set_volume(id.as_str(), f32::from(percent.min(100)) / 100.0)
                    .await?;
            }
        }
        Ok(())
    }

    /// Reads volume on the 0-100 scale.
    pub(crate) async fn volume(&self) -> Result<u8, RendererError> {
        match self {
            Self::Sonos { client, .. } => Ok(client.get_volume().await?),
            Self::HomeAssistant { id, client, .. } => {
                let level = client.volume_level(id.as_str()).await?;
                Ok((level.clamp(0.0, 1.0) * 100.0).round() as u8)
            }
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum RendererError {
    #[error(transparent)]
    Sonos(#[from] SonosClientError),
    #[error(transparent)]
    HomeAssistant(#[from] HomeAssistantError),
}

impl RendererError {
    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            Self::Sonos(error) => error.is_retryable(),
            Self::HomeAssistant(error) => error.is_retryable(),
        }
    }

    pub(crate) fn is_timeout(&self) -> bool {
        match self {
            Self::Sonos(error) => error.is_timeout(),
            Self::HomeAssistant(error) => error.is_timeout(),
        }
    }
}
