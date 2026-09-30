use std::collections::HashSet;
use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::{AirPlayConfig, PortAllocationError, SessionId, SonosConfig, ZoneId};
use crate::{allocate_rtsp_port, stable_virtual_hwaddr};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SonosZone {
    pub id: ZoneId,
    pub room_name: String,
    pub ip: IpAddr,
    pub model: String,
    pub rincon_id: String,
    pub is_visible_room: bool,
    pub is_group_coordinator: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct VirtualAirPlayEndpoint {
    pub zone_id: ZoneId,
    pub display_name: String,
    pub rtsp_port: u16,
    pub persisted_hwaddr: [u8; 6],
    pub pairing_store_path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct AirPlaySession {
    pub session_id: SessionId,
    pub zone_id: ZoneId,
    pub started_at: Instant,
    pub volume: Option<f32>,
}

#[derive(Debug)]
pub struct PcmFrame {
    /// Present for buffered delivery; returned only after processing or cancellation.
    pub buffered_permit: Option<crate::pcm_queue::PcmPermit>,
    /// FLUSH advances this counter before the next callback is admitted.
    pub playback_epoch: u64,
    pub sample_rate: u32,
    pub channels: u8,
    pub samples_f32_interleaved: Vec<f32>,
    /// Source presentation time mapped to the local monotonic clock, when available.
    pub presentation_time: Option<Instant>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamCodec {
    Mp3,
    Aac,
    Wav,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EncoderState {
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed(String),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StreamSession {
    pub session_id: SessionId,
    pub zone_id: ZoneId,
    pub codec: StreamCodec,
    pub generation: u64,
    pub local_url: Url,
    pub encoder_state: EncoderState,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ZoneSyncConfig {
    pub zone_id: ZoneId,
    pub offset_ms: i64,
}

pub fn filter_zones(zones: &[SonosZone], config: &SonosConfig) -> Vec<SonosZone> {
    let includes = normalized_set(&config.include_rooms);
    let excludes = normalized_set(&config.exclude_rooms);

    zones
        .iter()
        .filter(|zone| zone.is_visible_room)
        .filter(|zone| {
            includes.is_empty() || includes.contains(&zone.room_name.to_ascii_lowercase())
        })
        .filter(|zone| !excludes.contains(&zone.room_name.to_ascii_lowercase()))
        .cloned()
        .collect()
}

pub fn virtual_endpoint_for_zone(
    zone: &SonosZone,
    index: usize,
    airplay: &AirPlayConfig,
    state_dir: impl Into<PathBuf>,
) -> Result<VirtualAirPlayEndpoint, PortAllocationError> {
    let display_name = airplay.name_template.replace("{room}", &zone.room_name);
    let rtsp_port = allocate_rtsp_port(airplay.base_rtsp_port, index)?;
    let pairing_store_path = state_dir
        .into()
        .join("pairings")
        .join(format!("{}.json", zone.id.as_str()));

    Ok(VirtualAirPlayEndpoint {
        zone_id: zone.id.clone(),
        display_name,
        rtsp_port,
        persisted_hwaddr: stable_virtual_hwaddr(&zone.id),
        pairing_store_path,
    })
}

fn normalized_set(values: &[String]) -> HashSet<String> {
    values
        .iter()
        .map(|value| value.to_ascii_lowercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AirPlayConfig, SonosConfig};

    fn zone(room_name: &str, visible: bool) -> SonosZone {
        SonosZone {
            id: ZoneId::new(format!("RINCON_{room_name}")),
            room_name: room_name.to_owned(),
            ip: "192.0.2.10".parse().expect("ip"),
            model: "Sonos One".to_owned(),
            rincon_id: format!("RINCON_{room_name}"),
            is_visible_room: visible,
            is_group_coordinator: true,
        }
    }

    #[test]
    fn filters_zones_by_visibility_include_and_exclude() {
        let zones = vec![
            zone("Kitchen", true),
            zone("Office", true),
            zone("Hidden", false),
        ];
        let config = SonosConfig {
            include_rooms: vec!["Kitchen".to_owned(), "Office".to_owned()],
            exclude_rooms: vec!["Office".to_owned()],
            ..SonosConfig::default()
        };

        let filtered = filter_zones(&zones, &config);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].room_name, "Kitchen");
    }

    #[test]
    fn builds_virtual_endpoint_with_stable_identity() {
        let zone = zone("Kitchen", true);
        let airplay = AirPlayConfig::default();

        let endpoint =
            virtual_endpoint_for_zone(&zone, 2, &airplay, "/tmp/airsonos2").expect("endpoint");

        assert_eq!(endpoint.display_name, "Kitchen AirSonos2");
        assert_eq!(endpoint.rtsp_port, 5002);
        assert!(endpoint.pairing_store_path.ends_with("RINCON_Kitchen.json"));
    }
}
