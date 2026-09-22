use std::fs;
use std::net::IpAddr;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config at {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid configuration: {0}")]
    Invalid(String),
    #[error("failed to parse config TOML: {0}")]
    Parse(#[from] toml::de::Error),
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub airplay: AirPlayConfig,
    pub sonos: SonosConfig,
    pub stream: StreamConfig,
    pub diagnostics: DiagnosticsConfig,
    pub sync: SyncConfig,
}

impl Config {
    pub fn from_toml_str(toml: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(toml)?;
        config.validate()?;
        Ok(config)
    }

    /// Shared validation for file configuration and Home Assistant options.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |message: &str| ConfigError::Invalid(message.to_owned());
        if self.server.http_port == 0 || self.airplay.base_rtsp_port == 0 {
            return Err(invalid("HTTP and base RTSP ports must be in 1..=65535"));
        }
        let diagnostics = self
            .diagnostics
            .metrics_addr
            .parse::<std::net::SocketAddr>()
            .map_err(|_| invalid("diagnostics.metrics_addr must be an IP address and port"))?;
        if diagnostics.port() == 0 {
            return Err(invalid(
                "diagnostics.metrics_addr port must be in 1..=65535",
            ));
        }
        if self.server.state_dir.as_os_str().is_empty() {
            return Err(invalid("server.state_dir must not be empty"));
        }
        if self.airplay.name_template.trim().is_empty()
            || self.airplay.advertised_model.trim().is_empty()
        {
            return Err(invalid(
                "AirPlay name_template and advertised_model must not be empty",
            ));
        }
        if !(8_000..=192_000).contains(&self.airplay.output_sample_rate)
            || !(1..=2).contains(&self.airplay.output_channels)
        {
            return Err(invalid(
                "AirPlay output must have 8000..=192000 samples/second and 1..=2 channels",
            ));
        }
        if self.airplay.max_clients_per_zone == 0 {
            return Err(invalid("airplay.max_clients_per_zone must be positive"));
        }
        if !matches!(self.stream.codec.as_str(), "mp3" | "wav") {
            return Err(invalid("stream.codec must be mp3 or wav"));
        }
        if !(32..=320).contains(&self.stream.mp3_bitrate_kbps) {
            return Err(invalid("stream.mp3_bitrate_kbps must be in 32..=320"));
        }
        if !(-10_000..=10_000).contains(&self.sync.default_offset_ms)
            || self
                .sync
                .zone_offsets_ms
                .values()
                .any(|offset| !(-10_000..=10_000).contains(offset))
        {
            return Err(invalid(
                "sync offsets must be in -10000..=10000 milliseconds",
            ));
        }
        if self.sync.startup_sample_limit == 0
            || self.sync.startup_min_samples == 0
            || self.sync.startup_min_samples > self.sync.startup_sample_limit
        {
            return Err(invalid(
                "sync startup sample counts must be positive and min_samples must not exceed sample_limit",
            ));
        }
        Ok(())
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Self::from_toml_str(&contents)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct ServerConfig {
    pub bind: IpAddr,
    pub http_port: u16,
    pub state_dir: PathBuf,
    pub log_level: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0".parse().expect("valid default bind address"),
            http_port: 7000,
            state_dir: PathBuf::from("/var/lib/airsonos2"),
            log_level: "info".to_owned(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct AirPlayConfig {
    pub name_template: String,
    pub advertised_model: String,
    pub pin: String,
    pub rtsp_password: Option<String>,
    pub base_rtsp_port: u16,
    pub output_sample_rate: u32,
    pub output_channels: u8,
    pub max_clients_per_zone: usize,
}

impl Default for AirPlayConfig {
    fn default() -> Self {
        Self {
            name_template: "{room} AirSonos2".to_owned(),
            advertised_model: "AudioAccessory5,1".to_owned(),
            pin: "3939".to_owned(),
            rtsp_password: None,
            base_rtsp_port: 5000,
            output_sample_rate: 48_000,
            output_channels: 2,
            max_clients_per_zone: 10,
        }
    }
}

impl AirPlayConfig {
    pub fn rtsp_password(&self) -> Option<&str> {
        self.rtsp_password
            .as_deref()
            .filter(|password| !password.is_empty())
    }

    pub fn rtsp_password_enabled(&self) -> bool {
        self.rtsp_password().is_some()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct SonosConfig {
    pub auto_discover: bool,
    pub static_ips: Vec<IpAddr>,
    pub include_rooms: Vec<String>,
    pub exclude_rooms: Vec<String>,
    pub force_standalone_on_start: bool,
    pub stop_on_disconnect: bool,
    pub volume_mode: VolumeMode,
}

impl Default for SonosConfig {
    fn default() -> Self {
        Self {
            auto_discover: true,
            static_ips: Vec::new(),
            include_rooms: Vec::new(),
            exclude_rooms: Vec::new(),
            force_standalone_on_start: true,
            stop_on_disconnect: true,
            volume_mode: VolumeMode::Sonos,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumeMode {
    #[default]
    Sonos,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct StreamConfig {
    pub codec: String,
    pub mp3_bitrate_kbps: u16,
    pub prebuffer_ms: u64,
    pub startup_wait_ms: Option<u64>,
    pub http_chunked: bool,
    pub icy_metadata: bool,
    pub ffmpeg_path: PathBuf,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            codec: "mp3".to_owned(),
            mp3_bitrate_kbps: 320,
            prebuffer_ms: 500,
            startup_wait_ms: None,
            http_chunked: true,
            icy_metadata: true,
            ffmpeg_path: PathBuf::from("ffmpeg"),
        }
    }
}

impl StreamConfig {
    pub fn startup_wait_ms(&self) -> u64 {
        self.startup_wait_ms
            .unwrap_or_else(|| self.prebuffer_ms.min(250))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct DiagnosticsConfig {
    pub metrics_addr: String,
}

impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self {
            metrics_addr: "0.0.0.0:9100".to_owned(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct SyncConfig {
    pub default_offset_ms: i64,
    pub multi_select_window_ms: u64,
    pub start_deadline_ms: u64,
    pub startup_compensation: bool,
    pub startup_sample_limit: usize,
    pub startup_min_samples: usize,
    pub startup_max_compensation_ms: u64,
    pub play_command_spread_warn_ms: u64,
    pub zone_offsets_ms: std::collections::BTreeMap<String, i64>,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            default_offset_ms: 0,
            multi_select_window_ms: 750,
            start_deadline_ms: 2_500,
            startup_compensation: false,
            startup_sample_limit: 20,
            startup_min_samples: 3,
            startup_max_compensation_ms: 1_000,
            play_command_spread_warn_ms: 80,
            zone_offsets_ms: std::collections::BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_offsets_that_can_overflow_runtime_deadlines() {
        for value in [i64::MIN, -10_001, 10_001, i64::MAX] {
            assert!(
                Config::from_toml_str(&format!("[sync]\ndefault_offset_ms = {value}")).is_err()
            );
            assert!(
                Config::from_toml_str(&format!("[sync.zone_offsets_ms]\nKitchen = {value}"))
                    .is_err()
            );
        }
        assert!(
            Config::from_toml_str(
                "[sync]\ndefault_offset_ms = -10000\n[sync.zone_offsets_ms]\nKitchen = 10000"
            )
            .is_ok()
        );
    }

    #[test]
    fn config_defaults_match_public_interface() {
        let config = Config::default();

        assert_eq!(config.server.http_port, 7000);
        assert_eq!(config.airplay.pin, "3939");
        assert_eq!(config.airplay.advertised_model, "AudioAccessory5,1");
        assert_eq!(config.airplay.rtsp_password, None);
        assert_eq!(config.airplay.max_clients_per_zone, 10);
        assert_eq!(config.stream.codec, "mp3");
        assert_eq!(config.stream.mp3_bitrate_kbps, 320);
        assert_eq!(config.stream.startup_wait_ms(), 250);
        assert!(config.sonos.auto_discover);
        assert!(config.sonos.static_ips.is_empty());
        assert_eq!(config.diagnostics.metrics_addr, "0.0.0.0:9100");
        assert_eq!(config.sync.default_offset_ms, 0);
        assert_eq!(config.sync.multi_select_window_ms, 750);
        assert_eq!(config.sync.start_deadline_ms, 2_500);
        assert!(!config.sync.startup_compensation);
        assert_eq!(config.sync.startup_sample_limit, 20);
        assert_eq!(config.sync.startup_min_samples, 3);
        assert_eq!(config.sync.startup_max_compensation_ms, 1_000);
        assert_eq!(config.sync.play_command_spread_warn_ms, 80);
    }

    #[test]
    fn sonos_static_ips_can_be_configured() {
        let config = Config::from_toml_str(
            r#"
            [sonos]
            auto_discover = false
            static_ips = ["192.0.2.10", "2001:db8::10"]
            "#,
        )
        .expect("valid config");

        assert!(!config.sonos.auto_discover);
        assert_eq!(
            config.sonos.static_ips,
            vec![
                "192.0.2.10".parse::<IpAddr>().expect("ipv4"),
                "2001:db8::10".parse::<IpAddr>().expect("ipv6"),
            ]
        );
    }

    #[test]
    fn airplay_rtsp_password_can_be_configured() {
        let config = Config::from_toml_str(
            r#"
            [airplay]
            rtsp_password = "secret"
            "#,
        )
        .expect("valid config");

        assert_eq!(config.airplay.rtsp_password.as_deref(), Some("secret"));
        assert_eq!(config.airplay.rtsp_password(), Some("secret"));
        assert!(config.airplay.rtsp_password_enabled());
    }

    #[test]
    fn missing_or_empty_airplay_rtsp_password_is_disabled() {
        let missing = Config::from_toml_str("[airplay]\n").expect("valid config");
        let empty =
            Config::from_toml_str("[airplay]\nrtsp_password = \"\"\n").expect("valid config");

        assert_eq!(missing.airplay.rtsp_password, None);
        assert_eq!(missing.airplay.rtsp_password(), None);
        assert!(!missing.airplay.rtsp_password_enabled());
        assert_eq!(empty.airplay.rtsp_password.as_deref(), Some(""));
        assert_eq!(empty.airplay.rtsp_password(), None);
        assert!(!empty.airplay.rtsp_password_enabled());
    }

    #[test]
    fn partial_config_is_merged_with_defaults() {
        let config = Config::from_toml_str(
            r#"
            [server]
            http_port = 8001

            [sync.zone_offsets_ms]
            Kitchen = 120
            "#,
        )
        .expect("valid config");

        assert_eq!(config.server.http_port, 8001);
        assert_eq!(config.server.bind.to_string(), "0.0.0.0");
        assert_eq!(config.sync.zone_offsets_ms["Kitchen"], 120);
        assert_eq!(config.airplay.output_channels, 2);
    }

    #[test]
    fn sync_config_can_be_configured() {
        let config = Config::from_toml_str(
            r#"
            [sync]
            default_offset_ms = 10
            multi_select_window_ms = 600
            start_deadline_ms = 1800
            startup_compensation = false
            startup_sample_limit = 12
            startup_min_samples = 4
            startup_max_compensation_ms = 700
            play_command_spread_warn_ms = 40

            [sync.zone_offsets_ms]
            Kitchen = 120
            Office = 80
            "#,
        )
        .expect("valid config");

        assert_eq!(config.sync.default_offset_ms, 10);
        assert_eq!(config.sync.multi_select_window_ms, 600);
        assert_eq!(config.sync.start_deadline_ms, 1800);
        assert!(!config.sync.startup_compensation);
        assert_eq!(config.sync.startup_sample_limit, 12);
        assert_eq!(config.sync.startup_min_samples, 4);
        assert_eq!(config.sync.startup_max_compensation_ms, 700);
        assert_eq!(config.sync.play_command_spread_warn_ms, 40);
        assert_eq!(config.sync.zone_offsets_ms["Kitchen"], 120);
        assert_eq!(config.sync.zone_offsets_ms["Office"], 80);
    }

    #[test]
    fn config_can_load_from_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        fs::write(&path, "[stream]\nmp3_bitrate_kbps = 192\n").expect("write config");

        let config = Config::from_path(&path).expect("config loads");

        assert_eq!(config.stream.mp3_bitrate_kbps, 192);
    }
}
