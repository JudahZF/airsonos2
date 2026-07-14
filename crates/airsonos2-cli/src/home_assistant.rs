use std::collections::BTreeMap;
use std::fs;
use std::net::IpAddr;
use std::path::{Path, PathBuf};

use airsonos2_core::Config;
use serde::Deserialize;

const HA_BIND: &str = "0.0.0.0";
const HA_HTTP_PORT: u16 = 7000;
const HA_STATE_DIR: &str = "/data";
const HA_FFMPEG_PATH: &str = "/usr/bin/ffmpeg";
const HA_METRICS_ADDR: &str = "0.0.0.0:9100";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub(crate) struct HomeAssistantOptions {
    pub(crate) log_level: String,
    pub(crate) run_doctor_on_start: bool,
    pub(crate) advertise_addr: Option<String>,
    pub(crate) name_template: String,
    pub(crate) advertised_model: String,
    pub(crate) pin: String,
    pub(crate) rtsp_password: String,
    pub(crate) base_rtsp_port: u16,
    pub(crate) output_sample_rate: u32,
    pub(crate) output_channels: u8,
    pub(crate) max_clients_per_zone: usize,
    pub(crate) auto_discover: bool,
    pub(crate) static_ips: Vec<String>,
    pub(crate) include_rooms: Vec<String>,
    pub(crate) exclude_rooms: Vec<String>,
    pub(crate) force_standalone_on_start: bool,
    pub(crate) stop_on_disconnect: bool,
    pub(crate) stream_codec: String,
    pub(crate) mp3_bitrate_kbps: u16,
    pub(crate) prebuffer_ms: u64,
    pub(crate) startup_wait_ms: Option<u64>,
    pub(crate) default_offset_ms: i64,
    pub(crate) multi_select_window_ms: u64,
    pub(crate) start_deadline_ms: u64,
    pub(crate) startup_compensation: bool,
    pub(crate) startup_sample_limit: usize,
    pub(crate) startup_min_samples: usize,
    pub(crate) startup_max_compensation_ms: u64,
    pub(crate) play_command_spread_warn_ms: u64,
    pub(crate) zone_offsets: Vec<HomeAssistantZoneOffset>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct HomeAssistantZoneOffset {
    pub(crate) zone: String,
    pub(crate) offset_ms: i64,
}

impl Default for HomeAssistantOptions {
    fn default() -> Self {
        let config = Config::default();
        Self {
            log_level: config.server.log_level,
            run_doctor_on_start: false,
            advertise_addr: config.server.advertise_addr.map(|ip| ip.to_string()),
            name_template: config.airplay.name_template,
            advertised_model: config.airplay.advertised_model,
            pin: config.airplay.pin,
            rtsp_password: config.airplay.rtsp_password.unwrap_or_default(),
            base_rtsp_port: config.airplay.base_rtsp_port,
            output_sample_rate: config.airplay.output_sample_rate,
            output_channels: config.airplay.output_channels,
            max_clients_per_zone: config.airplay.max_clients_per_zone,
            auto_discover: config.sonos.auto_discover,
            static_ips: config
                .sonos
                .static_ips
                .into_iter()
                .map(|ip| ip.to_string())
                .collect(),
            include_rooms: config.sonos.include_rooms,
            exclude_rooms: config.sonos.exclude_rooms,
            force_standalone_on_start: config.sonos.force_standalone_on_start,
            stop_on_disconnect: config.sonos.stop_on_disconnect,
            stream_codec: config.stream.codec,
            mp3_bitrate_kbps: config.stream.mp3_bitrate_kbps,
            prebuffer_ms: config.stream.prebuffer_ms,
            startup_wait_ms: config.stream.startup_wait_ms,
            default_offset_ms: config.sync.default_offset_ms,
            multi_select_window_ms: config.sync.multi_select_window_ms,
            start_deadline_ms: config.sync.start_deadline_ms,
            startup_compensation: config.sync.startup_compensation,
            startup_sample_limit: config.sync.startup_sample_limit,
            startup_min_samples: config.sync.startup_min_samples,
            startup_max_compensation_ms: config.sync.startup_max_compensation_ms,
            play_command_spread_warn_ms: config.sync.play_command_spread_warn_ms,
            zone_offsets: config
                .sync
                .zone_offsets_ms
                .into_iter()
                .map(|(zone, offset_ms)| HomeAssistantZoneOffset { zone, offset_ms })
                .collect(),
        }
    }
}

impl HomeAssistantOptions {
    pub(crate) fn from_json_str(json: &str) -> anyhow::Result<Self> {
        Ok(serde_json::from_str(json)?)
    }

    pub(crate) fn to_config(&self) -> anyhow::Result<Config> {
        let mut config = Config::default();

        config.server.bind = HA_BIND.parse()?;
        config.server.advertise_addr = parse_optional_ip(self.advertise_addr.as_deref())?;
        config.server.http_port = HA_HTTP_PORT;
        config.server.state_dir = PathBuf::from(HA_STATE_DIR);
        config.server.log_level = self.log_level.clone();

        config.airplay.name_template = self.name_template.clone();
        config.airplay.advertised_model = self.advertised_model.clone();
        config.airplay.pin = self.pin.clone();
        config.airplay.rtsp_password = non_empty_password(&self.rtsp_password);
        config.airplay.base_rtsp_port = self.base_rtsp_port;
        config.airplay.output_sample_rate = self.output_sample_rate;
        config.airplay.output_channels = self.output_channels;
        config.airplay.max_clients_per_zone = self.max_clients_per_zone;

        config.sonos.auto_discover = self.auto_discover;
        config.sonos.static_ips = parse_static_ips(&self.static_ips)?;
        config.sonos.include_rooms = self.include_rooms.clone();
        config.sonos.exclude_rooms = self.exclude_rooms.clone();
        config.sonos.force_standalone_on_start = self.force_standalone_on_start;
        config.sonos.stop_on_disconnect = self.stop_on_disconnect;

        config.stream.codec = validate_stream_codec(&self.stream_codec)?.to_owned();
        config.stream.mp3_bitrate_kbps = self.mp3_bitrate_kbps;
        config.stream.prebuffer_ms = self.prebuffer_ms;
        config.stream.startup_wait_ms = self.startup_wait_ms;
        config.stream.ffmpeg_path = PathBuf::from(HA_FFMPEG_PATH);

        config.diagnostics.metrics_addr = HA_METRICS_ADDR.to_owned();

        config.sync.default_offset_ms = self.default_offset_ms;
        config.sync.multi_select_window_ms = self.multi_select_window_ms;
        config.sync.start_deadline_ms = self.start_deadline_ms;
        config.sync.startup_compensation = self.startup_compensation;
        config.sync.startup_sample_limit = self.startup_sample_limit;
        config.sync.startup_min_samples = self.startup_min_samples;
        config.sync.startup_max_compensation_ms = self.startup_max_compensation_ms;
        config.sync.play_command_spread_warn_ms = self.play_command_spread_warn_ms;
        config.sync.zone_offsets_ms = zone_offsets_map(&self.zone_offsets)?;

        Ok(config)
    }
}

pub(crate) fn render_config_file(options_path: &Path, output_path: &Path) -> anyhow::Result<()> {
    let options_json = fs::read_to_string(options_path)?;
    let options = HomeAssistantOptions::from_json_str(&options_json)?;
    let toml = render_config_toml(&options)?;

    if let Some(parent) = output_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    fs::write(output_path, toml)?;

    Ok(())
}

fn render_config_toml(options: &HomeAssistantOptions) -> anyhow::Result<String> {
    let config = options.to_config()?;
    Ok(toml::to_string_pretty(&config)?)
}

fn non_empty_password(password: &str) -> Option<String> {
    if password.is_empty() {
        None
    } else {
        Some(password.to_owned())
    }
}

fn parse_optional_ip(value: Option<&str>) -> anyhow::Result<Option<IpAddr>> {
    value
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<IpAddr>()
                .map_err(|error| anyhow::anyhow!("invalid advertise_addr {value:?}: {error}"))
        })
        .transpose()
}

fn parse_static_ips(values: &[String]) -> anyhow::Result<Vec<IpAddr>> {
    values
        .iter()
        .map(|value| {
            value
                .parse::<IpAddr>()
                .map_err(|error| anyhow::anyhow!("invalid static IP address {value:?}: {error}"))
        })
        .collect()
}

fn validate_stream_codec(codec: &str) -> anyhow::Result<&str> {
    match codec {
        "mp3" | "wav" => Ok(codec),
        other => anyhow::bail!("unsupported stream_codec {other:?}; expected mp3 or wav"),
    }
}

fn zone_offsets_map(values: &[HomeAssistantZoneOffset]) -> anyhow::Result<BTreeMap<String, i64>> {
    let mut offsets = BTreeMap::new();
    for value in values {
        if value.zone.is_empty() {
            anyhow::bail!("zone_offsets entries must include a non-empty zone");
        }
        offsets.insert(value.zone.clone(), value.offset_ms);
    }
    Ok(offsets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_config_defaults_except_home_assistant_runtime_paths() {
        let config = HomeAssistantOptions::default()
            .to_config()
            .expect("default options render");
        let defaults = Config::default();

        assert_eq!(config.server.bind, defaults.server.bind);
        assert_eq!(config.server.http_port, defaults.server.http_port);
        assert_eq!(config.server.log_level, defaults.server.log_level);
        assert_eq!(config.server.advertise_addr, defaults.server.advertise_addr);
        assert_eq!(config.server.state_dir, PathBuf::from(HA_STATE_DIR));
        assert_eq!(config.airplay, defaults.airplay);
        assert_eq!(config.sonos, defaults.sonos);
        assert_eq!(config.stream.codec, defaults.stream.codec);
        assert_eq!(
            config.stream.mp3_bitrate_kbps,
            defaults.stream.mp3_bitrate_kbps
        );
        assert_eq!(config.stream.prebuffer_ms, defaults.stream.prebuffer_ms);
        assert_eq!(
            config.stream.startup_wait_ms,
            defaults.stream.startup_wait_ms
        );
        assert_eq!(config.stream.http_chunked, defaults.stream.http_chunked);
        assert_eq!(config.stream.icy_metadata, defaults.stream.icy_metadata);
        assert_eq!(config.stream.ffmpeg_path, PathBuf::from(HA_FFMPEG_PATH));
        assert_eq!(config.diagnostics.metrics_addr, HA_METRICS_ADDR);
        assert_eq!(config.sync, defaults.sync);
    }

    #[test]
    fn empty_rtsp_password_becomes_none() {
        let options = HomeAssistantOptions {
            rtsp_password: String::new(),
            ..HomeAssistantOptions::default()
        };

        let config = options.to_config().expect("config");

        assert_eq!(config.airplay.rtsp_password, None);
    }

    #[test]
    fn advertise_addr_is_preserved_in_generated_toml() {
        let options = HomeAssistantOptions::from_json_str(r#"{"advertise_addr":"2001:db8::20"}"#)
            .expect("options");

        let toml = render_config_toml(&options).expect("render");
        let config = Config::from_toml_str(&toml).expect("generated config");

        assert!(toml.contains("advertise_addr = \"2001:db8::20\""));
        assert_eq!(
            config.server.advertise_addr,
            Some("2001:db8::20".parse::<IpAddr>().expect("ipv6"))
        );
    }

    #[test]
    fn advertise_addr_rejects_invalid_values() {
        let options = HomeAssistantOptions {
            advertise_addr: Some("not an ip".to_owned()),
            ..HomeAssistantOptions::default()
        };

        assert!(options.to_config().is_err());
    }

    #[test]
    fn static_ips_parse_ipv4_and_ipv6() {
        let options = HomeAssistantOptions {
            static_ips: vec!["192.0.2.10".to_owned(), "2001:db8::10".to_owned()],
            ..HomeAssistantOptions::default()
        };

        let config = options.to_config().expect("config");

        assert_eq!(
            config.sonos.static_ips,
            vec![
                "192.0.2.10".parse::<IpAddr>().expect("ipv4"),
                "2001:db8::10".parse::<IpAddr>().expect("ipv6"),
            ]
        );
    }

    #[test]
    fn static_ips_reject_invalid_values() {
        let options = HomeAssistantOptions {
            static_ips: vec!["not an ip".to_owned()],
            ..HomeAssistantOptions::default()
        };

        assert!(options.to_config().is_err());
    }

    #[test]
    fn stream_codec_accepts_mp3_and_wav_only() {
        for stream_codec in ["mp3", "wav"] {
            let options = HomeAssistantOptions {
                stream_codec: stream_codec.to_owned(),
                ..HomeAssistantOptions::default()
            };
            assert_eq!(
                options.to_config().expect("config").stream.codec,
                stream_codec
            );
        }

        let options = HomeAssistantOptions {
            stream_codec: "aac".to_owned(),
            ..HomeAssistantOptions::default()
        };
        assert!(options.to_config().is_err());
    }

    #[test]
    fn zone_offsets_become_sync_zone_offsets_ms() {
        let options = HomeAssistantOptions {
            zone_offsets: vec![
                HomeAssistantZoneOffset {
                    zone: "Kitchen".to_owned(),
                    offset_ms: 120,
                },
                HomeAssistantZoneOffset {
                    zone: "Office".to_owned(),
                    offset_ms: -40,
                },
            ],
            ..HomeAssistantOptions::default()
        };

        let config = options.to_config().expect("config");

        assert_eq!(config.sync.zone_offsets_ms["Kitchen"], 120);
        assert_eq!(config.sync.zone_offsets_ms["Office"], -40);
    }

    #[test]
    fn generated_toml_round_trips_through_config() {
        let options = HomeAssistantOptions {
            static_ips: vec!["192.0.2.10".to_owned()],
            stream_codec: "wav".to_owned(),
            zone_offsets: vec![HomeAssistantZoneOffset {
                zone: "Kitchen".to_owned(),
                offset_ms: 80,
            }],
            ..HomeAssistantOptions::default()
        };

        let toml = render_config_toml(&options).expect("render");
        let config = Config::from_toml_str(&toml).expect("round trip");

        assert_eq!(config.sonos.static_ips[0].to_string(), "192.0.2.10");
        assert_eq!(config.stream.codec, "wav");
        assert_eq!(config.sync.zone_offsets_ms["Kitchen"], 80);
    }
}
