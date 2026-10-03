use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use airsonos2_core::{Config, HomeAssistantConfig, SonosZone, allocate_rtsp_port};
use airsonos2_homeassistant::HomeAssistantClient;
use airsonos2_sonos::discover_sonos_zones_from_sources;
use serde::Serialize;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::time;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DoctorReport {
    pub checks: Vec<DoctorCheck>,
}

impl DoctorReport {
    pub fn ok(&self) -> bool {
        self.checks
            .iter()
            .all(|check| matches!(check.status, CheckStatus::Pass | CheckStatus::Warn))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DoctorCheck {
    pub name: String,
    pub status: CheckStatus,
    pub detail: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckStatus {
    Pass,
    Warn,
    Fail,
}

#[derive(Debug, Error)]
pub enum DoctorError {
    #[error("failed to run doctor: {0}")]
    Internal(String),
}

pub async fn run_doctor(config: &Config) -> Result<DoctorReport, DoctorError> {
    let mut checks = Vec::new();

    checks.push(check_ffmpeg(&config.stream.ffmpeg_path).await);
    checks.push(
        check_tcp_port(
            "HTTP stream port",
            config.server.bind,
            config.server.http_port,
        )
        .await,
    );

    for index in 0..6 {
        match allocate_rtsp_port(config.airplay.base_rtsp_port, index) {
            Ok(port) => {
                checks.push(
                    check_tcp_port("virtual AirPlay RTSP port", config.server.bind, port).await,
                );
            }
            Err(error) => checks.push(DoctorCheck {
                name: "virtual AirPlay RTSP port".to_owned(),
                status: CheckStatus::Fail,
                detail: error.to_string(),
            }),
        }
    }

    match discover_sonos_zones_from_sources(
        Duration::from_secs(2),
        &config.sonos.static_ips,
        config.sonos.auto_discover,
    )
    .await
    {
        Ok(zones) if zones.is_empty() => checks.push(DoctorCheck {
            name: "Sonos discovery".to_owned(),
            status: CheckStatus::Warn,
            detail: "no Sonos zones responded within 2 seconds".to_owned(),
        }),
        Ok(zones) => {
            checks.push(DoctorCheck {
                name: "Sonos discovery".to_owned(),
                status: CheckStatus::Pass,
                detail: format!("found {} Sonos zone(s)", zones.len()),
            });
            checks.extend(check_sonos_reachability(&zones).await);
        }
        Err(error) => checks.push(DoctorCheck {
            name: "Sonos discovery".to_owned(),
            status: CheckStatus::Fail,
            detail: error.to_string(),
        }),
    }

    if !config.home_assistant.media_players.is_empty() {
        checks.extend(check_home_assistant(&config.home_assistant).await);
    }

    checks.push(DoctorCheck {
        name: "mDNS AirPlay visibility".to_owned(),
        status: CheckStatus::Warn,
        detail:
            "requires a second host or iOS device to verify _airplay._tcp and _raop._tcp visibility"
                .to_owned(),
    });
    checks.push(DoctorCheck {
        name: "AirPlay timing UDP reachability".to_owned(),
        status: CheckStatus::Warn,
        detail: "receiver stack does not expose a local-only deterministic timing probe".to_owned(),
    });

    Ok(DoctorReport { checks })
}

async fn check_ffmpeg(path: &std::path::Path) -> DoctorCheck {
    let result = Command::new(path).arg("-version").output().await;

    match result {
        Ok(output) if output.status.success() => DoctorCheck {
            name: "ffmpeg encoder".to_owned(),
            status: CheckStatus::Pass,
            detail: String::from_utf8_lossy(&output.stdout)
                .lines()
                .next()
                .unwrap_or("ffmpeg responded")
                .to_owned(),
        },
        Ok(output) => DoctorCheck {
            name: "ffmpeg encoder".to_owned(),
            status: CheckStatus::Fail,
            detail: format!("ffmpeg exited with status {}", output.status),
        },
        Err(error) => DoctorCheck {
            name: "ffmpeg encoder".to_owned(),
            status: CheckStatus::Fail,
            detail: error.to_string(),
        },
    }
}

async fn check_tcp_port(name: &str, bind: IpAddr, port: u16) -> DoctorCheck {
    let addr = SocketAddr::new(bind, port);

    match TcpListener::bind(addr).await {
        Ok(listener) => {
            drop(listener);
            DoctorCheck {
                name: name.to_owned(),
                status: CheckStatus::Pass,
                detail: format!("{addr} is available"),
            }
        }
        Err(error) => DoctorCheck {
            name: name.to_owned(),
            status: CheckStatus::Fail,
            detail: format!("{addr} is not available: {error}"),
        },
    }
}

async fn check_sonos_reachability(zones: &[SonosZone]) -> Vec<DoctorCheck> {
    let mut checks = Vec::new();

    for zone in zones {
        let addr = SocketAddr::new(zone.ip, 1400);
        let status = time::timeout(Duration::from_secs(1), TcpStream::connect(addr)).await;
        match status {
            Ok(Ok(_)) => checks.push(DoctorCheck {
                name: format!("Sonos SOAP reachability: {}", zone.room_name),
                status: CheckStatus::Pass,
                detail: format!("{addr} accepted TCP connection"),
            }),
            Ok(Err(error)) => checks.push(DoctorCheck {
                name: format!("Sonos SOAP reachability: {}", zone.room_name),
                status: CheckStatus::Fail,
                detail: error.to_string(),
            }),
            Err(_) => checks.push(DoctorCheck {
                name: format!("Sonos SOAP reachability: {}", zone.room_name),
                status: CheckStatus::Fail,
                detail: "timed out connecting to port 1400".to_owned(),
            }),
        }
    }

    checks
}

async fn check_home_assistant(config: &HomeAssistantConfig) -> Vec<DoctorCheck> {
    let api_check = |status, detail| DoctorCheck {
        name: "Home Assistant API".to_owned(),
        status,
        detail,
    };
    let players = match HomeAssistantClient::from_config(config) {
        Ok(client) => client.media_players().await,
        Err(error) => Err(error),
    };
    let players = match players {
        Ok(players) => players,
        Err(error) => return vec![api_check(CheckStatus::Fail, error.to_string())],
    };

    let mut checks = vec![api_check(
        CheckStatus::Pass,
        format!("found {} media player(s)", players.len()),
    )];
    for entity_id in &config.media_players {
        let player = players.iter().find(|player| &player.entity_id == entity_id);
        let (status, detail) = match player {
            Some(player) if player.supports_play_media() => (
                CheckStatus::Pass,
                format!("{} is {}", player.name, player.state),
            ),
            Some(player) => (
                CheckStatus::Fail,
                format!("{} does not support play_media", player.name),
            ),
            None => (
                CheckStatus::Fail,
                "entity not found in Home Assistant".to_owned(),
            ),
        };
        checks.push(DoctorCheck {
            name: format!("Home Assistant player: {entity_id}"),
            status,
            detail,
        });
    }

    checks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doctor_report_treats_warnings_as_nonfatal() {
        let report = DoctorReport {
            checks: vec![
                DoctorCheck {
                    name: "pass".to_owned(),
                    status: CheckStatus::Pass,
                    detail: String::new(),
                },
                DoctorCheck {
                    name: "warn".to_owned(),
                    status: CheckStatus::Warn,
                    detail: String::new(),
                },
            ],
        };

        assert!(report.ok());
    }

    #[test]
    fn doctor_report_fails_when_any_check_fails() {
        let report = DoctorReport {
            checks: vec![DoctorCheck {
                name: "fail".to_owned(),
                status: CheckStatus::Fail,
                detail: String::new(),
            }],
        };

        assert!(!report.ok());
    }
}
