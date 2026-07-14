use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use airsonos2_core::{
    Config, PortAllocationError, SonosZone, filter_zones, virtual_endpoint_for_zone,
};
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

    match discover_sonos_zones_from_sources(
        Duration::from_secs(2),
        &config.sonos.static_ips,
        config.sonos.auto_discover,
    )
    .await
    {
        Ok(zones) if zones.is_empty() => {
            checks.push(DoctorCheck {
                name: "Sonos discovery".to_owned(),
                status: CheckStatus::Warn,
                detail: "no Sonos zones responded within 2 seconds".to_owned(),
            });
            checks.push(rtsp_ports_skipped_check("no Sonos zones discovered"));
        }
        Ok(zones) => {
            checks.push(DoctorCheck {
                name: "Sonos discovery".to_owned(),
                status: CheckStatus::Pass,
                detail: format!("found {} Sonos zone(s)", zones.len()),
            });
            checks.extend(check_rtsp_ports(&zones, config).await);
            checks.extend(check_sonos_reachability(&zones).await);
        }
        Err(error) => {
            checks.push(DoctorCheck {
                name: "Sonos discovery".to_owned(),
                status: CheckStatus::Fail,
                detail: error.to_string(),
            });
            checks.push(rtsp_ports_skipped_check("Sonos discovery failed"));
        }
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

fn rtsp_ports_skipped_check(reason: &str) -> DoctorCheck {
    DoctorCheck {
        name: "virtual AirPlay RTSP ports".to_owned(),
        status: CheckStatus::Warn,
        detail: format!("skipped: {reason}"),
    }
}

/// Derives the exact RTSP port each discovered zone would receive, matching the
/// endpoint allocation used by `serve`.
fn zone_rtsp_ports(
    zones: &[SonosZone],
    config: &Config,
) -> Vec<(String, Result<u16, PortAllocationError>)> {
    filter_zones(zones, &config.sonos)
        .iter()
        .enumerate()
        .map(|(index, zone)| {
            let port = virtual_endpoint_for_zone(
                zone,
                index,
                &config.airplay,
                config.server.state_dir.clone(),
            )
            .map(|endpoint| endpoint.rtsp_port);
            (zone.room_name.clone(), port)
        })
        .collect()
}

async fn check_rtsp_ports(zones: &[SonosZone], config: &Config) -> Vec<DoctorCheck> {
    let ports = zone_rtsp_ports(zones, config);
    if ports.is_empty() {
        return vec![rtsp_ports_skipped_check(
            "no visible Sonos rooms matched the current configuration",
        )];
    }

    let mut checks = Vec::new();
    for (room_name, port) in ports {
        let name = format!("virtual AirPlay RTSP port: {room_name}");
        match port {
            Ok(port) => checks.push(check_tcp_port(&name, config.server.bind, port).await),
            Err(error) => checks.push(DoctorCheck {
                name,
                status: CheckStatus::Fail,
                detail: error.to_string(),
            }),
        }
    }

    checks
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

#[cfg(test)]
mod tests {
    use super::*;
    use airsonos2_core::{AirPlayConfig, SonosConfig, ZoneId};

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
    fn derives_exact_rtsp_ports_for_filtered_zones() {
        let zones = vec![
            zone("Kitchen", true),
            zone("Hidden", false),
            zone("Office", true),
            zone("Bedroom", true),
        ];
        let config = Config {
            sonos: SonosConfig {
                exclude_rooms: vec!["Office".to_owned()],
                ..SonosConfig::default()
            },
            ..Config::default()
        };

        let ports = zone_rtsp_ports(&zones, &config);

        let base = config.airplay.base_rtsp_port;
        assert_eq!(ports.len(), 2);
        assert_eq!(ports[0].0, "Kitchen");
        assert_eq!(*ports[0].1.as_ref().expect("port"), base);
        assert_eq!(ports[1].0, "Bedroom");
        assert_eq!(*ports[1].1.as_ref().expect("port"), base + 1);
    }

    #[test]
    fn rtsp_port_overflow_is_reported_per_zone() {
        let zones = vec![zone("Kitchen", true), zone("Office", true)];
        let config = Config {
            airplay: AirPlayConfig {
                base_rtsp_port: u16::MAX,
                ..AirPlayConfig::default()
            },
            ..Config::default()
        };

        let ports = zone_rtsp_ports(&zones, &config);

        assert_eq!(ports.len(), 2);
        assert!(ports[0].1.is_ok());
        assert!(ports[1].1.is_err());
    }

    #[tokio::test]
    async fn rtsp_ports_warn_when_all_zones_are_filtered_out() {
        let checks = check_rtsp_ports(&[zone("Hidden", false)], &Config::default()).await;

        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, CheckStatus::Warn);
    }

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
