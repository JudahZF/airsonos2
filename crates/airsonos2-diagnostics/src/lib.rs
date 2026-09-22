use std::net::SocketAddr;
use std::time::Duration;

use airsonos2_core::{Config, SonosZone, allocate_rtsp_port, filter_zones};
use airsonos2_sonos::discover_sonos_zones_from_sources;
use serde::Serialize;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::time;

/// The service owner must cancel and await this listener during shutdown.
pub async fn serve_diagnostics(
    addr: SocketAddr,
    registry: airsonos2_stream::StreamRegistry,
    cancel: tokio_util::sync::CancellationToken,
) -> std::io::Result<()> {
    use axum::{Router, extract::State, routing::get};
    let router = Router::new()
        .route("/healthz", get(|| async { "ok\n" }))
        .route(
            "/metrics",
            get(
                |State(registry): State<airsonos2_stream::StreamRegistry>| async move {
                    (
                        [(
                            axum::http::header::CONTENT_TYPE,
                            "text/plain; version=0.0.4",
                        )],
                        registry.metrics_text().await,
                    )
                },
            ),
        )
        .with_state(registry);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(cancel.cancelled_owned())
        .await
}

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

    if let Err(error) = config.validate() {
        checks.push(DoctorCheck {
            name: "configuration".to_owned(),
            status: CheckStatus::Fail,
            detail: error.to_string(),
        });
        return Ok(DoctorReport { checks });
    }
    checks.push(check_encoder(config).await);
    checks.push(check_state_directory(&config.server.state_dir));
    let mut ports = vec![
        (
            "HTTP stream port".to_owned(),
            SocketAddr::new(config.server.bind, config.server.http_port),
        ),
        (
            "diagnostics listener".to_owned(),
            config
                .diagnostics
                .metrics_addr
                .parse()
                .expect("validated address"),
        ),
    ];

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
            let zones = filter_zones(&zones, &config.sonos);
            checks.push(DoctorCheck {
                name: "configured rooms".to_owned(),
                status: if zones.is_empty() {
                    CheckStatus::Warn
                } else {
                    CheckStatus::Pass
                },
                detail: format!(
                    "{} visible room(s) match include/exclude filters",
                    zones.len()
                ),
            });
            let (room_ports, errors) = room_port_plan(config, &zones);
            ports.extend(room_ports);
            checks.extend(errors);
            checks.extend(check_sonos_reachability(&zones).await);
        }
        Err(error) => checks.push(DoctorCheck {
            name: "Sonos discovery".to_owned(),
            status: CheckStatus::Fail,
            detail: error.to_string(),
        }),
    }

    checks.extend(check_listener_ports(ports).await);

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

async fn check_encoder(config: &Config) -> DoctorCheck {
    let mut check = DoctorCheck {
        name: "audio encoder".to_owned(),
        status: CheckStatus::Pass,
        detail: String::new(),
    };
    if config.stream.codec == "wav" {
        check.detail = "native WAV output; FFmpeg is not required".to_owned();
        return check;
    }
    let source = format!(
        "anullsrc=r={}:cl={}",
        config.airplay.output_sample_rate,
        if config.airplay.output_channels == 1 {
            "mono"
        } else {
            "stereo"
        }
    );
    let bitrate = format!("{}k", config.stream.mp3_bitrate_kbps);
    let child = Command::new(&config.stream.ffmpeg_path)
        .args([
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            &source,
            "-t",
            "0.1",
            "-c:a",
            "libmp3lame",
            "-b:a",
            &bitrate,
            "-f",
            "mp3",
            "pipe:1",
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn();
    let result = match child {
        Ok(mut child) => match time::timeout(Duration::from_secs(5), child.wait()).await {
            Ok(Ok(status)) if status.success() => {
                Ok("short MP3 encode succeeded for the configured format and bitrate".to_owned())
            }
            Ok(Ok(status)) => Err(format!(
                "MP3 encode exited with {status}; check libmp3lame and the configured format"
            )),
            Ok(Err(error)) => Err(error.to_string()),
            Err(_) => {
                let _ = child.start_kill();
                let _ = time::timeout(Duration::from_secs(1), child.wait()).await;
                Err("MP3 encode exceeded the five-second deadline".to_owned())
            }
        },
        Err(error) => Err(error.to_string()),
    };
    match result {
        Ok(detail) => check.detail = detail,
        Err(detail) => {
            check.status = CheckStatus::Fail;
            check.detail = detail;
        }
    }
    check
}

fn check_state_directory(path: &std::path::Path) -> DoctorCheck {
    let result = (|| -> std::io::Result<()> {
        use std::io::Write;
        for directory in [
            path.to_path_buf(),
            path.join("endpoints"),
            path.join("pairings"),
        ] {
            std::fs::create_dir_all(&directory)?;
            let mut probe = tempfile::Builder::new()
                .prefix(".airsonos2-doctor-")
                .tempfile_in(directory)?;
            probe.write_all(b"write probe")?;
            probe.flush()?;
        }
        Ok(())
    })();
    DoctorCheck {
        name: "state directory".to_owned(),
        status: if result.is_ok() {
            CheckStatus::Pass
        } else {
            CheckStatus::Fail
        },
        detail: match result {
            Ok(()) => format!(
                "{} and its endpoint/pairing directories are writable by this user",
                path.display()
            ),
            Err(error) => format!("{} is not writable by this user: {error}", path.display()),
        },
    }
}

type PortPlan = Vec<(String, SocketAddr)>;

fn room_port_plan(config: &Config, zones: &[SonosZone]) -> (PortPlan, Vec<DoctorCheck>) {
    let mut ports = Vec::new();
    let mut errors = Vec::new();
    for (index, zone) in zones.iter().enumerate() {
        match allocate_rtsp_port(config.airplay.base_rtsp_port, index) {
            Ok(port) => ports.push((
                format!("AirPlay RTSP: {}", zone.room_name),
                SocketAddr::new(config.server.bind, port),
            )),
            Err(error) => errors.push(DoctorCheck {
                name: format!("AirPlay RTSP: {}", zone.room_name),
                status: CheckStatus::Fail,
                detail: error.to_string(),
            }),
        }
    }
    (ports, errors)
}

async fn check_listener_ports(ports: PortPlan) -> Vec<DoctorCheck> {
    // Hold successful binds until every listener has been checked. This also catches
    // collisions between our own listeners, including wildcard/specific-IP conflicts.
    let mut listeners = Vec::new();
    let mut checks = Vec::new();
    for (name, addr) in ports {
        let result = TcpListener::bind(addr).await;
        let (status, detail) = match result {
            Ok(listener) => {
                listeners.push(listener);
                (CheckStatus::Pass, format!("{addr} is available"))
            }
            Err(error) => (
                CheckStatus::Fail,
                format!("{addr} is not available: {error}"),
            ),
        };
        checks.push(DoctorCheck {
            name,
            status,
            detail,
        });
    }
    checks
}

async fn check_sonos_reachability(zones: &[SonosZone]) -> Vec<DoctorCheck> {
    let mut tasks = tokio::task::JoinSet::new();
    let mut pending = zones.iter();
    let mut checks = Vec::new();
    loop {
        while tasks.len() < 4 {
            let Some(zone) = pending.next() else { break };
            let zone = zone.clone();
            tasks.spawn(async move {
                let addr = SocketAddr::new(zone.ip, 1400);
                let (status, detail) =
                    match time::timeout(Duration::from_secs(1), TcpStream::connect(addr)).await {
                        Ok(Ok(_)) => (CheckStatus::Pass, format!("{addr} accepted TCP connection")),
                        Ok(Err(error)) => (CheckStatus::Fail, error.to_string()),
                        Err(_) => (
                            CheckStatus::Fail,
                            "timed out connecting to port 1400".to_owned(),
                        ),
                    };
                DoctorCheck {
                    name: format!("Sonos SOAP reachability: {}", zone.room_name),
                    status,
                    detail,
                }
            });
        }
        match tasks.join_next().await {
            Some(Ok(check)) => checks.push(check),
            Some(Err(error)) => checks.push(DoctorCheck {
                name: "Sonos SOAP reachability".to_owned(),
                status: CheckStatus::Fail,
                detail: error.to_string(),
            }),
            None => break,
        }
    }
    checks.sort_by(|a, b| a.name.cmp(&b.name));
    checks
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zones(count: usize) -> Vec<SonosZone> {
        (0..count)
            .map(|index| SonosZone {
                id: airsonos2_core::ZoneId::new(index.to_string()),
                room_name: format!("Room {index}"),
                ip: "127.0.0.1".parse().expect("IP"),
                model: "Test".to_owned(),
                rincon_id: index.to_string(),
                is_visible_room: true,
                is_group_coordinator: true,
            })
            .collect()
    }

    #[test]
    fn plans_exactly_the_filtered_rooms_including_more_than_six() {
        let mut config = Config::default();
        let discovered = zones(7);
        let (ports, errors) = room_port_plan(&config, &discovered);
        assert!(errors.is_empty());
        assert_eq!(ports.len(), 7);
        assert_eq!(ports[6].1.port(), 5006);
        config.sonos.include_rooms.push("Room 6".to_owned());
        let (ports, errors) = room_port_plan(&config, &filter_zones(&discovered, &config.sonos));
        assert!(errors.is_empty());
        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].1.port(), 5000);
    }

    #[tokio::test]
    async fn occupied_diagnostics_and_own_listener_collisions_fail() {
        let occupied = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = occupied.local_addr().expect("address");
        let checks = check_listener_ports(vec![("diagnostics listener".to_owned(), addr)]).await;
        assert_eq!(checks[0].status, CheckStatus::Fail);
        drop(occupied);
        let checks = check_listener_ports(vec![
            ("stream".to_owned(), addr),
            ("diagnostics".to_owned(), addr),
        ])
        .await;
        assert_eq!(checks[0].status, CheckStatus::Pass);
        assert_eq!(checks[1].status, CheckStatus::Fail);
    }

    #[test]
    fn checks_state_subdirectories_without_leaving_probe_files() {
        let directory = tempfile::tempdir().expect("tempdir");
        assert_eq!(
            check_state_directory(directory.path()).status,
            CheckStatus::Pass
        );
        assert_eq!(
            std::fs::read_dir(directory.path().join("pairings"))
                .expect("read")
                .count(),
            0
        );
        std::fs::remove_dir(directory.path().join("pairings")).expect("remove empty directory");
        std::fs::write(directory.path().join("pairings"), b"not a directory").expect("write");
        assert_eq!(
            check_state_directory(directory.path()).status,
            CheckStatus::Fail
        );
    }

    #[tokio::test]
    async fn wav_does_not_require_ffmpeg_but_mp3_does() {
        let mut config = Config::default();
        config.stream.ffmpeg_path = "/does-not-exist/ffmpeg".into();
        config.stream.codec = "wav".to_owned();
        assert_eq!(check_encoder(&config).await.status, CheckStatus::Pass);
        config.stream.codec = "mp3".to_owned();
        assert_eq!(check_encoder(&config).await.status, CheckStatus::Fail);
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
