use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};
use std::time::{Duration, Instant};

use airsonos2_core::{SonosZone, ZoneId};
use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::task::JoinSet;
use tokio::time;
use url::Url;

use crate::client::SonosClient;
use crate::sonos_addr;
use crate::topology::ZoneGroupMember;
use crate::xml::{DeviceDescriptionError, parse_device_description};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveredDevice {
    pub location: Url,
    pub ip: IpAddr,
}

#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error("failed to bind SSDP socket: {0}")]
    Bind(std::io::Error),
    #[error("failed to send SSDP discovery request: {0}")]
    Send(std::io::Error),
    #[error("failed to receive SSDP response: {0}")]
    Receive(std::io::Error),
    #[error("failed to parse discovery URL: {0}")]
    Url(#[from] url::ParseError),
    #[error("failed to query device description at {url}: {source}")]
    DescriptionFetch { url: Url, source: reqwest::Error },
    #[error("failed to parse device description at {url}: {source}")]
    DescriptionParse {
        url: Url,
        source: Box<DeviceDescriptionError>,
    },
    #[error("device description exceeds the 1 MiB limit")]
    BodyTooLarge,
    #[error("discovery exhausted its overall budget without a usable topology")]
    NoTopology,
    #[error("failed to create direct-device HTTP client: {0}")]
    Http(#[from] reqwest::Error),
    #[error("failed to query Sonos topology: {0}")]
    Topology(#[from] crate::client::SonosClientError),
}

pub async fn discover_sonos_devices(
    timeout: Duration,
) -> Result<Vec<DiscoveredDevice>, DiscoveryError> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))
        .await
        .map_err(DiscoveryError::Bind)?;
    let request = concat!(
        "M-SEARCH * HTTP/1.1\r\n",
        "HOST: 239.255.255.250:1900\r\n",
        "MAN: \"ssdp:discover\"\r\n",
        "MX: 1\r\n",
        "ST: urn:schemas-upnp-org:device:ZonePlayer:1\r\n",
        "\r\n"
    );
    socket
        .send_to(
            request.as_bytes(),
            (Ipv4Addr::new(239, 255, 255, 250), 1900),
        )
        .await
        .map_err(DiscoveryError::Send)?;

    let deadline = Instant::now() + timeout;
    let mut buf = [0_u8; 2048];
    let mut locations = BTreeSet::new();

    while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
        let received = time::timeout(remaining, socket.recv_from(&mut buf)).await;
        let (len, from) = match received {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => return Err(DiscoveryError::Receive(error)),
            Err(_) => break,
        };

        if let Some(location) = parse_ssdp_location(&buf[..len]) {
            if let Ok(url) = Url::parse(&location)
                && device_url_ip(&url) == Some(from.ip())
            {
                locations.insert((url.to_string(), from.ip()));
            }
        }
    }

    Ok(locations
        .into_iter()
        .map(|(location, ip)| {
            let location = Url::parse(&location).expect("location parsed before insert");
            DiscoveredDevice { location, ip }
        })
        .collect())
}

pub async fn discover_sonos_zones(timeout: Duration) -> Result<Vec<SonosZone>, DiscoveryError> {
    discover_sonos_zones_from_sources(timeout, &[], true).await
}

pub async fn discover_sonos_zones_from_sources(
    timeout: Duration,
    static_ips: &[IpAddr],
    auto_discover: bool,
) -> Result<Vec<SonosZone>, DiscoveryError> {
    // Reserve most of the overall budget for HTTP, including fallback seeds.
    let deadline = time::Instant::now() + timeout;
    let multicast = if auto_discover {
        time::timeout(timeout / 3, discover_sonos_devices(timeout / 3))
            .await
            .unwrap_or(Err(DiscoveryError::NoTopology))
    } else {
        Ok(Vec::new())
    };
    let devices = merge_discovery_sources(multicast, static_ips)?;
    discover_devices_until(devices, deadline).await
}

fn merge_discovery_sources(
    multicast: Result<Vec<DiscoveredDevice>, DiscoveryError>,
    static_ips: &[IpAddr],
) -> Result<Vec<DiscoveredDevice>, DiscoveryError> {
    let mut devices = match multicast {
        Ok(devices) => devices,
        Err(error) if !static_ips.is_empty() => {
            tracing::warn!(%error, "multicast failed; trying configured static Sonos addresses");
            Vec::new()
        }
        Err(error) => return Err(error),
    };
    devices.extend(static_ips.iter().map(|ip| DiscoveredDevice {
        location: device_description_url(*ip).expect("IP literal URL"),
        ip: *ip,
    }));
    devices.sort_by(|a, b| {
        a.ip.cmp(&b.ip)
            .then(a.location.as_str().cmp(b.location.as_str()))
    });
    devices.dedup_by_key(|device| device.ip);
    Ok(devices)
}

async fn discover_devices_until(
    devices: Vec<DiscoveredDevice>,
    deadline: time::Instant,
) -> Result<Vec<SonosZone>, DiscoveryError> {
    if devices.is_empty() {
        return Ok(Vec::new());
    }
    let http = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(2))
        .build()?;
    let mut pending = devices.into_iter();
    let mut probes = JoinSet::new();
    let mut zones = BTreeMap::new();
    let mut topology = None;
    loop {
        while probes.len() < 4 {
            let Some(device) = pending.next() else { break };
            let http = http.clone();
            probes.spawn(async move {
                let zone = fetch_description(&http, &device).await?;
                let state = SonosClient::from_base_url(device.location.clone())?
                    .get_zone_group_state()
                    .await;
                Ok::<_, DiscoveryError>((zone, state))
            });
        }
        if probes.is_empty() {
            break;
        }
        match time::timeout_at(deadline, probes.join_next()).await {
            Ok(Some(Ok(Ok((zone, state))))) => {
                zones.insert(zone.id.clone(), zone);
                if topology.is_none()
                    && let Ok(members) = state
                    && members.iter().any(|member| {
                        zones.contains_key(&ZoneId::new(member.uuid.clone()))
                            || member
                                .location
                                .as_deref()
                                .and_then(|location| Url::parse(location).ok())
                                .as_ref()
                                .and_then(device_url_ip)
                                .is_some()
                    })
                {
                    topology = Some(members);
                }
            }
            Ok(Some(Ok(Err(error)))) => tracing::debug!(%error, "Sonos seed did not respond"),
            Ok(Some(Err(error))) => tracing::debug!(%error, "Sonos discovery probe failed"),
            _ => break,
        }
    }
    probes.abort_all();
    while probes.join_next().await.is_some() {}
    let topology = topology.ok_or(DiscoveryError::NoTopology)?;
    apply_topology(&mut zones, topology);
    Ok(zones.into_values().collect())
}

async fn fetch_description(
    http: &reqwest::Client,
    device: &DiscoveredDevice,
) -> Result<SonosZone, DiscoveryError> {
    let mut response = http
        .get(device.location.clone())
        .send()
        .await?
        .error_for_status()?;
    const MAX_DESCRIPTION_BYTES: usize = 1024 * 1024;
    if response
        .content_length()
        .is_some_and(|size| size > MAX_DESCRIPTION_BYTES as u64)
    {
        return Err(DiscoveryError::BodyTooLarge);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if chunk.len() > MAX_DESCRIPTION_BYTES - body.len() {
            return Err(DiscoveryError::BodyTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    let description = parse_device_description(&String::from_utf8_lossy(&body), device.ip)
        .map_err(|source| DiscoveryError::DescriptionParse {
            url: device.location.clone(),
            source: Box::new(source),
        })?;
    let rincon_id = description.rincon_id();
    Ok(SonosZone {
        id: ZoneId::new(rincon_id.clone()),
        room_name: description.room_name,
        ip: device.ip,
        model: description.model_name,
        rincon_id,
        // Only topology can establish that this device is a visible room.
        is_visible_room: false,
        is_group_coordinator: false,
    })
}

fn apply_topology(zones: &mut BTreeMap<ZoneId, SonosZone>, members: Vec<ZoneGroupMember>) {
    for member in members {
        let id = ZoneId::new(member.uuid.clone());
        if !zones.contains_key(&id)
            && let Some(ip) = member
                .location
                .as_deref()
                .and_then(|s| Url::parse(s).ok())
                .as_ref()
                .and_then(device_url_ip)
        {
            zones.insert(
                id.clone(),
                SonosZone {
                    id: id.clone(),
                    room_name: member.zone_name.clone(),
                    ip,
                    model: "Unknown (topology)".to_owned(),
                    rincon_id: member.uuid.clone(),
                    is_visible_room: false,
                    is_group_coordinator: false,
                },
            );
        }
        if let Some(zone) = zones.get_mut(&id) {
            zone.is_visible_room = member.is_visible_room;
            zone.is_group_coordinator = member.is_group_coordinator;
            if !member.zone_name.is_empty() {
                zone.room_name = member.zone_name;
            }
        }
    }
}

fn device_description_url(ip: IpAddr) -> Result<Url, url::ParseError> {
    Url::parse(&format!(
        "http://{}/xml/device_description.xml",
        sonos_addr(ip)
    ))
}

fn device_url_ip(url: &Url) -> Option<IpAddr> {
    if url.scheme() != "http" || !url.username().is_empty() || url.password().is_some() {
        return None;
    }
    match url.host()? {
        url::Host::Ipv4(ip) => Some(ip.into()),
        url::Host::Ipv6(ip) => Some(ip.into()),
        url::Host::Domain(_) => None,
    }
}

fn parse_ssdp_location(bytes: &[u8]) -> Option<String> {
    let response = String::from_utf8_lossy(bytes);
    response.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name.eq_ignore_ascii_case("location") {
            Some(value.trim().to_owned())
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fake_seed(topology: &'static str) -> (DiscoveredDevice, tokio::task::JoinHandle<()>) {
        use axum::{
            Router,
            routing::{get, post},
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("address");
        let app = Router::new()
            .route("/xml/device_description.xml", get(|| async {
                "<root><device><roomName>Kitchen &amp; Dining</roomName><modelName>Test</modelName><UDN>uuid:main</UDN></device></root>"
            }))
            .route("/ZoneGroupTopology/Control", post(move || async move { topology }));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
        (
            DiscoveredDevice {
                location: Url::parse(&format!("http://{addr}/xml/device_description.xml"))
                    .expect("url"),
                ip: addr.ip(),
            },
            task,
        )
    }

    #[tokio::test]
    async fn static_seed_survives_multicast_failure_and_preserves_filter_name() {
        let (seed, task) = fake_seed(r#"<ZoneGroups><ZoneGroup Coordinator="main"><ZoneGroupMember UUID="main" ZoneName="Kitchen &amp; Dining"/></ZoneGroup></ZoneGroups>"#).await;
        let mut devices = merge_discovery_sources(
            Err(DiscoveryError::Bind(std::io::Error::other(
                "no multicast interface",
            ))),
            &[seed.ip],
        )
        .expect("fallback");
        // Test server uses an ephemeral port; static production URLs use port 1400.
        devices[0].location = seed.location;
        let zones = discover_devices_until(devices, time::Instant::now() + Duration::from_secs(2))
            .await
            .expect("discovery");
        let mut config = airsonos2_core::SonosConfig::default();
        config.include_rooms.push("Kitchen & Dining".to_owned());
        assert_eq!(airsonos2_core::filter_zones(&zones, &config).len(), 1);
        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn skips_empty_topology_and_limits_total_discovery_time() {
        let (empty, empty_task) = fake_seed("<ZoneGroups/>").await;
        let (usable, usable_task) = fake_seed(r#"<ZoneGroups><ZoneGroup Coordinator="main"><ZoneGroupMember UUID="main" ZoneName="Kitchen"/></ZoneGroup></ZoneGroups>"#).await;
        let zones = discover_devices_until(
            vec![empty, usable.clone()],
            time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .expect("second topology");
        assert_eq!(zones[0].room_name, "Kitchen");
        assert!(zones[0].is_visible_room);
        assert!(matches!(
            discover_devices_until(vec![usable], time::Instant::now()).await,
            Err(DiscoveryError::NoTopology)
        ));
        empty_task.abort();
        usable_task.abort();
        let _ = empty_task.await;
        let _ = usable_task.await;
    }

    #[tokio::test]
    async fn rejects_oversized_and_redirected_device_descriptions() {
        use axum::{Router, response::Redirect, routing::get};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("address");
        let app = Router::new()
            .route("/large", get(|| async { "x".repeat(1024 * 1024 + 1) }))
            .route("/redirect", get(|| async { Redirect::temporary("/large") }));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("client");
        let mut device = DiscoveredDevice {
            ip: addr.ip(),
            location: Url::parse(&format!("http://{addr}/large")).expect("url"),
        };
        assert!(matches!(
            fetch_description(&http, &device).await,
            Err(DiscoveryError::BodyTooLarge)
        ));
        device.location.set_path("/redirect");
        assert!(matches!(
            fetch_description(&http, &device).await,
            Err(DiscoveryError::DescriptionParse { .. })
        ));
        task.abort();
        let _ = task.await;
    }

    #[test]
    fn parses_case_insensitive_ssdp_location() {
        let response = b"HTTP/1.1 200 OK\r\nLOCATION: http://192.0.2.4:1400/xml/device_description.xml\r\n\r\n";

        let location = parse_ssdp_location(response).expect("location");

        assert_eq!(location, "http://192.0.2.4:1400/xml/device_description.xml");
    }

    #[test]
    fn builds_device_description_url_for_static_ip() {
        let url = device_description_url("192.0.2.10".parse().expect("ip")).expect("url");

        assert_eq!(
            url.as_str(),
            "http://192.0.2.10:1400/xml/device_description.xml"
        );
    }

    #[test]
    fn builds_device_description_url_for_static_ipv6() {
        let url = device_description_url("2001:db8::10".parse().expect("ip")).expect("url");

        assert_eq!(
            url.as_str(),
            "http://[2001:db8::10]:1400/xml/device_description.xml"
        );
    }
}
