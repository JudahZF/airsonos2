use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use airsonos2_core::{SonosZone, ZoneId};
use thiserror::Error;
use tokio::net::UdpSocket;
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
        source: DeviceDescriptionError,
    },
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
            let url = Url::parse(&location)?;
            let ip = url
                .host_str()
                .and_then(|host| host.parse().ok())
                .unwrap_or(from.ip());
            locations.insert((url.to_string(), ip));
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
    let mut devices = if auto_discover {
        discover_sonos_devices(timeout).await?
    } else {
        Vec::new()
    };
    devices.extend(static_ips.iter().map(|ip| DiscoveredDevice {
        location: device_description_url(*ip).expect("static Sonos IP URL should be valid"),
        ip: *ip,
    }));
    devices.sort_by(|left, right| left.location.as_str().cmp(right.location.as_str()));
    devices.dedup_by(|left, right| left.location == right.location || left.ip == right.ip);

    let http = reqwest::Client::new();
    let mut zones = Vec::new();

    for device in devices {
        let xml = http
            .get(device.location.clone())
            .send()
            .await
            .map_err(|source| DiscoveryError::DescriptionFetch {
                url: device.location.clone(),
                source,
            })?
            .error_for_status()
            .map_err(|source| DiscoveryError::DescriptionFetch {
                url: device.location.clone(),
                source,
            })?
            .text()
            .await
            .map_err(|source| DiscoveryError::DescriptionFetch {
                url: device.location.clone(),
                source,
            })?;
        let description = parse_device_description(&xml, device.ip).map_err(|source| {
            DiscoveryError::DescriptionParse {
                url: device.location,
                source,
            }
        })?;
        let rincon_id = description.rincon_id();

        zones.push(SonosZone {
            id: ZoneId::new(rincon_id.clone()),
            room_name: description.room_name,
            ip: device.ip,
            model: description.model_name,
            rincon_id,
            is_visible_room: true,
            is_group_coordinator: false,
        });
    }

    enrich_zone_topology(zones).await
}

fn device_description_url(ip: IpAddr) -> Result<Url, url::ParseError> {
    Url::parse(&format!(
        "http://{}/xml/device_description.xml",
        sonos_addr(ip)
    ))
}

async fn enrich_zone_topology(mut zones: Vec<SonosZone>) -> Result<Vec<SonosZone>, DiscoveryError> {
    let Some(first_zone) = zones.first() else {
        return Ok(zones);
    };
    let topology = SonosClient::new(first_zone.ip)?
        .get_zone_group_state()
        .await?;
    let by_uuid: BTreeMap<String, ZoneGroupMember> = topology
        .into_iter()
        .map(|member| (member.uuid.clone(), member))
        .collect();

    for zone in &mut zones {
        if let Some(member) = by_uuid.get(&zone.rincon_id) {
            zone.is_visible_room = member.is_visible_room;
            zone.is_group_coordinator = member.is_group_coordinator;
            if !member.zone_name.is_empty() {
                zone.room_name.clone_from(&member.zone_name);
            }
        }
    }

    Ok(zones)
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

#[allow(dead_code)]
fn _socket_addr_from_location(url: &Url) -> Option<SocketAddr> {
    Some(SocketAddr::new(
        url.host_str()?.parse().ok()?,
        url.port().unwrap_or(1400),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

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
