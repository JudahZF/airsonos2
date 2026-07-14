use std::net::{IpAddr, SocketAddr};

pub mod client;
pub mod discovery;
pub mod soap;
pub mod topology;
pub mod xml;

pub use client::{SonosClient, SonosClientError};
pub use discovery::{
    DiscoveredDevice, DiscoveryError, discover_sonos_devices, discover_sonos_zones,
    discover_sonos_zones_from_sources,
};
pub use soap::{SoapAction, SoapService, set_av_transport_uri_metadata};
pub use topology::{ZoneGroupMember, parse_zone_group_state};
pub use xml::{DeviceDescription, DeviceDescriptionError, parse_device_description};

fn sonos_addr(ip: IpAddr) -> SocketAddr {
    SocketAddr::new(ip, 1400)
}
