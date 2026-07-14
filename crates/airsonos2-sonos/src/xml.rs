use std::net::IpAddr;

use serde::Deserialize;
use thiserror::Error;
use url::Url;

use crate::sonos_addr;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeviceDescription {
    pub room_name: String,
    pub model_name: String,
    pub udn: String,
    pub base_url: Url,
}

impl DeviceDescription {
    pub fn rincon_id(&self) -> String {
        self.udn.trim_start_matches("uuid:").to_owned()
    }
}

#[derive(Debug, Error)]
pub enum DeviceDescriptionError {
    #[error("device description XML did not contain a device element")]
    MissingDevice,
    #[error("device description was missing required field {0}")]
    MissingField(&'static str),
    #[error("failed to parse device description XML: {0}")]
    Parse(#[from] quick_xml::DeError),
    #[error("failed to build base URL: {0}")]
    Url(#[from] url::ParseError),
}

#[derive(Debug, Deserialize)]
struct Root {
    device: Option<Device>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Device {
    friendly_name: Option<String>,
    room_name: Option<String>,
    model_name: Option<String>,
    #[serde(rename = "UDN")]
    udn: Option<String>,
}

pub fn parse_device_description(
    xml: &str,
    ip: IpAddr,
) -> Result<DeviceDescription, DeviceDescriptionError> {
    let root: Root = quick_xml::de::from_str(xml)?;
    let device = root.device.ok_or(DeviceDescriptionError::MissingDevice)?;
    let room_name = device
        .room_name
        .or(device.friendly_name)
        .ok_or(DeviceDescriptionError::MissingField("roomName"))?;
    let model_name = device
        .model_name
        .ok_or(DeviceDescriptionError::MissingField("modelName"))?;
    let udn = device
        .udn
        .ok_or(DeviceDescriptionError::MissingField("UDN"))?;
    let base_url = Url::parse(&format!("http://{}", sonos_addr(ip)))?;

    Ok(DeviceDescription {
        room_name,
        model_name,
        udn,
        base_url,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sonos_device_description() {
        let xml = r#"
        <root>
          <device>
            <friendlyName>Kitchen</friendlyName>
            <roomName>Kitchen</roomName>
            <modelName>Sonos Play:1</modelName>
            <UDN>uuid:RINCON_000E58AAAAAA01400</UDN>
          </device>
        </root>
        "#;

        let description =
            parse_device_description(xml, "192.0.2.11".parse().expect("ip")).expect("description");

        assert_eq!(description.room_name, "Kitchen");
        assert_eq!(description.model_name, "Sonos Play:1");
        assert_eq!(description.rincon_id(), "RINCON_000E58AAAAAA01400");
        assert_eq!(description.base_url.as_str(), "http://192.0.2.11:1400/");
    }

    #[test]
    fn builds_bracketed_base_url_for_ipv6() {
        let xml = r#"
        <root>
          <device>
            <friendlyName>Kitchen</friendlyName>
            <roomName>Kitchen</roomName>
            <modelName>Sonos Play:1</modelName>
            <UDN>uuid:RINCON_000E58AAAAAA01400</UDN>
          </device>
        </root>
        "#;

        let description = parse_device_description(xml, "2001:db8::11".parse().expect("ip"))
            .expect("description");

        assert_eq!(description.base_url.as_str(), "http://[2001:db8::11]:1400/");
    }
}
