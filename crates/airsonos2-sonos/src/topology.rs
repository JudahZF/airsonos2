use quick_xml::Reader;
use quick_xml::events::Event;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ZoneGroupMember {
    pub uuid: String,
    pub zone_name: String,
    pub location: Option<String>,
    pub is_visible_room: bool,
    pub is_group_coordinator: bool,
}

/// Parse direct topology XML or the escaped payload inside a SOAP response.
/// An incomplete or malformed topology is never used to advertise rooms.
pub fn parse_zone_group_state(xml: &str) -> Vec<ZoneGroupMember> {
    let mut envelope = Reader::from_str(xml);
    let mut payload = None;
    loop {
        match envelope.read_event() {
            Ok(Event::Start(element)) if element.local_name().as_ref() == b"ZoneGroupState" => {
                let Ok(text) = envelope.read_text(element.name()) else {
                    return Vec::new();
                };
                let Ok(text) = text.decode() else {
                    return Vec::new();
                };
                let text = text.trim();
                payload = match text
                    .strip_prefix("<![CDATA[")
                    .and_then(|s| s.strip_suffix("]]>"))
                {
                    Some(cdata) => Some(cdata.to_owned()),
                    None => quick_xml::escape::unescape(text)
                        .ok()
                        .map(|s| s.into_owned()),
                };
                if payload.is_none() {
                    return Vec::new();
                }
                break;
            }
            Ok(Event::Eof) => break,
            Err(_) => return Vec::new(),
            _ => {}
        }
    }
    let mut reader = Reader::from_str(payload.as_deref().unwrap_or(xml));
    let mut members = Vec::new();
    let mut coordinator = None;
    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) | Ok(Event::Empty(element)) => {
                match element.local_name().as_ref() {
                    b"ZoneGroup" => coordinator = attr(&element, b"Coordinator"),
                    b"ZoneGroupMember" | b"Satellite" if coordinator.is_some() => {
                        let Some(uuid) = attr(&element, b"UUID").filter(|s| !s.is_empty()) else {
                            continue;
                        };
                        let satellite = element.local_name().as_ref() == b"Satellite";
                        members.push(ZoneGroupMember {
                            is_group_coordinator: !satellite
                                && coordinator.as_deref() == Some(&uuid),
                            uuid,
                            zone_name: attr(&element, b"ZoneName").unwrap_or_default(),
                            location: attr(&element, b"Location"),
                            is_visible_room: !satellite
                                && attr(&element, b"Invisible").as_deref() != Some("1"),
                        });
                    }
                    _ => {}
                }
            }
            Ok(Event::End(element)) if element.local_name().as_ref() == b"ZoneGroup" => {
                coordinator = None
            }
            Ok(Event::Eof) => break,
            Err(_) => return Vec::new(),
            _ => {}
        }
    }
    members
}

fn attr(element: &quick_xml::events::BytesStart<'_>, key: &[u8]) -> Option<String> {
    element.attributes().flatten().find_map(|attribute| {
        (attribute.key.as_ref() == key)
            .then(|| {
                attribute
                    .normalized_value(quick_xml::XmlVersion::Implicit1_0)
                    .ok()
                    .map(|value| value.into_owned())
            })
            .flatten()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_escaped_room_names_and_hides_nested_satellites() {
        let topology = r#"<ZoneGroups><ZoneGroup Coordinator="main"><ZoneGroupMember UUID="main" ZoneName="Kitchen &amp; Dining"><Satellite UUID="sub" ZoneName="Sub" /></ZoneGroupMember></ZoneGroup></ZoneGroups>"#;
        let soap = format!(
            "<Envelope><ZoneGroupState>{}</ZoneGroupState></Envelope>",
            quick_xml::escape::escape(topology)
        );
        let members = parse_zone_group_state(&soap);
        assert_eq!(members[0].zone_name, "Kitchen & Dining");
        assert!(members[0].is_visible_room);
        assert!(!members[1].is_visible_room);
        assert!(!members[1].is_group_coordinator);
    }

    #[test]
    fn parses_zone_group_state_members_and_coordinator() {
        let xml = r#"
        <ZoneGroups>
          <ZoneGroup Coordinator="RINCON_KITCHEN" ID="RINCON_KITCHEN:1">
            <ZoneGroupMember UUID="RINCON_KITCHEN" ZoneName="Kitchen" Location="http://192.0.2.1:1400/xml/device_description.xml" Invisible="0" />
            <ZoneGroupMember UUID="RINCON_OFFICE" ZoneName="Office" Location="http://192.0.2.2:1400/xml/device_description.xml" Invisible="1" />
          </ZoneGroup>
        </ZoneGroups>
        "#;

        let members = parse_zone_group_state(xml);

        assert_eq!(members.len(), 2);
        assert!(members[0].is_group_coordinator);
        assert!(members[0].is_visible_room);
        assert!(!members[1].is_visible_room);
    }
}
