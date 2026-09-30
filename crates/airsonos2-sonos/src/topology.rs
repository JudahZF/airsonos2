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
/// A structurally incomplete or malformed topology is never used to advertise
/// rooms. Attribute values that fail entity decoding keep their raw text.
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
    // quick-xml reports mismatched end tags but not elements left open at EOF.
    let mut open_elements = 0_usize;
    loop {
        let element = match reader.read_event() {
            Ok(Event::Start(element)) => {
                open_elements += 1;
                element
            }
            Ok(Event::Empty(element)) => element,
            Ok(Event::End(element)) => {
                open_elements -= 1;
                if element.local_name().as_ref() == b"ZoneGroup" {
                    coordinator = None;
                }
                continue;
            }
            Ok(Event::Eof) if open_elements == 0 => break,
            Ok(Event::Eof) | Err(_) => return Vec::new(),
            Ok(_) => continue,
        };
        match element.local_name().as_ref() {
            b"ZoneGroup" => coordinator = attr(&element, b"Coordinator"),
            b"ZoneGroupMember" | b"Satellite" if coordinator.is_some() => {
                let Some(uuid) = attr(&element, b"UUID").filter(|s| !s.is_empty()) else {
                    continue;
                };
                let satellite = element.local_name().as_ref() == b"Satellite";
                members.push(ZoneGroupMember {
                    is_group_coordinator: !satellite && coordinator.as_deref() == Some(&uuid),
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
    members
}

/// Falls back to the raw text when entity decoding fails (for example an
/// unescaped `&` in a room name), so the room keeps its UUID and name.
fn attr(element: &quick_xml::events::BytesStart<'_>, key: &[u8]) -> Option<String> {
    let attribute = element
        .attributes()
        .flatten()
        .find(|attribute| attribute.key.as_ref() == key)?;
    Some(
        match attribute.normalized_value(quick_xml::XmlVersion::Implicit1_0) {
            Ok(value) => value.into_owned(),
            Err(_) => String::from_utf8_lossy(&attribute.value).into_owned(),
        },
    )
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
    fn keeps_escaped_room_names_inside_cdata_payloads() {
        let soap = r#"<Envelope><ZoneGroupState><![CDATA[<ZoneGroups><ZoneGroup Coordinator="main"><ZoneGroupMember UUID="main" ZoneName="Kitchen &amp; Dining" /></ZoneGroup></ZoneGroups>]]></ZoneGroupState></Envelope>"#;
        let members = parse_zone_group_state(soap);
        assert_eq!(members[0].zone_name, "Kitchen & Dining");
    }

    #[test]
    fn keeps_raw_attribute_text_when_entity_decoding_fails() {
        let xml = r#"<ZoneGroups><ZoneGroup Coordinator="main"><ZoneGroupMember UUID="main" ZoneName="Bad &unknown; Name" /><ZoneGroupMember UUID="other" ZoneName="Kitchen & Dining" /></ZoneGroup></ZoneGroups>"#;
        let members = parse_zone_group_state(xml);
        assert_eq!(members.len(), 2);
        assert_eq!(members[0].zone_name, "Bad &unknown; Name");
        assert_eq!(members[1].zone_name, "Kitchen & Dining");
    }

    #[test]
    fn rejects_truncated_or_unbalanced_topology() {
        let truncated = r#"<ZoneGroups><ZoneGroup Coordinator="main"><ZoneGroupMember UUID="main" ZoneName="Kitchen" />"#;
        let soap = format!(
            "<Envelope><ZoneGroupState>{}</ZoneGroupState></Envelope>",
            quick_xml::escape::escape(truncated)
        );
        assert!(parse_zone_group_state(truncated).is_empty());
        assert!(parse_zone_group_state(&soap).is_empty());
        assert!(parse_zone_group_state("<ZoneGroups/></ZoneGroups>").is_empty());
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
