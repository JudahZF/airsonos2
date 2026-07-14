use std::borrow::Cow;

use quick_xml::events::Event;
use quick_xml::{Decoder, Reader};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ZoneGroupMember {
    pub uuid: String,
    pub zone_name: String,
    pub location: Option<String>,
    pub is_visible_room: bool,
    pub is_group_coordinator: bool,
}

pub fn parse_zone_group_state(xml: &str) -> Vec<ZoneGroupMember> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut groups = Vec::new();
    let mut current_coordinator = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) | Ok(Event::Empty(element)) => {
                if element.name().as_ref() == b"ZoneGroup" {
                    current_coordinator = attr(&element, b"Coordinator", reader.decoder());
                } else if element.name().as_ref() == b"ZoneGroupMember" {
                    let decoder = reader.decoder();
                    let uuid = attr(&element, b"UUID", decoder).unwrap_or_default();
                    let zone_name = attr(&element, b"ZoneName", decoder).unwrap_or_default();
                    let location = attr(&element, b"Location", decoder);
                    let invisible =
                        attr(&element, b"Invisible", decoder).unwrap_or_default() == "1";
                    let is_group_coordinator = current_coordinator.as_deref() == Some(&uuid);

                    groups.push(ZoneGroupMember {
                        uuid,
                        zone_name,
                        location,
                        is_visible_room: !invisible,
                        is_group_coordinator,
                    });
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(_) => break,
        }
    }

    groups
}

fn attr(
    element: &quick_xml::events::BytesStart<'_>,
    key: &[u8],
    decoder: Decoder,
) -> Option<String> {
    element.attributes().flatten().find_map(|attribute| {
        if attribute.key.as_ref() == key {
            // Fall back to the raw value if unescaping fails (e.g. malformed entity).
            #[allow(deprecated)]
            let value = attribute
                .decode_and_unescape_value(decoder)
                .map(Cow::into_owned)
                .unwrap_or_else(|_| String::from_utf8_lossy(attribute.value.as_ref()).into_owned());
            Some(value)
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn unescapes_attribute_values() {
        let xml = r#"
        <ZoneGroups>
          <ZoneGroup Coordinator="RINCON_A&amp;B" ID="RINCON_A&amp;B:1">
            <ZoneGroupMember UUID="RINCON_A&amp;B" ZoneName="Kitchen &amp; Den" Location="http://192.0.2.1:1400/xml/device_description.xml?a=1&amp;b=2" Invisible="0" />
          </ZoneGroup>
        </ZoneGroups>
        "#;

        let members = parse_zone_group_state(xml);

        assert_eq!(members.len(), 1);
        assert_eq!(members[0].uuid, "RINCON_A&B");
        assert_eq!(members[0].zone_name, "Kitchen & Den");
        assert_eq!(
            members[0].location.as_deref(),
            Some("http://192.0.2.1:1400/xml/device_description.xml?a=1&b=2")
        );
        assert!(members[0].is_group_coordinator);
    }

    #[test]
    fn preserves_raw_attribute_value_when_unescaping_fails() {
        let xml = r#"
        <ZoneGroups>
          <ZoneGroup Coordinator="RINCON_KITCHEN" ID="RINCON_KITCHEN:1">
            <ZoneGroupMember UUID="RINCON_KITCHEN" ZoneName="Kitchen &unknown; Den" Invisible="0" />
          </ZoneGroup>
        </ZoneGroups>
        "#;

        let members = parse_zone_group_state(xml);

        assert_eq!(members.len(), 1);
        assert_eq!(members[0].zone_name, "Kitchen &unknown; Den");
        assert!(members[0].is_group_coordinator);
    }
}
