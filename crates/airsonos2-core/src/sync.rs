use std::collections::BTreeMap;

use crate::ZoneId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ZoneDelay {
    pub zone_id: ZoneId,
    pub delay_ms: u64,
}

pub fn recommended_delay_for_zone(zone_offset_ms: i64, slowest_offset_ms: i64) -> u64 {
    slowest_offset_ms.saturating_sub(zone_offset_ms).max(0) as u64
}

pub fn delays_from_offsets(offsets: &BTreeMap<ZoneId, i64>) -> Vec<ZoneDelay> {
    let slowest = offsets.values().copied().max().unwrap_or_default();

    offsets
        .iter()
        .map(|(zone_id, offset)| ZoneDelay {
            zone_id: zone_id.clone(),
            delay_ms: recommended_delay_for_zone(*offset, slowest),
        })
        .collect()
}

/// Positive offsets describe late rooms. Delay faster rooms to the largest
/// configured latency; the default is only a fallback, never an added delay.
pub fn configured_delays(
    zones: &[(ZoneId, String)],
    explicit: &BTreeMap<String, i64>,
    default_offset_ms: i64,
) -> Vec<ZoneDelay> {
    let offsets = zones
        .iter()
        .map(|(id, name)| {
            let offset = explicit
                .get(&id.to_string())
                .or_else(|| explicit.get(name))
                .copied()
                .unwrap_or(default_offset_ms);
            (id.clone(), offset)
        })
        .collect();
    delays_from_offsets(&offsets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn faster_zones_get_delayed_to_slowest_zone() {
        let mut offsets = BTreeMap::new();
        offsets.insert(ZoneId::new("Kitchen"), 120);
        offsets.insert(ZoneId::new("Office"), 80);
        offsets.insert(ZoneId::new("Den"), 120);

        let delays = delays_from_offsets(&offsets);

        assert_eq!(delays[0].delay_ms, 0);
        assert_eq!(delays[1].delay_ms, 0);
        assert_eq!(delays[2].delay_ms, 40);
    }

    #[test]
    fn configured_offsets_share_calibration_and_runtime_semantics() {
        let zones = vec![
            (ZoneId::new("a"), "Kitchen".into()),
            (ZoneId::new("b"), "Office".into()),
            (ZoneId::new("c"), "Den".into()),
        ];
        let explicit = BTreeMap::from([("Kitchen".into(), -20), ("b".into(), 80)]);
        let delays = configured_delays(&zones, &explicit, 30);
        assert_eq!(
            delays.iter().map(|d| d.delay_ms).collect::<Vec<_>>(),
            [100, 0, 50]
        );
        assert!(
            configured_delays(&zones, &BTreeMap::new(), -40)
                .iter()
                .all(|d| d.delay_ms == 0)
        );
    }
}
