# Changelog

## 0.3.0

- Sonos rooms that play from one AirPlay device join one native Sonos group, so Sonos keeps them in sync, including drift. A room added later joins the rooms that already play. The new `native_sonos_groups` option, on by default, controls this.
- Sessions from one AirPlay device share one timeline, so every room starts on the same source sample.
- WAV room offsets now hold for the whole stream instead of only at the start.
- Late renderers, such as Home Assistant players, start at the live edge.

## 0.2.0

- Expose Home Assistant `media_player` entities, including Music Assistant players, as AirPlay 2 speakers with the `ha_media_players` option.
- Start after Home Assistant Core and use the Home Assistant API.
- When AirSonos2 cannot read a speaker's volume, report 20% to AirPlay instead of 100%.
- Show the rendered config in a read-only web view on the diagnostics listener, with a network scan for Sonos rooms and Home Assistant players.

## 0.1.0

- Package AirSonos2 as a Home Assistant app.
- Render structured Home Assistant options to `/data/config.toml`.
- Persist AirPlay pairings and endpoint identities under `/data`.
- Publish GHCR multi-architecture images for `amd64` and `aarch64`.
