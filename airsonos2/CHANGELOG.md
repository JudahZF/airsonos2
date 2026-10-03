# Changelog

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
