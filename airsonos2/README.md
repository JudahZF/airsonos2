# AirSonos2

AirSonos2 exposes legacy Sonos S2 rooms as virtual AirPlay 2 speakers. This Home Assistant app runs the AirSonos2 bridge with host networking so SSDP, mDNS, AirPlay, and Sonos HTTP pull streams can all use the local LAN directly.

Pairing stores, generated endpoint identities, and the rendered runtime configuration live under `/data`, so Home Assistant backups and app restarts preserve them.

## Install

Add this repository to the Home Assistant app store:

```text
https://github.com/judahfuller/airsonos2
```

Install the AirSonos2 app and start it. The app pulls the published multi-architecture image from:

```text
ghcr.io/judahfuller/airsonos2
```

## Runtime

On startup the app renders Home Assistant options to `/data/config.toml`, optionally runs `airsonos2 doctor --config /data/config.toml`, then starts `airsonos2 serve --config /data/config.toml`. If Sonos cannot reach automatically inferred stream URLs, set `advertise_addr` to the Home Assistant host's IPv4 or IPv6 LAN address.

The health endpoint is available at:

```text
http://<home-assistant-host>:7000/healthz
```
