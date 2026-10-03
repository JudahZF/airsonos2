# AirSonos2

AirSonos2 is an experimental Rust service that exposes legacy Sonos S2 rooms and Home Assistant media players as virtual AirPlay 2 speakers. It receives AirPlay PCM through `shairplay`, encodes a live MP3 stream with `ffmpeg`, and serves that stream over HTTP. It controls Sonos playback through local UPnP/SOAP and Home Assistant players through the Home Assistant REST API.

The primary target is a Linux LXC on Proxmox with flat-LAN-like multicast behavior between iOS devices, Sonos speakers, and the AirSonos2 host.

## Status

This repository contains the first working scaffold:

- Rust workspace with `core`, `airplay`, `sonos`, `homeassistant`, `stream`, `diagnostics`, and `cli` crates.
- `shairplay = "=0.5.0"` pinned with `ap2` and `resample` features.
- Sonos SSDP discovery, device XML parsing, zone topology parsing, and SOAP control actions.
- Home Assistant `media_player` control (`play_media`, `media_stop`, `volume_set`), which also covers Music Assistant players.
- Live stream registry, chunked MP3 HTTP routes, generated `ffmpeg` test tone, and supervised per-session `ffmpeg` encoder wrapper.
- CLI commands: `serve`, `discover`, `doctor`, `pairings list`, `pairings reset --zone`, and `calibrate --zones`.
- Docker, systemd, Nix dev shell, and GitHub Actions check workflow.

Real-device AirPlay 2 and Sonos sync acceptance still must be validated on hardware. AirPlay 2 is reverse engineered and may break with iOS updates. Sync between bridged Sonos rooms is the main goal; sync with native HomePods or native AirPlay speakers remains experimental because Sonos adds a separate HTTP pull buffer after AirPlay timing.

## Sync

When you multi-select several AirSonos2 speakers from AirPlay, sessions that start within `[sync].multi_select_window_ms` are treated as one startup cohort. AirSonos2 prepares each Sonos stream first, waits until every stream is ready or `[sync].start_deadline_ms` expires, then dispatches the Sonos `Play` commands concurrently.

There is no automatic startup compensation. SOAP round trips and HTTP stream timings are not acoustic latency measurements, so the former `[sync].startup_*` settings were removed. Old configs that still set them load normally and the keys are ignored.

For `stream.codec = "wav"`, cohorts also receive a shared future playback anchor with `[sync.zone_offsets_ms]` manual room offsets. MP3 remains supported, but sync is best-effort because it cannot use sample-aligned WAV anchors. Measure speaker-specific output delay with a microphone outside the service and set it as manual offsets.

## Development

Use the Nix dev shell:

```bash
nix develop
cargo fmt
cargo clippy --workspace --all-features -- -D warnings
cargo test --workspace --all-features
docker build -f packaging/docker/Dockerfile .
```

One-shot checks:

```bash
nix develop -c cargo fmt --check
nix develop -c cargo check --workspace --all-features
nix develop -c cargo clippy --workspace --all-features -- -D warnings
nix develop -c cargo test --workspace --all-features
nix develop -c docker build -f packaging/docker/Dockerfile .
```

## Configuration

Start from [docs/config.example.toml](docs/config.example.toml). When `[server].bind` is unspecified, AirSonos2 normally infers the local address used to reach each Sonos zone. Set `[server].advertise_addr` to a specific IPv4 or IPv6 LAN address when inference is unavailable or the host has multiple interfaces and Sonos must use a particular one. An IPv6 `advertise_addr` also needs `bind = "::"` so the stream listener accepts IPv6.

```bash
sudo install -d -o "$(id -un)" -g "$(id -gn)" /var/lib/airsonos2
cargo run -p airsonos2-cli -- serve --config docs/config.example.toml
```

For Sonos rooms on another VLAN where SSDP multicast is not forwarded, set known speaker
addresses under `[sonos]`:

```toml
[sonos]
auto_discover = false
static_ips = ["192.168.20.10", "192.168.20.11"]
```

You can also leave `auto_discover = true` and use `static_ips` as fallback discovery seeds, including when multicast fails. Discovery has one overall deadline, tries up to four seeds at once, and uses the first nonempty topology. Only topology-confirmed visible rooms are advertised; nested satellite speakers stay hidden. Endpoints are sorted by stable zone ID so discovery response order does not change RTSP assignments.

The source command above uses the example's `/var/lib/airsonos2` and assigns it to the current user. For the systemd service, create the `airsonos2` service user and use `sudo install -d -o airsonos2 -g airsonos2 /var/lib/airsonos2` instead; systemd also manages this directory through `StateDirectory=airsonos2`. Run doctor as the same user that runs the service.

### Home Assistant media players

AirSonos2 can also expose Home Assistant `media_player` entities, including Music Assistant players, as AirPlay 2 speakers:

```toml
[home_assistant]
url = "http://homeassistant.local:8123"
token = "<long-lived access token>"
media_players = ["media_player.kitchen_cast"]
```

On each AirPlay session, AirSonos2 calls `media_player.play_media` with its stream URL. The player fetches the stream from AirSonos2. Home Assistant does not report a player's IP, so AirSonos2 advertises the address of its default route; set `[server].advertise_addr` if the player cannot reach it. Run `airsonos2 discover` to list the entities that support `play_media`. Cast and Music Assistant add their own buffer, so expect drift when you group them with Sonos rooms. Home Assistant player endpoints come after the Sonos rooms in RTSP port order.

When AirSonos2 cannot read a speaker's volume at startup, it reports 20% to AirPlay. Home Assistant hides the volume of players that are off.

The service creates one virtual AirPlay endpoint per included Sonos room and per Home Assistant player. It persists virtual endpoint identity metadata under `state_dir/endpoints` and AirPlay 2 pairing keys under `state_dir/pairings`.

For normal AirPlay 2 pairing, keep `[airplay].pin` set to the HomeKit pairing PIN and leave `rtsp_password` unset. `rtsp_password` is only for legacy RTSP digest password authentication.

### Web GUI

`serve` hosts a config editor at `http://<host>:9100/`, on `[diagnostics].metrics_addr`. It validates changes with the same rules as the file, then replaces the config file atomically. Changes apply after a restart. The **Restart** button stops the bridge cleanly and exits with code 75, so systemd `Restart=on-failure` or a Docker restart policy starts it again. Saving rewrites the file as plain TOML, so comments in it are lost.

The GUI has no authentication. Anyone who can reach the diagnostics listener can change settings and restart the bridge. Set `metrics_addr` to a loopback or management address if the LAN is not trusted. The GUI never sends the Home Assistant token or the RTSP password to the browser; it only shows whether each one is set.

The service user must be able to write the config file and create files in its directory. For the systemd unit, for example: `sudo chown airsonos2:airsonos2 /etc/airsonos2 /etc/airsonos2/config.toml`.

The GUI starts before discovery. When no Sonos room or Home Assistant player can start, `serve` keeps the GUI up and retries every 10 seconds instead of exiting. Pass `--config-read-only` when another tool generates the file; the Home Assistant app does this.

## Commands

```bash
airsonos2 discover --config /etc/airsonos2/config.toml
airsonos2 doctor --config /etc/airsonos2/config.toml
airsonos2 serve --config /etc/airsonos2/config.toml
airsonos2 pairings list --config /etc/airsonos2/config.toml
airsonos2 pairings reset --zone RINCON_000E58AAAAAA01400 --config /etc/airsonos2/config.toml
airsonos2 calibrate --zones Kitchen,Office,Den --config /etc/airsonos2/config.toml
```

`pairings list` prints each pairing store file and the number of stored client keys. `pairings reset --zone` removes `state_dir/pairings/<zone-id>.json`. For a Home Assistant player, the zone id is its entity id.

## Troubleshooting

For AirPlay connection debugging, stop any running service and run:

```bash
RUST_LOG=airsonos2=debug,airsonos2_airplay=debug,shairplay=debug \
  nix develop -c cargo run -p airsonos2-cli -- serve --config config.toml
```

Run `doctor` while `serve` is stopped when checking port availability. If `serve` is running, occupied ports such as `7020`, `5020`, `5021`, and later per-zone RTSP ports are expected. A recent local check showed `airsonos2` listening on `7020`, `5020`, and `5021`.

If an AirPlay client reports "failed to connect" and the logs show `Max connections reached` during AirPlay 2 setup, raise `[airplay].max_clients_per_zone`. AirPlay 2 can open multiple RTSP/event connections for one playback session, so the default is `10`.

For playback delay or pause/resume glitches, run with debug logging and confirm the bridge is keeping sessions alive across pause and flush:

```bash
RUST_LOG=airsonos2=debug,airsonos2_airplay=debug,shairplay=debug \
  nix develop -c cargo run -p airsonos2-cli -- serve --config config.toml
```

Pause stops the affected room. Resume prepares a new downstream generation. FLUSH keeps the AirPlay session but closes its old HTTP body and replaces the Sonos stream so queued old audio is discarded. A slow room's network requests run independently from other rooms' PCM.

WAV headers are available during preparation, but PCM waits for the cohort's release. The MP3 path remains the compatibility default. Network and HTTP timings do not measure acoustic latency, and automatic sync compensation is disabled.

See [playback timing and offset migration](docs/playback-timing.md) before reusing existing room offsets. Positive offsets describe rooms measured late; `default_offset_ms` is only a fallback. WAV uses the normalized release delays; MP3 does not support this sample alignment mechanism. Missing source timing produces a visible best-effort fallback.

## License Notice

AirSonos2 is licensed as `MIT OR Apache-2.0`. The AirPlay receiver dependency `shairplay` is licensed `LGPL-3.0-or-later`; downstream distributors should review the LGPL obligations for their packaging model.

## Operational checks

`doctor` validates the selected codec, tests a short MP3 encode with a five-second deadline, and skips FFmpeg for native WAV. It discovers and filters the configured rooms before testing their exact RTSP port range, the HTTP listener, and the diagnostics listener. It writes and removes temporary probes in the state, endpoint and pairing directories to check the runtime user's access. Port checks require the service to be stopped.

Stream HTTP exposes `/healthz` for container and Home Assistant health checks. Detailed `/metrics` is available only on `[diagnostics].metrics_addr` (default `0.0.0.0:9100`). Set that address to a local or management interface when needed. Device discovery and SOAP connect directly, ignore ambient HTTP proxies, refuse redirects, and bound response sizes.

Release publication runs the reusable CI checks on the tagged commit. The tag, workspace Cargo version, and Home Assistant version must agree before either architecture is published.
