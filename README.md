# AirSonos2

AirSonos2 is an experimental Rust service that exposes legacy Sonos S2 rooms as virtual AirPlay 2 speakers. It receives AirPlay PCM through `shairplay`, encodes a live MP3 stream with `ffmpeg`, serves that stream over HTTP, and controls Sonos playback through local UPnP/SOAP.

The primary target is a Linux LXC on Proxmox with flat-LAN-like multicast behavior between iOS devices, Sonos speakers, and the AirSonos2 host.

## Status

This repository contains the first working scaffold:

- Rust workspace with `core`, `airplay`, `sonos`, `stream`, `diagnostics`, and `cli` crates.
- `shairplay = "=0.5.0"` pinned with `ap2` and `resample` features.
- Sonos SSDP discovery, device XML parsing, zone topology parsing, and SOAP control actions.
- Live stream registry, chunked MP3 HTTP routes, generated `ffmpeg` test tone, and supervised per-session `ffmpeg` encoder wrapper.
- CLI commands: `serve`, `discover`, `doctor`, `pairings list`, `pairings reset --zone`, and `calibrate --zones`.
- Docker, systemd, Nix dev shell, and GitHub Actions check workflow.

Real-device AirPlay 2 and Sonos sync acceptance still must be validated on hardware. AirPlay 2 is reverse engineered and may break with iOS updates. Sync between bridged Sonos rooms is the main goal; sync with native HomePods or native AirPlay speakers remains experimental because Sonos adds a separate HTTP pull buffer after AirPlay timing.

## Sync

When you multi-select several AirSonos2 speakers from AirPlay, sessions that start within `[sync].multi_select_window_ms` are treated as one startup cohort. AirSonos2 prepares each Sonos stream first, waits until every stream is ready or `[sync].start_deadline_ms` expires, then dispatches the Sonos `Play` commands concurrently.

With `[sync].startup_compensation = true`, AirSonos2 keeps recent per-room startup timing samples for `SetAVTransportURI`, HTTP subscriber connection, first bytes served, and `Play` command completion. Once a room has at least `[sync].startup_min_samples`, its rolling median startup lag is compared with the slowest room in the cohort and faster rooms are delayed up to `[sync].startup_max_compensation_ms`.

For `stream.codec = "wav"`, cohorts also receive a shared future playback anchor with automatic compensation plus `[sync.zone_offsets_ms]` manual room offsets. MP3 remains supported, but sync is best-effort because it cannot use sample-aligned WAV anchors. The automatic measurement is stream-arrival timing, not acoustic output; use manual offsets, or microphone calibration outside the service, for residual speaker-specific output delay.

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

Start from [docs/config.example.toml](docs/config.example.toml).

```bash
install -d ~/.local/state/airsonos2
cargo run -p airsonos2-cli -- serve --config docs/config.example.toml
```

For Sonos rooms on another VLAN where SSDP multicast is not forwarded, set known speaker
addresses under `[sonos]`:

```toml
[sonos]
auto_discover = false
static_ips = ["192.168.20.10", "192.168.20.11"]
```

You can also leave `auto_discover = true` and use `static_ips` as extra discovery seeds.

The service creates one virtual AirPlay endpoint per included Sonos room. It persists virtual endpoint identity metadata under `state_dir/endpoints` and AirPlay 2 pairing keys under `state_dir/pairings`.

For normal AirPlay 2 pairing, keep `[airplay].pin` set to the HomeKit pairing PIN and leave `rtsp_password` unset. `rtsp_password` is only for legacy RTSP digest password authentication.

## Commands

```bash
airsonos2 discover --config /etc/airsonos2/config.toml
airsonos2 doctor --config /etc/airsonos2/config.toml
airsonos2 serve --config /etc/airsonos2/config.toml
airsonos2 pairings list --config /etc/airsonos2/config.toml
airsonos2 pairings reset --zone RINCON_000E58AAAAAA01400 --config /etc/airsonos2/config.toml
airsonos2 calibrate --zones Kitchen,Office,Den --config /etc/airsonos2/config.toml
```

`pairings list` prints each pairing store file and the number of stored client keys. `pairings reset --zone` removes `state_dir/pairings/<zone-id>.json`.

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
