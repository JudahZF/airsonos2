# Reliability release checks

This record separates automated validation from physical playback acceptance.
The implementation baseline is documented in [reliability-baseline.md](reliability-baseline.md).
The recorded source checks passed. Native arm64 CI and physical acceptance remain
separate gates; their status is recorded below.

## Reproduce from tracked source

Clone the revision being reviewed into an empty directory. Run the pinned Nix
shell there; an external `CARGO_TARGET_DIR` may reuse compiled dependencies but
must not supply source files or generated configuration missing from the clone.

```sh
nix develop -c bash scripts/check-reliability.sh
# Also build and smoke-test both images on the Docker host architecture:
nix develop -c bash scripts/check-reliability.sh --packages
```

The gate checks formatting, all-target Clippy, locked workspace tests, standalone
receiver default/AP2-resample/video-HLS tests, workspace and receiver dependency
policy, and release-version validation. It builds the actual daemon and repeats
SIGTERM, occupied HTTP listener, and occupied diagnostics listener scenarios with
a local fake Sonos endpoint. The fixture checks readiness, metrics placement,
idle RTSP closure, exit status, and release of all owned listener ports.

The process fixture uses temporary state, private Linux loopback addresses, and
`CI=true` to suppress mDNS advertisements. It does not contact physical speakers.
Three complete cycles are the default; use `--cycles` for a longer process soak:

```sh
nix develop -c python3 scripts/test-process-lifecycle.py --binary target/debug/airsonos2 --cycles 20
```

Unit and protocol regressions cover authenticated controls, malformed packets,
ALAC sample counts, bounded paused audio, FLUSH sample epochs, HTTP EOF/header
races, failed encoder construction, delayed SOAP isolation, latest volume,
stale generations, retry limits, and cohort promotion. These tests establish
software invariants; they do not measure acoustic delivery.

## Automated acceptance record

| Item | Result |
| --- | --- |
| Tested source revision | `fe135a7a3e9efbdd4763684f9005b062dfc30500` |
| Clean tracked-source checkout | Passed; no tracked changes or ignored files before or after the gate |
| Workspace format, Clippy, tests | Passed with the pinned Rust 1.88 Nix shell |
| Receiver default / AP2-resample / video-HLS | All three passed with `--locked` |
| Workspace and receiver dependency policy | Advisories, bans, licenses, and sources passed for both manifests |
| Real daemon repeated SIGTERM and listener failures | Nine scenarios passed across three complete cycles |
| Generic container, Linux amd64 | Build and binary version smoke test passed |
| Home Assistant container, Linux amd64 | Build and binary version smoke test passed |
| Generic container, Linux arm64 | Pending native CI |
| Home Assistant container, Linux arm64 | Pending native CI |
| Release tag / Cargo / Home Assistant version consistency | Gate regression passed; no release tag created or published |

Validation ran on x86_64 Linux on 2026-09-22 from a fresh local clone, using an
external Cargo build cache. `scripts/check-reliability.sh --packages` exited zero.
All three idle-session SIGTERM measurements were approximately 1.1 ms to process
exit; these are local fixture observations, not active-playback or hardware latency.
Both container smoke tests reported `airsonos2 0.1.0`.

| Local image | Image ID (`linux/amd64`) |
| --- | --- |
| `airsonos2-reliability:generic` | `sha256:b66f9637a3dc8a7a2c05b5e4591b739163877494279aa8f4555a3a399fff825a` |
| `airsonos2-reliability:home-assistant` | `sha256:cf522066074ac27cf937246b8674395933e83c8e10053637d269f729c4b01867` |

Container builds and smoke tests must identify architecture and image digest.
A host-only build does not validate another architecture. Publication must use
checks for the same Git commit and must not bypass failed acceptance gates.

## Physical compatibility and limits

All physical playback, sender compatibility, acoustic skew/drift, pairing
persistence on real senders, and the one-hour multi-room soak are **unverified**.
The required configuration fields, playback matrix, and measurement procedure
are in [hardware-acceptance.md](hardware-acceptance.md). Acceptable acoustic limits
must be set before sync acceptance can be marked passed.

MP3 remains supported without the WAV alignment/offset mechanism. Unavailable
source presentation timing selects a visible best-effort fallback. Manual offset
semantics changed; existing installations must follow the migration example in
[playback-timing.md](playback-timing.md). Legacy AP1 access now requires explicit
credentials; see the [receiver compatibility notes](../vendor/shairplay/README.md).
