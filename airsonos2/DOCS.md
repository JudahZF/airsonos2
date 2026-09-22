# AirSonos2 Home Assistant App

## Networking

AirSonos2 uses host networking. This is intentional: AirPlay discovery and Sonos control depend on LAN multicast and device-to-device HTTP traffic that is unreliable through ordinary bridged container networking.

## Configuration

The app exposes flat Home Assistant options and renders them to `/data/config.toml` before the bridge starts. Runtime-only values are forced for Home Assistant:

```toml
[server]
bind = "0.0.0.0"
http_port = 7000
state_dir = "/data"

[stream]
ffmpeg_path = "/usr/bin/ffmpeg"

[diagnostics]
metrics_addr = "0.0.0.0:9100"
```

Use `static_ips` when SSDP multicast does not reach every Sonos speaker. Use `include_rooms` and `exclude_rooms` to limit which visible Sonos rooms receive AirPlay endpoints.

`stream_codec` supports `mp3` and `wav`. MP3 is the compatibility default. WAV can reduce startup latency when your Sonos devices accept it.

`zone_offsets` accepts entries like:

```yaml
- zone: Kitchen
  offset_ms: 120
- zone: Office
  offset_ms: -40
```

The `zone` value should match the Sonos room name or zone id used by the bridge.

## Diagnostics

Enable `run_doctor_on_start` to run startup diagnostics without blocking bridge startup. You can also open the app terminal and run:

```bash
airsonos2 doctor --config /data/config.toml
```

The bridge serves:

```text
/healthz
/metrics
/test-tone.mp3
/streams/<session>
```

## Listener addresses and diagnostics

`http_bind` selects the local IP address used by stream HTTP and the AirPlay listeners (default `0.0.0.0`). It must be reachable from the speakers and Home Assistant Supervisor. The HTTP port stays at 7000 so the Supervisor watchdog and stream health endpoint agree. `diagnostics_addr` accepts an IP address and port, such as `127.0.0.1:9201`, for detailed `/metrics` and `/healthz`. IPv6 addresses use brackets: `[::1]:9201`. These options are checked by the same Rust validation as file configuration.

The renderer rejects unsupported codecs and output formats. Output supports one or two channels at 8000–192000 Hz; a short doctor MP3 encode checks the installed encoder against the configured rate and bitrate. Doctor also checks the configured listener addresses and actual filtered room ports. Hardware visibility and playback remain separate acceptance checks.
