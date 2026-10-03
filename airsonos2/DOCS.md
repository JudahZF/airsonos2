# AirSonos2 Home Assistant App

## Networking

AirSonos2 uses host networking. This is intentional: AirPlay discovery and Sonos control depend on LAN multicast and device-to-device HTTP traffic that is unreliable through ordinary bridged container networking.

## Home Assistant and Music Assistant players

AirSonos2 can expose Home Assistant `media_player` entities as AirPlay 2 speakers. Music Assistant players are `media_player` entities too, so they work the same way. Add the entity ids to `ha_media_players`:

```yaml
ha_media_players:
  - media_player.kitchen_cast
  - media_player.office_music_assistant
```

When an AirPlay session starts, AirSonos2 calls `media_player.play_media` with the stream URL. AirPlay volume uses `media_player.volume_set`, and pause and disconnect use `media_player.media_stop`. The app uses the Home Assistant API through the Supervisor, so you do not need a token. The app starts after Home Assistant Core and reads the player names once at startup.

The speaker, or Music Assistant, fetches the stream from this host. For these players, AirSonos2 uses the address of the default route. If the speaker cannot reach that address, set `advertise_addr`.

Limits:

- Pick the entity of the player itself. The same speaker can show up as several entities, for example a native Sonos entity, a Cast entity, and a Music Assistant entity. Each entity id that you add becomes its own AirPlay speaker.
- Cast devices and Music Assistant add their own buffer. In a multi-select group with Sonos rooms, expect drift. With Music Assistant, the drift can be several seconds.
- Music Assistant 2.10 had a bug where `play_media` with a plain URL played a different track. If this happens, use the player's own entity, such as its Cast entity, instead of the Music Assistant entity.

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

Set `advertise_addr` to the Home Assistant host address that speakers should use when pulling audio streams if automatic route-based address detection is unavailable or chooses the wrong interface. The stream listener binds `http_bind`, so the address must use the same family: the default `0.0.0.0` needs an IPv4 address, and an IPv6 address needs `http_bind: "::"`.

```yaml
advertise_addr: 192.168.1.10
```

Use `static_ips` when SSDP multicast does not reach every Sonos speaker. Use `include_rooms` and `exclude_rooms` to limit which visible Sonos rooms receive AirPlay endpoints. Set `auto_discover` to `false` and leave `static_ips` empty if you only use Home Assistant players.

`stream_codec` supports `mp3` and `wav`. MP3 is the compatibility default. WAV can reduce startup latency when your Sonos devices accept it.

`zone_offsets` accepts entries like:

```yaml
- zone: Kitchen
  offset_ms: 120
- zone: Office
  offset_ms: -40
```

The `zone` value should match the Sonos room name or zone id, or the Home Assistant player name or entity id.

## Diagnostics

Enable `run_doctor_on_start` to run startup diagnostics without blocking bridge startup. You can also open the app terminal and run:

```bash
airsonos2 doctor --config /data/config.toml
```

The stream HTTP port (7000) serves:

```text
/healthz
/test-tone.mp3
/streams/<session>
```

The diagnostics listener (`diagnostics_addr`) serves `/healthz` and `/metrics`.

## Listener addresses and diagnostics

`http_bind` selects the local IP address used by stream HTTP and the AirPlay listeners (default `0.0.0.0`). It must be reachable from the speakers and Home Assistant Supervisor. The HTTP port stays at 7000 so the Supervisor watchdog and stream health endpoint agree. `diagnostics_addr` accepts an IP address and port, such as `127.0.0.1:9201`, for detailed `/metrics` and `/healthz`. IPv6 addresses use brackets: `[::1]:9201`. These options are checked by the same Rust validation as file configuration.

The renderer rejects unsupported codecs and output formats. Output supports one or two channels at 8000–192000 Hz; a short doctor MP3 encode checks the installed encoder against the configured rate and bitrate. Doctor also checks the configured listener addresses and actual filtered room ports. Hardware visibility and playback remain separate acceptance checks.
