# Hardware acceptance

Status: **unverified**. No physical sender, Sonos speaker, microphone recording,
or one-hour multi-room soak was available for this reliability implementation.
Automated sample tests and successful SOAP requests do not establish audible
synchronization or sender compatibility.

## Acceptance criteria

Before marking acoustic acceptance passed, the product owner must specify:

- Maximum pairwise startup skew, in milliseconds.
- Maximum pairwise skew after sustained playback, in milliseconds.
- Maximum drift over the soak interval, in milliseconds per hour.
- Allowed interruption and recovery time after a speaker disconnect or service restart.

These limits are **not defined yet**. Do not replace a missing limit with a passing
result. MP3 does not implement WAV sample alignment or manual offsets. Missing
source timing uses best-effort release. See [playback timing](playback-timing.md)
for the offset convention and compatibility limits.

## Configuration record

Create one record for each tested sender/receiver/codec combination. Keep the
configuration and raw measurements with the record; redact credentials and
pairing secrets.

| Field | Value to record |
| --- | --- |
| Build | Full Git revision, package/image digest, build platform |
| Sender | Device, OS version, app and version |
| Receiver path | AP1 or AP2; realtime or buffered; negotiated source/output formats |
| Rooms | Stable zone IDs, names, Sonos models and firmware versions |
| Network | Wired/Wi-Fi, relevant topology, host platform |
| Configuration | Codec, bitrate, sample rate, channel count, offsets, queue/startup settings |
| Authentication | Pairing/PIN policy, separate control connections, restart persistence |
| Capture | Test waveform hash, microphone/recorder model, sample rate, file location |
| Result | Start/end time, raw measurements, failures, logs, pass/fail/unverified |

## Playback coverage

Every row remains unverified until the matching configuration record exists.
Run Apple Music, Podcasts, YouTube, and Spotify through AirPlay where supported;
record unsupported or unavailable combinations explicitly.

| Codec | One room | Two rooms | Three rooms | Six rooms |
| --- | --- | --- | --- | --- |
| MP3 | Unverified | Unverified | Unverified | Unverified |
| WAV | Unverified | Unverified | Unverified | Unverified |

For each available combination, check endpoint discovery, correct room volume,
pause/resume, seek/FLUSH, sender reconnect, session replacement, and stopping the
correct room. Confirm that an unrelated room continues playing during another
room's slow response or disconnection. Check persistent pairing after restart
and legitimate authenticated control from a separate connection. Run `doctor`
on the actual deployment network, including Proxmox LXC when used.

## Repeatable acoustic experiment

1. Put at least two speakers within range of the same recorder. Record microphone
   distances so propagation delay can be reported separately from speaker delay.
2. Send the same click/sample sequence to both virtual endpoints. Keep the source
   waveform, sender settings, and capture settings constant between builds.
3. Capture repeated cold starts and report each room's first audible click time,
   pairwise skew distribution, and sample count. Do not use Play RTT as output time.
4. Continue for at least one hour. Measure fixed latency separately from drift;
   report skew at startup and at regular intervals across the capture.
5. During the soak, pause/resume, seek, disconnect one speaker, and restart the
   service. Record stale audio, dropouts, recovery times, and effects on other rooms.
6. Compare measurements with the agreed limits above. Attach raw audio and analysis,
   plus CPU/RSS/queue/drop logs for the same interval.

Current acoustic result: **unverified**. Current one-hour physical soak: **unverified**.
