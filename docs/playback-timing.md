# Playback timing and compatibility

A playback epoch identifies audio on one side of a FLUSH. The receiver flush
callback advances the epoch before admitting new PCM. The bridge rejects older
epochs and replaces the downstream stream, closing the old HTTP body and stopping
the old Sonos transport before preparing the replacement. This is necessary
because bytes already buffered inside a speaker cannot be recalled from HTTP.

## Native Sonos groups

With `[sync].native_sonos_groups = true` (the default), Sonos rooms in one
cohort that play from one AirPlay sender form one native Sonos group. The first
room plays its own stream. The other rooms join it with `x-rincon:` and Sonos
keeps them sample-synced, including drift. The sender is the IP address of the
connection that set up the stream. Rooms that play their own stream, including
Home Assistant players in the cohort, wait until the joins finish (at most 2 s),
so the cohort starts together. When a sender already plays on a Sonos room, a
room that starts later joins that room. The group shows in the Sonos app.

A member leaves the group when its session stops, pauses or restarts. When the
first room stops, the other rooms restart on their own streams and form a new
group. If a join fails, the room plays its own stream on its own.

Offsets apply per group, not per Sonos room: a group uses the offset of its
first room. Offsets still align a Sonos group with Home Assistant players.

## Source timing

AirPlay 2 buffered audio carries a sender network time for each playback
anchor. The receiver maps each sender's PTP timeline to one local origin, which
all sessions from that sender share. The first session picks the origin, and it
ends with the last session. One source sample therefore has one local
presentation time in every session, although each session's SETRATEANCHORTI
arrives at a different time. A sender clock that drifts against the local clock
moves all sessions together. A mapped start more than 2 s from now is rejected,
and that session uses its own local anchor.

Realtime (AirPlay 1) audio has no presentation time. A session without timing
produces a warning and selects best-effort start: no source sample alignment is
claimed for that session.

## WAV start and offsets

WAV subscribers can receive their single header before a cohort is ready. PCM
waits until the cohort supplies a common source sample cutoff and a delay per
room. Every room uses the same source cutoff. PCM published before the
renderer connects is dropped, so a renderer that connects late, such as a Home
Assistant player after Play, starts at the live edge. A queued backlog would
stay in its buffer as extra latency.

A room's delay is a delay line: each frame is sent at its presentation time
plus the delay. A frame without presentation time uses its arrival time. A delayed start does not work. It only sends a burst of queued
audio, and the speaker's start buffer absorbs that burst. Startup buffering is
bounded, so senders must continue providing current audio during preparation.

## Manual offset migration

Positive room offsets mean a room was measured late. Faster rooms are delayed
to the largest configured latency. `default_offset_ms` is the fallback for a
room without an explicit offset, not an amount added to every room. A zone ID
entry takes precedence over a room-name entry. Calibration and playback use the
same function.

For example, offsets `Kitchen = 120`, `Office = 80`, and default `100` produce
delays of 0 ms, 40 ms, and 20 ms respectively. Negative offsets are
valid relative measurements. Old configurations that treated positive offsets
as direct added delays must be recalibrated using this convention.

MP3 remains the default codec. It does not implement the WAV sample alignment
or offset mechanism, and configuration of offsets emits a warning. Native Sonos
groups work with MP3. There is no automatic compensation, and old `startup_*`
settings are ignored: SOAP round trips and HTTP body consumption are not
measurements of acoustic latency.

These rules establish sample-level behavior. Actual speaker skew and drift need
microphone measurements on named senders and speakers before any audible sync
claim can be made. See the release acceptance record for verified configurations.
