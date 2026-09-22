# Playback timing and compatibility

A playback epoch identifies audio on one side of a FLUSH. The receiver flush
callback advances the epoch before admitting new PCM. The bridge rejects older
epochs and replaces the downstream stream, closing the old HTTP body and stopping
the old Sonos transport before preparing the replacement. This is necessary
because bytes already buffered inside a speaker cannot be recalled from HTTP.

PCM presentation time is optional. A receiver supplies it only when source RTP
presentation time can be mapped to the local monotonic clock. Callback arrival
is not presentation time. Missing timing produces a warning and selects
best-effort release: no source sample alignment is claimed for that session.

WAV subscribers can receive their single header before a cohort is ready. PCM
waits until the cohort supplies a common source sample cutoff and a local release
deadline. Every room uses the same source cutoff. A manual delay moves only the
release deadline; it does not remove extra source samples. Startup buffering is
bounded, so senders must continue providing current audio during preparation.

## Manual offset migration

Positive room offsets mean a room was measured late. Faster rooms are delayed
to the largest configured latency. `default_offset_ms` is the fallback for a
room without an explicit offset, not an amount added to every room. A zone ID
entry takes precedence over a room-name entry. Calibration and playback use the
same function.

For example, offsets `Kitchen = 120`, `Office = 80`, and default `100` produce
release delays of 0 ms, 40 ms, and 20 ms respectively. Negative offsets are
valid relative measurements. Old configurations that treated positive offsets
as direct added delays must be recalibrated using this convention.

MP3 remains the default codec. It does not implement the WAV sample alignment
or offset mechanism, and configuration of offsets emits a warning. Automatic
compensation is disabled, including when old configurations request it: SOAP
round trips and HTTP body consumption are not measurements of acoustic latency.

These rules establish sample-level behavior. Actual speaker skew and drift need
microphone measurements on named senders and speakers before any audible sync
claim can be made. See the release acceptance record for verified configurations.
