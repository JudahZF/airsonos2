//! Public types and traits for the AirPlay server.

use std::sync::Arc;

/// Runtime protocol mode selection.
///
/// When the `ap2` feature is enabled, this controls whether the server
/// advertises itself as an AirPlay 1 (classic) or AirPlay 2 receiver.
/// Both modes share the same RTSP listener — the difference is in mDNS
/// advertisement and feature negotiation.
#[cfg(feature = "ap2")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AirPlayMode {
    /// Classic AirPlay 1: ALAC/AAC over RTP, RSA encryption, NTP timing.
    AirPlay1,
    /// AirPlay 2: buffered audio, ChaCha20 encryption, SRP pairing, PTP timing.
    #[default]
    AirPlay2,
}

/// Audio codec type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioCodec {
    /// Decoded PCM (f32 interleaved). Always delivered regardless of AP1/AP2.
    Pcm,
}

/// Audio format descriptor passed to [`AudioHandler::audio_init`].
#[derive(Debug, Clone, Copy)]
pub struct AudioFormat {
    /// Audio codec (always PCM for decoded output).
    pub codec: AudioCodec,
    /// Bits per sample (always 32 — samples are delivered as `&[f32]`).
    pub bits: u8,
    /// Number of channels.
    pub channels: u8,
    /// Sample rate in Hz.
    pub sample_rate: u32,
}

/// Trait for receiving AirPlay events and creating audio sessions.
///
/// `AudioHandler` is `Send + Sync` — all callbacks can be called from any thread
/// without blocking audio delivery. Metadata, volume, and artwork callbacks are
/// called directly from the RTSP handler thread, never from the audio path.
///
/// A new [`AudioSession`] is created for each audio stream via
/// [`audio_init`](AudioHandler::audio_init). The session only receives PCM samples.
pub trait AudioHandler: Send + Sync + 'static {
    /// Called when a new audio stream starts. Return a session to receive PCM data.
    fn audio_init(&self, format: AudioFormat) -> Box<dyn AudioSession>;

    // --- Metadata events (called from RTSP thread, never blocks audio) ---

    /// Volume change in dB (0.0 = max, -144.0 = mute).
    fn on_volume(&self, _volume: f32) {}
    /// Current receiver volume in dB for RTSP `GET_PARAMETER volume` queries.
    fn current_volume_db(&self) -> f32 {
        0.0
    }
    /// Track metadata (parsed from DMAP).
    fn on_metadata(&self, _metadata: &crate::proto::dmap::TrackMetadata) {}
    /// Album artwork (JPEG or PNG).
    fn on_coverart(&self, _coverart: &[u8]) {}
    /// Playback progress (start, current, end in RTP timestamps at 44100 Hz).
    fn on_progress(&self, _start: u32, _current: u32, _end: u32) {}
    /// A remote control interface is available (AP1 DACP).
    fn on_remote_control(&self, _remote: Arc<dyn RemoteControl>) {}

    // --- Connection lifecycle ---

    /// Called when a client connects.
    fn on_client_connected(&self, _addr: &str) {}
    /// Called when a client disconnects.
    fn on_client_disconnected(&self, _addr: &str) {}
    /// Playback rate changed (AP2 `SETRATEANCHORTIME`). `playing` is true when rate is non-zero.
    fn on_playback_rate(&self, _playing: bool) {}
    /// Called on runtime errors. Default: log at warn level.
    fn on_error(&self, error: &crate::error::ShairplayError) {
        tracing::warn!(%error, "AirPlay error");
    }
}

/// Storage for paired device keys. Implement this to persist pairing across restarts.
///
/// Without persistence, iPhones that previously paired will send encrypted data
/// on connect and fail because the server has no cached keys.
#[cfg(feature = "ap2")]
pub trait PairingStore: Send + Sync + 'static {
    /// Look up a paired device's Ed25519 public key by device ID.
    fn get(&self, device_id: &str) -> Option<[u8; 32]>;
    /// Save a paired device's Ed25519 public key.
    fn put(&self, device_id: &str, public_key: [u8; 32]);
    /// Remove a paired device.
    fn remove(&self, device_id: &str);
}

/// In-memory pairing store (lost on restart). Use for testing or wrap with file I/O.
#[cfg(feature = "ap2")]
#[derive(Default)]
pub struct MemoryPairingStore {
    keys: std::sync::Mutex<std::collections::HashMap<String, [u8; 32]>>,
}

#[cfg(feature = "ap2")]
impl PairingStore for MemoryPairingStore {
    fn get(&self, device_id: &str) -> Option<[u8; 32]> {
        self.keys.lock().ok()?.get(device_id).copied()
    }
    fn put(&self, device_id: &str, public_key: [u8; 32]) {
        if let Ok(mut keys) = self.keys.lock() {
            keys.insert(device_id.to_string(), public_key);
        }
    }
    fn remove(&self, device_id: &str) {
        if let Ok(mut keys) = self.keys.lock() {
            keys.remove(device_id);
        }
    }
}

/// Why an [`AudioSession`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioStopReason {
    /// RTSP `TEARDOWN` or an explicit stop command from the client.
    Teardown,
    /// Buffered audio TCP stream closed while playback was paused.
    StreamEndedWhilePaused,
    /// Buffered audio TCP stream closed during active playback.
    StreamEnded,
}

/// Per-connection audio session — hot path only.
///
/// Created by [`AudioHandler::audio_init`]. Dropped when the client disconnects.
/// Only receives decoded PCM samples and flush events. All metadata, volume,
/// and artwork events go to [`AudioHandler`] instead.
pub trait AudioSession: Send + Sync {
    /// Receive decoded f32 interleaved PCM audio samples.
    fn audio_process(&mut self, samples: &[f32]);
    /// PCM with a validated source presentation time in the local monotonic clock.
    /// `None` means the source clock mapping is unavailable; callback arrival is
    /// not a presentation timestamp. Existing clients retain the PCM callback.
    fn audio_process_timed(&mut self, samples: &[f32], _presentation_time: Option<std::time::Instant>) {
        self.audio_process(samples);
    }
    /// Offer buffered PCM without dropping source-timed audio when downstream is full.
    /// Return false without consuming any samples to retry this frame later. The
    /// receiver keeps it in its bounded queue and propagates TCP backpressure.
    fn audio_process_buffered(&mut self, samples: &[f32], presentation_time: Option<std::time::Instant>) -> bool {
        self.audio_process_timed(samples, presentation_time);
        true
    }
    /// Flush the audio buffer (e.g. on seek).
    fn audio_flush(&mut self) {}
    /// Called when the audio stream ends, before the session is dropped.
    fn audio_stopped(&mut self, _reason: AudioStopReason) {}
}

/// Playback command to send to the source device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteCommand {
    /// Start playback.
    Play,
    /// Pause playback.
    Pause,
    /// Skip to next track.
    NextTrack,
    /// Skip to previous track.
    PreviousTrack,
    /// Set volume (0-100).
    SetVolume(u8),
    /// Toggle shuffle mode.
    ToggleShuffle,
    /// Toggle repeat mode.
    ToggleRepeat,
    /// Stop playback.
    Stop,
}

/// Unified remote control interface for AP1 (DACP) and AP2 (MediaRemote).
pub trait RemoteControl: Send + Sync {
    /// Send a playback command to the source device.
    fn send_command(&self, cmd: RemoteCommand) -> Result<(), crate::error::ShairplayError>;
    /// Commands the source device supports. AP1 returns all; AP2 returns advertised set.
    fn available_commands(&self) -> Vec<RemoteCommand>;
}
