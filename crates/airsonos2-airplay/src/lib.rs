use std::collections::BTreeMap;
use std::fs;
use std::io::{ErrorKind, Write};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use airsonos2_core::{
    AirPlayConfig, PcmFrame, SessionId, VirtualAirPlayEndpoint, ZoneId, airplay_db_to_sonos_volume,
};
use shairplay::{
    AirPlayMode, AudioFormat, AudioHandler, AudioSession, AudioStopReason, BindConfig,
    PairingStore, RaopServer,
};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{debug, warn};

#[derive(Clone, Debug)]
pub enum AirPlayEvent {
    SessionStarted {
        session_id: SessionId,
        zone_id: ZoneId,
        format: PcmFormat,
    },
    Pcm {
        session_id: SessionId,
        zone_id: ZoneId,
        frame: PcmFrame,
    },
    PlaybackState {
        session_id: SessionId,
        zone_id: ZoneId,
        playing: bool,
    },
    Flushed {
        session_id: SessionId,
        playback_epoch: u64,
        zone_id: ZoneId,
    },
    Volume {
        zone_id: ZoneId,
        airplay_db: f32,
        sonos_volume: u8,
    },
    SessionStopped {
        session_id: SessionId,
        zone_id: ZoneId,
    },
    /// Buffered audio stream closed while AirPlay playback was paused.
    StreamEndedWhilePaused {
        session_id: SessionId,
        zone_id: ZoneId,
    },
    ClientConnected {
        zone_id: ZoneId,
        addr: String,
    },
    ClientDisconnected {
        zone_id: ZoneId,
        addr: String,
    },
    Error {
        zone_id: ZoneId,
        message: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PcmFormat {
    pub sample_rate: u32,
    pub channels: u8,
    pub bits: u8,
}

impl From<AudioFormat> for PcmFormat {
    fn from(value: AudioFormat) -> Self {
        Self {
            sample_rate: value.sample_rate,
            channels: value.channels,
            bits: value.bits,
        }
    }
}

#[derive(Clone)]
pub struct ZoneVolumeState {
    volume_db: Arc<AtomicU32>,
}

impl ZoneVolumeState {
    pub fn new(initial_volume_db: f32) -> Self {
        Self {
            volume_db: Arc::new(AtomicU32::new(initial_volume_db.to_bits())),
        }
    }

    pub fn set_volume_db(&self, volume_db: f32) {
        let volume_db = if volume_db.is_finite() {
            volume_db
        } else {
            -144.0
        };
        self.volume_db.store(volume_db.to_bits(), Ordering::Relaxed);
    }

    pub fn volume_db(&self) -> f32 {
        f32::from_bits(self.volume_db.load(Ordering::Relaxed))
    }
}

pub struct AirPlayEndpointRunner {
    zone_id: ZoneId,
    display_name: String,
    server: RaopServer,
    volume_state: ZoneVolumeState,
}

impl AirPlayEndpointRunner {
    pub fn build(
        endpoint: &VirtualAirPlayEndpoint,
        config: &AirPlayConfig,
        bind_addr: IpAddr,
        events: mpsc::UnboundedSender<AirPlayEvent>,
        volume_state: ZoneVolumeState,
    ) -> Result<Self, AirPlayError> {
        let handler = Arc::new(BridgeAudioHandler {
            zone_id: endpoint.zone_id.clone(),
            events,
            volume_state: volume_state.clone(),
            active_session: Arc::new(Mutex::new(None)),
        });
        let pairing_store = Arc::new(FilePairingStore::load(&endpoint.pairing_store_path)?);
        let rtsp_password_enabled = config.rtsp_password_enabled();

        debug!(
            zone_id = %endpoint.zone_id,
            display_name = %endpoint.display_name,
            rtsp_port = endpoint.rtsp_port,
            %bind_addr,
            pairing_store_path = %endpoint.pairing_store_path.display(),
            rtsp_password_enabled,
            "building AirPlay endpoint"
        );

        let mut builder = RaopServer::builder()
            .name(endpoint.display_name.clone())
            .model(config.advertised_model.clone())
            .hwaddr(endpoint.persisted_hwaddr)
            .pin(config.pin.clone())
            .pairing_store(pairing_store)
            .mode(AirPlayMode::AirPlay2)
            .bind(
                BindConfig::new()
                    .addrs([bind_addr])
                    .port(endpoint.rtsp_port)
                    .exact_port(),
            )
            .output_sample_rate(config.output_sample_rate)
            .output_max_channels(config.output_channels)
            .max_clients(config.max_clients_per_zone);

        if let Some(password) = config.rtsp_password() {
            builder = builder.password(password.to_owned());
        }

        let server = builder.build(handler)?;

        Ok(Self {
            zone_id: endpoint.zone_id.clone(),
            display_name: endpoint.display_name.clone(),
            server,
            volume_state,
        })
    }

    pub fn volume_state(&self) -> &ZoneVolumeState {
        &self.volume_state
    }

    pub async fn start(&mut self) -> Result<(), AirPlayError> {
        self.server.start().await?;
        Ok(())
    }

    pub async fn stop(&mut self) {
        self.server.stop().await;
    }

    pub fn zone_id(&self) -> &ZoneId {
        &self.zone_id
    }

    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    pub fn is_running(&self) -> bool {
        self.server.is_running()
    }
}

#[derive(Debug, Error)]
pub enum AirPlayError {
    #[error("shairplay failed: {0}")]
    Shairplay(#[from] shairplay::ShairplayError),
    #[error("pairing store at {path} failed: {source}")]
    PairingStore {
        path: PathBuf,
        #[source]
        source: PairingStoreError,
    },
}

impl AirPlayError {
    fn pairing_store(
        path: impl Into<PathBuf>,
        source: impl Into<PairingStoreError>,
    ) -> AirPlayError {
        AirPlayError::PairingStore {
            path: path.into(),
            source: source.into(),
        }
    }
}

#[derive(Debug, Error)]
pub enum PairingStoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("store mutex poisoned")]
    Poisoned,
}

#[derive(Debug)]
pub struct FilePairingStore {
    path: PathBuf,
    keys: Mutex<BTreeMap<String, [u8; 32]>>,
}

impl FilePairingStore {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, AirPlayError> {
        let path = path.as_ref().to_path_buf();
        let keys = match fs::read_to_string(&path) {
            Ok(contents) => serde_json::from_str(&contents)
                .map_err(|source| AirPlayError::pairing_store(path.clone(), source))?,
            Err(error) if error.kind() == ErrorKind::NotFound => BTreeMap::new(),
            Err(source) => return Err(AirPlayError::pairing_store(path, source)),
        };

        Ok(Self {
            path,
            keys: Mutex::new(keys),
        })
    }

    pub fn key_count(&self) -> Result<usize, AirPlayError> {
        let keys = self.keys.lock().map_err(|_| {
            AirPlayError::pairing_store(self.path.clone(), PairingStoreError::Poisoned)
        })?;
        Ok(keys.len())
    }

    fn flush(&self, keys: &BTreeMap<String, [u8; 32]>) -> Result<(), AirPlayError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .map_err(|source| AirPlayError::pairing_store(self.path.clone(), source))?;
        }

        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        let mut temp = tempfile::NamedTempFile::new_in(parent)
            .map_err(|source| AirPlayError::pairing_store(self.path.clone(), source))?;
        serde_json::to_writer_pretty(&mut temp, keys)
            .map_err(|source| AirPlayError::pairing_store(self.path.clone(), source))?;
        temp.write_all(b"\n")
            .map_err(|source| AirPlayError::pairing_store(self.path.clone(), source))?;
        temp.as_file_mut()
            .sync_all()
            .map_err(|source| AirPlayError::pairing_store(self.path.clone(), source))?;
        temp.persist(&self.path)
            .map_err(|error| AirPlayError::pairing_store(self.path.clone(), error.error))?;

        Ok(())
    }

    fn warn_flush_error(&self, error: AirPlayError) {
        warn!(
            path = %self.path.display(),
            %error,
            "failed to flush AirPlay pairing store"
        );
    }
}

impl PairingStore for FilePairingStore {
    fn get(&self, device_id: &str) -> Option<[u8; 32]> {
        match self.keys.lock() {
            Ok(keys) => keys.get(device_id).copied(),
            Err(_) => {
                warn!(
                    path = %self.path.display(),
                    "failed to read AirPlay pairing store: mutex poisoned"
                );
                None
            }
        }
    }

    fn put(&self, device_id: &str, public_key: [u8; 32]) {
        match self.keys.lock() {
            Ok(mut keys) => {
                keys.insert(device_id.to_owned(), public_key);
                if let Err(error) = self.flush(&keys) {
                    self.warn_flush_error(error);
                }
            }
            Err(_) => warn!(
                path = %self.path.display(),
                "failed to update AirPlay pairing store: mutex poisoned"
            ),
        }
    }

    fn remove(&self, device_id: &str) {
        match self.keys.lock() {
            Ok(mut keys) => {
                keys.remove(device_id);
                if let Err(error) = self.flush(&keys) {
                    self.warn_flush_error(error);
                }
            }
            Err(_) => warn!(
                path = %self.path.display(),
                "failed to update AirPlay pairing store: mutex poisoned"
            ),
        }
    }
}

struct BridgeAudioHandler {
    zone_id: ZoneId,
    events: mpsc::UnboundedSender<AirPlayEvent>,
    volume_state: ZoneVolumeState,
    active_session: Arc<Mutex<Option<SessionId>>>,
}

impl AudioHandler for BridgeAudioHandler {
    fn audio_init(&self, format: AudioFormat) -> Box<dyn AudioSession> {
        let session_id = SessionId::new();
        let pcm_format = PcmFormat::from(format);
        debug!(
            zone_id = %self.zone_id,
            %session_id,
            sample_rate = pcm_format.sample_rate,
            channels = pcm_format.channels,
            "AirPlay audio initialized"
        );
        if let Ok(mut active) = self.active_session.lock() {
            *active = Some(session_id);
        }

        let _ = self.events.send(AirPlayEvent::SessionStarted {
            session_id,
            zone_id: self.zone_id.clone(),
            format: pcm_format,
        });

        Box::new(BridgeAudioSession {
            session_id,
            zone_id: self.zone_id.clone(),
            format: pcm_format,
            events: self.events.clone(),
            active_session: self.active_session.clone(),
            stop_notified: false,
            playback_epoch: 0,
            warned_missing_timing: false,
        })
    }

    fn on_playback_rate(&self, playing: bool) {
        let session_id = self.active_session.lock().ok().and_then(|active| *active);
        if let Some(session_id) = session_id {
            debug!(
                zone_id = %self.zone_id,
                %session_id,
                playing,
                "AirPlay playback rate changed"
            );
            let _ = self.events.send(AirPlayEvent::PlaybackState {
                session_id,
                zone_id: self.zone_id.clone(),
                playing,
            });
        }
    }

    fn on_volume(&self, volume: f32) {
        self.volume_state.set_volume_db(volume);
        let _ = self.events.send(AirPlayEvent::Volume {
            zone_id: self.zone_id.clone(),
            airplay_db: volume,
            sonos_volume: airplay_db_to_sonos_volume(volume),
        });
    }

    fn current_volume_db(&self) -> f32 {
        self.volume_state.volume_db()
    }

    fn on_client_connected(&self, addr: &str) {
        debug!(zone_id = %self.zone_id, %addr, "AirPlay client connected");
        let _ = self.events.send(AirPlayEvent::ClientConnected {
            zone_id: self.zone_id.clone(),
            addr: addr.to_owned(),
        });
    }

    fn on_client_disconnected(&self, addr: &str) {
        debug!(zone_id = %self.zone_id, %addr, "AirPlay client disconnected");
        let _ = self.events.send(AirPlayEvent::ClientDisconnected {
            zone_id: self.zone_id.clone(),
            addr: addr.to_owned(),
        });
    }

    fn on_error(&self, error: &shairplay::ShairplayError) {
        warn!(zone_id = %self.zone_id, %error, "AirPlay receiver error");
        let _ = self.events.send(AirPlayEvent::Error {
            zone_id: self.zone_id.clone(),
            message: error.to_string(),
        });
    }
}

struct BridgeAudioSession {
    session_id: SessionId,
    zone_id: ZoneId,
    format: PcmFormat,
    events: mpsc::UnboundedSender<AirPlayEvent>,
    active_session: Arc<Mutex<Option<SessionId>>>,
    stop_notified: bool,
    playback_epoch: u64,
    warned_missing_timing: bool,
}

impl AudioSession for BridgeAudioSession {
    fn audio_process(&mut self, samples: &[f32]) {
        self.audio_process_timed(samples, None);
    }

    fn audio_process_timed(&mut self, samples: &[f32], presentation_time: Option<Instant>) {
        if presentation_time.is_none() && !self.warned_missing_timing {
            self.warned_missing_timing = true;
            warn!(session_id = %self.session_id, "source presentation timing unavailable; group playback uses best-effort release without sample alignment");
        }
        let frame = PcmFrame {
            playback_epoch: self.playback_epoch,
            sample_rate: self.format.sample_rate,
            channels: self.format.channels,
            samples_f32_interleaved: samples.to_vec(),
            presentation_time,
        };
        let _ = self.events.send(AirPlayEvent::Pcm {
            session_id: self.session_id,
            zone_id: self.zone_id.clone(),
            frame,
        });
    }

    fn audio_flush(&mut self) {
        self.playback_epoch = self.playback_epoch.saturating_add(1);
        debug!(
            zone_id = %self.zone_id,
            session_id = %self.session_id,
            "AirPlay audio buffer flushed"
        );
        let _ = self.events.send(AirPlayEvent::Flushed {
            session_id: self.session_id,
            playback_epoch: self.playback_epoch,
            zone_id: self.zone_id.clone(),
        });
    }

    fn audio_stopped(&mut self, reason: AudioStopReason) {
        if self.stop_notified {
            return;
        }
        self.stop_notified = true;

        match reason {
            AudioStopReason::StreamEndedWhilePaused => {
                debug!(
                    zone_id = %self.zone_id,
                    session_id = %self.session_id,
                    "AirPlay audio stream ended while paused; keeping bridge session alive"
                );
                let _ = self.events.send(AirPlayEvent::StreamEndedWhilePaused {
                    session_id: self.session_id,
                    zone_id: self.zone_id.clone(),
                });
            }
            AudioStopReason::Teardown | AudioStopReason::StreamEnded => {
                if let Ok(mut active) = self.active_session.lock()
                    && *active == Some(self.session_id)
                {
                    *active = None;
                }
                debug!(
                    zone_id = %self.zone_id,
                    session_id = %self.session_id,
                    ?reason,
                    "AirPlay audio session stopped"
                );
                let _ = self.events.send(AirPlayEvent::SessionStopped {
                    session_id: self.session_id,
                    zone_id: self.zone_id.clone(),
                });
            }
        }
    }
}

impl Drop for BridgeAudioSession {
    fn drop(&mut self) {
        if self.stop_notified {
            return;
        }

        if let Ok(mut active) = self.active_session.lock()
            && *active == Some(self.session_id)
        {
            *active = None;
        }
        let _ = self.events.send(AirPlayEvent::SessionStopped {
            session_id: self.session_id,
            zone_id: self.zone_id.clone(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_pairing_store_loads_as_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("missing.json");

        let store = FilePairingStore::load(&path).expect("missing store loads");

        assert_eq!(store.key_count().expect("count"), 0);
        assert!(store.get("ios-device-id").is_none());
    }

    #[test]
    fn put_persists_key_and_reload_returns_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("pairings.json");
        let key = [7; 32];

        let store = FilePairingStore::load(&path).expect("store loads");
        store.put("ios-device-id", key);

        let reloaded = FilePairingStore::load(&path).expect("store reloads");

        assert_eq!(reloaded.get("ios-device-id"), Some(key));
        assert_eq!(reloaded.key_count().expect("count"), 1);
    }

    #[test]
    fn remove_deletes_key_and_persists_deletion() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("pairings.json");
        let key = [9; 32];

        let store = FilePairingStore::load(&path).expect("store loads");
        store.put("ios-device-id", key);
        store.remove("ios-device-id");

        let reloaded = FilePairingStore::load(&path).expect("store reloads");

        assert!(reloaded.get("ios-device-id").is_none());
        assert_eq!(reloaded.key_count().expect("count"), 0);
    }

    #[test]
    fn invalid_json_returns_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("pairings.json");
        fs::write(&path, "{not json").expect("write invalid json");

        let error = FilePairingStore::load(&path).expect_err("invalid json should fail");

        assert!(matches!(error, AirPlayError::PairingStore { .. }));
    }

    #[test]
    fn audio_stopped_while_paused_keeps_session_alive() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let active_session = Arc::new(Mutex::new(Some(SessionId::new())));
        let session_id = active_session.lock().expect("lock").expect("session");
        let mut session = BridgeAudioSession {
            session_id,
            zone_id: ZoneId::new("RINCON_TEST"),
            format: PcmFormat {
                sample_rate: 48_000,
                channels: 2,
                bits: 32,
            },
            events: tx,
            active_session: active_session.clone(),
            stop_notified: false,
            playback_epoch: 0,
            warned_missing_timing: false,
        };

        session.audio_stopped(AudioStopReason::StreamEndedWhilePaused);
        drop(session);

        let event = rx.try_recv().expect("paused stream end event");
        assert!(matches!(event, AirPlayEvent::StreamEndedWhilePaused { .. }));
        assert!(rx.try_recv().is_err());
        assert_eq!(*active_session.lock().expect("lock"), Some(session_id));
    }

    #[test]
    fn audio_stopped_teardown_does_not_emit_session_stopped_on_drop() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let active_session = Arc::new(Mutex::new(Some(SessionId::new())));
        let session_id = active_session.lock().expect("lock").expect("session");
        let mut session = BridgeAudioSession {
            session_id,
            zone_id: ZoneId::new("RINCON_TEST"),
            format: PcmFormat {
                sample_rate: 48_000,
                channels: 2,
                bits: 32,
            },
            events: tx,
            active_session: active_session.clone(),
            stop_notified: false,
            playback_epoch: 0,
            warned_missing_timing: false,
        };

        session.audio_stopped(AudioStopReason::Teardown);
        drop(session);

        let event = rx.try_recv().expect("session stopped event");
        assert!(matches!(event, AirPlayEvent::SessionStopped { .. }));
        assert!(rx.try_recv().is_err());
        assert_eq!(*active_session.lock().expect("lock"), None);
    }

    #[test]
    fn audio_flush_emits_flushed_not_session_stopped() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let active_session = Arc::new(Mutex::new(Some(SessionId::new())));
        let session_id = active_session.lock().expect("lock").expect("session");
        let mut session = BridgeAudioSession {
            session_id,
            zone_id: ZoneId::new("RINCON_TEST"),
            format: PcmFormat {
                sample_rate: 48_000,
                channels: 2,
                bits: 32,
            },
            events: tx,
            active_session,
            stop_notified: false,
            playback_epoch: 0,
            warned_missing_timing: false,
        };

        session.audio_flush();

        let event = rx.try_recv().expect("flush event");
        assert!(matches!(
            event,
            AirPlayEvent::Flushed {
                playback_epoch: 1,
                ..
            }
        ));
        session.audio_process(&[0.5, 0.5]);
        match rx.try_recv().unwrap() {
            AirPlayEvent::Pcm { frame, .. } => {
                assert_eq!(frame.playback_epoch, 1);
                assert_eq!(frame.presentation_time, None);
            }
            other => panic!("expected new epoch PCM, got {other:?}"),
        }
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn invalid_key_length_returns_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("pairings.json");
        fs::write(&path, r#"{"ios-device-id":[1,2,3]}"#).expect("write invalid key");

        let error = FilePairingStore::load(&path).expect_err("invalid key should fail");

        assert!(matches!(error, AirPlayError::PairingStore { .. }));
    }
}
