//! AirPlay 2 buffered audio processor (stream type 103).
//!
//! Receives encrypted AAC packets over TCP, decrypts with ChaCha20-Poly1305,
//! decodes via symphonia, resamples/mixes down, and delivers F32LE PCM through
//! a timed playout buffer.
//!
//! Three concurrent tasks:
//! - **Receiver** (tokio): accepts TCP, decrypts, decodes, buffers by RTP timestamp
//! - **Command handler** (tokio): processes SetRate/Flush/Stop from RTSP thread
//! - **Delivery** (std::thread): timed playout using anchor-based scheduling

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use crate::codec::aac::{AacDecoder, AudioSsrc};
use crate::error::NetworkError;
use crate::raop::{AudioCodec, AudioFormat, AudioHandler, AudioStopReason};

/// RTP header length in bytes.
const RTP_HEADER_LEN: usize = 12;
/// Trailing nonce bytes appended to each ChaCha20-Poly1305 encrypted packet.
const NONCE_TRAIL_LEN: usize = 8;

#[derive(Debug, Clone)]
/// Output configuration passed from the server builder.
pub struct OutputConfig {
    /// Target sample rate, or None for source native rate.
    pub sample_rate: Option<u32>,
    /// Maximum output channels, or None to pass through.
    pub max_channels: Option<u8>,
}

#[derive(Debug)]
/// Commands sent from the RTSP handler thread to the playout engine.
pub enum PlayoutCommand {
    /// Set playback rate and anchor point. rate=0 means pause.
    SetRate {
        /// RTP timestamp at the anchor point.
        anchor_rtp: u32,
        /// Network time at the anchor point (ns).
        anchor_time_ns: u64,
        /// Playback rate (1 = playing, 0 = paused).
        rate: u32,
    },
    /// Flush buffered frames in the given RTP timestamp range.
    Flush {
        /// First timestamp to flush.
        from_seq: u32,
        /// Last timestamp to flush.
        until_seq: u32,
    },
    /// Stop playback and tear down.
    Stop,
}

struct PlayoutState {
    buffer: BTreeMap<u32, Vec<f32>>, // rtp_timestamp → F32 PCM samples
    anchor_rtp: u32,
    anchor_local: std::time::Instant,
    rate: u32,
    sample_rate: u32,
    source_sample_rate: u32,
    channels: u8,
    stopped: bool,
    stop_reason: Option<AudioStopReason>,
    format_changed: bool,
}

struct StopOnDrop {
    state: Arc<(Mutex<PlayoutState>, Condvar)>,
    delivery: Option<std::thread::JoinHandle<()>>,
}
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let (lock, wake) = &*self.state;
        let mut state = lock.lock().unwrap();
        state.stopped = true;
        state.buffer.clear();
        state.stop_reason.get_or_insert(AudioStopReason::Teardown);
        wake.notify_all();
        drop(state);
        if let Some(delivery) = self.delivery.take() {
            let _ = delivery.join();
        }
    }
}

/// TCP listener for buffered audio. Binds a port and spawns the processing pipeline.
pub struct BufferedAudioProcessor {
    /// TCP listener waiting for the iPhone to connect.
    pub listener: TcpListener,
    /// Port number the listener is bound to.
    pub port: u16,
}

impl BufferedAudioProcessor {
    /// Bind a TCP listener for buffered audio on the given address.
    pub async fn bind(addr: &str) -> Result<Self, NetworkError> {
        let listener = TcpListener::bind(addr).await?;
        let port = listener.local_addr()?.port();
        Ok(Self { listener, port })
    }

    /// Start the processing pipeline. Returns a command sender for playback control.
    pub fn start(
        self,
        shk: [u8; 32],
        output_config: OutputConfig,
        handler: Arc<dyn AudioHandler>,
        tasks: &mut tokio::task::JoinSet<()>,
    ) -> tokio::sync::mpsc::Sender<PlayoutCommand> {
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(64);
        let default_sr = output_config.sample_rate.unwrap_or(44100);

        let state = Arc::new((
            Mutex::new(PlayoutState {
                buffer: BTreeMap::new(),
                anchor_rtp: 0,
                anchor_local: std::time::Instant::now(),
                rate: 0,
                sample_rate: default_sr,
                source_sample_rate: 44100,
                channels: 2,
                stopped: false,
                stop_reason: None,
                format_changed: false,
            }),
            Condvar::new(),
        ));

        // Delivery thread
        let state2 = state.clone();
        let handler2 = handler.clone();
        let output_config2 = output_config.clone();
        let delivery = std::thread::spawn(move || {
            delivery_loop(state2, handler2, output_config2);
        });

        // Receiver task
        let state4 = state.clone();

        let mut children = tokio::task::JoinSet::new();
        children.spawn(async move {
            let (stream, addr) = match self.listener.accept().await {
                Ok(s) => s,
                Err(e) => {
                    warn!("Buffered audio accept failed: {e}");
                    return;
                }
            };
            info!(%addr, "Buffered audio client connected");
            receive_loop(stream, &shk, output_config, state4).await;
        });

        // Command handler
        let state3 = state.clone();
        let mut cmd_rx = cmd_rx;
        let cleanup = StopOnDrop {
            state: state.clone(),
            delivery: Some(delivery),
        };
        tasks.spawn(async move {
            let cleanup = cleanup;
            loop {
                let cmd = tokio::select! {
                    cmd = cmd_rx.recv() => match cmd { Some(cmd) => cmd, None => break },
                    _ = children.join_next() => break,
                };
                let (lock, cvar) = &*state3;
                let mut s = lock.lock().unwrap();
                match cmd {
                    PlayoutCommand::SetRate {
                        anchor_rtp,
                        anchor_time_ns: _,
                        rate,
                    } => {
                        s.anchor_rtp = anchor_rtp;
                        let was_paused = s.rate == 0;
                        s.rate = rate;
                        if rate == 0 {
                            info!("Playout paused");
                        } else {
                            // Set anchor so the earliest buffered frame is deliverable
                            // with a small lead time for smooth playback
                            if let Some(&first_ts) = s.buffer.keys().next() {
                                let lead_frames = s.source_sample_rate / 10; // 100ms lead
                                s.anchor_rtp = first_ts.wrapping_sub(lead_frames);
                            }
                            s.anchor_local = std::time::Instant::now();
                            let stale: Vec<u32> = s
                                .buffer
                                .keys()
                                .filter(|&&ts| (s.anchor_rtp.wrapping_sub(ts) as i32) > 0)
                                .copied()
                                .collect();
                            if !stale.is_empty() {
                                debug!(discarded = stale.len(), "Discarded stale frames");
                            }
                            for k in stale {
                                s.buffer.remove(&k);
                            }
                            if was_paused {
                                info!(anchor_rtp, "Playout started");
                            }
                        }
                        cvar.notify_all();
                    }
                    PlayoutCommand::Flush { from_seq, until_seq } => {
                        let keys: Vec<u32> = s
                            .buffer
                            .keys()
                            .filter(|&&ts| ts >= from_seq && ts <= until_seq)
                            .copied()
                            .collect();
                        for k in &keys {
                            s.buffer.remove(k);
                        }
                        debug!(flushed = keys.len(), "Flushed");
                    }
                    PlayoutCommand::Stop => {
                        s.stop_reason = Some(AudioStopReason::Teardown);
                        s.stopped = true;
                        s.buffer.clear();
                        cvar.notify_all();
                        break;
                    }
                }
            }
            drop(cleanup);
            children.abort_all();
            while children.join_next().await.is_some() {}
        });

        cmd_tx
    }
}

/// TCP receive loop: reads length-prefixed packets, decrypts, decodes, buffers.
async fn receive_loop(
    mut stream: TcpStream,
    shk: &[u8; 32],
    output_config: OutputConfig,
    state: Arc<(Mutex<PlayoutState>, Condvar)>,
) {
    use chacha20poly1305::{ChaCha20Poly1305, KeyInit, Nonce, aead::Aead, aead::Payload};

    let cipher = ChaCha20Poly1305::new(shk.into());
    let mut len_buf = [0u8; 2];
    let mut decoder: Option<AacDecoder> = None;
    let mut current_ssrc = AudioSsrc::None;
    let mut stream_resampler: Option<crate::codec::resample::StreamResampler> = None;
    let mut source_channels: u8 = 2;
    let mut output_channels: u8 = 2;

    loop {
        // Stop reading TCP at two seconds or 8 MiB of decoded PCM, whichever is
        // smaller. One bounded decoded packet may wait outside the queue.
        loop {
            let full = {
                let s = state.0.lock().unwrap();
                if s.stopped {
                    return;
                }
                let queued: usize = s.buffer.values().map(Vec::len).sum();
                queued >= pcm_budget(s.sample_rate, s.channels) / 2
            };
            if !full {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        if stream.read_exact(&mut len_buf).await.is_err() {
            break;
        }
        let total_len = u16::from_be_bytes(len_buf) as usize;
        if total_len < 2 {
            break;
        }

        let mut packet = vec![0u8; total_len - 2];
        if stream.read_exact(&mut packet).await.is_err() {
            break;
        }
        if packet.len() <= RTP_HEADER_LEN + NONCE_TRAIL_LEN {
            continue;
        }

        let timestamp = u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]]);
        let ssrc_val = u32::from_be_bytes([packet[8], packet[9], packet[10], packet[11]]);
        let ssrc = AudioSsrc::from_u32(ssrc_val);

        // Decrypt
        let pkt_len = packet.len();
        let mut nonce = [0u8; 12];
        nonce[4..12].copy_from_slice(&packet[pkt_len - NONCE_TRAIL_LEN..]);
        let aad = packet[4..12].to_vec();
        let ciphertext = &packet[RTP_HEADER_LEN..pkt_len - NONCE_TRAIL_LEN];

        let plaintext = match cipher.decrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        ) {
            Ok(p) => p,
            Err(_) => {
                debug!("Audio decrypt failed");
                continue;
            }
        };

        // Detect format change
        if ssrc != AudioSsrc::None && ssrc != current_ssrc {
            current_ssrc = ssrc;
            let src_sr = ssrc.sample_rate();
            let src_ch = ssrc.channels();
            info!(ssrc = ?ssrc, src_sr, src_ch, "Audio format detected");

            decoder = AacDecoder::new(src_sr, src_ch).ok();
            if decoder.is_none() {
                warn!("Failed to create AAC decoder for {:?}", ssrc);
            }

            let target_sr = output_config.sample_rate.unwrap_or(src_sr);
            let target_ch = output_config.max_channels.map(|max| src_ch.min(max)).unwrap_or(src_ch);

            stream_resampler = crate::codec::resample::StreamResampler::new(src_sr, target_sr, target_ch as usize);
            if stream_resampler.is_some() {
                debug!(from = src_sr, to = target_sr, "Resampler initialized");
            }

            source_channels = src_ch;
            output_channels = target_ch;

            // Signal format change to delivery thread
            let (lock, cvar) = &*state;
            let mut s = lock.lock().unwrap();
            s.sample_rate = target_sr;
            s.source_sample_rate = src_sr;
            s.channels = target_ch;
            s.format_changed = true;
            cvar.notify_all();
        }

        // Decode
        let pcm = if let Some(dec) = &mut decoder {
            dec.decode(&plaintext)
        } else {
            None
        };

        if let Some(pcm_data) = pcm {
            // Convert bytes to f32 samples for processing
            let mut samples: Vec<f32> = pcm_data
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();

            // Channel mixdown if needed
            if source_channels > output_channels {
                samples = crate::codec::resample::mixdown(&samples, source_channels as usize, output_channels as usize);
            }

            // Resample if needed
            if let Some(ref mut rs) = stream_resampler {
                samples = rs.process(&samples);
            }

            loop {
                let admitted = {
                    let (lock, cvar) = &*state;
                    let mut s = lock.lock().unwrap();
                    if s.stopped {
                        return;
                    }
                    let budget = pcm_budget(s.sample_rate, s.channels);
                    if samples.len() > budget {
                        return;
                    }
                    let queued: usize = s.buffer.values().map(Vec::len).sum();
                    if queued + samples.len() <= budget {
                        s.buffer.insert(timestamp, std::mem::take(&mut samples));
                        cvar.notify_all();
                        true
                    } else {
                        false
                    }
                };
                if admitted {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
    }
    debug!("Buffered audio receive loop ended");
    let (lock, cvar) = &*state;
    if let Ok(mut s) = lock.lock() {
        if s.stop_reason.is_none() {
            s.stop_reason = Some(if s.rate == 0 {
                AudioStopReason::StreamEndedWhilePaused
            } else {
                AudioStopReason::StreamEnded
            });
        }
        s.stopped = true;
        s.buffer.clear();
        cvar.notify_all();
    }
}

/// Timed playout delivery thread. Wakes on condvar, delivers due frames to AudioSession.
fn delivery_loop(
    state: Arc<(Mutex<PlayoutState>, Condvar)>,
    handler: Arc<dyn AudioHandler>,
    _output_config: OutputConfig,
) {
    let (lock, cvar) = &*state;
    let mut session: Option<Box<dyn crate::raop::AudioSession>> = None;

    loop {
        let mut s = lock.lock().unwrap();

        while !s.stopped && (s.rate == 0 || s.buffer.is_empty()) {
            s = cvar.wait(s).unwrap();
        }
        if s.stopped {
            let reason = s.stop_reason.unwrap_or(AudioStopReason::StreamEnded);
            drop(s);
            if let Some(mut sess) = session.take() {
                sess.audio_stopped(reason);
            }
            break;
        }

        // Lazy init or reinit session on format change
        if session.is_none() || s.format_changed {
            s.format_changed = false;
            let format = AudioFormat {
                codec: AudioCodec::Pcm,
                bits: 32,
                channels: s.channels,
                sample_rate: s.sample_rate,
            };
            info!(?format, "Audio session initialized");
            session = Some(handler.audio_init(format));
        }

        let elapsed_frames = source_frames(s.anchor_local.elapsed(), s.source_sample_rate);
        let target_rtp = s.anchor_rtp.wrapping_add(elapsed_frames);

        let ready: Vec<(u32, Vec<f32>)> = s
            .buffer
            .iter()
            .filter(|(ts, _)| (target_rtp.wrapping_sub(**ts) as i32) >= 0)
            .map(|(&ts, data)| (ts, data.clone()))
            .collect();

        for (ts, _) in &ready {
            s.buffer.remove(ts);
        }
        drop(s);

        if let Some(ref mut sess) = session {
            for (_, frame) in &ready {
                sess.audio_process(frame);
            }
        }

        if ready.is_empty() {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
    info!("Delivery loop ended");
}

fn source_frames(elapsed: std::time::Duration, sample_rate: u32) -> u32 {
    (elapsed.as_nanos() * u128::from(sample_rate) / 1_000_000_000) as u32
}

fn pcm_budget(sample_rate: u32, channels: u8) -> usize {
    (sample_rate as usize * channels as usize * 2).min(8 * 1024 * 1024 / 4)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn resampled_playout_uses_source_clock() {
        assert_eq!(source_frames(std::time::Duration::from_secs(1), 44100), 44100);
        assert_eq!(pcm_budget(48000, 2), 192000);
        assert!(pcm_budget(192000, 8) * 4 <= 8 * 1024 * 1024);
    }
}

#[cfg(test)]
mod ownership_tests {
    use super::*;
    struct Handler;
    impl AudioHandler for Handler {
        fn audio_init(&self, _: AudioFormat) -> Box<dyn crate::raop::AudioSession> {
            Box::new(Session)
        }
    }
    struct Session;
    impl crate::raop::AudioSession for Session {
        fn audio_process(&mut self, _: &[f32]) {}
    }

    #[tokio::test]
    async fn abandoned_and_partial_streams_release_listener_and_tasks() {
        for connect in [false, true, false, true] {
            let processor = BufferedAudioProcessor::bind("127.0.0.1:0").await.unwrap();
            let address = processor.listener.local_addr().unwrap();
            let mut tasks = tokio::task::JoinSet::new();
            let commands = processor.start(
                [0; 32],
                OutputConfig {
                    sample_rate: None,
                    max_channels: None,
                },
                Arc::new(Handler),
                &mut tasks,
            );
            let mut client = if connect {
                Some(TcpStream::connect(address).await.unwrap())
            } else {
                None
            };
            if let Some(client) = &mut client {
                use tokio::io::AsyncWriteExt;
                client.write_all(&[0]).await.unwrap(); // incomplete packet length
            }
            tokio::time::timeout(std::time::Duration::from_secs(1), tasks.shutdown())
                .await
                .unwrap();
            assert!(tasks.is_empty());
            assert!(commands.is_closed());
            assert!(TcpStream::connect(address).await.is_err());
        }
    }
}
