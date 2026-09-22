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
    /// Flush buffered packets in the inclusive RTP sequence range.
    Flush {
        /// First 16-bit sequence to flush.
        from_seq: u32,
        /// Last 16-bit sequence to flush.
        until_seq: u32,
    },
    /// Stop playback and tear down.
    Stop,
}

#[derive(Clone)]
struct BufferedFrame {
    sequence: u16,
    samples: Vec<f32>,
}

struct PlayoutState {
    buffer: BTreeMap<u32, BufferedFrame>, // source RTP timestamp → decoded packet
    epoch: u64,
    flush_range: Option<(u16, u16)>,
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

fn sequence_in_range(sequence: u16, from: u16, until: u16) -> bool {
    sequence.wrapping_sub(from) <= until.wrapping_sub(from)
}

impl PlayoutState {
    fn flush(&mut self, from: u16, until: u16) {
        self.epoch = self.epoch.wrapping_add(1);
        self.flush_range = Some((from, until));
        self.buffer
            .retain(|_, frame| !sequence_in_range(frame.sequence, from, until));
    }
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
    ) -> (tokio::sync::mpsc::Sender<PlayoutCommand>, tokio::task::AbortHandle) {
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(64);
        let default_sr = output_config.sample_rate.unwrap_or(44100);

        let state = Arc::new((
            Mutex::new(PlayoutState {
                buffer: BTreeMap::new(),
                epoch: 0,
                flush_range: None,
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
        let task = tasks.spawn(async move {
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
                        s.flush(from_seq as u16, until_seq as u16);
                        cvar.notify_all();
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

        (cmd_tx, task)
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
    let mut decode_epoch = 0;

    loop {
        // Stop reading TCP at two seconds or 8 MiB of decoded PCM, whichever is
        // smaller. One bounded decoded packet may wait outside the queue.
        loop {
            let full = {
                let s = state.0.lock().unwrap();
                if s.stopped {
                    return;
                }
                let queued: usize = s.buffer.values().map(|frame| frame.samples.len()).sum();
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

        let sequence = u16::from_be_bytes([packet[2], packet[3]]);
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

        let packet_epoch = {
            let mut s = state.0.lock().unwrap();
            if s.stopped {
                return;
            }
            if s.flush_range
                .is_some_and(|(from, until)| sequence_in_range(sequence, from, until))
            {
                continue;
            }
            // The old interval is no longer relevant once the sender advances;
            // retaining a 16-bit sequence filter forever would drop audio each wrap.
            if s.flush_range
                .is_some_and(|(_, until)| sequence.wrapping_sub(until) as i16 > 0)
            {
                s.flush_range = None;
            }
            s.epoch
        };
        if packet_epoch != decode_epoch {
            decode_epoch = packet_epoch;
            if current_ssrc != AudioSsrc::None {
                let source_rate = current_ssrc.sample_rate();
                decoder = AacDecoder::new(source_rate, source_channels).ok();
                stream_resampler = crate::codec::resample::StreamResampler::new(
                    source_rate,
                    output_config.sample_rate.unwrap_or(source_rate),
                    output_channels as usize,
                );
            }
        }

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

            if samples.is_empty() {
                continue;
            }
            loop {
                let admitted = {
                    let (lock, cvar) = &*state;
                    let mut s = lock.lock().unwrap();
                    if s.stopped {
                        return;
                    }
                    if s.epoch != packet_epoch {
                        break;
                    }
                    let budget = pcm_budget(s.sample_rate, s.channels);
                    if samples.len() > budget {
                        return;
                    }
                    let queued: usize = s.buffer.values().map(|frame| frame.samples.len()).sum();
                    if queued + samples.len() <= budget {
                        s.buffer.insert(
                            timestamp,
                            BufferedFrame {
                                sequence,
                                samples: std::mem::take(&mut samples),
                            },
                        );
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
    let mut delivered_epoch = 0;

    loop {
        let mut s = lock.lock().unwrap();

        while !s.stopped && s.epoch == delivered_epoch && (s.rate == 0 || s.buffer.is_empty()) {
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

        if delivered_epoch != s.epoch {
            delivered_epoch = s.epoch;
            drop(s);
            if let Some(sess) = &mut session {
                sess.audio_flush();
            }
            continue;
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

        let ready: Vec<(u32, BufferedFrame)> = s
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
                // There is no validated PTP-to-Instant mapping in this receiver yet.
                sess.audio_process_timed(&frame.samples, None);
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
            let (commands, _) = processor.start(
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

#[cfg(test)]
mod flush_tests {
    use super::*;
    fn state() -> PlayoutState {
        PlayoutState {
            buffer: BTreeMap::new(),
            epoch: 0,
            flush_range: None,
            anchor_rtp: 0,
            anchor_local: std::time::Instant::now(),
            rate: 1,
            sample_rate: 48000,
            source_sample_rate: 44100,
            channels: 2,
            stopped: false,
            stop_reason: None,
            format_changed: false,
        }
    }
    #[test]
    fn flush_uses_sequence_not_timestamp_and_wraps() {
        let mut s = state();
        for (timestamp, sequence) in [(10000, 65534), (10352, 65535), (10704, 0), (11056, 1), (11408, 2)] {
            s.buffer.insert(
                timestamp,
                BufferedFrame {
                    sequence,
                    samples: vec![sequence as f32],
                },
            );
        }
        s.flush(65535, 1);
        assert_eq!(s.buffer.keys().copied().collect::<Vec<_>>(), vec![10000, 11408]);
        assert_eq!(s.epoch, 1);
    }

    #[test]
    fn flush_callback_precedes_new_pcm_even_while_paused() {
        struct Handler(std::sync::mpsc::Sender<&'static str>);
        struct Session(std::sync::mpsc::Sender<&'static str>);
        impl AudioHandler for Handler {
            fn audio_init(&self, _: AudioFormat) -> Box<dyn crate::raop::AudioSession> {
                Box::new(Session(self.0.clone()))
            }
        }
        impl crate::raop::AudioSession for Session {
            fn audio_process(&mut self, samples: &[f32]) {
                self.0.send(if samples[0] == 1.0 { "old" } else { "new" }).unwrap();
            }
            fn audio_flush(&mut self) {
                self.0.send("flush").unwrap();
            }
        }
        let mut s = state();
        s.buffer.insert(
            0,
            BufferedFrame {
                sequence: 1,
                samples: vec![1.0, 1.0],
            },
        );
        let shared = Arc::new((Mutex::new(s), Condvar::new()));
        let worker_state = shared.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            delivery_loop(
                worker_state,
                Arc::new(Handler(tx)),
                OutputConfig {
                    sample_rate: None,
                    max_channels: None,
                },
            )
        });
        assert_eq!(rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap(), "old");
        {
            let mut s = shared.0.lock().unwrap();
            s.rate = 0;
            s.flush(1, 1);
            shared.1.notify_all();
        }
        assert_eq!(rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap(), "flush");
        {
            let mut s = shared.0.lock().unwrap();
            s.rate = 1;
            s.buffer.insert(
                0,
                BufferedFrame {
                    sequence: 2,
                    samples: vec![2.0, 2.0],
                },
            );
            shared.1.notify_all();
        }
        assert_eq!(rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap(), "new");
        shared.0.lock().unwrap().stopped = true;
        shared.1.notify_all();
        thread.join().unwrap();
    }
}
