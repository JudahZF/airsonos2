//! Realtime ALAC audio receiver (stream type 96).
//!
//! Receives UDP packets with RTP headers, decrypts with ChaCha20-Poly1305,
//! decodes ALAC, resamples/mixes down, and delivers f32 PCM immediately.

use std::sync::Arc;
use tokio::net::UdpSocket;
use tracing::{debug, info, warn};

use chacha20poly1305::{ChaCha20Poly1305, KeyInit, Nonce, aead::Aead, aead::Payload};

use crate::raop::{AudioCodec, AudioFormat, AudioHandler};

#[cfg(feature = "resample")]
use crate::codec::resample::StreamResampler;

const RTP_HEADER_LEN: usize = 12;
const NONCE_TRAIL_LEN: usize = 8;

/// Output configuration for resampling/mixdown.
pub struct OutputConfig {
    /// Target sample rate, or None for source native rate.
    pub sample_rate: Option<u32>,
    /// Maximum output channels, or None to pass through.
    pub max_channels: Option<u8>,
    /// Validated negotiated ALAC format.
    pub alac: crate::codec::alac::AlacConfig,
}

/// Run the realtime audio receiver loop.
pub async fn run(
    socket: UdpSocket,
    shk: [u8; 32],
    handler: Arc<dyn AudioHandler>,
    output_config: OutputConfig,
    mut commands: tokio::sync::mpsc::Receiver<super::buffered_audio::PlayoutCommand>,
) {
    let cipher = ChaCha20Poly1305::new((&shk).into());
    let mut buf = vec![0u8; 4096];
    let config = &output_config.alac;
    let mut decoder = crate::codec::alac::AlacDecoder::new(config.bit_depth as i32, config.num_channels as i32);
    decoder.set_info(&super::buffer::build_decoder_info(config));
    let src_sr = config.sample_rate;
    let src_ch = config.num_channels;
    let out_ch = output_config.max_channels.map(|m| src_ch.min(m)).unwrap_or(src_ch);
    let target_sr = output_config.sample_rate.unwrap_or(src_sr);
    #[cfg(feature = "resample")]
    let mut resampler = StreamResampler::new(src_sr, target_sr, out_ch as usize);
    let mut session: Option<Box<dyn crate::raop::AudioSession>> = None;

    let mut last_sequence: Option<u16> = None;
    let mut playing = true;
    info!("Realtime ALAC receiver started");

    loop {
        let n = tokio::select! {
            result = socket.recv(&mut buf) => match result {
                Ok(0) => break, Ok(n) => n,
                Err(error) => { warn!(%error, "Realtime receive failed"); break; }
            },
            command = commands.recv() => {
                match command {
                    Some(super::buffered_audio::PlayoutCommand::Flush { until_seq, .. }) => {
                        last_sequence = Some(until_seq as u16);
                        decoder.set_info(&super::buffer::build_decoder_info(config));
                        #[cfg(feature = "resample")]
                        { resampler = StreamResampler::new(src_sr, target_sr, out_ch as usize); }
                        if let Some(session) = &mut session { session.audio_flush(); }
                    }
                    Some(super::buffered_audio::PlayoutCommand::SetRate { rate, .. }) => { playing = rate != 0; },
                    Some(super::buffered_audio::PlayoutCommand::Stop) | None => break,
                }
                continue;
            }
        };

        let packet = &buf[..n];
        if packet.len() <= RTP_HEADER_LEN + NONCE_TRAIL_LEN {
            continue;
        }

        // Decrypt: nonce from trailing 8 bytes, AAD from RTP header bytes 4..12
        let pkt_len = packet.len();
        let mut nonce = [0u8; 12];
        nonce[4..12].copy_from_slice(&packet[pkt_len - NONCE_TRAIL_LEN..]);
        let aad = &packet[4..12];
        let ciphertext = &packet[RTP_HEADER_LEN..pkt_len - NONCE_TRAIL_LEN];

        let alac_data = match cipher.decrypt(Nonce::from_slice(&nonce), Payload { msg: ciphertext, aad }) {
            Ok(p) => p,
            Err(_) => {
                debug!("Realtime audio decrypt failed");
                continue;
            }
        };

        let sequence = u16::from_be_bytes([packet[2], packet[3]]);
        if last_sequence.is_some_and(|last| sequence.wrapping_sub(last) as i16 <= 0) {
            continue;
        }
        last_sequence = Some(sequence);
        if !playing {
            continue;
        }

        // Decode ALAC → f32 PCM
        let Some(mut samples) = decoder.decode_frame_f32(&alac_data) else {
            continue;
        };

        // Mixdown if needed
        #[cfg(feature = "resample")]
        if src_ch > out_ch {
            samples = crate::codec::resample::mixdown(&samples, src_ch as usize, out_ch as usize);
        }

        // Resample if needed
        #[cfg(feature = "resample")]
        if let Some(ref mut rs) = resampler {
            samples = rs.process(&samples);
        }

        if session.is_none() {
            session = Some(handler.audio_init(AudioFormat {
                codec: AudioCodec::Pcm,
                bits: 32,
                channels: out_ch,
                sample_rate: target_sr,
            }));
        }
        // Deliver immediately (realtime = no playout buffer)
        if let Some(ref mut sess) = session {
            sess.audio_process_timed(&samples, None);
        }
    }

    debug!("Realtime ALAC receiver ended");
}
