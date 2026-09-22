use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use airsonos2_core::{PcmFrame, PcmQueue};
use bytes::Bytes;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::task::JoinError;
use tokio::time;
use tracing::debug;

use airsonos2_core::StreamCodec;

use crate::pcm::{f32_pcm_to_s16le_bytes, write_f32_pcm_to_s16le};
use crate::registry::LiveStream;

#[derive(Clone, Debug)]
pub struct FfmpegEncoderConfig {
    pub ffmpeg_path: PathBuf,
    pub sample_rate: u32,
    pub channels: u8,
    pub mp3_bitrate_kbps: u16,
    pub codec: StreamCodec,
    pub queue_duration: Duration,
}

#[derive(Debug)]
pub struct FfmpegEncoder {
    input: Option<PcmQueue>,
    task: tokio::task::JoinHandle<Result<(), EncoderError>>,
}

impl FfmpegEncoder {
    pub fn spawn(config: FfmpegEncoderConfig, stream: LiveStream) -> Result<Self, EncoderError> {
        if config.codec == StreamCodec::Wav {
            return Ok(Self::spawn_wav(config, stream));
        }

        let mut child = Command::new(&config.ffmpeg_path)
            .arg("-hide_banner")
            .arg("-loglevel")
            .arg("error")
            .arg("-fflags")
            .arg("nobuffer")
            .arg("-f")
            .arg("s16le")
            .arg("-ar")
            .arg(config.sample_rate.to_string())
            .arg("-ac")
            .arg(config.channels.to_string())
            .arg("-i")
            .arg("pipe:0")
            .arg("-f")
            .arg("mp3")
            .arg("-codec:a")
            .arg("libmp3lame")
            .arg("-b:a")
            .arg(format!("{}k", config.mp3_bitrate_kbps))
            .arg("-flush_packets")
            .arg("1")
            .arg("-write_xing")
            .arg("0")
            .arg("pipe:1")
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|source| EncoderError::Spawn {
                path: config.ffmpeg_path.clone(),
                source,
            })?;

        let stdin = child
            .stdin
            .take()
            .ok_or(EncoderError::MissingPipe("stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or(EncoderError::MissingPipe("stdout"))?;
        let stderr = child.stderr.take();
        let input = PcmQueue::new(config.sample_rate, config.channels, config.queue_duration);
        stream.set_input_queue(input.clone());
        let rx = input.clone();

        let task = tokio::spawn(async move {
            let terminal = stream.clone();
            let input_stream = stream.clone();
            let writer = async move {
                let mut stdin = stdin;
                let mut first_pcm = true;
                let mut bytes = Vec::new();
                while let Some(frame) = rx.recv().await {
                    if !input_stream.accepts_epoch(frame.playback_epoch) {
                        continue;
                    }
                    if first_pcm {
                        first_pcm = false;
                        debug!(
                            sample_rate = frame.sample_rate,
                            channels = frame.channels,
                            samples = frame.samples_f32_interleaved.len(),
                            "first PCM frame written to ffmpeg"
                        );
                    }
                    write_f32_pcm_to_s16le(&frame.samples_f32_interleaved, &mut bytes);
                    stdin.write_all(&bytes).await?;
                }
                stdin.shutdown().await
            };
            let reader = async move {
                let mut stdout = stdout;
                let mut first_mp3 = true;
                let mut buf = vec![0_u8; 16 * 1024];
                loop {
                    let read = stdout.read(&mut buf).await?;
                    if read == 0 {
                        break;
                    }
                    if first_mp3 {
                        first_mp3 = false;
                        debug!(bytes = read, "first encoded chunk read from ffmpeg");
                    }
                    stream.publish(Bytes::copy_from_slice(&buf[..read]));
                }
                Ok::<(), std::io::Error>(())
            };
            let stderr_reader = async move {
                match stderr {
                    Some(stderr) => read_stderr_tail(stderr).await,
                    None => Ok(String::new()),
                }
            };
            // These futures remain children of this task. Cancellation drops all pipes
            // and the kill-on-drop process rather than detaching nested tasks.
            let result = tokio::select! {
                _ = terminal.closed() => return Ok(()),
                result = async { tokio::try_join!(writer, reader, stderr_reader, child.wait()) } => result,
            };
            let (_, _, stderr, status) = result?;
            if status.success() {
                Ok(())
            } else {
                Err(EncoderError::Exited {
                    status: status.code(),
                    stderr,
                })
            }
        });

        Ok(Self {
            input: Some(input),
            task,
        })
    }

    fn spawn_wav(config: FfmpegEncoderConfig, stream: LiveStream) -> Self {
        let input = PcmQueue::new(config.sample_rate, config.channels, config.queue_duration);
        stream.set_input_queue(input.clone());
        let rx = input.clone();
        let task = tokio::spawn(async move {
            stream.publish_prelude(Bytes::from(wav_stream_header(
                config.sample_rate,
                config.channels,
            )));
            if !stream.wait_for_playback_release().await {
                return Ok(());
            }
            let mut first_pcm = false;

            while let Some(frame) = tokio::select! {
                _ = stream.closed() => None,
                frame = rx.recv() => frame,
            } {
                if !stream.accepts_epoch(frame.playback_epoch) {
                    continue;
                }
                if !first_pcm {
                    first_pcm = true;
                    debug!(
                        sample_rate = frame.sample_rate,
                        channels = frame.channels,
                        samples = frame.samples_f32_interleaved.len(),
                        "first PCM frame received for WAV stream"
                    );
                }

                stream.publish_timed_pcm(
                    Bytes::from(f32_pcm_to_s16le_bytes(&frame.samples_f32_interleaved)),
                    frame.presentation_time,
                    frame.sample_rate,
                    frame.channels,
                );
            }

            Ok(())
        });

        Self {
            input: Some(input),
            task,
        }
    }

    pub fn try_write_frame(&self, frame: PcmFrame) -> Result<bool, EncoderError> {
        let input = self.input.as_ref().ok_or(EncoderError::InputClosed)?;
        if self.task.is_finished() || input.is_closed() {
            return Err(EncoderError::InputClosed);
        }
        Ok(input.push(frame).accepted)
    }

    pub async fn write_frame(&self, frame: PcmFrame) -> Result<(), EncoderError> {
        self.try_write_frame(frame).map(|_| ())
    }

    pub async fn shutdown(mut self) -> Result<(), EncoderError> {
        if let Some(input) = self.input.take() {
            input.close();
        }
        match time::timeout(Duration::from_secs(2), &mut self.task).await {
            Ok(joined) => joined?,
            Err(_) => {
                self.task.abort();
                let _ = (&mut self.task).await;
                Err(EncoderError::ShutdownTimeout)
            }
        }
    }
}

impl Drop for FfmpegEncoder {
    fn drop(&mut self) {
        if let Some(input) = &self.input {
            input.close();
            input.clear();
        }
        self.task.abort();
    }
}

async fn read_stderr_tail(
    mut stderr: impl tokio::io::AsyncRead + Unpin,
) -> std::io::Result<String> {
    const LIMIT: usize = 16 * 1024;
    let mut tail = Vec::with_capacity(LIMIT);
    let mut buffer = [0; 4096];
    loop {
        let read = stderr.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let excess = (tail.len() + read).saturating_sub(LIMIT);
        tail.drain(..excess);
        tail.extend_from_slice(&buffer[..read]);
    }
    Ok(String::from_utf8_lossy(&tail).into_owned())
}

fn wav_stream_header(sample_rate: u32, channels: u8) -> Vec<u8> {
    let bits_per_sample = 16_u16;
    let channels_u16 = channels as u16;
    let block_align = channels_u16 * (bits_per_sample / 8);
    let byte_rate = sample_rate * u32::from(block_align);

    let mut header = Vec::with_capacity(44);
    header.extend_from_slice(b"RIFF");
    header.extend_from_slice(&u32::MAX.to_le_bytes());
    header.extend_from_slice(b"WAVE");
    header.extend_from_slice(b"fmt ");
    header.extend_from_slice(&16_u32.to_le_bytes());
    header.extend_from_slice(&1_u16.to_le_bytes());
    header.extend_from_slice(&channels_u16.to_le_bytes());
    header.extend_from_slice(&sample_rate.to_le_bytes());
    header.extend_from_slice(&byte_rate.to_le_bytes());
    header.extend_from_slice(&block_align.to_le_bytes());
    header.extend_from_slice(&bits_per_sample.to_le_bytes());
    header.extend_from_slice(b"data");
    header.extend_from_slice(&u32::MAX.to_le_bytes());
    header
}

#[derive(Debug, Error)]
pub enum EncoderError {
    #[error("failed to spawn ffmpeg at {path}: {source}")]
    Spawn {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("ffmpeg child was missing {0} pipe")]
    MissingPipe(&'static str),
    #[error("ffmpeg input channel closed")]
    InputClosed,
    #[error("ffmpeg exited with status {status:?}: {stderr}")]
    Exited { status: Option<i32>, stderr: String },
    #[error("ffmpeg shutdown timed out")]
    ShutdownTimeout,
    #[error("ffmpeg IO failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("ffmpeg task failed: {0}")]
    Join(#[from] JoinError),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_stream() -> LiveStream {
        use airsonos2_core::{EncoderState, SessionId, StreamSession, ZoneId};
        LiveStream::new(StreamSession {
            session_id: SessionId::new(),
            zone_id: ZoneId::new("test"),
            codec: StreamCodec::Wav,
            generation: 1,
            local_url: url::Url::parse("http://localhost/stream.wav").unwrap(),
            encoder_state: EncoderState::Running,
        })
    }

    fn wav_config() -> FfmpegEncoderConfig {
        FfmpegEncoderConfig {
            ffmpeg_path: "unused".into(),
            sample_rate: 1000,
            channels: 1,
            mp3_bitrate_kbps: 128,
            codec: StreamCodec::Wav,
            queue_duration: Duration::from_secs(3),
        }
    }

    #[tokio::test]
    async fn configured_delay_preserves_starting_sample_beyond_adapter_budget() {
        let stream = test_stream();
        let encoder = FfmpegEncoder::spawn(wav_config(), stream.clone()).unwrap();
        let (header, mut subscriber) = stream.attach_subscriber();
        if header.is_none() {
            assert_eq!(subscriber.recv().await.unwrap().bytes.len(), 44);
        }
        let start = std::time::Instant::now();
        stream.set_playback_plan(start, start + Duration::from_millis(350));
        for index in 0..10 {
            encoder
                .try_write_frame(PcmFrame {
                    buffered_permit: None,
                    playback_epoch: 0,
                    sample_rate: 1000,
                    channels: 1,
                    samples_f32_interleaved: vec![if index == 0 { 0.25 } else { 0.5 }; 100],
                    presentation_time: Some(start + Duration::from_millis(index * 100)),
                })
                .unwrap();
        }
        let bytes = tokio::time::timeout(Duration::from_secs(1), subscriber.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(start.elapsed() >= Duration::from_millis(350));
        assert_eq!(i16::from_le_bytes([bytes.bytes[0], bytes.bytes[1]]), 8192);
        stream.close();
        encoder.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn wav_waits_for_shared_sample_and_rejects_old_epoch() {
        let first = test_stream();
        let second = test_stream();
        first.set_playback_epoch(1);
        second.set_playback_epoch(1);
        let first_encoder = FfmpegEncoder::spawn(wav_config(), first.clone()).unwrap();
        let (header, mut first_rx) = first.attach_subscriber();
        if header.is_none() {
            assert_eq!(first_rx.recv().await.unwrap().bytes.len(), 44);
        }
        let second_encoder = FfmpegEncoder::spawn(wav_config(), second.clone()).unwrap();
        let (header, mut second_rx) = second.attach_subscriber();
        if header.is_none() {
            assert_eq!(second_rx.recv().await.unwrap().bytes.len(), 44);
        }
        let source_time = std::time::Instant::now();
        for encoder in [&first_encoder, &second_encoder] {
            encoder
                .write_frame(PcmFrame {
                    buffered_permit: None,
                    playback_epoch: 0,
                    sample_rate: 1000,
                    channels: 1,
                    samples_f32_interleaved: vec![-1.0; 8],
                    presentation_time: Some(source_time),
                })
                .await
                .unwrap();
            encoder
                .write_frame(PcmFrame {
                    buffered_permit: None,
                    playback_epoch: 1,
                    sample_rate: 1000,
                    channels: 1,
                    samples_f32_interleaved: vec![0.25, 0.5, 0.75, 1.0],
                    presentation_time: Some(source_time),
                })
                .await
                .unwrap();
        }
        tokio::task::yield_now().await;
        assert!(first_rx.try_recv().is_err());
        assert!(second_rx.try_recv().is_err());
        let release = std::time::Instant::now() + Duration::from_millis(20);
        let cutoff = source_time + Duration::from_millis(2);
        first.set_playback_plan(cutoff, release);
        second.set_playback_plan(cutoff, release + Duration::from_millis(20));
        let a = tokio::time::timeout(Duration::from_secs(1), first_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(std::time::Instant::now() >= release);
        let b = tokio::time::timeout(Duration::from_secs(1), second_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(a.bytes, b.bytes);
        assert_eq!(a.bytes.as_ref(), f32_pcm_to_s16le_bytes(&[0.75, 1.0]));
        first_encoder.shutdown().await.unwrap();
        second_encoder.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stderr_retains_only_bounded_tail_and_drains_input() {
        let mut bytes = vec![b'x'; 128 * 1024];
        bytes.extend_from_slice(b"final error");
        let tail = read_stderr_tail(bytes.as_slice()).await.unwrap();
        assert_eq!(tail.len(), 16 * 1024);
        assert!(tail.ends_with("final error"));
    }
}
