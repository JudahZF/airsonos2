use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use airsonos2_core::PcmFrame;
use bytes::Bytes;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::task::JoinError;
use tokio::time;
use tracing::debug;

use airsonos2_core::StreamCodec;

use crate::pcm::f32_pcm_to_s16le_bytes;
use crate::registry::LiveStream;

#[derive(Clone, Debug)]
pub struct FfmpegEncoderConfig {
    pub ffmpeg_path: PathBuf,
    pub sample_rate: u32,
    pub channels: u8,
    pub mp3_bitrate_kbps: u16,
    pub codec: StreamCodec,
}

#[derive(Debug)]
pub struct FfmpegEncoder {
    input: Option<mpsc::Sender<PcmFrame>>,
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
        let (input, mut rx) = mpsc::channel::<PcmFrame>(512);
        let first_pcm = Arc::new(AtomicBool::new(false));
        let first_mp3 = Arc::new(AtomicBool::new(false));
        let first_pcm_reader = first_pcm.clone();
        let first_mp3_reader = first_mp3.clone();

        let task = tokio::spawn(async move {
            let terminal = stream.clone();
            let writer = async move {
                let mut stdin = stdin;
                while let Some(frame) = rx.recv().await {
                    if !first_pcm_reader.swap(true, Ordering::Relaxed) {
                        debug!(
                            sample_rate = frame.sample_rate,
                            channels = frame.channels,
                            samples = frame.samples_f32_interleaved.len(),
                            "first PCM frame written to ffmpeg"
                        );
                    }
                    let bytes = f32_pcm_to_s16le_bytes(&frame.samples_f32_interleaved);
                    stdin.write_all(&bytes).await?;
                }
                stdin.shutdown().await
            };
            let reader = async move {
                let mut stdout = stdout;
                let mut buf = vec![0_u8; 16 * 1024];
                loop {
                    let read = stdout.read(&mut buf).await?;
                    if read == 0 {
                        break;
                    }
                    if !first_mp3_reader.swap(true, Ordering::Relaxed) {
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
        let (input, mut rx) = mpsc::channel::<PcmFrame>(512);
        let task = tokio::spawn(async move {
            let mut sent_header = false;
            let mut first_pcm = false;

            while let Some(frame) = tokio::select! {
                _ = stream.closed() => None,
                frame = rx.recv() => frame,
            } {
                if !first_pcm {
                    first_pcm = true;
                    debug!(
                        sample_rate = frame.sample_rate,
                        channels = frame.channels,
                        samples = frame.samples_f32_interleaved.len(),
                        "first PCM frame received for WAV stream"
                    );
                }

                if !sent_header {
                    sent_header = true;
                    stream.publish_prelude(Bytes::from(wav_stream_header(
                        config.sample_rate,
                        config.channels,
                    )));
                    debug!("WAV stream header published");
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

    /// A stalled encoder must not suspend other rooms. False means the bounded
    /// input queue was full and this realtime frame was dropped.
    pub fn try_write_frame(&self, frame: PcmFrame) -> Result<bool, EncoderError> {
        match self
            .input
            .as_ref()
            .ok_or(EncoderError::InputClosed)?
            .try_send(frame)
        {
            Ok(()) => Ok(true),
            Err(mpsc::error::TrySendError::Full(_)) => Ok(false),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(EncoderError::InputClosed),
        }
    }

    pub async fn write_frame(&self, frame: PcmFrame) -> Result<(), EncoderError> {
        self.input
            .as_ref()
            .ok_or(EncoderError::InputClosed)?
            .send(frame)
            .await
            .map_err(|_| EncoderError::InputClosed)
    }

    pub async fn shutdown(mut self) -> Result<(), EncoderError> {
        self.input.take();
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

    #[tokio::test]
    async fn stderr_retains_only_bounded_tail_and_drains_input() {
        let mut bytes = vec![b'x'; 128 * 1024];
        bytes.extend_from_slice(b"final error");
        let tail = read_stderr_tail(bytes.as_slice()).await.unwrap();
        assert_eq!(tail.len(), 16 * 1024);
        assert!(tail.ends_with("final error"));
    }
}
