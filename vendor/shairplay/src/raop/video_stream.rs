//! Video stream receiver for AirPlay 2 screen mirroring (stream type 110).
//!
//! Accepts a TCP connection, reads 128-byte headers + variable-length payloads,
//! classifies packets, decrypts Payload types, and delivers to VideoSession.

use bytes::BytesMut;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, trace, warn};

use crate::crypto::video_cipher::VideoCipher;
use crate::raop::video::{PacketKind, VideoPacket, VideoSession};

const VIDEO_HEADER_LEN: usize = 128;
const MAX_VIDEO_PAYLOAD_LEN: usize = 32 * 1024 * 1024;

/// Calls [`VideoSession::on_video_end`] exactly once, including when the
/// receiver future is cancelled before or during streaming.
struct EndOnDrop(Box<dyn VideoSession>);

impl Drop for EndOnDrop {
    fn drop(&mut self) {
        self.0.on_video_end();
    }
}

/// Run the video stream receiver. Accepts one TCP connection and processes packets.
///
/// The session is ended when the returned future completes or is dropped.
pub fn run(listener: TcpListener, cipher: VideoCipher, session: Box<dyn VideoSession>) -> impl Future<Output = ()> {
    let mut session = EndOnDrop(session);
    async move {
        let (stream, addr) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                warn!("Video stream accept failed: {e}");
                return;
            }
        };
        info!(%addr, "Video stream client connected");
        process(stream, cipher, session.0.as_mut()).await;
    }
}

async fn process(mut stream: TcpStream, mut cipher: VideoCipher, session: &mut dyn VideoSession) {
    let mut header = [0u8; VIDEO_HEADER_LEN];

    loop {
        // Read 128-byte header
        if stream.read_exact(&mut header).await.is_err() {
            debug!("Video stream ended");
            break;
        }

        // Parse header fields (little-endian)
        let payload_len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let packet_type = u16::from_le_bytes([header[4], header[5]]);
        let timestamp = u64::from_le_bytes([
            header[8], header[9], header[10], header[11], header[12], header[13], header[14], header[15],
        ]);

        if payload_len == 0 {
            continue;
        }
        if payload_len > MAX_VIDEO_PAYLOAD_LEN {
            warn!(payload_len, "Video payload exceeds maximum allowed size");
            break;
        }

        // Read payload
        let mut payload = BytesMut::zeroed(payload_len);
        if stream.read_exact(&mut payload).await.is_err() {
            debug!("Video stream ended during payload read");
            break;
        }

        // Classify packet
        let kind = match packet_type {
            1 => {
                if payload.len() >= 8 && &payload[4..8] == b"hvc1" {
                    PacketKind::HvcC
                } else {
                    PacketKind::AvcC
                }
            }
            0 | 4096 => PacketKind::Payload,
            5 => PacketKind::Plist,
            other => PacketKind::Other(other),
        };

        // Decrypt payload packets
        if matches!(kind, PacketKind::Payload) {
            cipher.decrypt(&mut payload);
        }

        trace!(?kind, timestamp, payload_len, "Video packet");
        session.on_video(VideoPacket {
            kind,
            timestamp,
            payload: payload.freeze(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Session(std::sync::mpsc::Sender<()>);
    impl VideoSession for Session {
        fn on_video(&mut self, _: VideoPacket) {}
        fn on_video_end(&mut self) {
            self.0.send(()).unwrap();
        }
    }

    #[tokio::test]
    async fn cancelled_receiver_ends_session_once() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (ended, end_events) = std::sync::mpsc::channel();
        let task = tokio::spawn(run(
            listener,
            VideoCipher::new(&[0; 16], &[0; 16]),
            Box::new(Session(ended)),
        ));
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(end_events.try_recv().is_ok());
        assert!(end_events.try_recv().is_err());
    }
}
