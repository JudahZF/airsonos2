//! AirPlay 2 encrypted event and remote control channels.
//!
//! After initial SETUP, the client connects to the event TCP port.
//! All traffic is encrypted with ChaCha20-Poly1305 using HKDF-derived keys.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::crypto::chacha_transport::EncryptedChannel;
use crate::error::NetworkError;

/// Handle for sending commands through the event channel.
#[derive(Clone)]
pub struct EventSender {
    tx: mpsc::Sender<Vec<u8>>,
}

impl EventSender {
    /// Create from an existing channel sender.
    pub fn from_tx(tx: mpsc::Sender<Vec<u8>>) -> Self {
        Self { tx }
    }

    /// Send a plaintext message (will be encrypted before transmission).
    pub fn send(&self, data: Vec<u8>) -> Result<(), NetworkError> {
        self.tx
            .try_send(data)
            .map_err(|_| NetworkError::Mdns("event channel closed".into()))
    }
}

/// Async event channel that accepts one encrypted TCP connection.
pub struct EventChannel {
    listener: TcpListener,
    /// Port number the listener is bound to.
    pub port: u16,
}

impl EventChannel {
    /// Bind a TCP listener on any available port at the given address.
    pub async fn bind(addr: &str) -> Result<Self, NetworkError> {
        let listener = TcpListener::bind(addr).await?;
        let port = listener.local_addr()?.port();
        debug!(port, "Event channel listening");
        Ok(Self { listener, port })
    }

    /// Run the event channel. Returns an EventSender for sending commands.
    pub async fn run(self, channel: EncryptedChannel) -> EventSender {
        let (tx, rx) = mpsc::channel(16);
        let sender = EventSender { tx };

        let (stream, addr) = match self.listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                warn!("Event channel accept failed: {e}");
                return sender;
            }
        };
        info!(%addr, "Event channel client connected");

        tokio::spawn(Self::handle(stream, channel, rx));
        sender
    }

    /// Handle a connected event channel stream (public for use from handlers).
    pub async fn handle_stream(stream: TcpStream, channel: EncryptedChannel, cmd_rx: mpsc::Receiver<Vec<u8>>) {
        Self::handle(stream, channel, cmd_rx).await;
    }

    async fn handle(mut stream: TcpStream, mut channel: EncryptedChannel, mut cmd_rx: mpsc::Receiver<Vec<u8>>) {
        let mut buf = vec![0u8; 4096];
        let mut encrypted_buf = Vec::new();
        loop {
            tokio::select! {
                result = stream.read(&mut buf) => {
                    match result {
                        Ok(0) => { debug!("Event channel closed by client"); break; }
                        Ok(n) => {
                            if encrypted_buf.len() + n > 8192 { break; }
                            encrypted_buf.extend_from_slice(&buf[..n]);
                            debug!(n, "Event channel data received");
                            match channel.decrypt_ctx.decrypt(&encrypted_buf) {
                                Ok((plain, consumed)) => {
                                    if consumed > 0 { encrypted_buf.drain(..consumed); }
                                    if !plain.is_empty() {
                                        debug!(len = plain.len(), "Event channel message received");
                                    }
                                }
                                Err(e) => { warn!("Event channel decrypt error: {e}"); break; }
                            }
                        }
                        Err(e) => { warn!("Event channel read error: {e}"); break; }
                    }
                }
                data = cmd_rx.recv() => {
                    let Some(data) = data else { break; };
                    debug!(len = data.len(), "Sending on event channel");
                    let encrypted = match channel.encrypt_ctx.encrypt(&data) {
                        Ok(e) => e,
                        Err(e) => { warn!("Event channel encrypt error: {e}"); break; }
                    };
                    if let Err(e) = stream.write_all(&encrypted).await {
                        warn!("Event channel write error: {e}"); break;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::chacha_transport::CipherContext;

    #[tokio::test]
    async fn event_channel_binds() {
        let ch = EventChannel::bind("0.0.0.0:0").await.unwrap();
        assert!(ch.port > 0);
    }

    #[tokio::test]
    async fn event_channel_roundtrip() {
        let ch = EventChannel::bind("0.0.0.0:0").await.unwrap();
        let port = ch.port;

        let key = [0x42u8; 32];
        let server_channel = EncryptedChannel {
            encrypt_ctx: CipherContext::new(key),
            decrypt_ctx: CipherContext::new(key),
        };

        // Spawn server
        let handle = tokio::spawn(async move { ch.run(server_channel).await });

        // Client connects and sends encrypted data
        let mut client = TcpStream::connect(format!("127.0.0.1:{port}")).await.unwrap();
        let mut client_enc = CipherContext::new(key);
        let encrypted = client_enc.encrypt(b"test event").unwrap();
        client.write_all(&encrypted).await.unwrap();

        // Close triggers server exit
        drop(client);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
    }
}

#[cfg(test)]
mod failure_tests {
    use super::*;
    #[tokio::test]
    async fn invalid_ciphertext_closes_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let (_tx, rx) = mpsc::channel(16);
        let task = tokio::spawn(EventChannel::handle_stream(
            stream,
            EncryptedChannel::events(&[7; 32]).unwrap(),
            rx,
        ));
        client
            .write_all(&[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
            .await
            .unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), client.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        task.await.unwrap();
    }
}
