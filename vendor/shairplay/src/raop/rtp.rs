//! AP1 RTP audio streaming — UDP and TCP receiver with ALAC decode.
//!
//! Manages the full AP1 audio receive pipeline:
//!
//! ```text
//! iPhone → RTP/UDP or RTP/TCP → RaopRtp → RaopBuffer (decrypt+decode) → AudioSession
//! ```
//!
//! Two transport modes:
//! - **UDP** (default): data, control, and timing on separate UDP sockets.
//!   Control channel carries retransmit responses (payload type 0x56).
//! - **TCP**: single TCP connection with `$`-prefixed interleaved framing.
//!   No retransmits (reliable transport).

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{Mutex, watch};
use tracing::info;

use crate::codec::alac::AlacConfig;
use crate::error::{NetworkError, ShairplayError};
use crate::raop::buffer::{RAOP_PACKET_LEN, RaopBuffer};
use crate::raop::{AudioCodec, AudioFormat, AudioHandler};

/// Sentinel value for [`RtpState::flush`] indicating no flush is pending.
const NO_FLUSH: i32 = -42;

/// Determine the bind address for RTP sockets.
/// Uses the specific local IP for routable addresses (respects BindConfig).
/// Falls back to unspecified for link-local IPv6 — the iPhone may send RTP
/// packets from a different address than the RTSP connection used.
fn rtp_bind_addr(local: IpAddr) -> IpAddr {
    match local {
        IpAddr::V6(v6) if (v6.segments()[0] & 0xffc0) == 0xfe80 => IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
        other => other,
    }
}

fn bind_udp(addr: SocketAddr) -> Result<UdpSocket, ShairplayError> {
    let socket = std::net::UdpSocket::bind(addr).map_err(NetworkError::Io)?;
    socket.set_nonblocking(true).map_err(NetworkError::Io)?;
    UdpSocket::from_std(socket)
        .map_err(NetworkError::Io)
        .map_err(Into::into)
}

fn bind_tcp(addr: SocketAddr) -> Result<TcpListener, ShairplayError> {
    let listener = std::net::TcpListener::bind(addr).map_err(NetworkError::Io)?;
    listener.set_nonblocking(true).map_err(NetworkError::Io)?;
    TcpListener::from_std(listener)
        .map_err(NetworkError::Io)
        .map_err(Into::into)
}

/// Parse the SDP `c=` remote address to raw IP bytes for DACP callbacks.
/// Handles "IP6 ::1" prefix and IPv4-mapped IPv6 addresses.
pub(crate) fn remote_addr_bytes(remote: &str) -> Vec<u8> {
    let addr_str = remote.strip_prefix("IP6 ").unwrap_or(remote);
    if let Ok(ip) = addr_str.parse::<IpAddr>() {
        match ip {
            IpAddr::V4(v4) => v4.octets().to_vec(),
            IpAddr::V6(v6) => v6.octets().to_vec(),
        }
    } else {
        vec![]
    }
}

/// Configuration for creating an AP1 RTP session, parsed from SDP.
pub struct RtpConfig {
    /// SDP `c=` remote address string (e.g. "192.168.1.5").
    pub remote: String,
    /// Local IP address to bind sockets to.
    pub local_addr: IpAddr,
    /// SDP `a=rtpmap` attribute.
    pub rtpmap: String,
    /// SDP `a=fmtp` attribute (ALAC configuration).
    pub fmtp: String,
    /// 128-bit AES session key (decrypted from SDP).
    pub aes_key: [u8; 16],
    /// 128-bit AES initialization vector.
    pub aes_iv: [u8; 16],
    /// If set, resample decoded audio to this rate.
    pub output_sample_rate: Option<u32>,
    /// Full socket address of the remote peer (preserves scope_id for link-local IPv6).
    pub remote_socket: std::net::SocketAddr,
}

/// AP1 RTP streaming session.
///
/// Owns the UDP/TCP sockets, the packet buffer, and the ALAC decoder.
/// Created when the iPhone sends the SDP ANNOUNCE. Started during RTSP SETUP,
/// which binds ports and spawns the receive task.
///
/// Dropped when the RTSP connection closes, which sends a shutdown signal
/// to the receive task via the [`watch`] channel.
pub struct RaopRtp {
    tasks: tokio::task::JoinSet<()>,
    handler: Arc<dyn AudioHandler>,
    /// SDP `c=` remote address string (e.g. "192.168.1.5").
    remote: String,
    /// Local IP address to bind sockets to (matches the RTSP connection's interface).
    local_addr: IpAddr,
    /// If set, resample decoded audio to this rate before delivery.
    output_sample_rate: Option<u32>,
    /// Parsed ALAC stream configuration.
    config: AlacConfig,
    /// Shared packet buffer (decrypt + decode on queue, dequeue in order).
    buffer: Arc<Mutex<RaopBuffer>>,
    /// Shared mutable state for cross-task event delivery.
    state: Arc<std::sync::atomic::AtomicI32>,
    /// Send `true` to shut down the receive task.
    shutdown_tx: Option<watch::Sender<bool>>,
    /// iPhone's control port (0 = no retransmits).
    control_rport: u16,
    /// Local control port (bound by us).
    pub(crate) control_lport: u16,
    /// Local timing port (bound by us).
    pub(crate) timing_lport: u16,
    /// Local data port (bound by us).
    pub(crate) data_lport: u16,
    /// Full socket address of the remote peer.
    remote_socket: std::net::SocketAddr,
}

impl RaopRtp {
    /// Create a new RTP session from SDP parameters and AES session keys.
    /// Does not bind sockets or start receiving — call [`start`](Self::start) for that.
    pub fn new(callbacks: Arc<dyn AudioHandler>, config: RtpConfig) -> Option<Self> {
        let buffer = RaopBuffer::new(&config.rtpmap, &config.fmtp, &config.aes_key, &config.aes_iv)?;
        let alac_config = buffer.config().clone();
        Some(Self {
            tasks: tokio::task::JoinSet::new(),
            handler: callbacks,
            remote: config.remote,
            local_addr: config.local_addr,
            output_sample_rate: config.output_sample_rate,
            remote_socket: config.remote_socket,
            config: alac_config,
            buffer: Arc::new(Mutex::new(buffer)),
            state: Arc::new(std::sync::atomic::AtomicI32::new(NO_FLUSH)),
            shutdown_tx: None,
            control_rport: 0,
            control_lport: 0,
            timing_lport: 0,
            data_lport: 0,
        })
    }

    /// Bind UDP/TCP sockets and spawn the async receive task.
    ///
    /// Returns `(control_port, timing_port, data_port)` — the local ports
    /// that the iPhone should send RTP packets to.
    ///
    /// # Transport modes
    ///
    /// - `use_udp = true`: binds 3 UDP sockets (data, control, timing).
    ///   Control channel receives retransmit responses (RTP payload type 0x56).
    /// - `use_udp = false`: binds 1 TCP listener. iPhone connects and sends
    ///   `$`-prefixed interleaved RTP frames.
    pub fn start(
        &mut self,
        use_udp: bool,
        control_rport: u16,
        timing_rport: u16,
    ) -> Result<(u16, u16, u16), ShairplayError> {
        self.control_rport = control_rport;
        info!(use_udp, control_rport, timing_rport, remote = %self.remote, "AP1 RTP starting");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        if use_udp {
            let bind_addr = SocketAddr::new(rtp_bind_addr(self.local_addr), 0);
            let csock = bind_udp(bind_addr)?;
            let tsock = bind_udp(bind_addr)?;
            let dsock = bind_udp(bind_addr)?;
            self.control_lport = csock.local_addr().map_err(NetworkError::Io)?.port();
            self.timing_lport = tsock.local_addr().map_err(NetworkError::Io)?.port();
            self.data_lport = dsock.local_addr().map_err(NetworkError::Io)?.port();

            // Spawn NTP timing responder for this connection.
            let remote_sockaddr = self.remote_socket;
            let mut timing_addr = remote_sockaddr;
            timing_addr.set_port(timing_rport);
            self.tasks.spawn(super::ntp::run_ntp_responder(tsock, timing_addr));

            let config = self.config.clone();
            let mut session = self.handler.audio_init(AudioFormat {
                codec: AudioCodec::Pcm,
                bits: 32,
                channels: config.num_channels,
                sample_rate: self.output_sample_rate.unwrap_or(config.sample_rate),
            });

            #[cfg(feature = "resample")]
            let mut resampler = if let Some(target) = self.output_sample_rate {
                if target != config.sample_rate {
                    crate::codec::resample::StreamResampler::new(
                        config.sample_rate,
                        target,
                        config.num_channels as usize,
                    )
                } else {
                    None
                }
            } else {
                None
            };

            let buffer = self.buffer.clone();
            let state = self.state.clone();
            // If control_rport is 0, the iPhone doesn't support retransmits.
            let no_resend = control_rport == 0;
            let _remote_for_task = self.remote.clone();

            self.tasks.spawn(async move {
                let mut shutdown_rx = shutdown_rx;
                let mut data_packet = [0u8; RAOP_PACKET_LEN];
                let mut ctrl_packet = [0u8; RAOP_PACKET_LEN];
                loop {
                    // Drain flush events only — metadata goes through AudioHandler now.
                    {
                        let flush = state.swap(NO_FLUSH, std::sync::atomic::Ordering::AcqRel);
                        if flush != NO_FLUSH {
                            buffer.lock().await.flush(flush);
                            session.audio_flush();
                        }
                    }

                    tokio::select! {
                        // Data channel: audio RTP packets.
                        result = dsock.recv_from(&mut data_packet) => {
                            if let Ok((len, _)) = result
                                && len >= 12 {
                                    let mut buf = buffer.lock().await;
                                    buf.queue(&data_packet[..len], true);
                                    while let Some(samples) = buf.dequeue(no_resend) {
                                        {
                                            #[cfg(feature = "resample")]
                                            if let Some(ref mut rs) = resampler {
                                                let resampled = rs.process(samples);
                                                session.audio_process(&resampled);
                                            } else {
                                                session.audio_process(samples);
                                            }
                                            #[cfg(not(feature = "resample"))]
                                            session.audio_process(samples);
                                        }
                                    }
                                }
                        }
                        // Control channel: retransmit responses (payload type 0x56).
                        result = csock.recv_from(&mut ctrl_packet) => {
                            if let Ok((len, _)) = result
                                && len >= 12 && (ctrl_packet[1] & !0x80) == 0x56 {
                                    let mut buf = buffer.lock().await;
                                    // Retransmit packets have a 4-byte header before the original RTP.
                                    if len > 4 { buf.queue(&ctrl_packet[4..len], true); }
                                }
                        }
                        _ = shutdown_rx.changed() => break,
                    }
                }
                // AudioSession dropped here → triggers cleanup in the app.
            });
        } else {
            // TCP interleaved mode: single connection, `$`-prefixed framing.
            let listener = bind_tcp(SocketAddr::new(rtp_bind_addr(self.local_addr), 0))?;
            self.data_lport = listener.local_addr().map_err(NetworkError::Io)?.port();

            let config = self.config.clone();
            let mut session = self.handler.audio_init(AudioFormat {
                codec: AudioCodec::Pcm,
                bits: 32,
                channels: config.num_channels,
                sample_rate: self.output_sample_rate.unwrap_or(config.sample_rate),
            });

            #[cfg(feature = "resample")]
            let mut resampler = if let Some(target) = self.output_sample_rate {
                if target != config.sample_rate {
                    crate::codec::resample::StreamResampler::new(
                        config.sample_rate,
                        target,
                        config.num_channels as usize,
                    )
                } else {
                    None
                }
            } else {
                None
            };

            let buffer = self.buffer.clone();
            let state = self.state.clone();
            let _remote_for_tcp = self.remote.clone();

            self.tasks.spawn(async move {
                use tokio::io::AsyncReadExt;
                let mut shutdown_rx = shutdown_rx;

                // Wait for the iPhone to connect.
                let stream = tokio::select! {
                    result = listener.accept() => match result {
                        Ok((s, _)) => s,
                        Err(_) => return,
                    },
                    _ = shutdown_rx.changed() => return,
                };

                let mut reader = tokio::io::BufReader::new(stream);
                let mut packet_buf = Vec::with_capacity(RAOP_PACKET_LEN + 4);
                let mut read_buf = [0u8; 4096];

                'tcp: loop {
                    // Drain flush events only — metadata goes through AudioHandler now.
                    {
                        let flush = state.swap(NO_FLUSH, std::sync::atomic::Ordering::AcqRel);
                        if flush != NO_FLUSH {
                            buffer.lock().await.flush(flush);
                            session.audio_flush();
                        }
                    }

                    tokio::select! {
                        result = reader.read(&mut read_buf) => {
                            match result {
                                Ok(0) | Err(_) => break,
                                Ok(n) => packet_buf.extend_from_slice(&read_buf[..n]),
                            }
                            if packet_buf.len() > RAOP_PACKET_LEN * 4 {
                                tracing::warn!("TCP RTP buffer exceeded safety limit");
                                break;
                            }
                            // TCP interleaved: each frame is `$ <channel> <len_hi> <len_lo> <rtp...>`.
                            while packet_buf.len() >= 4 {
                                if packet_buf[0] != b'$' || packet_buf[1] != 0 {
                                    packet_buf.drain(..1);
                                    continue;
                                }
                                let rtp_len = ((packet_buf[2] as usize) << 8) | packet_buf[3] as usize;
                                if rtp_len > RAOP_PACKET_LEN {
                                    tracing::warn!(rtp_len, "TCP RTP frame exceeded maximum size, closing");
                                    packet_buf.clear();
                                    break 'tcp;
                                }
                                if packet_buf.len() < 4 + rtp_len { break; }
                                let mut buf = buffer.lock().await;
                                buf.queue(&packet_buf[4..4 + rtp_len], false);
                                if let Some(samples) = buf.dequeue(true) {
                                    {
                                            #[cfg(feature = "resample")]
                                            if let Some(ref mut rs) = resampler {
                                                let resampled = rs.process(samples);
                                                session.audio_process(&resampled);
                                            } else {
                                                session.audio_process(samples);
                                            }
                                            #[cfg(not(feature = "resample"))]
                                            session.audio_process(samples);
                                        }
                                }
                                drop(buf);
                                packet_buf.drain(..4 + rtp_len);
                            }
                        }
                        _ = shutdown_rx.changed() => break,
                    }
                }
            });
        }

        Ok((self.control_lport, self.timing_lport, self.data_lport))
    }

    /// Request a buffer flush up to the given sequence number.
    pub fn flush(&self, next_seq: i32) {
        self.state.store(next_seq, std::sync::atomic::Ordering::Release);
    }

    /// Stop and await owned receive and timing tasks.
    pub async fn shutdown(&mut self) {
        self.stop();
        self.tasks.shutdown().await;
    }

    /// Stop the receive task and flush the buffer.
    pub fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
        self.tasks.abort_all();
    }
}
