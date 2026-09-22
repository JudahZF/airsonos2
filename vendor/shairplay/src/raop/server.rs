//! AirPlay server builder and lifecycle.

use super::connection::RaopShared;
use super::types::*;
use crate::crypto::pairing::Pairing;
use crate::crypto::rsa::RsaKey;
use crate::error::{ServerError, ShairplayError};
use crate::net::mdns::{AirPlayServiceInfo, MdnsService};
use crate::net::server::{BindConfig, HttpServer};
use std::sync::Arc;

const AIRPORT_KEY: &str = include_str!("../../airport.key");

fn airport_rsakey() -> Arc<RsaKey> {
    use std::sync::OnceLock;
    static KEY: OnceLock<Arc<RsaKey>> = OnceLock::new();
    KEY.get_or_init(|| Arc::new(RsaKey::from_pem(AIRPORT_KEY).expect("built-in airport.key is invalid")))
        .clone()
}

fn random_hwaddr() -> Vec<u8> {
    use rand::RngCore;

    let mut hwaddr = [0u8; super::MAX_HWADDR_LEN];
    rand::thread_rng().fill_bytes(&mut hwaddr);
    // Locally administered, unicast MAC address.
    hwaddr[0] = (hwaddr[0] | 0x02) & !0x01;
    hwaddr.to_vec()
}

#[cfg(feature = "ap2")]
fn derive_pi_from_hwaddr(hwaddr: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(hwaddr);
    let hash = hasher.finalize();
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        hash[0],
        hash[1],
        hash[2],
        hash[3],
        hash[4],
        hash[5],
        (hash[6] & 0x0f) | 0x40, // version 4
        hash[7],
        (hash[8] & 0x3f) | 0x80, // variant 1
        hash[9],
        hash[10],
        hash[11],
        hash[12],
        hash[13],
        hash[14],
        hash[15]
    )
}

/// Builder for [`RaopServer`].
pub struct RaopServerBuilder {
    max_clients: usize,
    hwaddr: Option<Vec<u8>>,
    password: Option<String>,
    name: String,
    model: String,
    bind: BindConfig,
    #[cfg(feature = "ap2")]
    pairing_store: Option<Arc<dyn PairingStore>>,
    #[cfg(feature = "ap2")]
    mode: AirPlayMode,
    output_sample_rate: Option<u32>,
    output_max_channels: Option<u8>,
    #[cfg(feature = "ap2")]
    pin: Option<String>,
    #[cfg(feature = "video")]
    video_handler: Option<Arc<dyn crate::raop::video::VideoHandler>>,
    #[cfg(feature = "hls")]
    hls_handler: Option<Arc<dyn crate::raop::hls::HlsHandler>>,
}

impl Default for RaopServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RaopServerBuilder {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self {
            max_clients: 10,
            hwaddr: None,
            password: None,
            name: "Shairplay".to_string(),
            model: crate::raop::config::GLOBAL_MODEL.to_string(),
            bind: BindConfig::default(),
            #[cfg(feature = "ap2")]
            pairing_store: None,
            #[cfg(feature = "ap2")]
            mode: AirPlayMode::default(),
            output_sample_rate: None,
            output_max_channels: None,
            #[cfg(feature = "ap2")]
            pin: None,
            #[cfg(feature = "video")]
            video_handler: None,
            #[cfg(feature = "hls")]
            hls_handler: None,
        }
    }

    /// Set the maximum number of concurrent connections. Default: 10.
    pub fn max_clients(mut self, n: usize) -> Self {
        self.max_clients = n;
        self
    }
    /// Set the 6-byte hardware address for mDNS registration.
    pub fn hwaddr(mut self, addr: impl Into<Vec<u8>>) -> Self {
        self.hwaddr = Some(addr.into());
        self
    }
    /// Set an optional HTTP Digest authentication password.
    pub fn password(mut self, pw: impl Into<String>) -> Self {
        self.password = Some(pw.into());
        self
    }
    /// Set the RTSP listening port. Default: 5000.
    pub fn port(mut self, port: u16) -> Self {
        self.bind.port = port;
        self
    }
    /// Set full bind configuration (address, port, auto-sensing, IPv6).
    pub fn bind(mut self, config: BindConfig) -> Self {
        self.bind = config;
        self
    }
    /// Set the AirPlay display name. Default: "Shairplay".
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set the advertised Apple hardware model identifier.
    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }

    /// Set a pairing store for persisting device keys across restarts.
    /// Without this, iPhones must re-pair on every server restart.
    #[cfg(feature = "ap2")]
    pub fn pairing_store(mut self, store: Arc<dyn PairingStore>) -> Self {
        self.pairing_store = Some(store);
        self
    }

    /// Set the AirPlay protocol mode. Default: [`AirPlayMode::AirPlay2`].
    ///
    /// Use [`AirPlayMode::AirPlay1`] to advertise as a classic receiver even
    /// when the `ap2` feature is compiled in.
    #[cfg(feature = "ap2")]
    pub fn mode(mut self, mode: AirPlayMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set the desired output sample rate. The library resamples to this rate.
    /// Default: source native rate (no resampling).
    pub fn output_sample_rate(mut self, rate: u32) -> Self {
        self.output_sample_rate = Some(rate);
        self
    }

    /// Set the maximum output channels. Sources with more channels are mixed down.
    /// Sources with fewer channels are passed through (no upmixing).
    /// Default: pass through native channel count.
    pub fn output_max_channels(mut self, channels: u8) -> Self {
        self.output_max_channels = Some(channels);
        self
    }

    #[cfg(feature = "ap2")]
    /// Set the HomeKit pairing PIN. Default: "3939".
    pub fn pin(mut self, pin: impl Into<String>) -> Self {
        self.pin = Some(pin.into());
        self
    }

    #[cfg(feature = "video")]
    /// Set a video handler for screen mirroring (experimental).
    pub fn video_handler(mut self, handler: Arc<dyn crate::raop::video::VideoHandler>) -> Self {
        self.video_handler = Some(handler);
        self
    }

    #[cfg(feature = "hls")]
    /// Set an HLS handler for YouTube/video URL playback.
    pub fn hls_handler(mut self, handler: Arc<dyn crate::raop::hls::HlsHandler>) -> Self {
        self.hls_handler = Some(handler);
        self
    }

    /// Build the server with the given audio handler.
    pub fn build(self, handler: Arc<dyn AudioHandler>) -> Result<RaopServer, ShairplayError> {
        if self.max_clients == 0 {
            return Err(ServerError::MaxClients(0).into());
        }
        if let Some(password) = self.password.as_ref()
            && password.len() > super::MAX_PASSWORD_LEN
        {
            return Err(ServerError::InvalidPassword(password.len()).into());
        }
        let rsakey = airport_rsakey();
        let pairing = Arc::new(Pairing::generate()?);
        let hwaddr = match self.hwaddr {
            Some(addr) if addr.len() == super::MAX_HWADDR_LEN => addr,
            Some(addr) => return Err(ServerError::InvalidHwAddr(addr.len()).into()),
            None => random_hwaddr(),
        };

        #[cfg(feature = "ap2")]
        let pairing_id = derive_pi_from_hwaddr(&hwaddr);
        #[cfg(feature = "ap2")]
        let airplay_name = self.name.clone();
        #[cfg(feature = "ap2")]
        let airplay_model = self.model.clone();

        #[cfg(not(feature = "resample"))]
        if self.output_sample_rate.is_some() {
            return Err(ServerError::InvalidConfig("output rate requires resample feature".into()).into());
        }
        if self
            .output_sample_rate
            .is_some_and(|rate| !(8000..=192000).contains(&rate))
            || self
                .output_max_channels
                .is_some_and(|channels| !(1..=2).contains(&channels))
        {
            return Err(ServerError::InvalidConfig("unsupported output format".into()).into());
        }
        let shared = Arc::new(RaopShared {
            #[cfg(feature = "ap2")]
            controller_session: Default::default(),
            rsakey,
            pairing,
            hwaddr: hwaddr.clone(),
            password: self.password.unwrap_or_default(),
            handler,
            #[cfg(feature = "ap2")]
            pairing_store: self
                .pairing_store
                .unwrap_or_else(|| Arc::new(MemoryPairingStore::default())),
            output_sample_rate: self.output_sample_rate,
            output_max_channels: self.output_max_channels,
            #[cfg(feature = "ap2")]
            pin: self.pin,
            #[cfg(feature = "video")]
            video_handler: self.video_handler,
            #[cfg(feature = "video")]
            video_ekey: Arc::new(std::sync::RwLock::new(None)),
            #[cfg(feature = "video")]
            video_eiv: Arc::new(std::sync::RwLock::new(None)),
            #[cfg(feature = "ap2")]
            pairing_id,
            #[cfg(feature = "ap2")]
            airplay_name,
            #[cfg(feature = "ap2")]
            airplay_model,
            #[cfg(feature = "hls")]
            hls_handler: self.hls_handler,
        });

        let mut httpd = HttpServer::new(shared.clone(), self.max_clients);
        httpd.set_bind_config(self.bind.clone());

        Ok(RaopServer {
            shared,
            httpd,
            mdns: None,
            bind: self.bind,
            name: self.name,
            model: self.model,
            hwaddr,
            #[cfg(feature = "ap2")]
            mode: self.mode,
        })
    }
}

/// The main AirPlay/RAOP server.
///
/// Listens for RTSP connections, handles pairing and encryption,
/// decodes audio, and delivers f32 PCM samples via [`AudioSession`].
/// Automatically registers mDNS services for network discovery.
pub struct RaopServer {
    shared: Arc<RaopShared>,
    httpd: HttpServer,
    mdns: Option<MdnsService>,
    bind: BindConfig,
    name: String,
    model: String,
    hwaddr: Vec<u8>,
    #[cfg(feature = "ap2")]
    mode: AirPlayMode,
}

impl RaopServer {
    /// Create a new server builder.
    pub fn builder() -> RaopServerBuilder {
        RaopServerBuilder::new()
    }

    /// Start the server: bind ports, register mDNS services, begin accepting connections.
    ///
    /// mDNS registration is skipped when the `CI` environment variable is set
    /// (Bonjour/Avahi is typically unavailable on CI runners).
    pub async fn start(&mut self) -> Result<(), ShairplayError> {
        let _actual_port = self.httpd.start(self.bind.port).await?;

        if std::env::var("CI").is_err() {
            let info = self.service_info();
            let mut mdns = MdnsService::new()?;
            mdns.register_raop(&info)?;
            #[cfg(feature = "ap2")]
            if self.mode == AirPlayMode::AirPlay2 {
                mdns.register_airplay(&info)?;
            }
            self.mdns = Some(mdns);
        }

        Ok(())
    }

    /// Whether the server is currently running.
    pub fn is_running(&self) -> bool {
        self.httpd.is_running()
    }

    /// Stop the server: unregister mDNS services and close all listeners.
    pub async fn stop(&mut self) {
        if let Some(mut mdns) = self.mdns.take() {
            mdns.unregister_raop();
            mdns.unregister_airplay();
        }
        self.httpd.stop().await;
    }

    /// Get the mDNS service info for this server.
    pub fn service_info(&self) -> AirPlayServiceInfo {
        #[cfg(feature = "ap2")]
        {
            if self.mode == AirPlayMode::AirPlay2 {
                let device_id = crate::util::hwaddr_airplay(&self.hwaddr);
                let (_, vk) = crate::crypto::pairing_homekit::server_keypair(&device_id);
                let pk_hex: String = vk.as_bytes().iter().map(|b| format!("{b:02x}")).collect();
                let pi = self.shared.pairing_id.clone();
                return AirPlayServiceInfo::new_airplay2(
                    &self.name,
                    self.httpd.port(),
                    &self.hwaddr,
                    !self.shared.password.is_empty(),
                    &pk_hex,
                    &pi,
                    &self.model,
                );
            }
        }
        AirPlayServiceInfo::new(
            &self.name,
            self.httpd.port(),
            &self.hwaddr,
            !self.shared.password.is_empty(),
            &self.model,
        )
    }
}

#[cfg(all(test, feature = "ap2"))]
mod controller_acceptance {
    use super::*;
    use crate::crypto::tlv::{TlvType, TlvValues};
    use crate::net::server::{ConnectionHandler, HttpdCallbacks};
    use crate::proto::http::{HttpRequest, HttpResponse};
    use chacha20poly1305::{ChaCha20Poly1305, KeyInit, aead::Aead};
    use ed25519_dalek::{Signer, SigningKey};

    #[derive(Default)]
    struct Handler(std::sync::Mutex<Vec<f32>>, std::sync::Mutex<Vec<bool>>);
    struct Session;
    impl AudioSession for Session {
        fn audio_process(&mut self, _: &[f32]) {}
    }
    impl AudioHandler for Handler {
        fn audio_init(&self, _: AudioFormat) -> Box<dyn AudioSession> {
            Box::new(Session)
        }
        fn on_playback_rate(&self, playing: bool) {
            self.1.lock().unwrap().push(playing);
        }
        fn on_volume(&self, volume: f32) {
            self.0.lock().unwrap().push(volume);
        }
    }
    fn request(
        conn: &mut dyn ConnectionHandler,
        method: &str,
        path: &str,
        content_type: &str,
        body: &[u8],
    ) -> HttpResponse {
        let mut request = HttpRequest::new();
        request
            .add_data(
                format!(
                    "{method} {path} RTSP/1.0\r\nCSeq: 1\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            )
            .unwrap();
        request.add_data(body).unwrap();
        conn.conn_request(&request)
    }
    fn body(response: &HttpResponse) -> &[u8] {
        let bytes = response.get_data();
        &bytes[bytes.windows(4).position(|window| window == b"\r\n\r\n").unwrap() + 4..]
    }
    fn verify(conn: &mut dyn ConnectionHandler, identifier: &str, key: &SigningKey) {
        let secret = x25519_dalek::StaticSecret::from([13; 32]);
        let public = x25519_dalek::PublicKey::from(&secret);
        let mut m1 = TlvValues::new();
        m1.add(TlvType::State as u8, &[1]);
        m1.add(TlvType::PublicKey as u8, public.as_bytes());
        let response = request(conn, "POST", "/pair-verify", "application/octet-stream", &m1.encode());
        assert_eq!(response.status_code(), 200);
        let m2 = TlvValues::decode(body(&response)).unwrap();
        let remote = <[u8; 32]>::try_from(m2.get_type(TlvType::PublicKey).unwrap()).unwrap();
        let shared = secret.diffie_hellman(&x25519_dalek::PublicKey::from(remote));
        let signed = [public.as_bytes().as_slice(), identifier.as_bytes(), remote.as_slice()].concat();
        let mut inner = TlvValues::new();
        inner.add(TlvType::Identifier as u8, identifier.as_bytes());
        inner.add(TlvType::Signature as u8, &key.sign(&signed).to_bytes());
        let mut encryption_key = [0; 32];
        hkdf::Hkdf::<sha2::Sha512>::new(Some(b"Pair-Verify-Encrypt-Salt"), shared.as_bytes())
            .expand(b"Pair-Verify-Encrypt-Info", &mut encryption_key)
            .unwrap();
        let encrypted = ChaCha20Poly1305::new((&encryption_key).into())
            .encrypt(b"\0\0\0\0PV-Msg03".into(), inner.encode().as_slice())
            .unwrap();
        let mut m3 = TlvValues::new();
        m3.add(TlvType::State as u8, &[3]);
        m3.add(TlvType::EncryptedData as u8, &encrypted);
        let response = request(conn, "POST", "/pair-verify", "application/octet-stream", &m3.encode());
        let m4 = TlvValues::decode(body(&response)).unwrap();
        assert_eq!(m4.get_type(TlvType::State), Some(&[4][..]));
    }
    fn setup(conn: &mut dyn ConnectionHandler) {
        let mut stream = plist::Dictionary::new();
        stream.insert("type".into(), plist::Value::Integer(103.into()));
        stream.insert("shk".into(), plist::Value::Data(vec![7; 32]));
        let mut setup = plist::Dictionary::new();
        setup.insert(
            "streams".into(),
            plist::Value::Array(vec![plist::Value::Dictionary(stream)]),
        );
        let mut bytes = Vec::new();
        plist::to_writer_binary(&mut bytes, &plist::Value::Dictionary(setup)).unwrap();
        assert_eq!(
            request(conn, "SETUP", "/stream", "application/x-apple-binary-plist", &bytes).status_code(),
            200
        );
    }
    #[tokio::test]
    async fn verified_separate_controller_survives_replacement_and_rejects_other_clients() {
        let handler = Arc::new(Handler::default());
        let server = RaopServer::builder().pin("1234").build(handler.clone()).unwrap();
        let key = SigningKey::from_bytes(&[17; 32]);
        server.shared.pairing_store.put("owner", key.verifying_key().to_bytes());
        server.shared.pairing_store.put("other", key.verifying_key().to_bytes());
        let connect = || {
            server
                .shared
                .conn_init("127.0.0.1:7000".parse().unwrap(), "127.0.0.1:8000".parse().unwrap())
                .unwrap()
        };
        let mut owner = connect();
        verify(owner.as_mut(), "owner", &key);
        setup(owner.as_mut());
        let original = server
            .shared
            .controller_session
            .lock()
            .unwrap()
            .playout
            .clone()
            .unwrap();
        let mut control = connect();
        assert_eq!(
            request(
                control.as_mut(),
                "SET_PARAMETER",
                "/stream",
                "text/parameters",
                b"volume: -20\r\n"
            )
            .status_code(),
            401
        );
        verify(control.as_mut(), "owner", &key);
        assert_eq!(
            request(
                control.as_mut(),
                "SET_PARAMETER",
                "/stream",
                "text/parameters",
                b"volume: -20\r\n"
            )
            .status_code(),
            200
        );
        control.shutdown().await;
        assert!(
            !original.is_closed(),
            "control disconnect must not stop the owner's stream"
        );
        let mut other = connect();
        verify(other.as_mut(), "other", &key);
        assert_eq!(
            request(
                other.as_mut(),
                "SET_PARAMETER",
                "/stream",
                "text/parameters",
                b"volume: -5\r\n"
            )
            .status_code(),
            403
        );
        assert_eq!(handler.0.lock().unwrap().as_slice(), &[-20.0]);
        setup(control.as_mut());
        tokio::time::timeout(std::time::Duration::from_secs(1), original.closed())
            .await
            .unwrap();
        let replacement = server
            .shared
            .controller_session
            .lock()
            .unwrap()
            .playout
            .clone()
            .unwrap();
        for method in ["TEARDOWN", "SET_PARAMETER", "SETRATEANCHORTIME"] {
            assert_eq!(
                request(owner.as_mut(), method, "/stream", "text/parameters", b"volume: -1\r\n").status_code(),
                409
            );
        }
        assert!(
            !replacement.is_closed(),
            "late old-owner commands must not control the replacement"
        );
        owner.shutdown().await;
        drop(owner);
        assert!(!replacement.is_closed(), "old owner shutdown must not stop replacement");
        assert_eq!(
            request(
                control.as_mut(),
                "SET_PARAMETER",
                "/stream",
                "text/parameters",
                b"volume: -15\r\n"
            )
            .status_code(),
            200
        );
        control.shutdown().await;
        drop(control);
        assert!(server.shared.controller_session.lock().unwrap().owner.is_none());
    }
    #[tokio::test]
    async fn saturated_control_queue_rejects_mutation_but_teardown_always_cancels() {
        use crate::raop::buffered_audio::PlayoutCommand;
        let handler = Arc::new(Handler::default());
        let server = RaopServer::builder().pin("1234").build(handler.clone()).unwrap();
        let key = SigningKey::from_bytes(&[17; 32]);
        server.shared.pairing_store.put("owner", key.verifying_key().to_bytes());
        let mut control = server
            .shared
            .conn_init("127.0.0.1:7000".parse().unwrap(), "127.0.0.1:8000".parse().unwrap())
            .unwrap();
        verify(control.as_mut(), "owner", &key);
        let (commands, pending) = tokio::sync::mpsc::channel(64);
        for _ in 0..64 {
            commands
                .try_send(PlayoutCommand::SetRate {
                    anchor_rtp: 0,
                    anchor_time_ns: 0,
                    rate: 0,
                })
                .unwrap();
        }
        let media = tokio::spawn(std::future::pending::<()>());
        {
            let mut active = server.shared.controller_session.lock().unwrap();
            active.owner = Some("owner".into());
            active.owner_connection = Some("different-media-connection".into());
            active.playout = Some(commands);
            active.media_abort = Some(media.abort_handle());
        }
        let mut plist = Vec::new();
        plist::to_writer_binary(
            &mut plist,
            &plist::Value::Dictionary(plist::Dictionary::from_iter([
                ("rate", plist::Value::Integer(1.into())),
                ("flushFromSeq", plist::Value::Integer(1.into())),
                ("flushUntilSeq", plist::Value::Integer(10.into())),
            ])),
        )
        .unwrap();
        for method in ["SETRATEANCHORTIME", "FLUSHBUFFERED"] {
            assert_eq!(
                request(
                    control.as_mut(),
                    method,
                    "/stream",
                    "application/x-apple-binary-plist",
                    &plist
                )
                .status_code(),
                503
            );
        }
        assert_eq!(pending.len(), 64);
        drop(pending);
        let closed = request(
            control.as_mut(),
            "FLUSHBUFFERED",
            "/stream",
            "application/x-apple-binary-plist",
            &plist,
        );
        assert_eq!(closed.status_code(), 454);
        assert!(String::from_utf8_lossy(closed.get_data()).contains("CSeq: 1"));
        assert!(
            handler.1.lock().unwrap().is_empty(),
            "failed rate admission must not emit a playback callback"
        );
        assert_eq!(
            request(control.as_mut(), "TEARDOWN", "/stream", "text/parameters", &[]).status_code(),
            200
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), media)
                .await
                .unwrap()
                .unwrap_err()
                .is_cancelled()
        );
        assert!(server.shared.controller_session.lock().unwrap().owner.is_none());
    }
}
