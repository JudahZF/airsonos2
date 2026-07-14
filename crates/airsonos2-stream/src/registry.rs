use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use airsonos2_core::{SessionId, StreamSession};
use bytes::Bytes;
use tokio::sync::{RwLock, broadcast, watch};
use tracing::{debug, info};

#[derive(Clone, Debug)]
pub struct StreamRegistry {
    inner: Arc<RwLock<HashMap<SessionId, LiveStream>>>,
}

impl StreamRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn create(&self, session: StreamSession) -> LiveStream {
        let stream = LiveStream::new(session);
        self.inner
            .write()
            .await
            .insert(stream.session.session_id, stream.clone());
        stream
    }

    pub async fn insert(&self, stream: LiveStream) {
        self.inner
            .write()
            .await
            .insert(stream.session.session_id, stream);
    }

    pub async fn get(&self, session_id: &SessionId) -> Option<LiveStream> {
        self.inner.read().await.get(session_id).cloned()
    }

    pub async fn remove(&self, session_id: &SessionId) -> Option<LiveStream> {
        self.inner.write().await.remove(session_id)
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    pub async fn metrics_text(&self) -> String {
        let streams = self.inner.read().await;
        let active = streams.len();
        let bytes_served: u64 = streams.values().map(LiveStream::bytes_served).sum();
        let bytes_dropped: u64 = streams.values().map(LiveStream::bytes_dropped).sum();
        let bytes_skipped: u64 = streams
            .values()
            .map(|stream| stream.timing().bytes_skipped)
            .sum();

        format!(
            "# HELP airsonos2_stream_sessions Active live stream sessions.\n\
             # TYPE airsonos2_stream_sessions gauge\n\
             airsonos2_stream_sessions {active}\n\
             # HELP airsonos2_http_bytes_served Bytes delivered to HTTP subscribers.\n\
             # TYPE airsonos2_http_bytes_served counter\n\
             airsonos2_http_bytes_served {bytes_served}\n\
             # HELP airsonos2_stream_bytes_dropped Bytes dropped before HTTP subscriber connected.\n\
             # TYPE airsonos2_stream_bytes_dropped counter\n\
             airsonos2_stream_bytes_dropped {bytes_dropped}\n\
             # HELP airsonos2_stream_bytes_skipped Bytes skipped to align playback with the live edge.\n\
             # TYPE airsonos2_stream_bytes_skipped counter\n\
             airsonos2_stream_bytes_skipped {bytes_skipped}\n"
        )
    }
}

impl Default for StreamRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of stream timing for diagnostics.
#[derive(Clone, Debug, Default)]
pub struct StreamTiming {
    pub encoded_bytes: u64,
    pub bytes_served: u64,
    pub bytes_dropped: u64,
    pub bytes_skipped: u64,
    pub chunks_dropped: u64,
    pub subscriber_connected: bool,
    pub playback_anchor_at: Option<Instant>,
    pub first_encoded_at: Option<Instant>,
    pub first_served_at: Option<Instant>,
    pub subscriber_connected_at: Option<Instant>,
}

#[derive(Clone, Copy, Debug, Default)]
enum PlaybackAnchorState {
    #[default]
    Unset,
    At(Instant),
    NextTimedPcm,
}

#[derive(Clone, Debug)]
pub struct LiveStream {
    pub session: StreamSession,
    sender: broadcast::Sender<Bytes>,
    prelude: Arc<std::sync::Mutex<Option<Bytes>>>,
    encoded_bytes: Arc<AtomicU64>,
    bytes_served: Arc<AtomicU64>,
    bytes_dropped: Arc<AtomicU64>,
    bytes_skipped: Arc<AtomicU64>,
    chunks_dropped: Arc<AtomicU64>,
    subscriber_connected: Arc<AtomicBool>,
    delivery_open: Arc<AtomicBool>,
    ready: watch::Sender<bool>,
    subscriber: watch::Sender<bool>,
    playback_anchor: Arc<std::sync::Mutex<PlaybackAnchorState>>,
    first_encoded_at: Arc<std::sync::Mutex<Option<Instant>>>,
    first_served_at: Arc<std::sync::Mutex<Option<Instant>>>,
    subscriber_connected_at: Arc<std::sync::Mutex<Option<Instant>>>,
}

impl LiveStream {
    pub fn new(session: StreamSession) -> Self {
        let (sender, _) = broadcast::channel(256);
        let (ready, _) = watch::channel(false);
        let (subscriber, _) = watch::channel(false);
        Self {
            session,
            sender,
            prelude: Arc::new(std::sync::Mutex::new(None)),
            encoded_bytes: Arc::new(AtomicU64::new(0)),
            bytes_served: Arc::new(AtomicU64::new(0)),
            bytes_dropped: Arc::new(AtomicU64::new(0)),
            bytes_skipped: Arc::new(AtomicU64::new(0)),
            chunks_dropped: Arc::new(AtomicU64::new(0)),
            subscriber_connected: Arc::new(AtomicBool::new(false)),
            delivery_open: Arc::new(AtomicBool::new(true)),
            ready,
            subscriber,
            playback_anchor: Arc::new(std::sync::Mutex::new(PlaybackAnchorState::Unset)),
            first_encoded_at: Arc::new(std::sync::Mutex::new(None)),
            first_served_at: Arc::new(std::sync::Mutex::new(None)),
            subscriber_connected_at: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Publish encoded stream bytes. Before an HTTP subscriber connects, chunks are
    /// dropped so Sonos starts at the live edge instead of replaying startup audio.
    pub fn publish(&self, bytes: Bytes) {
        self.publish_inner(bytes, false);
    }

    fn publish_inner(&self, bytes: Bytes, bypass_delivery_hold: bool) {
        if bytes.is_empty() {
            return;
        }

        let len = bytes.len() as u64;
        self.encoded_bytes.fetch_add(len, Ordering::Relaxed);
        self.note_first_encoded(len);
        let _ = self.ready.send(true);

        if !self.subscriber_connected.load(Ordering::Acquire)
            || (!bypass_delivery_hold && !self.delivery_open.load(Ordering::Acquire))
        {
            self.bytes_dropped.fetch_add(len, Ordering::Relaxed);
            self.chunks_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }

        self.send_served(bytes);
    }

    /// Publish timed WAV PCM. Before playback is anchored, or when a frame is
    /// older than that anchor, PCM is skipped so Sonos starts near the live edge.
    pub fn publish_timed_pcm(
        &self,
        bytes: Bytes,
        presentation_time: Option<Instant>,
        sample_rate: u32,
        channels: u8,
    ) {
        if bytes.is_empty() {
            return;
        }

        let len = bytes.len() as u64;
        self.encoded_bytes.fetch_add(len, Ordering::Relaxed);
        self.note_first_encoded(len);
        let _ = self.ready.send(true);

        if !self.subscriber_connected.load(Ordering::Acquire)
            || !self.delivery_open.load(Ordering::Acquire)
        {
            self.bytes_dropped.fetch_add(len, Ordering::Relaxed);
            self.chunks_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let anchor = {
            let Some(mut anchor) = self.playback_anchor.lock().ok() else {
                self.bytes_skipped.fetch_add(len, Ordering::Relaxed);
                self.chunks_dropped.fetch_add(1, Ordering::Relaxed);
                return;
            };
            match *anchor {
                PlaybackAnchorState::Unset => {
                    self.bytes_skipped.fetch_add(len, Ordering::Relaxed);
                    self.chunks_dropped.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                PlaybackAnchorState::At(anchor) => anchor,
                PlaybackAnchorState::NextTimedPcm => {
                    let frame_anchor = presentation_time.unwrap_or_else(Instant::now);
                    *anchor = PlaybackAnchorState::At(frame_anchor);
                    frame_anchor
                }
            }
        };

        let Some(presentation_time) = presentation_time else {
            self.send_served(bytes);
            return;
        };

        let frame_bytes = usize::from(channels) * 2;
        if frame_bytes == 0 || sample_rate == 0 {
            self.send_served(bytes);
            return;
        }

        let frame_count = bytes.len() / frame_bytes;
        let duration = Duration::from_secs_f64(frame_count as f64 / sample_rate as f64);
        let frame_end = presentation_time + duration;

        if frame_end <= anchor {
            self.bytes_skipped.fetch_add(len, Ordering::Relaxed);
            self.chunks_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }

        if presentation_time < anchor {
            let skip_duration = anchor.saturating_duration_since(presentation_time);
            let skip_frames = ((skip_duration.as_secs_f64() * sample_rate as f64).ceil() as usize)
                .min(frame_count);
            let skip_bytes = skip_frames * frame_bytes;
            if skip_bytes > 0 {
                self.bytes_skipped
                    .fetch_add(skip_bytes as u64, Ordering::Relaxed);
            }
            if skip_bytes >= bytes.len() {
                self.chunks_dropped.fetch_add(1, Ordering::Relaxed);
                return;
            }
            self.send_served(bytes.slice(skip_bytes..));
            return;
        }

        self.send_served(bytes);
    }

    fn note_first_encoded(&self, len: u64) {
        if self
            .first_encoded_at
            .lock()
            .ok()
            .and_then(|mut t| {
                if t.is_none() {
                    *t = Some(Instant::now());
                    Some(())
                } else {
                    None
                }
            })
            .is_some()
        {
            debug!(
                session_id = %self.session.session_id,
                bytes = len,
                "first encoded stream bytes produced"
            );
        }
    }

    fn send_served(&self, bytes: Bytes) {
        let len = bytes.len() as u64;
        self.bytes_served.fetch_add(len, Ordering::Relaxed);
        if let Ok(mut at) = self.first_served_at.lock()
            && at.is_none()
        {
            *at = Some(Instant::now());
            debug!(
                session_id = %self.session.session_id,
                bytes = len,
                "first stream bytes served"
            );
        }
        let _ = self.sender.send(bytes);
    }

    /// Publish bytes that must prefix every subscriber response, such as a WAV header.
    pub fn publish_prelude(&self, bytes: Bytes) {
        if bytes.is_empty() {
            return;
        }

        if let Ok(mut prelude) = self.prelude.lock() {
            *prelude = Some(bytes.clone());
        }

        self.publish_inner(bytes, true);
    }

    /// Called when Sonos (or another client) connects to the HTTP stream.
    pub fn on_subscriber_connected(&self) {
        if self
            .subscriber_connected
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let now = Instant::now();
        if let Ok(mut at) = self.subscriber_connected_at.lock() {
            *at = Some(now);
        }
        let _ = self.subscriber.send(true);

        let timing = self.timing();
        let startup_ms = timing
            .first_encoded_at
            .map(|first| now.saturating_duration_since(first).as_millis());
        info!(
            session_id = %self.session.session_id,
            encoded_bytes = timing.encoded_bytes,
            bytes_dropped = timing.bytes_dropped,
            chunks_dropped = timing.chunks_dropped,
            startup_ms,
            "HTTP subscriber connected; streaming from live edge"
        );
    }

    pub fn attach_subscriber(&self) -> (Option<Bytes>, broadcast::Receiver<Bytes>) {
        let subscriber = self.sender.subscribe();
        let prelude = if let Ok(prelude) = self.prelude.lock() {
            let bytes = prelude.clone();
            self.on_subscriber_connected();
            bytes
        } else {
            self.on_subscriber_connected();
            None
        };
        (prelude, subscriber)
    }

    /// Drop live audio until coordinated downstream playback is ready.
    pub fn hold_delivery(&self) {
        self.delivery_open.store(false, Ordering::Release);
    }

    pub fn open_delivery(&self) {
        self.delivery_open.store(true, Ordering::Release);
    }

    pub fn set_playback_anchor(&self, anchor: Instant) {
        if let Ok(mut at) = self.playback_anchor.lock() {
            *at = PlaybackAnchorState::At(anchor);
        }
        let timing = self.timing();
        info!(
            session_id = %self.session.session_id,
            encoded_bytes = timing.encoded_bytes,
            bytes_dropped = timing.bytes_dropped,
            bytes_skipped = timing.bytes_skipped,
            "stream playback anchor set"
        );
    }

    pub fn arm_playback_anchor_on_next_timed_pcm(&self) {
        if let Ok(mut at) = self.playback_anchor.lock() {
            *at = PlaybackAnchorState::NextTimedPcm;
        }
        let timing = self.timing();
        info!(
            session_id = %self.session.session_id,
            encoded_bytes = timing.encoded_bytes,
            bytes_dropped = timing.bytes_dropped,
            bytes_skipped = timing.bytes_skipped,
            "stream playback anchor armed for next timed PCM"
        );
    }

    pub fn clear_playback_anchor(&self) {
        if let Ok(mut at) = self.playback_anchor.lock() {
            *at = PlaybackAnchorState::Unset;
        }
    }

    /// Waits until the encoder has produced stream data or the timeout elapses.
    pub async fn wait_until_ready(&self, timeout: Duration) -> bool {
        if *self.ready.borrow() {
            return true;
        }

        let mut ready_rx = self.ready.subscribe();
        tokio::select! {
            changed = ready_rx.changed() => changed.is_ok() && *ready_rx.borrow(),
            _ = tokio::time::sleep(timeout) => *self.ready.borrow(),
        }
    }

    /// Waits until an HTTP subscriber connects or the timeout elapses.
    pub async fn wait_for_subscriber(&self, timeout: Duration) -> bool {
        if *self.subscriber.borrow() {
            return true;
        }

        let mut subscriber_rx = self.subscriber.subscribe();
        tokio::select! {
            changed = subscriber_rx.changed() => changed.is_ok() && *subscriber_rx.borrow(),
            _ = tokio::time::sleep(timeout) => *self.subscriber.borrow(),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Bytes> {
        self.sender.subscribe()
    }

    pub fn prelude(&self) -> Option<Bytes> {
        self.prelude.lock().ok().and_then(|prelude| prelude.clone())
    }

    pub fn bytes_served(&self) -> u64 {
        self.bytes_served.load(Ordering::Relaxed)
    }

    pub fn bytes_encoded(&self) -> u64 {
        self.encoded_bytes.load(Ordering::Relaxed)
    }

    pub fn bytes_dropped(&self) -> u64 {
        self.bytes_dropped.load(Ordering::Relaxed)
    }

    pub fn timing(&self) -> StreamTiming {
        StreamTiming {
            encoded_bytes: self.bytes_encoded(),
            bytes_served: self.bytes_served(),
            bytes_dropped: self.bytes_dropped(),
            bytes_skipped: self.bytes_skipped.load(Ordering::Relaxed),
            chunks_dropped: self.chunks_dropped.load(Ordering::Relaxed),
            subscriber_connected: self.subscriber_connected.load(Ordering::Relaxed),
            playback_anchor_at: self.playback_anchor.lock().ok().and_then(|t| match *t {
                PlaybackAnchorState::At(anchor) => Some(anchor),
                PlaybackAnchorState::Unset | PlaybackAnchorState::NextTimedPcm => None,
            }),
            first_encoded_at: self.first_encoded_at.lock().ok().and_then(|t| *t),
            first_served_at: self.first_served_at.lock().ok().and_then(|t| *t),
            subscriber_connected_at: self.subscriber_connected_at.lock().ok().and_then(|t| *t),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use airsonos2_core::{EncoderState, StreamCodec, ZoneId};
    use url::Url;

    fn session() -> StreamSession {
        StreamSession {
            session_id: SessionId::new(),
            zone_id: ZoneId::new("RINCON_TEST"),
            codec: StreamCodec::Mp3,
            generation: 1,
            local_url: Url::parse("http://127.0.0.1:7000/streams/test.mp3").expect("url"),
            encoder_state: EncoderState::Starting,
        }
    }

    #[tokio::test]
    async fn stream_registry_lifecycle() {
        let registry = StreamRegistry::new();
        let session = session();
        let id = session.session_id;

        let stream = registry.create(session).await;
        stream.on_subscriber_connected();
        stream.publish(Bytes::from_static(b"abc"));

        assert_eq!(registry.len().await, 1);
        assert_eq!(stream.bytes_served(), 3);
        assert_eq!(stream.bytes_dropped(), 0);
        assert!(registry.remove(&id).await.is_some());
        assert!(registry.is_empty().await);
    }

    #[tokio::test]
    async fn wait_until_ready_triggers_after_publish() {
        let stream = LiveStream::new(session());
        let waiter = stream.clone();
        let notify =
            tokio::spawn(async move { waiter.wait_until_ready(Duration::from_secs(1)).await });

        tokio::time::sleep(Duration::from_millis(20)).await;
        stream.publish(Bytes::from_static(b"mp3"));

        assert!(notify.await.expect("wait task"));
    }

    #[tokio::test]
    async fn wait_until_ready_times_out_when_no_data() {
        let stream = LiveStream::new(session());

        assert!(!stream.wait_until_ready(Duration::from_millis(50)).await);
    }

    #[tokio::test]
    async fn drops_chunks_before_subscriber_connects() {
        let stream = LiveStream::new(session());

        stream.publish(Bytes::from_static(b"old"));
        assert_eq!(stream.bytes_encoded(), 3);
        assert_eq!(stream.bytes_dropped(), 3);
        assert_eq!(stream.bytes_served(), 0);

        stream.on_subscriber_connected();
        stream.publish(Bytes::from_static(b"live"));

        assert_eq!(stream.bytes_served(), 4);
        assert_eq!(stream.bytes_dropped(), 3);
    }

    #[tokio::test]
    async fn subscriber_only_receives_live_edge_chunks() {
        let stream = LiveStream::new(session());
        stream.publish(Bytes::from_static(b"stale"));

        stream.on_subscriber_connected();
        let mut rx = stream.subscribe();
        stream.publish(Bytes::from_static(b"live"));

        let chunk = rx.try_recv().expect("live chunk");
        assert_eq!(&chunk[..], b"live");
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn staggered_subscribers_wait_for_the_same_delivery_gate() {
        let stream = LiveStream::new(session());
        stream.hold_delivery();
        stream.on_subscriber_connected();
        let mut first = stream.subscribe();
        stream.publish(Bytes::from_static(b"before"));
        let mut second = stream.subscribe();
        stream.publish(Bytes::from_static(b"still-before"));

        assert!(first.try_recv().is_err());
        assert!(second.try_recv().is_err());
        stream.open_delivery();
        stream.publish(Bytes::from_static(b"together"));

        assert_eq!(&first.try_recv().expect("first")[..], b"together");
        assert_eq!(&second.try_recv().expect("second")[..], b"together");
    }

    #[tokio::test]
    async fn prelude_is_retained_when_published_before_subscriber_connects() {
        let stream = LiveStream::new(session());

        stream.publish_prelude(Bytes::from_static(b"header"));
        stream.publish(Bytes::from_static(b"stale"));

        assert_eq!(
            stream.prelude().expect("prelude"),
            Bytes::from_static(b"header")
        );
        assert_eq!(stream.bytes_dropped(), 11);
        assert_eq!(stream.bytes_served(), 0);

        stream.on_subscriber_connected();
        let mut rx = stream.subscribe();
        stream.publish(Bytes::from_static(b"live"));

        let chunk = rx.try_recv().expect("live chunk");
        assert_eq!(&chunk[..], b"live");
    }

    #[tokio::test]
    async fn attached_subscriber_receives_prelude_published_after_attach() {
        let stream = LiveStream::new(session());
        let (prelude, mut rx) = stream.attach_subscriber();

        assert!(prelude.is_none());
        stream.publish_prelude(Bytes::from_static(b"header"));

        let chunk = rx.try_recv().expect("late prelude chunk");
        assert_eq!(&chunk[..], b"header");
    }

    #[tokio::test]
    async fn held_delivery_still_sends_late_prelude_to_attached_subscriber() {
        let stream = LiveStream::new(session());
        stream.hold_delivery();
        let (prelude, mut rx) = stream.attach_subscriber();

        assert!(prelude.is_none());
        stream.publish_prelude(Bytes::from_static(b"header"));
        stream.publish(Bytes::from_static(b"held"));

        let chunk = rx.try_recv().expect("late prelude chunk");
        assert_eq!(&chunk[..], b"header");
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn attached_subscriber_gets_existing_prelude_snapshot() {
        let stream = LiveStream::new(session());
        stream.publish_prelude(Bytes::from_static(b"header"));

        let (prelude, mut rx) = stream.attach_subscriber();
        stream.publish(Bytes::from_static(b"live"));

        assert_eq!(prelude.expect("prelude"), Bytes::from_static(b"header"));
        let chunk = rx.try_recv().expect("live chunk");
        assert_eq!(&chunk[..], b"live");
    }

    #[tokio::test]
    async fn replaced_stream_does_not_receive_old_stream_chunks() {
        let registry = StreamRegistry::new();
        let original_session = session();
        let session_id = original_session.session_id;
        let original = registry.create(original_session.clone()).await;
        let _ = registry.remove(&session_id).await;
        let replacement = registry.create(original_session).await;

        replacement.on_subscriber_connected();
        let mut rx = replacement.subscribe();
        original.on_subscriber_connected();
        original.publish(Bytes::from_static(b"old"));
        replacement.publish(Bytes::from_static(b"new"));

        let chunk = rx.try_recv().expect("replacement chunk");
        assert_eq!(&chunk[..], b"new");
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn replaced_wav_stream_has_independent_prelude() {
        let registry = StreamRegistry::new();
        let mut original_session = session();
        original_session.codec = StreamCodec::Wav;
        let session_id = original_session.session_id;
        let original = registry.create(original_session.clone()).await;
        original.publish_prelude(Bytes::from_static(b"old-header"));

        let _ = registry.remove(&session_id).await;
        let replacement = registry.create(original_session).await;

        assert!(replacement.prelude().is_none());
        replacement.publish_prelude(Bytes::from_static(b"new-header"));

        assert_eq!(
            original.prelude().expect("original prelude"),
            Bytes::from_static(b"old-header")
        );
        assert_eq!(
            replacement.prelude().expect("replacement prelude"),
            Bytes::from_static(b"new-header")
        );
    }

    #[tokio::test]
    async fn timed_pcm_waits_for_playback_anchor() {
        let stream = LiveStream::new(session());
        stream.on_subscriber_connected();
        let mut rx = stream.subscribe();

        stream.publish_timed_pcm(
            Bytes::from_static(&[1, 2, 3, 4]),
            Some(Instant::now()),
            1_000,
            2,
        );

        assert!(rx.try_recv().is_err());
        assert_eq!(stream.timing().bytes_skipped, 4);
        assert_eq!(stream.bytes_served(), 0);
    }

    #[tokio::test]
    async fn timed_pcm_skips_frames_before_anchor() {
        let stream = LiveStream::new(session());
        stream.on_subscriber_connected();
        let mut rx = stream.subscribe();
        let frame_start = Instant::now();
        stream.set_playback_anchor(frame_start + Duration::from_millis(5));

        let bytes = Bytes::from((0_u8..40).collect::<Vec<_>>());
        stream.publish_timed_pcm(bytes, Some(frame_start), 1_000, 2);

        let chunk = rx.try_recv().expect("trimmed live chunk");
        assert_eq!(chunk.len(), 20);
        assert_eq!(&chunk[..4], &[20, 21, 22, 23]);
        assert_eq!(stream.timing().bytes_skipped, 20);
        assert_eq!(stream.bytes_served(), 20);
    }

    #[tokio::test]
    async fn timed_pcm_after_anchor_is_delivered() {
        let stream = LiveStream::new(session());
        stream.on_subscriber_connected();
        let mut rx = stream.subscribe();
        let anchor = Instant::now();
        stream.set_playback_anchor(anchor);

        stream.publish_timed_pcm(
            Bytes::from_static(b"live"),
            Some(anchor + Duration::from_millis(1)),
            1_000,
            2,
        );

        let chunk = rx.try_recv().expect("live chunk");
        assert_eq!(&chunk[..], b"live");
        assert_eq!(stream.timing().bytes_skipped, 0);
    }

    #[tokio::test]
    async fn anchor_on_next_timed_pcm_delivers_first_frame_from_own_presentation_time() {
        let stream = LiveStream::new(session());
        stream.on_subscriber_connected();
        stream.arm_playback_anchor_on_next_timed_pcm();
        let mut rx = stream.subscribe();
        let frame_start = Instant::now();

        stream.publish_timed_pcm(Bytes::from_static(b"live"), Some(frame_start), 1_000, 2);

        let chunk = rx.try_recv().expect("anchored chunk");
        assert_eq!(&chunk[..], b"live");
        assert_eq!(stream.timing().playback_anchor_at, Some(frame_start));
        assert_eq!(stream.timing().bytes_skipped, 0);
    }

    #[tokio::test]
    async fn old_pre_anchor_timed_pcm_is_skipped_after_arming() {
        let stream = LiveStream::new(session());
        stream.on_subscriber_connected();
        let mut rx = stream.subscribe();
        let old_frame_start = Instant::now();

        stream.publish_timed_pcm(Bytes::from_static(b"old1"), Some(old_frame_start), 1_000, 2);
        stream.arm_playback_anchor_on_next_timed_pcm();
        let live_frame_start = old_frame_start + Duration::from_millis(20);
        stream.publish_timed_pcm(
            Bytes::from_static(b"live"),
            Some(live_frame_start),
            1_000,
            2,
        );

        let chunk = rx.try_recv().expect("live chunk");
        assert_eq!(&chunk[..], b"live");
        assert!(rx.try_recv().is_err());
        assert_eq!(stream.timing().playback_anchor_at, Some(live_frame_start));
        assert_eq!(stream.timing().bytes_skipped, 4);
    }

    #[tokio::test]
    async fn rearming_replaces_existing_anchor_on_next_timed_pcm() {
        let stream = LiveStream::new(session());
        stream.on_subscriber_connected();
        let mut rx = stream.subscribe();
        let original_anchor = Instant::now();
        stream.set_playback_anchor(original_anchor);

        stream.publish_timed_pcm(
            Bytes::from_static(b"old1"),
            Some(original_anchor + Duration::from_millis(1)),
            1_000,
            2,
        );
        stream.arm_playback_anchor_on_next_timed_pcm();
        let replacement_anchor = original_anchor + Duration::from_millis(50);
        stream.publish_timed_pcm(
            Bytes::from_static(b"new1"),
            Some(replacement_anchor),
            1_000,
            2,
        );

        let old = rx.try_recv().expect("old anchored chunk");
        assert_eq!(&old[..], b"old1");
        let new = rx.try_recv().expect("new anchored chunk");
        assert_eq!(&new[..], b"new1");
        assert_eq!(stream.timing().playback_anchor_at, Some(replacement_anchor));
    }

    #[tokio::test]
    async fn wait_for_subscriber_triggers_on_connect() {
        let stream = LiveStream::new(session());
        let waiter = stream.clone();
        let notify =
            tokio::spawn(async move { waiter.wait_for_subscriber(Duration::from_secs(1)).await });

        tokio::time::sleep(Duration::from_millis(20)).await;
        stream.on_subscriber_connected();

        assert!(notify.await.expect("wait task"));
    }
}
