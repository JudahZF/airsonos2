use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use airsonos2_core::{PcmQueue, SessionId, StreamSession};
use bytes::Bytes;
use tokio::sync::{RwLock, broadcast, watch};
use tracing::{debug, info};

#[derive(Debug, Default)]
struct Totals([AtomicU64; 3]);

#[derive(Debug, Default)]
struct Accounting {
    values: [u64; 3],
    totals: Option<Arc<Totals>>,
}

const CONSUMED: usize = 0;
const DROPPED: usize = 1;
const SKIPPED: usize = 2;

#[derive(Clone, Debug)]
pub struct StreamRegistry {
    inner: Arc<RwLock<HashMap<SessionId, LiveStream>>>,
    totals: Arc<Totals>,
    completed_pcm_drops: Arc<AtomicU64>,
}

impl StreamRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            totals: Arc::new(Totals::default()),
            completed_pcm_drops: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn create(&self, session: StreamSession) -> LiveStream {
        let stream = LiveStream::new(session);
        self.insert(stream.clone()).await;
        stream
    }

    pub async fn insert(&self, stream: LiveStream) {
        stream.attach_totals(self.totals.clone());
        let mut streams = self.inner.write().await;
        if let Some(previous) = streams.insert(stream.session.session_id, stream) {
            self.complete_queue(&previous);
        }
    }

    fn complete_queue(&self, stream: &LiveStream) {
        stream.close();
        if let Some(queue) = stream.input_queue.get() {
            self.completed_pcm_drops
                .fetch_add(queue.stats().dropped_frames, Ordering::Relaxed);
        }
    }

    pub async fn get(&self, session_id: &SessionId) -> Option<LiveStream> {
        self.inner.read().await.get(session_id).cloned()
    }

    pub async fn remove(&self, session_id: &SessionId) -> Option<LiveStream> {
        let mut streams = self.inner.write().await;
        let stream = streams.remove(session_id);
        if let Some(stream) = &stream {
            self.complete_queue(stream);
        }
        stream
    }

    pub async fn close_all(&self) {
        for (_, stream) in self.inner.write().await.drain() {
            self.complete_queue(&stream);
        }
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
        let bytes_served = self.totals.0[CONSUMED].load(Ordering::Relaxed);
        let bytes_dropped = self.totals.0[DROPPED].load(Ordering::Relaxed);
        let bytes_skipped = self.totals.0[SKIPPED].load(Ordering::Relaxed);
        let queues: Vec<_> = streams
            .values()
            .filter_map(|stream| stream.input_queue.get())
            .map(PcmQueue::stats)
            .collect();
        let queue_bytes: usize = queues.iter().map(|queue| queue.bytes).sum();
        let queue_retained_bytes: usize = queues.iter().map(|queue| queue.retained_bytes).sum();
        let queue_byte_limit: usize = queues.iter().map(|queue| queue.byte_limit).sum();
        let queue_drops = self.completed_pcm_drops.load(Ordering::Relaxed)
            + queues.iter().map(|queue| queue.dropped_frames).sum::<u64>();
        let queue_duration_ms: u128 = queues.iter().map(|queue| queue.duration.as_millis()).sum();
        let queue_age_ms = queues
            .iter()
            .map(|queue| queue.oldest_age.as_millis())
            .max()
            .unwrap_or(0);

        format!(
            "# HELP airsonos2_stream_sessions Active live stream sessions.\n\
             # TYPE airsonos2_stream_sessions gauge\n\
             airsonos2_stream_sessions {active}\n\
             # HELP airsonos2_http_body_bytes_consumed Bytes polled from HTTP bodies; not socket or acoustic delivery.\n\
             # TYPE airsonos2_http_body_bytes_consumed counter\n\
             airsonos2_http_body_bytes_consumed {bytes_served}\n\
             # HELP airsonos2_stream_bytes_dropped Bytes dropped before HTTP subscriber connected.\n\
             # TYPE airsonos2_stream_bytes_dropped counter\n\
             airsonos2_stream_bytes_dropped {bytes_dropped}\n\
             # HELP airsonos2_stream_bytes_skipped Bytes skipped to align playback with the live edge.\n\
             # TYPE airsonos2_stream_bytes_skipped counter\n\
             airsonos2_stream_bytes_skipped {bytes_skipped}\n\
             # HELP airsonos2_pcm_queue_bytes PCM payload bytes currently held by encoder queues.\n\
             # TYPE airsonos2_pcm_queue_bytes gauge\n\
             airsonos2_pcm_queue_bytes {queue_bytes}\n\
             # TYPE airsonos2_pcm_queue_retained_bytes gauge\n\
             airsonos2_pcm_queue_retained_bytes {queue_retained_bytes}\n\
             # TYPE airsonos2_pcm_queue_byte_limit gauge\n\
             airsonos2_pcm_queue_byte_limit {queue_byte_limit}\n\
             # TYPE airsonos2_pcm_queue_dropped_frames counter\n\
             airsonos2_pcm_queue_dropped_frames {queue_drops}\n\
             # HELP airsonos2_pcm_queue_duration_ms Sum of queued PCM duration in milliseconds.\n\
             # TYPE airsonos2_pcm_queue_duration_ms gauge\n\
             airsonos2_pcm_queue_duration_ms {queue_duration_ms}\n\
             # HELP airsonos2_pcm_queue_oldest_age_ms Oldest encoder queue arrival age in milliseconds.\n\
             # TYPE airsonos2_pcm_queue_oldest_age_ms gauge\n\
             airsonos2_pcm_queue_oldest_age_ms {queue_age_ms}\n"
        )
    }
}

impl Default for StreamRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Bounded output backlog: at most 64 chunks of 16 KiB, plus one shared input
/// allocation (at most 1 MiB). Stalled HTTP consumers reconnect at the live edge.
#[derive(Clone, Debug)]
pub struct EncodedChunk {
    pub bytes: Bytes,
    pub queued_at: Instant,
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
    sender: broadcast::Sender<EncodedChunk>,
    prelude: Arc<std::sync::Mutex<Option<Bytes>>>,
    encoded_bytes: Arc<AtomicU64>,
    accounting: Arc<Mutex<Accounting>>,
    input_queue: Arc<OnceLock<PcmQueue>>,
    chunks_dropped: Arc<AtomicU64>,
    subscriber_connected: Arc<AtomicBool>,
    ready: watch::Sender<bool>,
    subscriber: watch::Sender<bool>,
    closed: watch::Sender<bool>,
    playback_release: watch::Sender<Option<Instant>>,
    playback_epoch: Arc<AtomicU64>,
    subscriber_count: Arc<AtomicU64>,
    playback_anchor: Arc<std::sync::Mutex<PlaybackAnchorState>>,
    first_encoded_at: Arc<OnceLock<Instant>>,
    first_served_at: Arc<OnceLock<Instant>>,
    subscriber_connected_at: Arc<OnceLock<Instant>>,
}

impl LiveStream {
    pub fn new(session: StreamSession) -> Self {
        let (sender, _) = broadcast::channel(64);
        let (ready, _) = watch::channel(false);
        let (subscriber, _) = watch::channel(false);
        let (closed, _) = watch::channel(false);
        let (playback_release, _) = watch::channel(None);
        Self {
            session,
            sender,
            prelude: Arc::new(std::sync::Mutex::new(None)),
            encoded_bytes: Arc::new(AtomicU64::new(0)),
            accounting: Arc::new(Mutex::new(Accounting::default())),
            input_queue: Arc::new(OnceLock::new()),
            chunks_dropped: Arc::new(AtomicU64::new(0)),
            subscriber_connected: Arc::new(AtomicBool::new(false)),
            ready,
            subscriber,
            closed,
            playback_release,
            playback_epoch: Arc::new(AtomicU64::new(0)),
            subscriber_count: Arc::new(AtomicU64::new(0)),
            playback_anchor: Arc::new(std::sync::Mutex::new(PlaybackAnchorState::Unset)),
            first_encoded_at: Arc::new(OnceLock::new()),
            first_served_at: Arc::new(OnceLock::new()),
            subscriber_connected_at: Arc::new(OnceLock::new()),
        }
    }

    /// Publish encoded stream bytes. Before an HTTP subscriber connects, chunks are
    /// dropped so Sonos starts at the live edge instead of replaying startup audio.
    pub fn publish(&self, bytes: Bytes) {
        if bytes.is_empty() || self.is_closed() {
            return;
        }

        let len = bytes.len() as u64;
        self.encoded_bytes.fetch_add(len, Ordering::Relaxed);
        self.note_first_encoded(len);
        self.ready.send_if_modified(|ready| {
            let changed = !*ready;
            *ready = true;
            changed
        });

        if !self.subscriber_connected.load(Ordering::Acquire) {
            self.add_counter(DROPPED, len);
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
        if bytes.is_empty() || self.is_closed() {
            return;
        }

        let len = bytes.len() as u64;
        self.encoded_bytes.fetch_add(len, Ordering::Relaxed);
        self.note_first_encoded(len);
        self.ready.send_if_modified(|ready| {
            let changed = !*ready;
            *ready = true;
            changed
        });

        if !self.subscriber_connected.load(Ordering::Acquire) {
            self.add_counter(DROPPED, len);
            self.chunks_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let anchor = {
            let Some(mut anchor) = self.playback_anchor.lock().ok() else {
                self.add_counter(SKIPPED, len);
                self.chunks_dropped.fetch_add(1, Ordering::Relaxed);
                return;
            };
            match *anchor {
                PlaybackAnchorState::Unset => {
                    self.add_counter(SKIPPED, len);
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
            self.add_counter(SKIPPED, len);
            self.chunks_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }

        if presentation_time < anchor {
            let skip_duration = anchor.saturating_duration_since(presentation_time);
            let skip_frames = ((skip_duration.as_secs_f64() * sample_rate as f64).ceil() as usize)
                .min(frame_count);
            let skip_bytes = skip_frames * frame_bytes;
            if skip_bytes > 0 {
                self.add_counter(SKIPPED, skip_bytes as u64);
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

    pub fn set_input_queue(&self, queue: PcmQueue) {
        let _ = self.input_queue.set(queue);
    }

    fn attach_totals(&self, totals: Arc<Totals>) {
        let mut accounting = self.accounting.lock().expect("stream accounting lock");
        if accounting.totals.is_none() {
            for (index, value) in accounting.values.iter().enumerate() {
                totals.0[index].fetch_add(*value, Ordering::Relaxed);
            }
            accounting.totals = Some(totals);
        }
    }

    fn add_counter(&self, index: usize, value: u64) {
        let mut accounting = self.accounting.lock().expect("stream accounting lock");
        accounting.values[index] += value;
        if let Some(totals) = &accounting.totals {
            totals.0[index].fetch_add(value, Ordering::Relaxed);
        }
    }

    fn counter(&self, index: usize) -> u64 {
        self.accounting
            .lock()
            .expect("stream accounting lock")
            .values[index]
    }

    fn note_first_encoded(&self, len: u64) {
        if self.first_encoded_at.set(Instant::now()).is_ok() {
            debug!(session_id = %self.session.session_id, bytes = len, "first encoded stream bytes produced");
        }
    }

    fn send_served(&self, bytes: Bytes) {
        if bytes.len() > 1024 * 1024 {
            self.add_counter(DROPPED, bytes.len() as u64);
            self.close();
            return;
        }
        let queued_at = Instant::now();
        for start in (0..bytes.len()).step_by(16 * 1024) {
            let end = (start + 16 * 1024).min(bytes.len());
            let _ = self.sender.send(EncodedChunk {
                bytes: bytes.slice(start..end),
                queued_at,
            });
        }
    }

    /// Called only when an HTTP body is polled for this chunk.
    pub fn record_body_consumed(&self, bytes: usize) {
        self.add_counter(CONSUMED, bytes as u64);
        if self.first_served_at.set(Instant::now()).is_ok() {
            debug!(session_id = %self.session.session_id, bytes, "first HTTP body bytes consumed");
        }
    }

    /// Publish bytes that must prefix every subscriber response, such as a WAV header.
    pub fn publish_prelude(&self, bytes: Bytes) {
        if bytes.is_empty() || self.is_closed() {
            return;
        }

        if let Ok(mut prelude) = self.prelude.lock() {
            if prelude.is_none() {
                *prelude = Some(bytes.clone());
                // Attachment holds this same lock across subscribing and reading the header.
                self.publish(bytes);
            }
        }
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
        let _ = self.subscriber_connected_at.set(now);
        self.subscriber.send_replace(true);

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

    pub fn attach_subscriber(&self) -> (Option<Bytes>, broadcast::Receiver<EncodedChunk>) {
        let prelude = self.prelude.lock().expect("prelude lock poisoned");
        let subscriber = self.sender.subscribe();
        self.subscriber_count.fetch_add(1, Ordering::Relaxed);
        self.on_subscriber_connected();
        (prelude.clone(), subscriber)
    }

    pub(crate) fn detach_subscriber(&self) {
        let _attachment = self.prelude.lock().expect("prelude lock poisoned");
        if self.subscriber_count.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.subscriber_connected.store(false, Ordering::Release);
            self.subscriber.send_replace(false);
        }
    }

    pub fn close(&self) {
        self.closed.send_replace(true);
        if let Some(queue) = self.input_queue.get() {
            queue.close();
            queue.clear();
        }
    }

    pub fn is_closed(&self) -> bool {
        *self.closed.borrow()
    }

    pub async fn closed(&self) {
        let mut closed = self.closed.subscribe();
        let _ = closed.wait_for(|closed| *closed).await;
    }

    pub fn set_playback_anchor(&self, anchor: Instant) {
        if let Ok(mut at) = self.playback_anchor.lock() {
            *at = PlaybackAnchorState::At(anchor);
        }
        self.playback_release.send_replace(Some(anchor));
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
        self.playback_release.send_replace(Some(Instant::now()));
        let timing = self.timing();
        info!(
            session_id = %self.session.session_id,
            encoded_bytes = timing.encoded_bytes,
            bytes_dropped = timing.bytes_dropped,
            bytes_skipped = timing.bytes_skipped,
            "stream playback anchor armed for next timed PCM"
        );
    }

    pub fn set_playback_plan(&self, source_cutoff: Instant, release_at: Instant) {
        if let Ok(mut anchor) = self.playback_anchor.lock() {
            *anchor = PlaybackAnchorState::At(source_cutoff);
        }
        self.playback_release.send_replace(Some(release_at));
    }

    pub async fn wait_for_playback_release(&self) -> bool {
        let mut release = self.playback_release.subscribe();
        loop {
            let deadline = *release.borrow_and_update();
            match deadline {
                Some(deadline) => tokio::select! {
                    biased;
                    _ = self.closed() => return false,
                    changed = release.changed() => if changed.is_err() { return false; },
                    _ = tokio::time::sleep_until(deadline.into()) => return true,
                },
                None => tokio::select! {
                    _ = self.closed() => return false,
                    changed = release.changed() => if changed.is_err() { return false; },
                },
            }
        }
    }

    pub fn set_playback_epoch(&self, epoch: u64) {
        self.playback_epoch.store(epoch, Ordering::Release);
    }

    pub fn accepts_epoch(&self, epoch: u64) -> bool {
        !self.is_closed() && self.playback_epoch.load(Ordering::Acquire) == epoch
    }

    pub fn clear_playback_anchor(&self) {
        self.playback_release.send_replace(None);
        if let Ok(mut at) = self.playback_anchor.lock() {
            *at = PlaybackAnchorState::Unset;
        }
    }

    /// Waits until the encoder has produced stream data or the timeout elapses.
    pub async fn wait_until_ready(&self, timeout: Duration) -> bool {
        let mut ready = self.ready.subscribe();
        tokio::select! {
            result = ready.wait_for(|ready| *ready) => result.is_ok(),
            _ = self.closed() => false,
            _ = tokio::time::sleep(timeout) => false,
        }
    }

    /// Waits until an HTTP subscriber connects or the timeout elapses.
    pub async fn wait_for_subscriber(&self, timeout: Duration) -> bool {
        let mut subscriber = self.subscriber.subscribe();
        tokio::select! {
            result = subscriber.wait_for(|connected| *connected) => result.is_ok(),
            _ = self.closed() => false,
            _ = tokio::time::sleep(timeout) => false,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<EncodedChunk> {
        self.sender.subscribe()
    }

    pub fn prelude(&self) -> Option<Bytes> {
        self.prelude.lock().ok().and_then(|prelude| prelude.clone())
    }

    pub fn bytes_served(&self) -> u64 {
        self.counter(CONSUMED)
    }

    pub fn bytes_encoded(&self) -> u64 {
        self.encoded_bytes.load(Ordering::Relaxed)
    }

    pub fn bytes_dropped(&self) -> u64 {
        self.counter(DROPPED)
    }

    pub fn timing(&self) -> StreamTiming {
        StreamTiming {
            encoded_bytes: self.bytes_encoded(),
            bytes_served: self.bytes_served(),
            bytes_dropped: self.bytes_dropped(),
            bytes_skipped: self.counter(SKIPPED),
            chunks_dropped: self.chunks_dropped.load(Ordering::Relaxed),
            subscriber_connected: self.subscriber_connected.load(Ordering::Relaxed),
            playback_anchor_at: self.playback_anchor.lock().ok().and_then(|t| match *t {
                PlaybackAnchorState::At(anchor) => Some(anchor),
                PlaybackAnchorState::Unset | PlaybackAnchorState::NextTimedPcm => None,
            }),
            first_encoded_at: self.first_encoded_at.get().copied(),
            first_served_at: self.first_served_at.get().copied(),
            subscriber_connected_at: self.subscriber_connected_at.get().copied(),
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

    #[test]
    fn concurrent_header_publication_and_attachment_deliver_one_header() {
        for _ in 0..32 {
            let stream = LiveStream::new(session());
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let publisher = stream.clone();
            let ready = barrier.clone();
            let thread = std::thread::spawn(move || {
                ready.wait();
                publisher.publish_prelude(Bytes::from_static(b"header"));
            });
            barrier.wait();
            let (prelude, mut receiver) = stream.attach_subscriber();
            thread.join().unwrap();
            let headers = usize::from(prelude.is_some()) + usize::from(receiver.try_recv().is_ok());
            assert_eq!(headers, 1);
            assert!(receiver.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn readiness_is_retained_without_watch_receivers() {
        let stream = LiveStream::new(session());
        stream.publish(Bytes::from_static(b"ready"));
        assert!(stream.wait_until_ready(Duration::ZERO).await);
        let _subscriber = stream.attach_subscriber();
        assert!(stream.wait_for_subscriber(Duration::ZERO).await);
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
        assert_eq!(stream.bytes_served(), 0);
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

        assert_eq!(stream.bytes_served(), 0);
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
        assert_eq!(&chunk.bytes[..], b"live");
        assert!(rx.try_recv().is_err());
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
        assert_eq!(&chunk.bytes[..], b"live");
    }

    #[tokio::test]
    async fn attached_subscriber_receives_prelude_published_after_attach() {
        let stream = LiveStream::new(session());
        let (prelude, mut rx) = stream.attach_subscriber();

        assert!(prelude.is_none());
        stream.publish_prelude(Bytes::from_static(b"header"));

        let chunk = rx.try_recv().expect("late prelude chunk");
        assert_eq!(&chunk.bytes[..], b"header");
    }

    #[tokio::test]
    async fn attached_subscriber_gets_existing_prelude_snapshot() {
        let stream = LiveStream::new(session());
        stream.publish_prelude(Bytes::from_static(b"header"));

        let (prelude, mut rx) = stream.attach_subscriber();
        stream.publish(Bytes::from_static(b"live"));

        assert_eq!(prelude.expect("prelude"), Bytes::from_static(b"header"));
        let chunk = rx.try_recv().expect("live chunk");
        assert_eq!(&chunk.bytes[..], b"live");
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
        assert_eq!(&chunk.bytes[..], b"new");
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
        assert_eq!(chunk.bytes.len(), 20);
        assert_eq!(&chunk.bytes[..4], &[20, 21, 22, 23]);
        assert_eq!(stream.timing().bytes_skipped, 20);
        assert_eq!(stream.bytes_served(), 0);
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
        assert_eq!(&chunk.bytes[..], b"live");
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
        assert_eq!(&chunk.bytes[..], b"live");
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
        assert_eq!(&chunk.bytes[..], b"live");
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
        assert_eq!(&old.bytes[..], b"old1");
        let new = rx.try_recv().expect("new anchored chunk");
        assert_eq!(&new.bytes[..], b"new1");
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
