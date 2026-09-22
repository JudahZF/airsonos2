//! Repeatable in-process WAV/HTTP workload. It measures neither Sonos nor acoustics.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use airsonos2_core::{EncoderState, PcmFrame, SessionId, StreamCodec, StreamSession, ZoneId};
use airsonos2_stream::{FfmpegEncoder, FfmpegEncoderConfig, StreamRegistry, build_stream_router};
use axum::body::Body;
use futures_util::StreamExt;
use http::Request;
use tower::ServiceExt;

struct CountingAllocator;
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
// The benchmark forwards every allocation unchanged to the system allocator.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn pcm() -> PcmFrame {
    PcmFrame {
        buffered_permit: None,
        playback_epoch: 0,
        sample_rate: 48_000,
        channels: 2,
        samples_f32_interleaved: vec![0.25; 960],
        presentation_time: None,
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if std::env::args().nth(1).as_deref() == Some("--conversion") {
        let samples = vec![0.25; 960];
        let before = ALLOCATIONS.load(Ordering::Relaxed);
        let start = Instant::now();
        for _ in 0..10_000 {
            std::hint::black_box(airsonos2_stream::pcm::f32_pcm_to_s16le_bytes(
                std::hint::black_box(&samples),
            ));
        }
        let original_us = start.elapsed().as_micros();
        let original_allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
        let before = ALLOCATIONS.load(Ordering::Relaxed);
        let start = Instant::now();
        let mut scratch = Vec::new();
        for _ in 0..10_000 {
            airsonos2_stream::pcm::write_f32_pcm_to_s16le(
                std::hint::black_box(&samples),
                &mut scratch,
            );
            std::hint::black_box(&scratch);
        }
        let reused_us = start.elapsed().as_micros();
        let reused_allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
        println!(
            "conversion_frames=10000 original_allocations={original_allocations} reused_allocations={reused_allocations} original_us={original_us} reused_us={reused_us}"
        );
        return;
    }
    let rooms: usize = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "1".to_owned())
        .parse()
        .unwrap();
    let frames: usize = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "500".to_owned())
        .parse()
        .unwrap();
    let registry = StreamRegistry::new();
    let mut active = Vec::new();
    let mut startup_us = Vec::new();
    let mut eof_us = Vec::new();
    // Repeated startup samples, then keep the final sessions for paced steady playback.
    for round in 0..20 {
        for room in 0..rooms {
            let started = Instant::now();
            let session_id = SessionId::new();
            let stream = registry
                .create(StreamSession {
                    session_id,
                    zone_id: ZoneId::new(room.to_string()),
                    codec: StreamCodec::Wav,
                    generation: 1,
                    local_url: format!("http://127.0.0.1/streams/{session_id}.wav")
                        .parse()
                        .unwrap(),
                    encoder_state: EncoderState::Running,
                })
                .await;
            let encoder = FfmpegEncoder::spawn(
                FfmpegEncoderConfig {
                    ffmpeg_path: "unused".into(),
                    sample_rate: 48_000,
                    channels: 2,
                    mp3_bitrate_kbps: 320,
                    codec: StreamCodec::Wav,
                    queue_duration: Duration::from_millis(2750),
                },
                stream.clone(),
            )
            .unwrap();
            let response = build_stream_router(registry.clone(), "unused".into())
                .oneshot(
                    Request::builder()
                        .uri(format!("/streams/{session_id}.wav"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let mut body = response.into_body().into_data_stream();
            assert_eq!(body.next().await.unwrap().unwrap().len(), 44);
            stream.arm_playback_anchor_on_next_timed_pcm();
            encoder.write_frame(pcm()).await.unwrap();
            assert_eq!(body.next().await.unwrap().unwrap().len(), 1920);
            startup_us.push(started.elapsed().as_micros());
            if round == 19 {
                active.push((session_id, encoder, body));
            } else {
                let stopped = Instant::now();
                registry.remove(&session_id).await;
                assert!(body.next().await.is_none());
                eof_us.push(stopped.elapsed().as_micros());
                encoder.shutdown().await.unwrap();
            }
        }
    }
    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let started = Instant::now();
    let mut interval = tokio::time::interval(Duration::from_millis(10));
    for _ in 0..frames {
        interval.tick().await;
        for (_, encoder, _) in &active {
            encoder.write_frame(pcm()).await.unwrap();
        }
        for (_, _, body) in &mut active {
            assert_eq!(body.next().await.unwrap().unwrap().len(), 1920);
        }
    }
    let elapsed_ms = started.elapsed().as_millis();
    let steady_allocations = ALLOCATIONS.load(Ordering::Relaxed) - allocations;
    for (session_id, encoder, mut body) in active {
        let stopped = Instant::now();
        registry.remove(&session_id).await;
        assert!(body.next().await.is_none());
        eof_us.push(stopped.elapsed().as_micros());
        encoder.shutdown().await.unwrap();
    }
    startup_us.sort_unstable();
    eof_us.sort_unstable();
    let percentile = |values: &[u128], percent: usize| {
        values
            .get((values.len().saturating_sub(1) * percent) / 100)
            .copied()
            .unwrap_or(0)
    };
    println!(
        "rooms={rooms} frames={frames} elapsed_ms={elapsed_ms} steady_allocations={steady_allocations} startup_samples={} startup_p50_us={} startup_p95_us={} stop_eof_p95_us={}",
        startup_us.len(),
        percentile(&startup_us, 50),
        percentile(&startup_us, 95),
        percentile(&eof_us, 95)
    );
    print!("{}", registry.metrics_text().await);
}
