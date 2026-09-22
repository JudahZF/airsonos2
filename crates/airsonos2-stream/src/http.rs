use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, RawQuery, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{Response, StatusCode};
use axum::routing::get;
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::io::ReaderStream;

use airsonos2_core::{SessionId, StreamCodec};
use tracing::debug;

use crate::registry::StreamRegistry;

#[derive(Clone, Debug)]
pub struct HttpState {
    registry: StreamRegistry,
    ffmpeg_path: PathBuf,
}

pub fn build_stream_router(registry: StreamRegistry, ffmpeg_path: PathBuf) -> Router {
    let state = HttpState {
        registry,
        ffmpeg_path,
    };

    Router::new()
        .route("/healthz", get(healthz))
        .route("/streams/{session_id}", get(stream_session))
        .route("/test-tone.mp3", get(test_tone))
        .with_state(state)
}

pub async fn serve_stream_http(
    addr: SocketAddr,
    registry: StreamRegistry,
    ffmpeg_path: PathBuf,
) -> Result<(), HttpServerError> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(HttpServerError::Bind)?;
    axum::serve(listener, build_stream_router(registry, ffmpeg_path))
        .await
        .map_err(HttpServerError::Serve)?;
    Ok(())
}

async fn healthz() -> &'static str {
    "ok\n"
}

async fn stream_session(
    Path(session_id): Path<String>,
    RawQuery(query): RawQuery,
    State(state): State<HttpState>,
) -> Result<Response<Body>, StatusCode> {
    let session_id = session_id
        .strip_suffix(".mp3")
        .or_else(|| session_id.strip_suffix(".wav"))
        .or_else(|| session_id.strip_suffix(".aac"))
        .unwrap_or(&session_id)
        .to_owned();
    let session_id = SessionId::parse(&session_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let stream = state
        .registry
        .get(&session_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    if let Some(generation) = stream_generation(query.as_deref())?
        && generation != stream.session.generation
    {
        return Err(StatusCode::NOT_FOUND);
    }
    let (prelude, subscriber) = stream.attach_subscriber();
    debug!(%session_id, "HTTP stream subscriber attached");
    let live_stream = BroadcastStream::new(subscriber).filter_map(|message| async {
        match message {
            Ok(bytes) => Some(Ok::<Bytes, Infallible>(bytes)),
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(_)) => None,
        }
    });
    let prelude_stream = stream::iter(prelude.map(Ok::<Bytes, Infallible>));
    let terminal = stream.clone();
    let guard = SubscriberGuard(stream.clone());
    let body_stream = prelude_stream
        .chain(live_stream)
        .take_until(async move { terminal.closed().await })
        .map(move |chunk| {
            let _ = &guard;
            chunk
        });

    Ok(stream_response(
        stream.session.codec,
        Body::from_stream(body_stream),
    ))
}

struct SubscriberGuard(crate::registry::LiveStream);

impl Drop for SubscriberGuard {
    fn drop(&mut self) {
        self.0.detach_subscriber();
    }
}

fn stream_generation(query: Option<&str>) -> Result<Option<u64>, StatusCode> {
    let Some(query) = query else {
        return Ok(None);
    };

    for pair in query.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        if key == "gen" {
            return value
                .parse::<u64>()
                .map(Some)
                .map_err(|_| StatusCode::BAD_REQUEST);
        }
    }

    Ok(None)
}

async fn test_tone(State(state): State<HttpState>) -> Result<Response<Body>, StatusCode> {
    let mut child = Command::new(&state.ffmpeg_path)
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        .arg("-f")
        .arg("lavfi")
        .arg("-i")
        .arg("sine=frequency=440:sample_rate=44100")
        .arg("-f")
        .arg("mp3")
        .arg("-codec:a")
        .arg("libmp3lame")
        .arg("-b:a")
        .arg("128k")
        .arg("pipe:1")
        .kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let stdout = child.stdout.take().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let stream = ReaderStream::new(stdout).map(move |chunk| {
        let _ = &child;
        chunk
    });

    Ok(stream_response(StreamCodec::Mp3, Body::from_stream(stream)))
}

fn stream_response(codec: StreamCodec, body: Body) -> Response<Body> {
    let content_type = match codec {
        StreamCodec::Mp3 => "audio/mpeg",
        StreamCodec::Aac => "audio/aac",
        StreamCodec::Wav => "audio/wav",
    };

    Response::builder()
        .header(CONTENT_TYPE, content_type)
        .header(CACHE_CONTROL, "no-store")
        .body(body)
        .expect("valid response")
}

#[derive(Debug, Error)]
pub enum HttpServerError {
    #[error("failed to bind stream HTTP server: {0}")]
    Bind(std::io::Error),
    #[error("stream HTTP server failed: {0}")]
    Serve(std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use airsonos2_core::{EncoderState, StreamCodec, StreamSession, ZoneId};
    use futures_util::StreamExt;
    use http::{Request, StatusCode};
    use tower::ServiceExt;
    use url::Url;

    #[tokio::test]
    async fn removal_and_replacement_close_attached_bodies() {
        for replace in [false, true] {
            let registry = StreamRegistry::new();
            let session = StreamSession {
                session_id: SessionId::new(),
                zone_id: ZoneId::new("TEST"),
                codec: StreamCodec::Wav,
                generation: 1,
                local_url: Url::parse("http://localhost/stream.wav").unwrap(),
                encoder_state: EncoderState::Running,
            };
            let id = session.session_id;
            let live = registry.create(session.clone()).await;
            let response = build_stream_router(registry.clone(), PathBuf::from("ffmpeg"))
                .oneshot(
                    Request::builder()
                        .uri(format!("/streams/{id}.wav"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let mut body = response.into_body().into_data_stream();
            live.publish(Bytes::from_static(b"queued-old-data"));
            if replace {
                registry
                    .create(StreamSession {
                        generation: 2,
                        ..session
                    })
                    .await;
            } else {
                registry.remove(&id).await;
            }
            assert!(
                tokio::time::timeout(std::time::Duration::from_secs(1), body.next())
                    .await
                    .expect("body must close")
                    .is_none()
            );
            assert!(live.is_closed());
            drop(body);
            assert!(!live.timing().subscriber_connected);
        }
    }

    #[tokio::test]
    async fn streams_registered_mp3_chunks() {
        let registry = StreamRegistry::new();
        let session_id = SessionId::new();
        let stream = registry
            .create(StreamSession {
                session_id,
                zone_id: ZoneId::new("RINCON_TEST"),
                codec: StreamCodec::Mp3,
                generation: 7,
                local_url: Url::parse("http://127.0.0.1:7000/streams/test.mp3").expect("url"),
                encoder_state: EncoderState::Running,
            })
            .await;
        let app = build_stream_router(registry, PathBuf::from("ffmpeg"));
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/streams/{session_id}.mp3"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CONTENT_TYPE], "audio/mpeg");

        stream.on_subscriber_connected();
        let mut body = response.into_body().into_data_stream();
        stream.publish(Bytes::from_static(b"mp3"));
        let chunk = body
            .next()
            .await
            .expect("body chunk")
            .expect("body chunk ok");
        assert_eq!(&chunk[..], b"mp3");
    }

    #[tokio::test]
    async fn streams_registered_wav_chunks() {
        let registry = StreamRegistry::new();
        let session_id = SessionId::new();
        let stream = registry
            .create(StreamSession {
                session_id,
                zone_id: ZoneId::new("RINCON_TEST"),
                codec: StreamCodec::Wav,
                generation: 7,
                local_url: Url::parse("http://127.0.0.1:7000/streams/test.wav").expect("url"),
                encoder_state: EncoderState::Running,
            })
            .await;
        let app = build_stream_router(registry, PathBuf::from("ffmpeg"));
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/streams/{session_id}.wav"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CONTENT_TYPE], "audio/wav");

        stream.on_subscriber_connected();
        let mut body = response.into_body().into_data_stream();
        stream.publish(Bytes::from_static(b"wav"));
        let chunk = body
            .next()
            .await
            .expect("body chunk")
            .expect("body chunk ok");
        assert_eq!(&chunk[..], b"wav");
    }

    #[tokio::test]
    async fn rejects_bad_session_id() {
        let app = build_stream_router(StreamRegistry::new(), PathBuf::from("ffmpeg"));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/streams/nope.mp3")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn stream_generation_query_attaches_when_current() {
        let registry = StreamRegistry::new();
        let session_id = SessionId::new();
        registry
            .create(StreamSession {
                session_id,
                zone_id: ZoneId::new("RINCON_TEST"),
                codec: StreamCodec::Wav,
                generation: 3,
                local_url: Url::parse("http://127.0.0.1:7000/streams/test.wav?gen=3").expect("url"),
                encoder_state: EncoderState::Running,
            })
            .await;
        let app = build_stream_router(registry, PathBuf::from("ffmpeg"));

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/streams/{session_id}.wav?gen=3"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn stream_generation_query_rejects_stale_generation() {
        let registry = StreamRegistry::new();
        let session_id = SessionId::new();
        registry
            .create(StreamSession {
                session_id,
                zone_id: ZoneId::new("RINCON_TEST"),
                codec: StreamCodec::Wav,
                generation: 4,
                local_url: Url::parse("http://127.0.0.1:7000/streams/test.wav?gen=4").expect("url"),
                encoder_state: EncoderState::Running,
            })
            .await;
        let app = build_stream_router(registry, PathBuf::from("ffmpeg"));

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/streams/{session_id}.wav?gen=3"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn stream_without_generation_query_still_attaches() {
        let registry = StreamRegistry::new();
        let session_id = SessionId::new();
        registry
            .create(StreamSession {
                session_id,
                zone_id: ZoneId::new("RINCON_TEST"),
                codec: StreamCodec::Wav,
                generation: 5,
                local_url: Url::parse("http://127.0.0.1:7000/streams/test.wav?gen=5").expect("url"),
                encoder_state: EncoderState::Running,
            })
            .await;
        let app = build_stream_router(registry, PathBuf::from("ffmpeg"));

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/streams/{session_id}.wav"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }
}
