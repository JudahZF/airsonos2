use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;

pub(crate) struct FakeSonos {
    pub client: SonosClient,
    requests: mpsc::UnboundedReceiver<(String, oneshot::Sender<()>)>,
    task: JoinHandle<()>,
}

impl FakeSonos {
    pub(crate) async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = SonosClient::from_base_url(
            Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap(),
        )
        .unwrap();
        let (tx, requests) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut children = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = children.join_next(), if !children.is_empty() => {},
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.unwrap();
                        let tx = tx.clone();
                        children.spawn(async move {
                            let mut request = Vec::new();
                            let mut buffer = [0; 4096];
                            loop {
                                let read = socket.read(&mut buffer).await.unwrap();
                                if read == 0 { return; }
                                request.extend_from_slice(&buffer[..read]);
                                let text = String::from_utf8_lossy(&request);
                                if let Some(end) = text.find("\r\n\r\n") {
                                    let length = text[..end].lines().find_map(|line| line.to_ascii_lowercase().strip_prefix("content-length: ").and_then(|length| length.parse::<usize>().ok())).unwrap_or(0);
                                    if request.len() >= end + 4 + length { break; }
                                }
                                assert!(request.len() < 64 * 1024);
                            }
                            let (release, wait) = oneshot::channel();
                            if tx.send((String::from_utf8(request).unwrap(), release)).is_err() { return; }
                            let _ = wait.await;
                            let _ = socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await;
                        });
                    }
                }
            }
        });
        Self {
            client,
            requests,
            task,
        }
    }

    pub(crate) fn renderer(&self) -> Renderer {
        crate::tests::sonos(ZoneId::new("TEST"), self.client.clone())
    }

    pub(crate) async fn request(&mut self) -> (String, oneshot::Sender<()>) {
        tokio::time::timeout(Duration::from_secs(2), self.requests.recv())
            .await
            .expect("request timeout")
            .expect("request")
    }
}

impl Drop for FakeSonos {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn volume_burst_keeps_only_latest_pending_value() {
    let mut fake = FakeSonos::start().await;
    let (tx, _) = mpsc::unbounded_channel();
    let worker = ZoneWorker::new(fake.renderer(), tx, Duration::ZERO, Duration::ZERO);
    worker.set_volume(1);
    let (body, release) = fake.request().await;
    assert!(body.contains("<DesiredVolume>1</DesiredVolume>"));
    for value in 2..=100 {
        worker.set_volume(value);
    }
    assert!(fake.requests.try_recv().is_err());
    release.send(()).unwrap();
    let (body, release) = fake.request().await;
    assert!(body.contains("<DesiredVolume>100</DesiredVolume>"));
    release.send(()).unwrap();
    worker.shutdown().await;
    assert!(fake.requests.try_recv().is_err());
}

#[tokio::test]
async fn replacement_waits_for_inflight_stop_before_setting_uri() {
    let mut fake = FakeSonos::start().await;
    let (tx, _) = mpsc::unbounded_channel();
    let (prepared_tx, mut prepared_rx) = mpsc::unbounded_channel();
    let worker = ZoneWorker::new(fake.renderer(), tx.clone(), Duration::ZERO, Duration::ZERO);
    let session_id = SessionId::new();
    let zone_id = ZoneId::new("TEST");
    worker.command(TransportCommand::Stop {
        session_id,
        zone_id: zone_id.clone(),
        generation: 1,
    });
    let (body, release_stop) = fake.request().await;
    assert!(body.contains("#Stop"));
    let prepared = crate::tests::prepared_downstream(session_id, zone_id.clone(), StreamCodec::Mp3);
    worker.command(TransportCommand::Prepare(Box::new(StreamPrepare {
        session_id,
        zone_id: zone_id.clone(),
        generation: 2,
        renderer: crate::tests::sonos(zone_id, fake.client.clone()),
        local_url: prepared.live_stream.session.local_url.clone(),
        live_stream: prepared.live_stream,
        force_standalone_on_start: false,
        prepared_tx,
        result_tx: tx,
    })));
    assert!(fake.requests.try_recv().is_err());
    release_stop.send(()).unwrap();
    let (body, release) = fake.request().await;
    assert!(body.contains("#Stop"));
    release.send(()).unwrap();
    let (body, release) = fake.request().await;
    assert!(body.contains("#SetAVTransportURI"));
    release.send(()).unwrap();
    assert_eq!(prepared_rx.recv().await.unwrap().generation, 2);
    worker.shutdown().await;
}

#[tokio::test]
async fn play_timeout_is_unknown() {
    let mut fake = FakeSonos::start().await;
    let (tx, mut results) = mpsc::unbounded_channel();
    let worker = ZoneWorker::new(fake.renderer(), tx, Duration::ZERO, Duration::ZERO);
    let mut prepared =
        crate::tests::prepared_downstream(SessionId::new(), ZoneId::new("TEST"), StreamCodec::Mp3);
    prepared.renderer = fake.renderer();
    worker.command(TransportCommand::Play(Box::new(prepared)));
    let (_, _hold_response) = fake.request().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(6)).await;
    let result = results.recv().await.unwrap();
    assert_eq!(result.outcome, DownstreamStartOutcome::Unknown);
    worker.shutdown().await;
}

#[tokio::test]
async fn disconnect_during_play_stops_after_play_returns() {
    let mut fake = FakeSonos::start().await;
    let (tx, _) = mpsc::unbounded_channel();
    let worker = ZoneWorker::new(fake.renderer(), tx, Duration::ZERO, Duration::ZERO);
    let mut prepared =
        crate::tests::prepared_downstream(SessionId::new(), ZoneId::new("TEST"), StreamCodec::Mp3);
    prepared.renderer = fake.renderer();
    let stop = TransportCommand::Stop {
        session_id: prepared.session_id,
        zone_id: prepared.zone_id.clone(),
        generation: prepared.generation,
    };
    worker.command(TransportCommand::Play(Box::new(prepared)));
    let (body, release) = fake.request().await;
    assert!(body.contains("#Play"));
    worker.command(stop);
    release.send(()).unwrap();
    let (body, release) = fake.request().await;
    assert!(body.contains("#Stop"));
    release.send(()).unwrap();
    worker.shutdown().await;
    assert!(fake.requests.try_recv().is_err());
}

#[tokio::test]
async fn grouped_member_leaves_group_before_stop_barrier() {
    let mut fake = FakeSonos::start().await;
    let (tx, _) = mpsc::unbounded_channel();
    let (prepared_tx, mut prepared_rx) = mpsc::unbounded_channel();
    let worker = ZoneWorker::new(fake.renderer(), tx.clone(), Duration::ZERO, Duration::ZERO);
    let session_id = SessionId::new();
    let zone_id = ZoneId::new("TEST");
    let prepared = crate::tests::prepared_downstream(session_id, zone_id.clone(), StreamCodec::Mp3);
    worker.command(TransportCommand::Prepare(Box::new(StreamPrepare {
        session_id,
        zone_id: zone_id.clone(),
        generation: 1,
        renderer: crate::tests::sonos(zone_id, fake.client.clone()),
        local_url: prepared.live_stream.session.local_url.clone(),
        live_stream: prepared.live_stream,
        force_standalone_on_start: true,
        prepared_tx,
        result_tx: tx,
    })));
    for action in [
        "#BecomeCoordinatorOfStandaloneGroup",
        "#Stop",
        "#SetAVTransportURI",
    ] {
        let (body, release) = fake.request().await;
        assert!(body.contains(action), "expected {action}");
        release.send(()).unwrap();
    }
    assert_eq!(prepared_rx.recv().await.unwrap().generation, 1);
    worker.shutdown().await;
}

#[tokio::test]
async fn home_assistant_player_receives_stream_url_in_play() {
    let mut fake = FakeSonos::start().await;
    let (tx, _) = mpsc::unbounded_channel();
    let (prepared_tx, mut prepared_rx) = mpsc::unbounded_channel();
    let renderer = Renderer::HomeAssistant {
        id: ZoneId::new("media_player.kitchen"),
        name: "Kitchen".to_owned(),
        client: HomeAssistantClient::new(fake.client.base_url(), "token").unwrap(),
    };
    let worker = ZoneWorker::new(renderer.clone(), tx.clone(), Duration::ZERO, Duration::ZERO);
    let session_id = SessionId::new();
    let zone_id = renderer.id().clone();
    let live_stream =
        crate::tests::prepared_downstream(session_id, zone_id.clone(), StreamCodec::Mp3)
            .live_stream;
    let local_url = live_stream.session.local_url.clone();
    worker.command(TransportCommand::Prepare(Box::new(StreamPrepare {
        session_id,
        zone_id,
        generation: 1,
        renderer,
        local_url: local_url.clone(),
        live_stream,
        force_standalone_on_start: false,
        prepared_tx,
        result_tx: tx,
    })));
    // No Stop barrier or URI load: the URL goes out with Play.
    let prepared = prepared_rx.recv().await.unwrap();
    assert!(fake.requests.try_recv().is_err());
    worker.command(TransportCommand::Play(Box::new(prepared)));
    let (body, release) = fake.request().await;
    assert!(body.starts_with("POST /api/services/media_player/play_media "));
    assert!(body.contains(r#""entity_id":"media_player.kitchen""#));
    assert!(body.contains(&format!(r#""media_content_id":"{local_url}""#)));
    release.send(()).unwrap();
    worker.shutdown().await;
}
