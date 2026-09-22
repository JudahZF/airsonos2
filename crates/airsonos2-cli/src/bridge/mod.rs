use super::*;
use tokio::sync::watch;

#[derive(Clone)]
pub(super) enum TransportCommand {
    Idle,
    Prepare(SonosStreamPrepare),
    Play(PreparedDownstream),
    Stop {
        session_id: SessionId,
        zone_id: ZoneId,
        generation: u64,
    },
}

pub(super) struct ZoneWorker {
    transport: watch::Sender<Option<TransportCommand>>,
    volume: watch::Sender<Option<u8>>,
    transport_task: JoinHandle<()>,
    volume_task: JoinHandle<()>,
}

impl ZoneWorker {
    pub(super) fn new(
        client: SonosClient,
        result_tx: mpsc::UnboundedSender<DownstreamStartResult>,
        subscriber_wait: Duration,
        prebuffer: Duration,
    ) -> Self {
        let (transport, mut commands) = watch::channel(None);
        let transport_client = client.clone();
        let transport_task = tokio::spawn(async move {
            while commands.changed().await.is_ok() {
                let command = commands.borrow_and_update().clone();
                match command {
                    Some(TransportCommand::Prepare(start)) => {
                        prepare(start, &commands, subscriber_wait, prebuffer).await;
                    }
                    Some(TransportCommand::Play(stream)) => {
                        let started = Instant::now();
                        let mut timing = stream.timing.clone();
                        let outcome = match stream.client.play().await {
                            Ok(()) => {
                                timing.play_ms = Some(started.elapsed().as_millis() as u64);
                                DownstreamStartOutcome::Started
                            }
                            Err(error) if error.is_timeout() => {
                                warn!(zone_id = %stream.zone_id, "Play timed out; playback is unknown, retrying the same stream");
                                DownstreamStartOutcome::Unknown
                            }
                            Err(error) => {
                                warn!(zone_id = %stream.zone_id, "Play failed: {error}");
                                if error.is_retryable() {
                                    DownstreamStartOutcome::Failed
                                } else {
                                    DownstreamStartOutcome::PermanentFailure
                                }
                            }
                        };
                        let _ = result_tx.send(DownstreamStartResult {
                            session_id: stream.session_id,
                            zone_id: stream.zone_id,
                            generation: stream.generation,
                            outcome,
                            timing: (outcome == DownstreamStartOutcome::Started).then_some(timing),
                        });
                    }
                    Some(TransportCommand::Stop {
                        session_id,
                        zone_id,
                        generation,
                    }) => {
                        let outcome = match transport_client.stop().await {
                            Ok(()) => DownstreamStartOutcome::Stopped,
                            Err(error) => {
                                warn!(%zone_id, "Stop failed: {error}");
                                DownstreamStartOutcome::StopUnknown
                            }
                        };
                        let _ = result_tx.send(DownstreamStartResult {
                            session_id,
                            zone_id,
                            generation,
                            outcome,
                            timing: None,
                        });
                    }
                    Some(TransportCommand::Idle) | None => {}
                }
            }
        });
        let (volume, mut volumes) = watch::channel(None);
        let volume_task = tokio::spawn(async move {
            while volumes.changed().await.is_ok() {
                let value = *volumes.borrow_and_update();
                if let Some(value) = value
                    && let Err(error) = client.set_volume(value).await
                {
                    warn!("Sonos volume request failed: {error}");
                }
            }
        });
        Self {
            transport,
            volume,
            transport_task,
            volume_task,
        }
    }

    pub(super) fn command(&self, command: TransportCommand) {
        self.transport.send_replace(Some(command));
    }

    pub(super) fn set_volume(&self, value: u8) {
        self.volume.send_replace(Some(value));
    }

    pub(super) async fn shutdown(mut self) {
        // Closing senders drains the latest command, including the final Stop.
        let (replacement, _) = watch::channel(None);
        drop(std::mem::replace(&mut self.transport, replacement));
        let (replacement, _) = watch::channel(None);
        drop(std::mem::replace(&mut self.volume, replacement));
        let _ = (&mut self.transport_task).await;
        let _ = (&mut self.volume_task).await;
    }
}

impl Drop for ZoneWorker {
    fn drop(&mut self) {
        self.transport_task.abort();
        self.volume_task.abort();
    }
}

async fn prepare(
    start: SonosStreamPrepare,
    commands: &watch::Receiver<Option<TransportCommand>>,
    subscriber_wait: Duration,
    prebuffer: Duration,
) {
    let failure = |error: airsonos2_sonos::SonosClientError| {
        warn!(zone_id = %start.zone_id, "Sonos prepare failed: {error}");
        let _ = start.result_tx.send(DownstreamStartResult {
            session_id: start.session_id,
            zone_id: start.zone_id.clone(),
            generation: start.generation,
            outcome: if error.is_retryable() {
                DownstreamStartOutcome::Failed
            } else {
                DownstreamStartOutcome::PermanentFailure
            },
            timing: None,
        });
    };
    // Never cancel an in-flight Stop. Every replacement passes this barrier,
    // even when watch coalesces an earlier pause/stop command.
    if let Err(error) = start.client.stop().await {
        failure(error);
        return;
    }
    if commands.has_changed().unwrap_or(true) {
        return;
    }
    if start.force_standalone_on_start {
        if let Err(error) = start.client.become_coordinator_of_standalone_group().await {
            failure(error);
            return;
        }
        if commands.has_changed().unwrap_or(true) {
            return;
        }
    }
    let prepare_started = Instant::now();
    if let Err(error) = start
        .client
        .set_av_transport_uri(
            start.local_url.as_str(),
            &format!("{} AirSonos2", start.zone.room_name),
        )
        .await
    {
        failure(error);
        return;
    }
    let set_uri_ms = prepare_started.elapsed().as_millis() as u64;
    if commands.has_changed().unwrap_or(true) {
        return;
    }
    // Readiness waits can be cancelled after the network command completes.
    let mut changed = commands.clone();
    tokio::select! {
        _ = changed.changed() => return,
        _ = async {
            start.live_stream.wait_for_subscriber(subscriber_wait).await;
            if start.live_stream.session.codec == StreamCodec::Wav {
                start.live_stream.wait_until_ready(prebuffer).await;
            }
        } => {}
    }
    let timing = start.live_stream.timing();
    let _ = start.prepared_tx.send(PreparedDownstream {
        session_id: start.session_id,
        zone_id: start.zone_id,
        generation: start.generation,
        zone_room_name: start.zone.room_name,
        client: start.client,
        live_stream: start.live_stream,
        timing: ZoneStartupTiming {
            set_uri_ms: Some(set_uri_ms),
            subscriber_connect_ms: timing
                .subscriber_connected_at
                .map(|at| at.saturating_duration_since(prepare_started).as_millis() as u64),
            first_bytes_ms: timing
                .first_served_at
                .map(|at| at.saturating_duration_since(prepare_started).as_millis() as u64),
            play_ms: None,
        },
    });
}

#[cfg(test)]
pub(super) mod tests;
