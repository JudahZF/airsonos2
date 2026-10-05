use super::*;
use crate::renderer::RendererError;
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Clone)]
pub(super) enum TransportCommand {
    Prepare(Box<StreamPrepare>),
    Play(Box<PreparedDownstream>),
    /// Play once the group members have joined, so the whole group starts together.
    PlayGroup(Box<PreparedDownstream>, GroupJoins),
    /// Join the native Sonos group of the player with RINCON id `coordinator`.
    Join {
        stream: Box<PreparedDownstream>,
        client: SonosClient,
        coordinator: String,
        joins: GroupJoins,
    },
    Stop {
        session_id: SessionId,
        zone_id: ZoneId,
        generation: u64,
    },
}

/// Members that still have to join a native Sonos group. A member that never
/// runs its join leaves the coordinator to wait for the timeout.
#[derive(Clone, Debug)]
pub(super) struct GroupJoins(Arc<watch::Sender<usize>>);

impl GroupJoins {
    const WAIT: Duration = Duration::from_secs(2);

    pub(super) fn new(members: usize) -> Self {
        Self(Arc::new(watch::Sender::new(members)))
    }

    fn joined(&self) {
        self.0
            .send_modify(|pending| *pending = pending.saturating_sub(1));
    }

    async fn wait(&self) {
        let mut pending = self.0.subscribe();
        let _ = tokio::time::timeout(Self::WAIT, pending.wait_for(|pending| *pending == 0)).await;
    }
}

pub(super) struct ZoneWorker {
    transport: watch::Sender<Option<TransportCommand>>,
    volume: watch::Sender<Option<u8>>,
    transport_task: JoinHandle<()>,
    volume_task: JoinHandle<()>,
}

impl ZoneWorker {
    pub(super) fn new(
        renderer: Renderer,
        result_tx: mpsc::UnboundedSender<DownstreamStartResult>,
        subscriber_wait: Duration,
        prebuffer: Duration,
    ) -> Self {
        let (transport, mut commands) = watch::channel(None);
        let transport_renderer = renderer.clone();
        let transport_task = tokio::spawn(async move {
            // This zone joined a native Sonos group. Only this task sends its
            // transport commands, so it alone tracks that.
            let mut joined = false;
            while commands.changed().await.is_ok() {
                let command = commands.borrow_and_update().clone();
                match command {
                    Some(TransportCommand::Prepare(start)) => {
                        // A member must leave the group before the Stop barrier.
                        if std::mem::take(&mut joined)
                            && let Err(error) = transport_renderer.leave_group().await
                        {
                            warn!(zone_id = %start.zone_id, "leaving the Sonos group failed: {error}");
                        }
                        prepare(*start, &commands, subscriber_wait, prebuffer).await;
                    }
                    Some(TransportCommand::Play(stream)) => {
                        let result = stream
                            .renderer
                            .play(&stream.live_stream.session.local_url)
                            .await;
                        report_start(&result_tx, &stream, "Play", result);
                    }
                    Some(TransportCommand::PlayGroup(stream, joins)) => {
                        let mut changed = commands.clone();
                        tokio::select! {
                            _ = changed.changed() => continue,
                            _ = joins.wait() => {}
                        }
                        let result = stream
                            .renderer
                            .play(&stream.live_stream.session.local_url)
                            .await;
                        report_start(&result_tx, &stream, "Play", result);
                    }
                    Some(TransportCommand::Join {
                        stream,
                        client,
                        coordinator,
                        joins,
                    }) => {
                        let result = client
                            .join_group(&coordinator)
                            .await
                            .map_err(RendererError::from);
                        // A timed-out join may have taken effect.
                        joined = !result.as_ref().is_err_and(|error| !error.is_timeout());
                        joins.joined();
                        report_start(&result_tx, &stream, "Group join", result);
                    }
                    Some(TransportCommand::Stop {
                        session_id,
                        zone_id,
                        generation,
                    }) => {
                        // Leaving the group stops a member; members reject Stop.
                        let result = if std::mem::take(&mut joined) {
                            transport_renderer.leave_group().await
                        } else {
                            transport_renderer.stop().await
                        };
                        let outcome = match result {
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
                        });
                    }
                    None => {}
                }
            }
        });
        let (volume, mut volumes) = watch::channel(None);
        let volume_task = tokio::spawn(async move {
            while volumes.changed().await.is_ok() {
                let value = *volumes.borrow_and_update();
                if let Some(value) = value
                    && let Err(error) = renderer.set_volume(value).await
                {
                    warn!(zone_id = %renderer.id(), "volume request failed: {error}");
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

fn report_start(
    result_tx: &mpsc::UnboundedSender<DownstreamStartResult>,
    stream: &PreparedDownstream,
    action: &str,
    result: Result<(), RendererError>,
) {
    let outcome = match result {
        Ok(()) => DownstreamStartOutcome::Started,
        Err(error) if error.is_timeout() => {
            warn!(zone_id = %stream.zone_id, "{action} timed out; playback is unknown, retrying the same stream");
            DownstreamStartOutcome::Unknown
        }
        Err(error) => {
            warn!(zone_id = %stream.zone_id, "{action} failed: {error}");
            if error.is_retryable() {
                DownstreamStartOutcome::Failed
            } else {
                DownstreamStartOutcome::PermanentFailure
            }
        }
    };
    let _ = result_tx.send(DownstreamStartResult {
        session_id: stream.session_id,
        zone_id: stream.zone_id.clone(),
        generation: stream.generation,
        outcome,
    });
}

async fn prepare(
    start: StreamPrepare,
    commands: &watch::Receiver<Option<TransportCommand>>,
    subscriber_wait: Duration,
    prebuffer: Duration,
) {
    let failure = |error: RendererError| {
        warn!(zone_id = %start.zone_id, "prepare failed: {error}");
        let _ = start.result_tx.send(DownstreamStartResult {
            session_id: start.session_id,
            zone_id: start.zone_id.clone(),
            generation: start.generation,
            outcome: if error.is_retryable() {
                DownstreamStartOutcome::Failed
            } else {
                DownstreamStartOutcome::PermanentFailure
            },
        });
    };
    // Home Assistant players get the stream URL in Play. `play_media` replaces whatever
    // they play, so they need no Stop barrier either.
    let sonos = match &start.renderer {
        Renderer::Sonos { zone, client } => Some((zone, client)),
        Renderer::HomeAssistant { .. } => None,
    };
    if let Some((zone, client)) = sonos {
        // Group members reject AVTransport commands, so leave the group before the
        // Stop barrier.
        if start.force_standalone_on_start
            && let Err(error) = client.become_coordinator_of_standalone_group().await
        {
            failure(error.into());
            return;
        }
        // Never cancel an in-flight Stop. Every replacement passes this barrier,
        // even when watch coalesces an earlier pause/stop command.
        if let Err(error) = client.stop().await {
            failure(error.into());
            return;
        }
        if commands.has_changed().unwrap_or(true) {
            return;
        }
        if let Err(error) = client
            .set_av_transport_uri(
                start.local_url.as_str(),
                &format!("{} AirSonos2", zone.room_name),
            )
            .await
        {
            failure(error.into());
            return;
        }
        if commands.has_changed().unwrap_or(true) {
            return;
        }
    }
    // Readiness waits can be cancelled after the network command completes.
    let mut changed = commands.clone();
    tokio::select! {
        _ = changed.changed() => return,
        _ = async {
            // Home Assistant players connect only after Play.
            if sonos.is_some() {
                start.live_stream.wait_for_subscriber(subscriber_wait).await;
            }
            if start.live_stream.session.codec == StreamCodec::Wav {
                start.live_stream.wait_until_ready(prebuffer).await;
            }
        } => {}
    }
    let _ = start.prepared_tx.send(PreparedDownstream {
        session_id: start.session_id,
        zone_id: start.zone_id,
        generation: start.generation,
        renderer: start.renderer,
        live_stream: start.live_stream,
    });
}

#[cfg(test)]
pub(super) mod tests;
