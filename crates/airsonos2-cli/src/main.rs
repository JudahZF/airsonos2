use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use airsonos2_airplay::{
    AirPlayEndpointRunner, AirPlayEvent, FilePairingStore, PcmFormat, ZoneVolumeState,
};
use airsonos2_core::{
    Config, EncoderState, SessionId, SonosZone, StartupDelayEstimator, StreamCodec, StreamSession,
    VirtualAirPlayEndpoint, ZoneId, ZoneStartupTiming, combine_sync_delay, delays_from_offsets,
    filter_zones, sonos_volume_to_airplay_db, virtual_endpoint_for_zone,
};
use airsonos2_diagnostics::{CheckStatus, run_doctor};
use airsonos2_sonos::{SonosClient, discover_sonos_zones_from_sources};
use airsonos2_stream::{
    FfmpegEncoder, FfmpegEncoderConfig, LiveStream, StreamRegistry, serve_stream_http,
};
use clap::{Parser, Subcommand};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use url::Url;

mod home_assistant;

/// How long to keep a bridge session alive after the buffered audio stream
/// closes while AirPlay playback is paused.
const PAUSED_SESSION_GRACE_SECS: u64 = 60;
const DOWNSTREAM_RETRY_BASE_MS: u64 = 500;
const DOWNSTREAM_RETRY_MAX_MS: u64 = 5_000;

#[derive(Debug, Parser)]
#[command(
    name = "airsonos2",
    version,
    about = "AirPlay 2 bridge for legacy Sonos rooms"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve {
        #[arg(long, default_value = "/etc/airsonos2/config.toml")]
        config: PathBuf,
    },
    Discover {
        #[arg(long, default_value = "/etc/airsonos2/config.toml")]
        config: PathBuf,
    },
    Doctor {
        #[arg(long, default_value = "/etc/airsonos2/config.toml")]
        config: PathBuf,
    },
    Pairings {
        #[command(subcommand)]
        command: PairingsCommand,
        #[arg(long, default_value = "/etc/airsonos2/config.toml")]
        config: PathBuf,
    },
    Calibrate {
        #[arg(long, value_delimiter = ',')]
        zones: Vec<String>,
        #[arg(long, default_value = "/etc/airsonos2/config.toml")]
        config: PathBuf,
    },
    #[command(hide = true)]
    RenderHaConfig {
        #[arg(long, default_value = "/data/options.json")]
        options: PathBuf,
        #[arg(long, default_value = "/data/config.toml")]
        output: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum PairingsCommand {
    List,
    Reset {
        #[arg(long)]
        zone: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Serve { config } => {
            let config = Config::from_path(config)?;
            init_tracing(&config);
            serve(config).await
        }
        Command::Discover { config } => {
            let config = load_config_or_default(&config)?;
            init_tracing(&config);
            discover(&config).await
        }
        Command::Doctor { config } => {
            let config = load_config_or_default(&config)?;
            init_tracing(&config);
            doctor(&config).await
        }
        Command::Pairings { command, config } => {
            let config = load_config_or_default(&config)?;
            pairings(command, &config)
        }
        Command::Calibrate { zones, config } => {
            let config = load_config_or_default(&config)?;
            calibrate(&zones, &config)
        }
        Command::RenderHaConfig { options, output } => {
            home_assistant::render_config_file(&options, &output)?;
            Ok(())
        }
    }
}

fn init_tracing(config: &Config) {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| config.server.log_level.clone().into());
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .try_init();
}

fn load_config_or_default(path: &Path) -> anyhow::Result<Config> {
    if path.exists() {
        Ok(Config::from_path(path)?)
    } else {
        Ok(Config::default())
    }
}

async fn discover(config: &Config) -> anyhow::Result<()> {
    let zones = discover_sonos_zones_from_sources(
        Duration::from_secs(3),
        &config.sonos.static_ips,
        config.sonos.auto_discover,
    )
    .await?;

    if zones.is_empty() {
        println!("No Sonos rooms discovered.");
        return Ok(());
    }

    print_zones(&zones);
    Ok(())
}

async fn doctor(config: &Config) -> anyhow::Result<()> {
    let report = run_doctor(config).await?;

    for check in &report.checks {
        let status = match check.status {
            CheckStatus::Pass => "PASS",
            CheckStatus::Warn => "WARN",
            CheckStatus::Fail => "FAIL",
        };
        println!("{status:>4}  {:<36} {}", check.name, check.detail);
    }

    if !report.ok() {
        std::process::exit(1);
    }

    Ok(())
}

fn pairings(command: PairingsCommand, config: &Config) -> anyhow::Result<()> {
    let pairing_dir = config.server.state_dir.join("pairings");

    match command {
        PairingsCommand::List => {
            if !pairing_dir.exists() {
                println!("No pairings found at {}", pairing_dir.display());
                return Ok(());
            }

            let mut paths = fs::read_dir(&pairing_dir)?
                .map(|entry| entry.map(|entry| entry.path()))
                .collect::<Result<Vec<_>, _>>()?;
            paths.retain(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "json")
            });
            paths.sort();

            if paths.is_empty() {
                println!("No pairings found at {}", pairing_dir.display());
                return Ok(());
            }

            for path in paths {
                let store = FilePairingStore::load(&path)?;
                println!(
                    "{}: {} stored client key(s)",
                    path.display(),
                    store.key_count()?
                );
            }
        }
        PairingsCommand::Reset { zone } => {
            let path = pairing_dir.join(format!("{zone}.json"));
            if path.exists() {
                fs::remove_file(&path)?;
                println!("Removed {}", path.display());
            } else {
                println!("No pairing store found for zone {zone}");
            }
        }
    }

    Ok(())
}

fn calibrate(zones: &[String], config: &Config) -> anyhow::Result<()> {
    if zones.is_empty() {
        anyhow::bail!("--zones must contain at least one room or zone id");
    }

    let offsets = zones
        .iter()
        .map(|zone| {
            let offset = config
                .sync
                .zone_offsets_ms
                .get(zone)
                .copied()
                .unwrap_or(config.sync.default_offset_ms);
            (ZoneId::new(zone.clone()), offset)
        })
        .collect();
    let delays = delays_from_offsets(&offsets);

    println!("Current delay plan from configured offsets:");
    for delay in delays {
        println!("{}: delay {} ms", delay.zone_id, delay.delay_ms);
    }
    println!(
        "Play the calibration click track through an AirPlay group, then raise offsets for rooms that arrive late."
    );

    Ok(())
}

async fn serve(config: Config) -> anyhow::Result<()> {
    fs::create_dir_all(config.server.state_dir.join("endpoints"))?;
    fs::create_dir_all(config.server.state_dir.join("pairings"))?;

    let discovered = discover_sonos_zones_from_sources(
        Duration::from_secs(5),
        &config.sonos.static_ips,
        config.sonos.auto_discover,
    )
    .await?;
    let zones = filter_zones(&discovered, &config.sonos);
    if zones.is_empty() {
        anyhow::bail!("no visible Sonos rooms matched the current configuration");
    }

    info!("starting AirSonos2 for {} Sonos zone(s)", zones.len());
    print_zones(&zones);

    let registry = StreamRegistry::new();
    let http_addr = SocketAddr::new(config.server.bind, config.server.http_port);
    let http_task = tokio::spawn(serve_stream_http(
        http_addr,
        registry.clone(),
        config.stream.ffmpeg_path.clone(),
    ));
    info!("stream HTTP server listening on {}", http_addr);

    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let (cleanup_tx, cleanup_rx) = mpsc::unbounded_channel();
    let endpoints = build_endpoints(&zones, &config)?;
    persist_endpoint_identities(&config.server.state_dir, &endpoints)?;
    let clients = build_sonos_clients(&zones)?;
    let volume_states = load_zone_volume_states(&clients).await;
    let mut runners =
        start_airplay_endpoints(&endpoints, &config, events_tx, &volume_states).await?;
    let mut runtime =
        BridgeRuntime::new(config.clone(), registry, clients, volume_states, cleanup_tx);

    tokio::select! {
        result = runtime.run(events_rx, cleanup_rx) => result?,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            info!("shutdown signal received");
        }
        http_result = http_task => {
            let result = http_result?;
            result?;
            warn!("stream HTTP server exited");
        }
    }

    runtime.shutdown().await;
    for runner in &mut runners {
        runner.stop().await;
    }

    Ok(())
}

fn print_zones(zones: &[SonosZone]) {
    println!(
        "{:<24} {:<15} {:<28} {:<8} Model",
        "Room", "IP", "RINCON", "Coord"
    );
    for zone in zones {
        println!(
            "{:<24} {:<15} {:<28} {:<8} {}",
            zone.room_name, zone.ip, zone.rincon_id, zone.is_group_coordinator, zone.model
        );
    }
}

fn build_endpoints(
    zones: &[SonosZone],
    config: &Config,
) -> anyhow::Result<Vec<VirtualAirPlayEndpoint>> {
    zones
        .iter()
        .enumerate()
        .map(|(index, zone)| {
            virtual_endpoint_for_zone(
                zone,
                index,
                &config.airplay,
                config.server.state_dir.clone(),
            )
            .map_err(Into::into)
        })
        .collect()
}

fn persist_endpoint_identities(
    state_dir: &Path,
    endpoints: &[VirtualAirPlayEndpoint],
) -> anyhow::Result<()> {
    let endpoint_dir = state_dir.join("endpoints");
    fs::create_dir_all(&endpoint_dir)?;

    for endpoint in endpoints {
        let path = endpoint_dir.join(format!("{}.json", endpoint.zone_id));
        fs::write(path, serde_json::to_vec_pretty(endpoint)?)?;
    }

    Ok(())
}

async fn start_airplay_endpoints(
    endpoints: &[VirtualAirPlayEndpoint],
    config: &Config,
    events_tx: mpsc::UnboundedSender<AirPlayEvent>,
    volume_states: &HashMap<ZoneId, ZoneVolumeState>,
) -> anyhow::Result<Vec<AirPlayEndpointRunner>> {
    let mut runners = Vec::new();

    for endpoint in endpoints {
        let volume_state = volume_states
            .get(&endpoint.zone_id)
            .cloned()
            .unwrap_or_else(|| ZoneVolumeState::new(0.0));
        let mut runner = AirPlayEndpointRunner::build(
            endpoint,
            &config.airplay,
            config.server.bind,
            events_tx.clone(),
            volume_state,
        )?;
        runner.start().await?;
        info!(
            zone = %endpoint.zone_id,
            name = %endpoint.display_name,
            port = endpoint.rtsp_port,
            volume_db = runner.volume_state().volume_db(),
            "AirPlay endpoint started"
        );
        runners.push(runner);
    }

    Ok(runners)
}

async fn load_zone_volume_states(
    clients: &HashMap<ZoneId, (SonosZone, SonosClient)>,
) -> HashMap<ZoneId, ZoneVolumeState> {
    let mut volume_states = HashMap::new();

    for (zone_id, (zone, client)) in clients {
        match client.get_volume().await {
            Ok(sonos_volume) => {
                let volume_db = sonos_volume_to_airplay_db(sonos_volume);
                info!(
                    %zone_id,
                    room = %zone.room_name,
                    sonos_volume,
                    volume_db,
                    "loaded Sonos volume for AirPlay reporting"
                );
                volume_states.insert(zone_id.clone(), ZoneVolumeState::new(volume_db));
            }
            Err(error) => {
                warn!(
                    %zone_id,
                    room = %zone.room_name,
                    "failed to read Sonos volume, defaulting AirPlay volume to max: {error:#}"
                );
                volume_states.insert(zone_id.clone(), ZoneVolumeState::new(0.0));
            }
        }
    }

    volume_states
}

async fn refresh_zone_volume_state(
    zone_id: &ZoneId,
    client: &SonosClient,
    volume_state: &ZoneVolumeState,
) {
    match client.get_volume().await {
        Ok(sonos_volume) => {
            let volume_db = sonos_volume_to_airplay_db(sonos_volume);
            volume_state.set_volume_db(volume_db);
            info!(
                %zone_id,
                sonos_volume,
                volume_db,
                "refreshed Sonos volume for AirPlay reporting"
            );
        }
        Err(error) => {
            warn!(
                %zone_id,
                "failed to refresh Sonos volume for AirPlay reporting: {error:#}"
            );
        }
    }
}

fn build_sonos_clients(
    zones: &[SonosZone],
) -> anyhow::Result<HashMap<ZoneId, (SonosZone, SonosClient)>> {
    zones
        .iter()
        .map(|zone| {
            let client = SonosClient::new(zone.ip)?;
            Ok((zone.id.clone(), (zone.clone(), client)))
        })
        .collect()
}

struct BridgeRuntime {
    config: Config,
    registry: StreamRegistry,
    sonos: HashMap<ZoneId, (SonosZone, SonosClient)>,
    volume_states: HashMap<ZoneId, ZoneVolumeState>,
    encoders: HashMap<SessionId, FfmpegEncoder>,
    sessions: HashMap<SessionId, ZoneId>,
    session_formats: HashMap<SessionId, PcmFormat>,
    downstream_generations: HashMap<SessionId, u64>,
    downstream_lifecycles: HashMap<SessionId, DownstreamLifecycle>,
    downstream_reset_needed: HashSet<SessionId>,
    desired_playback: HashMap<SessionId, bool>,
    /// Last known Sonos playback state per session (`true` = playing).
    playback_state: HashMap<SessionId, bool>,
    playback_tasks: HashMap<SessionId, JoinHandle<()>>,
    paused_cleanup_tasks: HashMap<SessionId, JoinHandle<()>>,
    downstream_retry_tasks: HashMap<SessionId, JoinHandle<()>>,
    downstream_retry_attempts: HashMap<SessionId, u32>,
    cleanup_tx: mpsc::UnboundedSender<SessionId>,
    downstream_result_tx: mpsc::UnboundedSender<DownstreamStartResult>,
    downstream_result_rx: mpsc::UnboundedReceiver<DownstreamStartResult>,
    downstream_retry_tx: mpsc::UnboundedSender<DownstreamRetry>,
    downstream_retry_rx: mpsc::UnboundedReceiver<DownstreamRetry>,
    prepared_tx: mpsc::UnboundedSender<PreparedDownstream>,
    prepared_rx: mpsc::UnboundedReceiver<PreparedDownstream>,
    cohort_wake_tx: mpsc::UnboundedSender<()>,
    cohort_wake_rx: mpsc::UnboundedReceiver<()>,
    sync_cohorts: VecDeque<SyncCohort>,
    next_cohort_id: u64,
    startup_estimator: StartupDelayEstimator,
}

struct SonosStreamPrepare {
    session_id: SessionId,
    zone_id: ZoneId,
    generation: u64,
    cohort_id: u64,
    lifecycle: DownstreamLifecycle,
    zone: SonosZone,
    client: SonosClient,
    live_stream: LiveStream,
    local_url: Url,
    force_standalone_on_start: bool,
    prepared_tx: mpsc::UnboundedSender<PreparedDownstream>,
    result_tx: mpsc::UnboundedSender<DownstreamStartResult>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DownstreamStartOutcome {
    Started,
    Failed,
    Cancelled,
}

type DownstreamLifecycle = Arc<CancellationToken>;

#[derive(Clone, Debug)]
struct DownstreamStartResult {
    session_id: SessionId,
    zone_id: ZoneId,
    generation: u64,
    outcome: DownstreamStartOutcome,
    timing: Option<ZoneStartupTiming>,
}

#[derive(Debug)]
struct SyncCohort {
    id: u64,
    opened_at: Instant,
    window_deadline: Instant,
    start_deadline: Instant,
    sessions: Vec<SessionId>,
    prepared: HashMap<SessionId, PreparedDownstream>,
}

#[derive(Clone, Debug)]
struct PreparedDownstream {
    session_id: SessionId,
    zone_id: ZoneId,
    generation: u64,
    cohort_id: u64,
    lifecycle: DownstreamLifecycle,
    zone_room_name: String,
    client: SonosClient,
    live_stream: LiveStream,
    timing: ZoneStartupTiming,
}

#[derive(Clone, Debug)]
struct DownstreamRetry {
    session_id: SessionId,
    zone_id: ZoneId,
    generation: u64,
}

impl BridgeRuntime {
    fn new(
        config: Config,
        registry: StreamRegistry,
        sonos: HashMap<ZoneId, (SonosZone, SonosClient)>,
        volume_states: HashMap<ZoneId, ZoneVolumeState>,
        cleanup_tx: mpsc::UnboundedSender<SessionId>,
    ) -> Self {
        let (downstream_result_tx, downstream_result_rx) = mpsc::unbounded_channel();
        let (downstream_retry_tx, downstream_retry_rx) = mpsc::unbounded_channel();
        let (prepared_tx, prepared_rx) = mpsc::unbounded_channel();
        let (cohort_wake_tx, cohort_wake_rx) = mpsc::unbounded_channel();
        let startup_estimator = StartupDelayEstimator::new(
            config.sync.startup_sample_limit,
            config.sync.startup_min_samples,
        );
        Self {
            config,
            registry,
            sonos,
            volume_states,
            encoders: HashMap::new(),
            sessions: HashMap::new(),
            session_formats: HashMap::new(),
            downstream_generations: HashMap::new(),
            downstream_lifecycles: HashMap::new(),
            downstream_reset_needed: HashSet::new(),
            desired_playback: HashMap::new(),
            playback_state: HashMap::new(),
            playback_tasks: HashMap::new(),
            paused_cleanup_tasks: HashMap::new(),
            downstream_retry_tasks: HashMap::new(),
            downstream_retry_attempts: HashMap::new(),
            cleanup_tx,
            downstream_result_tx,
            downstream_result_rx,
            downstream_retry_tx,
            downstream_retry_rx,
            prepared_tx,
            prepared_rx,
            cohort_wake_tx,
            cohort_wake_rx,
            sync_cohorts: VecDeque::new(),
            next_cohort_id: 0,
            startup_estimator,
        }
    }

    async fn run(
        &mut self,
        mut events_rx: mpsc::UnboundedReceiver<AirPlayEvent>,
        mut cleanup_rx: mpsc::UnboundedReceiver<SessionId>,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                event = events_rx.recv() => {
                    let Some(event) = event else { break };
                    if let Err(error) = self.handle_event(event).await {
                        warn!("bridge event failed: {error:#}");
                    }
                }
                session_id = cleanup_rx.recv() => {
                    let Some(session_id) = session_id else { continue };
                    if self.sessions.contains_key(&session_id)
                        && self.desired_playback.get(&session_id) == Some(&false)
                    {
                        info!(
                            %session_id,
                            grace_secs = PAUSED_SESSION_GRACE_SECS,
                            "paused session grace period expired; stopping bridge session"
                        );
                        if let Err(error) = self.stop_session(session_id, None).await {
                            warn!(%session_id, "failed to stop expired paused session: {error:#}");
                        }
                    }
                }
                result = self.downstream_result_rx.recv() => {
                    let Some(result) = result else { continue };
                    self.handle_downstream_start_result(result);
                }
                retry = self.downstream_retry_rx.recv() => {
                    let Some(retry) = retry else { continue };
                    if let Err(error) = self.handle_downstream_retry(retry).await {
                        warn!("downstream retry failed: {error:#}");
                    }
                }
                prepared = self.prepared_rx.recv() => {
                    let Some(prepared) = prepared else { continue };
                    self.handle_prepared_downstream(prepared).await;
                }
                wake = self.cohort_wake_rx.recv() => {
                    if wake.is_some() {
                        self.maybe_start_sync_cohorts();
                    }
                }
            }
        }

        Ok(())
    }

    fn cancel_paused_cleanup(&mut self, session_id: SessionId) {
        if let Some(task) = self.paused_cleanup_tasks.remove(&session_id) {
            task.abort();
        }
    }

    fn clear_session_runtime_state(&mut self, session_id: SessionId) {
        self.playback_state.remove(&session_id);
        self.session_formats.remove(&session_id);
        // Keep the generation tombstone: a late result must not match a later
        // reuse of this AirPlay session id.
        self.invalidate_prepared_downstream(session_id);
        self.downstream_lifecycles.remove(&session_id);
        self.downstream_reset_needed.remove(&session_id);
        self.desired_playback.remove(&session_id);
        self.downstream_retry_attempts.remove(&session_id);
        self.cancel_paused_cleanup(session_id);
        self.cancel_downstream_retry(session_id);
        if let Some(task) = self.playback_tasks.remove(&session_id) {
            task.abort();
        }
    }

    fn schedule_paused_cleanup(&mut self, session_id: SessionId) {
        self.cancel_paused_cleanup(session_id);
        let tx = self.cleanup_tx.clone();
        let task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(PAUSED_SESSION_GRACE_SECS)).await;
            let _ = tx.send(session_id);
        });
        self.paused_cleanup_tasks.insert(session_id, task);
    }

    fn cancel_downstream_retry(&mut self, session_id: SessionId) {
        if let Some(task) = self.downstream_retry_tasks.remove(&session_id) {
            task.abort();
        }
    }

    fn schedule_downstream_retry(
        &mut self,
        session_id: SessionId,
        zone_id: ZoneId,
        generation: u64,
    ) {
        self.cancel_downstream_retry(session_id);
        let attempt = self.downstream_retry_attempt(session_id);
        let delay = downstream_retry_delay(attempt);
        let tx = self.downstream_retry_tx.clone();
        let task = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = tx.send(DownstreamRetry {
                session_id,
                zone_id,
                generation,
            });
        });
        self.downstream_retry_tasks.insert(session_id, task);
    }

    fn downstream_retry_attempt(&mut self, session_id: SessionId) -> u32 {
        let attempts = self
            .downstream_retry_attempts
            .entry(session_id)
            .or_insert(0);
        let current = *attempts;
        *attempts = attempts.saturating_add(1);
        current
    }

    fn handle_downstream_start_result(&mut self, result: DownstreamStartResult) {
        if self.downstream_generations.get(&result.session_id) != Some(&result.generation) {
            debug!(
                session_id = %result.session_id,
                zone_id = %result.zone_id,
                generation = result.generation,
                "ignoring stale downstream start result"
            );
            return;
        }

        if let Some(timing) = &result.timing {
            self.startup_estimator
                .record(result.zone_id.clone(), timing);
        }

        match result.outcome {
            DownstreamStartOutcome::Started => {
                if self.desired_playback.get(&result.session_id) == Some(&true) {
                    self.downstream_reset_needed.remove(&result.session_id);
                    self.downstream_retry_attempts.remove(&result.session_id);
                    self.cancel_downstream_retry(result.session_id);
                }
            }
            DownstreamStartOutcome::Failed => {
                self.downstream_reset_needed.insert(result.session_id);
                if self.desired_playback.get(&result.session_id) == Some(&true) {
                    self.schedule_downstream_retry(
                        result.session_id,
                        result.zone_id,
                        result.generation,
                    );
                }
            }
            DownstreamStartOutcome::Cancelled => {}
        }
    }

    async fn handle_downstream_retry(&mut self, retry: DownstreamRetry) -> anyhow::Result<()> {
        self.downstream_retry_tasks.remove(&retry.session_id);
        if self.downstream_generations.get(&retry.session_id) != Some(&retry.generation) {
            debug!(
                session_id = %retry.session_id,
                zone_id = %retry.zone_id,
                generation = retry.generation,
                "ignoring stale downstream retry"
            );
            return Ok(());
        }
        if self.desired_playback.get(&retry.session_id) == Some(&true)
            && self.downstream_reset_needed.contains(&retry.session_id)
        {
            self.restart_downstream_for_play(retry.session_id, retry.zone_id)
                .await?;
        }
        Ok(())
    }

    async fn handle_event(&mut self, event: AirPlayEvent) -> anyhow::Result<()> {
        match event {
            AirPlayEvent::SessionStarted {
                session_id,
                zone_id,
                format,
            } => self.start_session(session_id, zone_id, format).await,
            AirPlayEvent::Pcm {
                session_id, frame, ..
            } => {
                if let Some(encoder) = self.encoders.get(&session_id) {
                    encoder.write_frame(frame).await?;
                }
                Ok(())
            }
            AirPlayEvent::PlaybackState {
                session_id,
                zone_id,
                playing,
            } => self.set_playback_state(session_id, zone_id, playing).await,
            AirPlayEvent::Flushed { session_id, .. } => {
                debug!(%session_id, "AirPlay buffer flushed; keeping bridge session alive");
                if let Some(stream) = self.registry.get(&session_id).await {
                    stream.arm_playback_anchor_on_next_timed_pcm();
                }
                if self.desired_playback.get(&session_id) == Some(&false)
                    || self.playback_state.get(&session_id) == Some(&false)
                {
                    self.downstream_reset_needed.insert(session_id);
                }
                Ok(())
            }
            AirPlayEvent::StreamEndedWhilePaused {
                session_id,
                zone_id,
            } => {
                if self.sessions.contains_key(&session_id)
                    && self.desired_playback.get(&session_id) != Some(&true)
                {
                    debug!(
                        %session_id,
                        %zone_id,
                        "AirPlay audio stream ended while paused; deferring bridge session cleanup"
                    );
                    self.downstream_reset_needed.insert(session_id);
                    self.schedule_paused_cleanup(session_id);
                } else {
                    debug!(
                        %session_id,
                        %zone_id,
                        "ignoring late AirPlay stream-ended event after playback resumed"
                    );
                }
                Ok(())
            }
            AirPlayEvent::Volume {
                zone_id,
                sonos_volume,
                ..
            } => {
                if let Some((_, client)) = self.sonos.get(&zone_id) {
                    client.set_volume(sonos_volume).await?;
                }
                Ok(())
            }
            AirPlayEvent::SessionStopped {
                session_id,
                zone_id,
            } => self.stop_session(session_id, Some(zone_id)).await,
            AirPlayEvent::ClientConnected { zone_id, addr } => {
                info!(%zone_id, %addr, "AirPlay client connected");
                if let (Some((_, client)), Some(volume_state)) =
                    (self.sonos.get(&zone_id), self.volume_states.get(&zone_id))
                {
                    let client = client.clone();
                    let volume_state = volume_state.clone();
                    let zone_id = zone_id.clone();
                    tokio::spawn(async move {
                        refresh_zone_volume_state(&zone_id, &client, &volume_state).await;
                    });
                }
                Ok(())
            }
            AirPlayEvent::ClientDisconnected { zone_id, addr } => {
                // This callback identifies only the receiver zone, not the audio
                // session. A stale control connection can disconnect after a new
                // session has already started on the same zone, so exact session
                // teardown is handled by SessionStopped instead.
                info!(%zone_id, %addr, "AirPlay client disconnected");
                Ok(())
            }
            AirPlayEvent::Error { zone_id, message } => {
                warn!(%zone_id, %message, "AirPlay receiver error");
                Ok(())
            }
        }
    }

    async fn create_downstream_stream(
        &self,
        session_id: SessionId,
        zone_id: ZoneId,
        zone_ip: IpAddr,
        format: PcmFormat,
        generation: u64,
    ) -> anyhow::Result<(LiveStream, FfmpegEncoder, Url)> {
        let stream_codec = stream_codec(&self.config.stream.codec)?;
        let local_url =
            stream_url_for_zone(&self.config, zone_ip, session_id, stream_codec, generation)
                .await?;
        let stream_session = StreamSession {
            session_id,
            zone_id,
            codec: stream_codec,
            generation,
            local_url: local_url.clone(),
            encoder_state: EncoderState::Starting,
        };
        let live_stream = self.registry.create(stream_session).await;
        let encoder = FfmpegEncoder::spawn(
            FfmpegEncoderConfig {
                ffmpeg_path: self.config.stream.ffmpeg_path.clone(),
                sample_rate: format.sample_rate,
                channels: format.channels,
                mp3_bitrate_kbps: self.config.stream.mp3_bitrate_kbps,
                codec: stream_codec,
            },
            live_stream.clone(),
        )?;

        Ok((live_stream, encoder, local_url))
    }

    fn next_downstream_generation(&mut self, session_id: SessionId) -> (u64, DownstreamLifecycle) {
        self.cancel_downstream_lifecycle(session_id);
        self.remove_session_from_sync_cohort(session_id);
        let generation = self.downstream_generations.entry(session_id).or_insert(0);
        *generation = generation.saturating_add(1);
        let lifecycle = Arc::new(CancellationToken::new());
        self.downstream_lifecycles
            .insert(session_id, lifecycle.clone());
        (*generation, lifecycle)
    }

    fn schedule_cohort_wake(&self, delay: Duration) {
        let tx = self.cohort_wake_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = tx.send(());
        });
    }

    fn remove_session_from_sync_cohort(&mut self, session_id: SessionId) {
        let mut removed = false;
        for cohort in &mut self.sync_cohorts {
            let old_len = cohort.sessions.len();
            cohort.sessions.retain(|id| *id != session_id);
            removed |= cohort.sessions.len() != old_len;
            cohort.prepared.remove(&session_id);
        }
        self.sync_cohorts
            .retain(|cohort| !cohort.sessions.is_empty());
        if removed {
            let _ = self.cohort_wake_tx.send(());
        }
    }

    fn cancel_downstream_lifecycle(&self, session_id: SessionId) {
        if let Some(lifecycle) = self.downstream_lifecycles.get(&session_id) {
            lifecycle.cancel();
        }
    }

    fn invalidate_prepared_downstream(&mut self, session_id: SessionId) {
        self.cancel_downstream_lifecycle(session_id);
        self.remove_session_from_sync_cohort(session_id);
        if let Some(generation) = self.downstream_generations.get_mut(&session_id) {
            *generation = generation.saturating_add(1);
        }
    }

    fn prepared_downstream_is_current(&self, prepared: &PreparedDownstream) -> bool {
        self.downstream_generations.get(&prepared.session_id) == Some(&prepared.generation)
            && self.desired_playback.get(&prepared.session_id) == Some(&true)
            && self
                .downstream_lifecycles
                .get(&prepared.session_id)
                .is_some_and(|lifecycle| Arc::ptr_eq(lifecycle, &prepared.lifecycle))
            && !prepared.lifecycle.is_cancelled()
    }

    fn add_session_to_sync_cohort(&mut self, session_id: SessionId) -> u64 {
        let now = Instant::now();
        let multi_select_window = Duration::from_millis(self.config.sync.multi_select_window_ms);
        let start_deadline = Duration::from_millis(self.config.sync.start_deadline_ms);
        let should_open = self
            .sync_cohorts
            .back()
            .is_none_or(|cohort| now >= cohort.window_deadline);

        if should_open {
            self.next_cohort_id = self.next_cohort_id.saturating_add(1);
            self.sync_cohorts.push_back(SyncCohort {
                id: self.next_cohort_id,
                opened_at: now,
                window_deadline: now + multi_select_window,
                start_deadline: now + start_deadline,
                sessions: Vec::new(),
                prepared: HashMap::new(),
            });
            self.schedule_cohort_wake(multi_select_window);
            self.schedule_cohort_wake(start_deadline);
        }

        let cohort = self.sync_cohorts.back_mut().expect("cohort was opened");
        if !cohort.sessions.contains(&session_id) {
            cohort.sessions.push(session_id);
        }
        cohort.id
    }

    async fn handle_prepared_downstream(&mut self, prepared: PreparedDownstream) {
        if !self.prepared_downstream_is_current(&prepared) {
            debug!(
                session_id = %prepared.session_id,
                zone_id = %prepared.zone_id,
                generation = prepared.generation,
                "ignoring stale prepared downstream"
            );
            return;
        }
        self.startup_estimator
            .record(prepared.zone_id.clone(), &prepared.timing);

        if let Some(cohort) = self.sync_cohorts.iter_mut().find(|cohort| {
            cohort.id == prepared.cohort_id && cohort.sessions.contains(&prepared.session_id)
        }) {
            cohort.prepared.insert(prepared.session_id, prepared);
            self.maybe_start_sync_cohorts();
        } else {
            debug!("ignoring prepared downstream from replaced cohort");
        }
    }

    fn retry_unprepared_downstream(&mut self, session_id: SessionId) {
        self.cancel_downstream_lifecycle(session_id);
        if let Some(task) = self.playback_tasks.remove(&session_id) {
            task.abort();
        }
        let generation = self.downstream_generations.entry(session_id).or_insert(0);
        *generation = generation.saturating_add(1);
        let generation = *generation;
        self.downstream_reset_needed.insert(session_id);
        if self.desired_playback.get(&session_id) == Some(&true)
            && let Some(zone_id) = self.sessions.get(&session_id).cloned()
        {
            self.schedule_downstream_retry(session_id, zone_id, generation);
        }
    }

    fn maybe_start_sync_cohorts(&mut self) {
        while let Some(cohort) = self.sync_cohorts.front() {
            let now = Instant::now();
            if !sync_cohort_should_start(cohort, now) {
                return;
            }
            let all_prepared = cohort.sessions.len() == cohort.prepared.len();
            let deadline_expired = now >= cohort.start_deadline;
            let cohort = self.sync_cohorts.pop_front().expect("cohort exists");
            let mut prepared = Vec::new();
            let mut unprepared = Vec::new();
            for session_id in cohort.sessions {
                if let Some(stream) = cohort.prepared.get(&session_id) {
                    prepared.push(stream.clone());
                } else {
                    unprepared.push(session_id);
                }
            }
            for session_id in unprepared {
                self.retry_unprepared_downstream(session_id);
            }
            if prepared.is_empty() {
                continue;
            }
            let age_ms = cohort.opened_at.elapsed().as_millis();
            info!(
                sessions = prepared.len(),
                age_ms, all_prepared, deadline_expired, "starting AirPlay multi-select sync cohort"
            );
            self.play_prepared_downstreams(prepared);
        }
    }

    fn apply_sync_anchors(&self, prepared: &[PreparedDownstream]) {
        let zone_ids: Vec<ZoneId> = prepared
            .iter()
            .map(|stream| stream.zone_id.clone())
            .collect();
        let base_anchor = Instant::now() + Duration::from_millis(120);
        for stream in prepared {
            let auto_delay_ms = if self.config.sync.startup_compensation {
                self.startup_estimator.automatic_delay_ms(
                    &zone_ids,
                    &stream.zone_id,
                    self.config.sync.startup_max_compensation_ms,
                )
            } else {
                0
            };
            let manual_offset = self
                .config
                .sync
                .zone_offsets_ms
                .get(&stream.zone_room_name)
                .copied()
                .unwrap_or(0);
            let delay = combine_sync_delay(
                auto_delay_ms,
                manual_offset,
                self.config.sync.default_offset_ms,
            );
            if stream.live_stream.session.codec == StreamCodec::Wav {
                stream.live_stream.set_playback_anchor(base_anchor + delay);
            } else if prepared.len() > 1 {
                info!(
                    session_id = %stream.session_id,
                    zone_id = %stream.zone_id,
                    codec = ?stream.live_stream.session.codec,
                    "coordinated Play is best-effort without WAV playback anchors"
                );
            }
        }
    }

    fn play_prepared_downstreams(&self, prepared: Vec<PreparedDownstream>) {
        self.apply_sync_anchors(&prepared);

        let play_started_at = Instant::now();
        let result_tx = self.downstream_result_tx.clone();
        let mut tasks = Vec::with_capacity(prepared.len());
        for stream in prepared {
            tasks.push(tokio::spawn(async move {
                info!(session_id = %stream.session_id, zone_id = %stream.zone_id, "starting Sonos playback");
                let play_start = Instant::now();
                let mut startup_timing = stream.timing.clone();
                let play_result = tokio::select! {
                    biased;
                    _ = stream.lifecycle.cancelled() => None,
                    result = stream.client.play() => Some(result),
                };
                let outcome = match play_result {
                    None => DownstreamStartOutcome::Cancelled,
                    Some(Ok(())) => {
                        startup_timing.play_ms = Some(play_start.elapsed().as_millis() as u64);
                        let timing = stream.live_stream.timing();
                        debug!(
                            session_id = %stream.session_id,
                            zone_id = %stream.zone_id,
                            elapsed_ms = play_start.elapsed().as_millis(),
                            skipped_bytes = timing.bytes_skipped,
                            "Sonos play request completed"
                        );
                        DownstreamStartOutcome::Started
                    }
                    Some(Err(error)) => {
                        if error.is_timeout() {
                            startup_timing.play_ms = Some(play_start.elapsed().as_millis() as u64);
                            warn!(
                                session_id = %stream.session_id,
                                zone_id = %stream.zone_id,
                                "Sonos play request timed out; keeping stream alive because playback state is ambiguous: {error:#}"
                            );
                            DownstreamStartOutcome::Started
                        } else {
                            warn!(session_id = %stream.session_id, zone_id = %stream.zone_id, "Sonos play request failed: {error:#}");
                            DownstreamStartOutcome::Failed
                        }
                    }
                };
                (stream, outcome, startup_timing)
            }));
        }
        let warn_ms = self.config.sync.play_command_spread_warn_ms;
        tokio::spawn(async move {
            let mut completed = Vec::with_capacity(tasks.len());
            for task in tasks {
                match task.await {
                    Ok(result) => completed.push(result),
                    Err(error) => warn!("Sonos play task failed to join: {error}"),
                }
            }
            for (stream, outcome, timing) in completed {
                if outcome == DownstreamStartOutcome::Started && !stream.lifecycle.is_cancelled() {
                    stream.live_stream.open_delivery();
                }
                let _ = result_tx.send(DownstreamStartResult {
                    session_id: stream.session_id,
                    zone_id: stream.zone_id,
                    generation: stream.generation,
                    outcome,
                    timing: Some(timing),
                });
            }
            let spread_ms = play_started_at.elapsed().as_millis() as u64;
            if spread_ms > warn_ms {
                warn!(
                    spread_ms,
                    warn_ms, "coordinated Sonos Play commands completed slowly"
                );
            }
        });
    }

    fn start_sonos_prepare_task(&self, start: SonosStreamPrepare) -> JoinHandle<()> {
        let subscriber_wait = Duration::from_millis(self.config.stream.startup_wait_ms());
        let prebuffer_ms = self.config.stream.prebuffer_ms;
        let SonosStreamPrepare {
            session_id,
            zone_id,
            generation,
            cohort_id,
            lifecycle,
            zone,
            client,
            live_stream,
            local_url,
            force_standalone_on_start,
            prepared_tx,
            result_tx,
        } = start;
        tokio::spawn(async move {
            if force_standalone_on_start {
                info!(%session_id, %zone_id, "setting Sonos zone standalone");
                let started_at = Instant::now();
                let standalone_result = tokio::select! {
                    biased;
                    _ = lifecycle.cancelled() => return,
                    result = client.become_coordinator_of_standalone_group() => result,
                };
                if let Err(error) = standalone_result {
                    warn!(%session_id, %zone_id, "Sonos standalone request failed: {error:#}");
                    if !lifecycle.is_cancelled() {
                        let _ = result_tx.send(DownstreamStartResult {
                            session_id,
                            zone_id: zone_id.clone(),
                            generation,
                            outcome: DownstreamStartOutcome::Failed,
                            timing: None,
                        });
                    }
                    return;
                }
                debug!(
                    %session_id,
                    %zone_id,
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "Sonos standalone request completed"
                );
            } else {
                debug!(
                    %session_id,
                    %zone_id,
                    "Sonos zone already appears standalone; skipping standalone request"
                );
            }

            info!(%session_id, %zone_id, %local_url, "setting Sonos stream URI");
            let prepare_started_at = Instant::now();
            let uri_started_at = Instant::now();
            let title = format!("{} AirSonos2", zone.room_name);
            let set_uri_result = tokio::select! {
                biased;
                _ = lifecycle.cancelled() => return,
                result = client.set_av_transport_uri(local_url.as_str(), &title) => result,
            };
            if let Err(error) = set_uri_result {
                warn!(%session_id, %zone_id, "Sonos stream URI request failed: {error:#}");
                if !lifecycle.is_cancelled() {
                    let _ = result_tx.send(DownstreamStartResult {
                        session_id,
                        zone_id: zone_id.clone(),
                        generation,
                        outcome: DownstreamStartOutcome::Failed,
                        timing: None,
                    });
                }
                return;
            }
            debug!(
                %session_id,
                %zone_id,
                elapsed_ms = uri_started_at.elapsed().as_millis(),
                "Sonos stream URI request completed"
            );

            let subscriber_ready = tokio::select! {
                biased;
                _ = lifecycle.cancelled() => return,
                ready = live_stream.wait_for_subscriber(subscriber_wait) => ready,
            };
            if live_stream.session.codec == StreamCodec::Wav {
                let prebuffer = Duration::from_millis(prebuffer_ms);
                let ready = tokio::select! {
                    biased;
                    _ = lifecycle.cancelled() => return,
                    ready = live_stream.wait_until_ready(prebuffer) => ready,
                };
                if !ready {
                    warn!(
                        %session_id,
                        %zone_id,
                        prebuffer_ms = prebuffer.as_millis(),
                        "WAV stream not ready before prebuffer timeout; starting playback anyway"
                    );
                }
            }
            let timing = live_stream.timing();
            if subscriber_ready {
                let connect_lag_ms = timing.subscriber_connected_at.and_then(|connected| {
                    timing
                        .first_encoded_at
                        .map(|first| connected.saturating_duration_since(first).as_millis())
                });
                info!(
                    %session_id,
                    %zone_id,
                    encoded_bytes = timing.encoded_bytes,
                    bytes_dropped = timing.bytes_dropped,
                    chunks_dropped = timing.chunks_dropped,
                    connect_lag_ms,
                    "Sonos HTTP subscriber connected; starting playback at live edge"
                );
            } else {
                warn!(
                    %session_id,
                    %zone_id,
                    subscriber_wait_ms = subscriber_wait.as_millis(),
                    encoded_bytes = timing.encoded_bytes,
                    bytes_dropped = timing.bytes_dropped,
                    "Sonos HTTP subscriber not connected before timeout; starting playback anyway"
                );
            }

            let stream_timing = live_stream.timing();
            let timing = ZoneStartupTiming {
                set_uri_ms: Some(uri_started_at.elapsed().as_millis() as u64),
                subscriber_connect_ms: stream_timing
                    .subscriber_connected_at
                    .map(|at| at.saturating_duration_since(prepare_started_at).as_millis() as u64),
                first_bytes_ms: stream_timing
                    .first_served_at
                    .map(|at| at.saturating_duration_since(prepare_started_at).as_millis() as u64),
                play_ms: None,
            };
            if lifecycle.is_cancelled() {
                return;
            }
            if prepared_tx
                .send(PreparedDownstream {
                    session_id,
                    zone_id: zone_id.clone(),
                    generation,
                    cohort_id,
                    lifecycle: lifecycle.clone(),
                    zone_room_name: zone.room_name,
                    client,
                    live_stream,
                    timing,
                })
                .is_err()
            {
                let _ = result_tx.send(DownstreamStartResult {
                    session_id,
                    zone_id: zone_id.clone(),
                    generation,
                    outcome: DownstreamStartOutcome::Failed,
                    timing: None,
                });
            }
        })
    }

    async fn start_session(
        &mut self,
        session_id: SessionId,
        zone_id: ZoneId,
        format: PcmFormat,
    ) -> anyhow::Result<()> {
        let old_sessions: Vec<SessionId> = self
            .sessions
            .iter()
            .filter_map(|(sid, zid)| (zid == &zone_id && *sid != session_id).then_some(*sid))
            .collect();
        for old_id in old_sessions {
            info!(%old_id, %zone_id, "replacing prior bridge session for zone");
            self.stop_session(old_id, Some(zone_id.clone())).await?;
        }
        self.cancel_paused_cleanup(session_id);

        let (zone, client) = self
            .sonos
            .get(&zone_id)
            .ok_or_else(|| anyhow::anyhow!("no Sonos client for zone {zone_id}"))?
            .clone();
        self.clear_session_runtime_state(session_id);
        let (generation, lifecycle) = self.next_downstream_generation(session_id);
        let (live_stream, encoder, local_url) = self
            .create_downstream_stream(session_id, zone_id.clone(), zone.ip, format, generation)
            .await?;

        self.encoders.insert(session_id, encoder);
        self.sessions.insert(session_id, zone_id.clone());
        self.session_formats.insert(session_id, format);
        self.desired_playback.insert(session_id, true);
        info!(%session_id, %zone_id, %local_url, "bridge session started");

        let force_standalone_on_start =
            self.config.sonos.force_standalone_on_start && !zone.is_group_coordinator;
        live_stream.hold_delivery();
        let cohort_id = self.add_session_to_sync_cohort(session_id);
        let task = self.start_sonos_prepare_task(SonosStreamPrepare {
            session_id,
            zone_id: zone_id.clone(),
            generation,
            cohort_id,
            lifecycle,
            zone,
            client,
            live_stream,
            local_url,
            force_standalone_on_start,
            prepared_tx: self.prepared_tx.clone(),
            result_tx: self.downstream_result_tx.clone(),
        });
        self.playback_tasks.insert(session_id, task);

        Ok(())
    }

    async fn restart_downstream_for_play(
        &mut self,
        session_id: SessionId,
        zone_id: ZoneId,
    ) -> anyhow::Result<()> {
        self.downstream_reset_needed.insert(session_id);
        self.cancel_downstream_retry(session_id);
        let Some(format) = self.session_formats.get(&session_id).copied() else {
            warn!(%session_id, %zone_id, "cannot restart downstream stream without session format");
            return Ok(());
        };
        let Some((zone, client)) = self.sonos.get(&zone_id).cloned() else {
            warn!(%session_id, %zone_id, "cannot restart downstream stream without Sonos client");
            return Ok(());
        };

        if let Some(task) = self.playback_tasks.remove(&session_id) {
            task.abort();
        }
        if let Some(encoder) = self.encoders.remove(&session_id)
            && let Err(error) = encoder.shutdown().await
        {
            warn!(%session_id, "encoder shutdown failed during downstream reset: {error}");
        }
        self.registry.remove(&session_id).await;

        let (generation, lifecycle) = self.next_downstream_generation(session_id);
        let (live_stream, encoder, local_url) = self
            .create_downstream_stream(session_id, zone_id.clone(), zone.ip, format, generation)
            .await?;
        self.encoders.insert(session_id, encoder);

        info!(
            %session_id,
            %zone_id,
            generation,
            %local_url,
            "recreated downstream stream for AirPlay playback"
        );

        live_stream.hold_delivery();
        let cohort_id = self.add_session_to_sync_cohort(session_id);
        let task = self.start_sonos_prepare_task(SonosStreamPrepare {
            session_id,
            zone_id: zone_id.clone(),
            generation,
            cohort_id,
            lifecycle,
            zone,
            client,
            live_stream,
            local_url,
            force_standalone_on_start: false,
            prepared_tx: self.prepared_tx.clone(),
            result_tx: self.downstream_result_tx.clone(),
        });
        self.playback_tasks.insert(session_id, task);

        Ok(())
    }

    async fn set_playback_state(
        &mut self,
        session_id: SessionId,
        zone_id: ZoneId,
        playing: bool,
    ) -> anyhow::Result<()> {
        if !self.sessions.contains_key(&session_id) {
            return Ok(());
        }

        self.desired_playback.insert(session_id, playing);
        let previous = self.playback_state.insert(session_id, playing);
        if playing {
            self.cancel_paused_cleanup(session_id);
            if should_restart_downstream_for_play(
                previous,
                self.downstream_reset_needed.contains(&session_id),
            ) {
                return self.restart_downstream_for_play(session_id, zone_id).await;
            }
            return Ok(());
        }

        self.downstream_reset_needed.insert(session_id);
        self.cancel_downstream_retry(session_id);
        self.invalidate_prepared_downstream(session_id);
        if let Some(task) = self.playback_tasks.remove(&session_id) {
            task.abort();
        }
        let Some((_, client)) = self.sonos.get(&zone_id).cloned() else {
            return Ok(());
        };
        let task = tokio::spawn(async move {
            info!(%session_id, %zone_id, "stopping Sonos playback for AirPlay pause");
            if let Err(error) = client.stop().await {
                warn!(%session_id, %zone_id, "Sonos stop request failed for AirPlay pause: {error:#}");
            }
        });
        self.playback_tasks.insert(session_id, task);

        Ok(())
    }

    async fn stop_session(
        &mut self,
        session_id: SessionId,
        fallback_zone_id: Option<ZoneId>,
    ) -> anyhow::Result<()> {
        let zone_id = self.sessions.remove(&session_id).or(fallback_zone_id);
        self.clear_session_runtime_state(session_id);
        if let Some(encoder) = self.encoders.remove(&session_id)
            && let Err(error) = encoder.shutdown().await
        {
            warn!(%session_id, "encoder shutdown failed: {error}");
        }
        self.registry.remove(&session_id).await;

        if self.config.sonos.stop_on_disconnect
            && let Some(zone_id) = zone_id
            && let Some((_, client)) = self.sonos.get(&zone_id)
        {
            client.stop().await?;
        }

        info!(%session_id, "bridge session stopped");
        Ok(())
    }

    async fn shutdown(&mut self) {
        let session_ids: Vec<_> = self.encoders.keys().copied().collect();
        for session_id in session_ids {
            if let Err(error) = self.stop_session(session_id, None).await {
                error!(%session_id, "failed to stop session during shutdown: {error}");
            }
        }
    }
}

async fn stream_url_for_zone(
    config: &Config,
    zone_ip: IpAddr,
    session_id: SessionId,
    codec: StreamCodec,
    generation: u64,
) -> anyhow::Result<Url> {
    let host = if config.server.bind.is_unspecified() {
        local_ip_for_remote(zone_ip).await.unwrap_or(zone_ip)
    } else {
        config.server.bind
    };
    let extension = match codec {
        StreamCodec::Mp3 => "mp3",
        StreamCodec::Aac => "aac",
        StreamCodec::Wav => "wav",
    };
    let mut url = Url::parse(&format!(
        "http://{}:{}/streams/{session_id}.{extension}",
        host, config.server.http_port
    ))?;
    url.query_pairs_mut()
        .append_pair("gen", &generation.to_string());

    Ok(url)
}

fn should_restart_downstream_for_play(previous_playing: Option<bool>, reset_needed: bool) -> bool {
    previous_playing == Some(false) || reset_needed
}

fn sync_cohort_should_start(cohort: &SyncCohort, now: Instant) -> bool {
    let window_closed = now >= cohort.window_deadline;
    let deadline_expired = now >= cohort.start_deadline;
    let all_prepared =
        !cohort.sessions.is_empty() && cohort.sessions.len() == cohort.prepared.len();
    // The admission window is what lets a later AirPlay selection join the same
    // coordinated start. Delivery stays gated while we wait, so every subscriber
    // still begins at the same live edge when the cohort starts.
    window_closed && (all_prepared || deadline_expired)
}

fn downstream_retry_delay(attempt: u32) -> Duration {
    let multiplier = 1_u64.checked_shl(attempt.min(8)).unwrap_or(u64::MAX);
    Duration::from_millis(
        DOWNSTREAM_RETRY_BASE_MS
            .saturating_mul(multiplier)
            .min(DOWNSTREAM_RETRY_MAX_MS),
    )
}

fn stream_codec(codec: &str) -> anyhow::Result<StreamCodec> {
    match codec {
        "mp3" => Ok(StreamCodec::Mp3),
        "wav" | "pcm" => Ok(StreamCodec::Wav),
        other => anyhow::bail!("unsupported stream codec {other:?}; expected mp3 or wav"),
    }
}

async fn local_ip_for_remote(remote: IpAddr) -> Option<IpAddr> {
    let bind = match remote {
        IpAddr::V4(_) => "0.0.0.0:0",
        IpAddr::V6(_) => "[::]:0",
    };
    let socket = UdpSocket::bind(bind).await.ok()?;
    socket.connect((remote, 1400)).await.ok()?;
    socket.local_addr().ok().map(|addr| addr.ip())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn runtime() -> BridgeRuntime {
        let (cleanup_tx, _cleanup_rx) = mpsc::unbounded_channel();
        BridgeRuntime::new(
            Config::default(),
            StreamRegistry::new(),
            HashMap::new(),
            HashMap::new(),
            cleanup_tx,
        )
    }

    fn format() -> PcmFormat {
        PcmFormat {
            sample_rate: 44_100,
            channels: 2,
            bits: 16,
        }
    }

    fn zone_id() -> ZoneId {
        ZoneId::new("RINCON_TEST")
    }

    fn live_stream_for(session_id: SessionId, zone_id: ZoneId, codec: StreamCodec) -> LiveStream {
        LiveStream::new(StreamSession {
            session_id,
            zone_id,
            codec,
            generation: 1,
            local_url: Url::parse("http://127.0.0.1:7000/streams/test.wav").expect("url"),
            encoder_state: EncoderState::Starting,
        })
    }

    fn prepared_downstream(
        session_id: SessionId,
        zone_id: ZoneId,
        codec: StreamCodec,
    ) -> PreparedDownstream {
        PreparedDownstream {
            session_id,
            zone_id: zone_id.clone(),
            generation: 1,
            cohort_id: 0,
            lifecycle: Arc::new(CancellationToken::new()),
            zone_room_name: "Kitchen".to_owned(),
            client: SonosClient::from_base_url(
                Url::parse("http://127.0.0.1:1400").expect("sonos url"),
            )
            .expect("client"),
            live_stream: live_stream_for(session_id, zone_id, codec),
            timing: ZoneStartupTiming {
                subscriber_connect_ms: Some(100),
                ..ZoneStartupTiming::default()
            },
        }
    }

    fn cohort_with_single_prepared_session(now: Instant) -> SyncCohort {
        let session_id = SessionId::new();
        let mut cohort = SyncCohort {
            id: 1,
            opened_at: now,
            window_deadline: now + Duration::from_millis(750),
            start_deadline: now + Duration::from_millis(2_500),
            sessions: vec![session_id],
            prepared: HashMap::new(),
        };
        cohort.prepared.insert(
            session_id,
            prepared_downstream(session_id, zone_id(), StreamCodec::Mp3),
        );
        cohort
    }

    #[test]
    fn single_prepared_session_waits_for_multi_select_window() {
        let now = Instant::now();
        let cohort = cohort_with_single_prepared_session(now);

        assert!(!sync_cohort_should_start(&cohort, now));
        assert!(!sync_cohort_should_start(
            &cohort,
            now + Duration::from_millis(700)
        ));
    }

    #[test]
    fn single_prepared_session_starts_once_window_closes() {
        let now = Instant::now();
        let cohort = cohort_with_single_prepared_session(now);

        assert!(sync_cohort_should_start(
            &cohort,
            now + Duration::from_millis(750)
        ));
    }

    #[test]
    fn expired_start_deadline_does_not_bypass_multi_select_window() {
        let now = Instant::now();
        let mut cohort = cohort_with_single_prepared_session(now);
        cohort.start_deadline = now + Duration::from_millis(500);

        assert!(!sync_cohort_should_start(
            &cohort,
            now + Duration::from_millis(500)
        ));
    }

    #[test]
    fn mixed_preparation_waits_until_deadline() {
        let now = Instant::now();
        let first = SessionId::new();
        let mut cohort = SyncCohort {
            id: 1,
            opened_at: now,
            window_deadline: now + Duration::from_millis(750),
            start_deadline: now + Duration::from_millis(2_500),
            sessions: vec![first, SessionId::new()],
            prepared: HashMap::new(),
        };
        cohort.prepared.insert(
            first,
            prepared_downstream(first, zone_id(), StreamCodec::Mp3),
        );

        assert!(!sync_cohort_should_start(
            &cohort,
            now + Duration::from_millis(800)
        ));
        assert!(sync_cohort_should_start(
            &cohort,
            now + Duration::from_millis(2_500)
        ));
    }

    #[test]
    fn unprepared_cohort_waits_for_start_deadline() {
        let now = Instant::now();
        let cohort = SyncCohort {
            id: 1,
            opened_at: now,
            window_deadline: now + Duration::from_millis(750),
            start_deadline: now + Duration::from_millis(2_500),
            sessions: vec![SessionId::new()],
            prepared: HashMap::new(),
        };

        assert!(!sync_cohort_should_start(
            &cohort,
            now + Duration::from_millis(800)
        ));
        assert!(sync_cohort_should_start(
            &cohort,
            now + Duration::from_millis(2_500)
        ));
    }

    #[tokio::test]
    async fn replacement_cohort_does_not_drain_unprepared_predecessor() {
        let mut runtime = runtime();
        let first = SessionId::new();
        let first_id = runtime.add_session_to_sync_cohort(first);
        runtime
            .sync_cohorts
            .back_mut()
            .expect("cohort")
            .window_deadline = Instant::now();
        let second = SessionId::new();
        let second_id = runtime.add_session_to_sync_cohort(second);
        assert_ne!(first_id, second_id);
        assert_eq!(runtime.sync_cohorts.len(), 2);
    }

    #[tokio::test]
    async fn late_preparation_stays_with_its_original_cohort() {
        let mut runtime = runtime();
        let first = SessionId::new();
        let first_zone = zone_id();
        let first_lifecycle = Arc::new(CancellationToken::new());
        runtime.downstream_generations.insert(first, 1);
        runtime.desired_playback.insert(first, true);
        runtime
            .downstream_lifecycles
            .insert(first, first_lifecycle.clone());
        let first_id = runtime.add_session_to_sync_cohort(first);
        runtime
            .sync_cohorts
            .back_mut()
            .expect("cohort")
            .window_deadline = Instant::now();
        let second = SessionId::new();
        let second_id = runtime.add_session_to_sync_cohort(second);
        runtime.sync_cohorts[0].window_deadline = Instant::now() + Duration::from_secs(1);

        let mut prepared = prepared_downstream(first, first_zone, StreamCodec::Mp3);
        prepared.cohort_id = first_id;
        prepared.lifecycle = first_lifecycle;
        runtime.handle_prepared_downstream(prepared).await;

        assert_ne!(first_id, second_id);
        assert!(runtime.sync_cohorts[0].prepared.contains_key(&first));
        assert!(runtime.sync_cohorts[1].prepared.is_empty());
    }

    #[tokio::test]
    async fn deadline_retries_unprepared_members_instead_of_abandoning_them() {
        let mut runtime = runtime();
        let unprepared_id = SessionId::new();
        let zone_id = zone_id();
        runtime.sessions.insert(unprepared_id, zone_id);
        runtime.desired_playback.insert(unprepared_id, true);
        runtime.downstream_generations.insert(unprepared_id, 4);
        runtime
            .downstream_lifecycles
            .insert(unprepared_id, Arc::new(CancellationToken::new()));
        runtime.sync_cohorts.push_back(SyncCohort {
            id: 1,
            opened_at: Instant::now() - Duration::from_secs(3),
            window_deadline: Instant::now() - Duration::from_secs(2),
            start_deadline: Instant::now() - Duration::from_secs(1),
            sessions: vec![unprepared_id],
            prepared: HashMap::new(),
        });

        runtime.maybe_start_sync_cohorts();

        assert!(runtime.sync_cohorts.is_empty());
        assert_eq!(runtime.downstream_generations.get(&unprepared_id), Some(&5));
        assert!(runtime.downstream_reset_needed.contains(&unprepared_id));
        assert!(runtime.downstream_retry_tasks.contains_key(&unprepared_id));
        runtime.cancel_downstream_retry(unprepared_id);
    }

    #[tokio::test]
    async fn cohort_creation_and_joining_within_multi_select_window() {
        let mut runtime = runtime();
        let first = SessionId::new();
        let second = SessionId::new();

        let first_id = runtime.add_session_to_sync_cohort(first);
        let second_id = runtime.add_session_to_sync_cohort(second);

        assert_eq!(first_id, second_id);
        assert_eq!(
            runtime.sync_cohorts.front().expect("cohort").sessions,
            vec![first, second]
        );
    }

    #[tokio::test]
    async fn pause_marks_session_as_needing_downstream_reset() {
        let mut runtime = runtime();
        let session_id = SessionId::new();
        let zone_id = zone_id();
        runtime.sessions.insert(session_id, zone_id.clone());
        runtime.desired_playback.insert(session_id, true);
        runtime.downstream_generations.insert(session_id, 1);
        let lifecycle = Arc::new(CancellationToken::new());
        runtime
            .downstream_lifecycles
            .insert(session_id, lifecycle.clone());
        let cohort_id = runtime.add_session_to_sync_cohort(session_id);
        let mut prepared = prepared_downstream(session_id, zone_id.clone(), StreamCodec::Mp3);
        prepared.cohort_id = cohort_id;
        prepared.lifecycle = lifecycle;
        runtime
            .sync_cohorts
            .front_mut()
            .expect("cohort")
            .prepared
            .insert(session_id, prepared);

        runtime
            .set_playback_state(session_id, zone_id, false)
            .await
            .expect("pause");

        assert_eq!(runtime.desired_playback.get(&session_id), Some(&false));
        assert!(runtime.downstream_reset_needed.contains(&session_id));
        assert!(runtime.sync_cohorts.is_empty());
    }

    #[test]
    fn play_after_pause_requests_downstream_reset() {
        assert!(should_restart_downstream_for_play(Some(false), true));
    }

    #[test]
    fn duplicate_play_retries_when_downstream_reset_is_still_needed() {
        assert!(should_restart_downstream_for_play(Some(true), true));
        assert!(!should_restart_downstream_for_play(Some(true), false));
    }

    #[test]
    fn wav_streams_receive_compensated_anchors() {
        let mut runtime = runtime();
        runtime.config.stream.codec = "wav".to_owned();
        runtime
            .config
            .sync
            .zone_offsets_ms
            .insert("Kitchen".to_owned(), 80);
        let session_id = SessionId::new();
        let prepared = prepared_downstream(session_id, zone_id(), StreamCodec::Wav);

        runtime.apply_sync_anchors(std::slice::from_ref(&prepared));

        assert!(prepared.live_stream.timing().playback_anchor_at.is_some());
    }

    #[test]
    fn mp3_streams_do_not_use_sample_anchors() {
        let runtime = runtime();
        let session_id = SessionId::new();
        let prepared = prepared_downstream(session_id, zone_id(), StreamCodec::Mp3);

        runtime.apply_sync_anchors(std::slice::from_ref(&prepared));

        assert!(prepared.live_stream.timing().playback_anchor_at.is_none());
    }

    #[tokio::test]
    async fn slow_play_does_not_block_ingestion_and_drops_preplay_audio() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let addr = listener.local_addr().expect("listener address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut request = vec![0_u8; 4096];
            let _ = socket.read(&mut request).await.expect("read request");
            tokio::time::sleep(Duration::from_millis(200)).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .expect("write response");
        });

        let runtime = runtime();
        let session_id = SessionId::new();
        let zone_id = zone_id();
        let live_stream = live_stream_for(session_id, zone_id.clone(), StreamCodec::Mp3);
        live_stream.hold_delivery();
        live_stream.on_subscriber_connected();
        let mut subscriber = live_stream.subscribe();
        let prepared = PreparedDownstream {
            session_id,
            zone_id,
            generation: 1,
            cohort_id: 1,
            lifecycle: Arc::new(CancellationToken::new()),
            zone_room_name: "Kitchen".to_owned(),
            client: SonosClient::from_base_url(
                Url::parse(&format!("http://{addr}")).expect("sonos url"),
            )
            .expect("client"),
            live_stream: live_stream.clone(),
            timing: ZoneStartupTiming::default(),
        };

        let started_at = Instant::now();
        runtime.play_prepared_downstreams(vec![prepared]);
        assert!(started_at.elapsed() < Duration::from_millis(50));
        live_stream.publish(b"stale".as_slice().into());
        assert!(subscriber.try_recv().is_err());

        server.await.expect("server task");
        tokio::time::sleep(Duration::from_millis(50)).await;
        live_stream.publish(b"live".as_slice().into());
        assert_eq!(&subscriber.recv().await.expect("live audio")[..], b"live");
    }

    #[tokio::test]
    async fn successful_current_downstream_result_clears_reset_marker() {
        let mut runtime = runtime();
        let session_id = SessionId::new();
        let zone_id = zone_id();
        runtime.downstream_generations.insert(session_id, 3);
        runtime.downstream_reset_needed.insert(session_id);
        runtime.desired_playback.insert(session_id, true);
        runtime.downstream_retry_attempts.insert(session_id, 2);

        runtime.handle_downstream_start_result(DownstreamStartResult {
            session_id,
            zone_id,
            generation: 3,
            outcome: DownstreamStartOutcome::Started,
            timing: None,
        });

        assert!(!runtime.downstream_reset_needed.contains(&session_id));
        assert!(!runtime.downstream_retry_attempts.contains_key(&session_id));
    }

    #[tokio::test]
    async fn failed_current_downstream_result_keeps_reset_and_schedules_retry() {
        let mut runtime = runtime();
        let session_id = SessionId::new();
        let zone_id = zone_id();
        runtime.downstream_generations.insert(session_id, 4);
        runtime.desired_playback.insert(session_id, true);

        runtime.handle_downstream_start_result(DownstreamStartResult {
            session_id,
            zone_id,
            generation: 4,
            outcome: DownstreamStartOutcome::Failed,
            timing: None,
        });

        assert!(runtime.downstream_reset_needed.contains(&session_id));
        assert!(runtime.downstream_retry_tasks.contains_key(&session_id));
        runtime.cancel_downstream_retry(session_id);
    }

    #[tokio::test]
    async fn stale_downstream_result_is_ignored() {
        let mut runtime = runtime();
        let session_id = SessionId::new();
        let zone_id = zone_id();
        runtime.downstream_generations.insert(session_id, 5);
        runtime.downstream_reset_needed.insert(session_id);
        runtime.desired_playback.insert(session_id, true);

        runtime.handle_downstream_start_result(DownstreamStartResult {
            session_id,
            zone_id,
            generation: 4,
            outcome: DownstreamStartOutcome::Started,
            timing: None,
        });

        assert!(runtime.downstream_reset_needed.contains(&session_id));
    }

    #[tokio::test]
    async fn stale_prepared_downstream_is_ignored() {
        let mut runtime = runtime();
        let session_id = SessionId::new();
        let zone_id = zone_id();
        runtime.downstream_generations.insert(session_id, 2);
        let cohort_id = runtime.add_session_to_sync_cohort(session_id);

        let mut prepared = prepared_downstream(session_id, zone_id, StreamCodec::Mp3);
        prepared.generation = 1;
        prepared.cohort_id = cohort_id;
        runtime.handle_prepared_downstream(prepared).await;

        assert!(
            runtime
                .sync_cohorts
                .front()
                .expect("cohort")
                .prepared
                .is_empty()
        );
    }

    #[tokio::test]
    async fn same_zone_connection_disconnect_keeps_audio_session_valid() {
        let mut runtime = runtime();
        let kept = SessionId::new();
        let kept_zone = zone_id();
        let cohort_id = runtime.add_session_to_sync_cohort(kept);
        runtime.sessions.insert(kept, kept_zone.clone());
        runtime.downstream_generations.insert(kept, 1);
        runtime.desired_playback.insert(kept, true);

        runtime
            .handle_event(AirPlayEvent::ClientDisconnected {
                zone_id: kept_zone,
                addr: "127.0.0.1:1".to_owned(),
            })
            .await
            .expect("disconnect");

        assert_eq!(runtime.downstream_generations.get(&kept), Some(&1));
        assert!(
            runtime
                .sync_cohorts
                .iter()
                .any(|cohort| cohort.id == cohort_id)
        );
    }

    #[tokio::test]
    async fn removed_member_late_prepare_cannot_start_standalone() {
        let mut runtime = runtime();
        let session_id = SessionId::new();
        let zone = zone_id();
        runtime.sessions.insert(session_id, zone.clone());
        runtime.downstream_generations.insert(session_id, 1);
        runtime.desired_playback.insert(session_id, true);
        let cohort_id = runtime.add_session_to_sync_cohort(session_id);
        runtime.invalidate_prepared_downstream(session_id);
        let mut prepared = prepared_downstream(session_id, zone, StreamCodec::Mp3);
        prepared.cohort_id = cohort_id;
        runtime.handle_prepared_downstream(prepared).await;

        assert!(runtime.sync_cohorts.is_empty());
        assert!(runtime.downstream_result_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn late_stream_ended_while_desired_playback_true_is_ignored() {
        let mut runtime = runtime();
        let session_id = SessionId::new();
        let zone_id = zone_id();
        runtime.sessions.insert(session_id, zone_id.clone());
        runtime.desired_playback.insert(session_id, true);

        runtime
            .handle_event(AirPlayEvent::StreamEndedWhilePaused {
                session_id,
                zone_id,
            })
            .await
            .expect("stream ended");

        assert!(!runtime.downstream_reset_needed.contains(&session_id));
        assert!(!runtime.paused_cleanup_tasks.contains_key(&session_id));
    }

    #[tokio::test]
    async fn stop_clears_session_downstream_state_and_tasks() {
        let mut runtime = runtime();
        let session_id = SessionId::new();
        let zone_id = zone_id();
        runtime.sessions.insert(session_id, zone_id);
        runtime.session_formats.insert(session_id, format());
        runtime.desired_playback.insert(session_id, true);
        runtime.downstream_generations.insert(session_id, 8);
        runtime.downstream_reset_needed.insert(session_id);
        runtime.schedule_paused_cleanup(session_id);
        runtime.playback_tasks.insert(
            session_id,
            tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }),
        );

        runtime
            .stop_session(session_id, None)
            .await
            .expect("stop session");

        assert!(!runtime.sessions.contains_key(&session_id));
        assert!(!runtime.session_formats.contains_key(&session_id));
        assert!(!runtime.desired_playback.contains_key(&session_id));
        assert_eq!(runtime.downstream_generations.get(&session_id), Some(&9));
        assert!(!runtime.downstream_reset_needed.contains(&session_id));
        assert!(!runtime.paused_cleanup_tasks.contains_key(&session_id));
        assert!(!runtime.playback_tasks.contains_key(&session_id));
    }

    #[test]
    fn retry_delay_backs_off_to_maximum() {
        assert_eq!(
            downstream_retry_delay(0),
            Duration::from_millis(DOWNSTREAM_RETRY_BASE_MS)
        );
        assert_eq!(
            downstream_retry_delay(1),
            Duration::from_millis(DOWNSTREAM_RETRY_BASE_MS * 2)
        );
        assert_eq!(
            downstream_retry_delay(8),
            Duration::from_millis(DOWNSTREAM_RETRY_MAX_MS)
        );
    }
}
