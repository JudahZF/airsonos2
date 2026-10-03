use std::collections::{HashMap, VecDeque};
use std::fs;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use airsonos2_airplay::{
    AirPlayEndpointRunner, AirPlayEvent, FilePairingStore, PcmFormat, ZoneVolumeState,
};
use airsonos2_core::{
    Config, EncoderState, HomeAssistantConfig, ServerConfig, SessionId, SonosZone, StreamCodec,
    StreamSession, VirtualAirPlayEndpoint, ZoneId, configured_delays, filter_zones,
    sonos_volume_to_airplay_db, virtual_endpoint_for_zone,
};
use airsonos2_diagnostics::{
    BridgeStatus, CheckStatus, ConfigUi, Speaker, run_doctor, serve_diagnostics,
};
use airsonos2_homeassistant::{HomeAssistantClient, MediaPlayer, object_id};
use airsonos2_sonos::{SonosClient, discover_sonos_zones_from_sources};
use airsonos2_stream::{
    FfmpegEncoder, FfmpegEncoderConfig, LiveStream, StreamRegistry, serve_stream_http,
};
use clap::{Parser, Subcommand};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use url::Url;

mod bridge;
mod home_assistant;
mod renderer;
use bridge::{TransportCommand, ZoneWorker};
use renderer::Renderer;

/// How long to keep a bridge session alive after the buffered audio stream
/// closes while AirPlay playback is paused.
const PAUSED_SESSION_GRACE_SECS: u64 = 60;
const DOWNSTREAM_RETRY_BASE_MS: u64 = 500;
const DOWNSTREAM_RETRY_MAX_MS: u64 = 5_000;
/// AirPlay volume reported when a renderer cannot read its own. Home Assistant hides the
/// volume of players that are off, and a maximum default would make the first session loud.
const FALLBACK_VOLUME_PERCENT: u8 = 20;
/// Pick the default-route source address for stream URLs when the renderer IP is unknown.
/// No packets are sent to them.
const DEFAULT_ROUTE_PROBE_V4: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
const DEFAULT_ROUTE_PROBE_V6: IpAddr = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1));
/// Delay between startup attempts when no speaker can start.
const SETUP_RETRY: Duration = Duration::from_secs(10);
/// `serve` exits with this code (EX_TEMPFAIL) when the web GUI asks for a restart, so
/// systemd `Restart=on-failure` and Docker restart policies start it again.
const RESTART_EXIT_CODE: i32 = 75;

#[derive(Debug, Parser)]
#[command(
    name = "airsonos2",
    version,
    about = "AirPlay 2 bridge for legacy Sonos rooms and Home Assistant media players"
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
        /// Show the config read-only in the web GUI, because another tool generates it.
        #[arg(long)]
        config_read_only: bool,
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
        Command::Serve {
            config: path,
            config_read_only,
        } => {
            let config = Config::from_path(&path)?;
            init_tracing(&config);
            if serve(config, path, config_read_only).await? == ServeExit::Restart {
                std::process::exit(RESTART_EXIT_CODE);
            }
            Ok(())
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
    } else {
        print_zones(&zones);
    }

    if config.home_assistant.url.is_some() {
        let players = match HomeAssistantClient::from_config(&config.home_assistant) {
            Ok(client) => client.media_players().await,
            Err(error) => Err(error),
        };
        println!();
        match players {
            Ok(players) => print_media_players(&players),
            Err(error) => println!("Cannot list Home Assistant media players: {error}"),
        }
    }

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

    let rooms = zones
        .iter()
        .map(|zone| (ZoneId::new(zone.clone()), zone.clone()))
        .collect::<Vec<_>>();
    let delays = configured_delays(
        &rooms,
        &config.sync.zone_offsets_ms,
        config.sync.default_offset_ms,
    );

    println!("Current delay plan from configured offsets:");
    for delay in delays {
        println!("{}: delay {} ms", delay.zone_id, delay.delay_ms);
    }
    println!(
        "Play the calibration click track through an AirPlay group, then raise offsets for rooms that arrive late."
    );

    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
enum ServeExit {
    Stopped,
    Restart,
}

async fn serve(
    config: Config,
    config_path: PathBuf,
    config_read_only: bool,
) -> anyhow::Result<ServeExit> {
    let diagnostics_addr: SocketAddr = config.diagnostics.metrics_addr.parse()?;
    fs::create_dir_all(config.server.state_dir.join("endpoints"))?;
    fs::create_dir_all(config.server.state_dir.join("pairings"))?;

    // Start the GUI first, so it can fix a config that finds no speakers.
    let registry = StreamRegistry::new();
    let restart = tokio_util::sync::CancellationToken::new();
    let (status_tx, status_rx) = tokio::sync::watch::channel(BridgeStatus::Starting);
    let config_ui = ConfigUi {
        path: config_path,
        read_only: config_read_only,
        running: config.clone(),
        started_at_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_millis() as u64),
        status: status_rx,
        restart: restart.clone(),
    };
    let diagnostics_cancel = tokio_util::sync::CancellationToken::new();
    let mut diagnostics_task = tokio::spawn(serve_diagnostics(
        diagnostics_addr,
        registry.clone(),
        config_ui,
        diagnostics_cancel.clone(),
    ));
    info!("config GUI listening on http://{diagnostics_addr}/");
    // Also before discovery, so watchdogs on `/healthz` see a live process while it retries.
    let http_addr = SocketAddr::new(config.server.bind, config.server.http_port);
    let mut http_task = tokio::spawn(serve_stream_http(
        http_addr,
        registry.clone(),
        config.stream.ffmpeg_path.clone(),
    ));
    info!("stream HTTP server listening on {}", http_addr);
    // Created once: it holds the only signal handler for the life of the process.
    let mut exit = Box::pin(async {
        tokio::select! {
            signal = shutdown_signal() => signal.map(|()| ServeExit::Stopped),
            () = restart.cancelled() => Ok(ServeExit::Restart),
            result = &mut diagnostics_task => listener_exit(result),
            result = &mut http_task => listener_exit(result),
        }
    });

    // `Err` holds the result of an exit before any speaker started.
    let renderers = loop {
        let attempt = tokio::select! {
            attempt = discover_renderers(&config) => attempt,
            result = &mut exit => break Err(result),
        };
        match attempt {
            Ok(renderers) => break Ok(renderers),
            Err(error) => {
                error!(
                    "cannot start any speaker: {error:#}; retrying in {} s",
                    SETUP_RETRY.as_secs()
                );
                status_tx.send_replace(BridgeStatus::Setup {
                    error: format!("{error:#}"),
                });
            }
        }
        tokio::select! {
            () = tokio::time::sleep(SETUP_RETRY) => {}
            result = &mut exit => break Err(result),
        }
    };
    let renderers = match renderers {
        Ok(renderers) => renderers,
        Err(result) => {
            drop(exit);
            if !http_task.is_finished() {
                http_task.abort();
                let _ = http_task.await;
            }
            diagnostics_cancel.cancel();
            if !diagnostics_task.is_finished()
                && tokio::time::timeout(Duration::from_secs(10), &mut diagnostics_task)
                    .await
                    .is_err()
            {
                warn!("shutdown exceeded its 10 second deadline; cancelling remaining work");
            }
            return result;
        }
    };

    info!("starting AirSonos2 for {} renderer(s)", renderers.len());

    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let (cleanup_tx, cleanup_rx) = mpsc::unbounded_channel();
    let endpoints = build_endpoints(&renderers, &config)?;
    persist_endpoint_identities(&config.server.state_dir, &endpoints)?;
    let speakers = renderers
        .iter()
        .zip(&endpoints)
        .map(|(renderer, endpoint)| Speaker {
            room: renderer.name().to_owned(),
            airplay_name: endpoint.display_name.clone(),
            rtsp_port: endpoint.rtsp_port,
        })
        .collect();
    let renderers: HashMap<ZoneId, Renderer> = renderers
        .into_iter()
        .map(|renderer| (renderer.id().clone(), renderer))
        .collect();
    let volume_states = load_volume_states(&renderers).await;
    let mut runners =
        start_airplay_endpoints(&endpoints, &config, events_tx, &volume_states).await?;
    let mut runtime = BridgeRuntime::new(
        config.clone(),
        registry.clone(),
        renderers,
        volume_states,
        cleanup_tx,
    );

    status_tx.send_replace(BridgeStatus::Running { speakers });

    let result = tokio::select! {
        result = runtime.run(events_rx, cleanup_rx) => result.map(|()| ServeExit::Stopped),
        result = &mut exit => result,
    };
    drop(exit);

    // Stop admission before draining sessions. Errors must pass through cleanup.
    if !http_task.is_finished() {
        http_task.abort();
        let _ = http_task.await;
    }
    diagnostics_cancel.cancel();
    registry.close_all().await;
    let cleanup = async {
        if !diagnostics_task.is_finished() {
            let _ = (&mut diagnostics_task).await;
        }
        for runner in &mut runners {
            runner.stop().await;
        }
        runtime.shutdown().await;
    };
    if tokio::time::timeout(Duration::from_secs(10), cleanup)
        .await
        .is_err()
    {
        diagnostics_task.abort();
        warn!("shutdown exceeded its 10 second deadline; cancelling remaining work");
    }
    result
}

/// Maps the end of a listener task to the result of `serve`.
fn listener_exit<E: Into<anyhow::Error>>(
    result: Result<Result<(), E>, tokio::task::JoinError>,
) -> anyhow::Result<ServeExit> {
    match result {
        Ok(result) => result.map(|()| ServeExit::Stopped).map_err(Into::into),
        Err(error) => Err(error.into()),
    }
}

/// Finds the speakers to bridge. Fails when none match, so `serve` can retry.
async fn discover_renderers(config: &Config) -> anyhow::Result<Vec<Renderer>> {
    let discovered = discover_sonos_zones_from_sources(
        Duration::from_secs(5),
        &config.sonos.static_ips,
        config.sonos.auto_discover,
    )
    .await?;
    let zones = filter_zones(&discovered, &config.sonos);
    if !zones.is_empty() {
        print_zones(&zones);
    }
    let mut renderers = sonos_renderers(zones)?;
    renderers.extend(home_assistant_renderers(&config.home_assistant).await?);
    if renderers.is_empty() {
        anyhow::bail!("no Sonos rooms or Home Assistant players matched the current configuration");
    }
    Ok(renderers)
}

async fn shutdown_signal() -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            signal = tokio::signal::ctrl_c() => signal?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    info!("shutdown signal received");
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

fn print_media_players(players: &[MediaPlayer]) {
    let players: Vec<&MediaPlayer> = players
        .iter()
        .filter(|player| player.supports_play_media())
        .collect();
    if players.is_empty() {
        println!("No Home Assistant media players support play_media.");
        return;
    }

    println!("{:<40} {:<28} State", "Home Assistant entity", "Name");
    for player in players {
        println!(
            "{:<40} {:<28} {}",
            player.entity_id, player.name, player.state
        );
    }
}

fn sonos_renderers(zones: Vec<SonosZone>) -> anyhow::Result<Vec<Renderer>> {
    zones
        .into_iter()
        .map(|zone| {
            Ok(Renderer::Sonos {
                client: SonosClient::new(zone.ip)?,
                zone,
            })
        })
        .collect()
}

/// Builds renderers for the configured Home Assistant players. Names come from Home
/// Assistant. When it does not answer, the entity id is the name, and playback still works
/// once Home Assistant is up.
async fn home_assistant_renderers(config: &HomeAssistantConfig) -> anyhow::Result<Vec<Renderer>> {
    if config.media_players.is_empty() {
        return Ok(Vec::new());
    }

    let client = HomeAssistantClient::from_config(config)?;
    let players = client
        .media_players()
        .await
        .inspect_err(|error| {
            warn!(
                "cannot read Home Assistant players; using entity ids as AirPlay names: {error:#}"
            );
        })
        .ok();

    Ok(config
        .media_players
        .iter()
        .map(|entity_id| {
            let player = players
                .as_deref()
                .and_then(|players| players.iter().find(|player| &player.entity_id == entity_id));
            match (&players, player) {
                (Some(_), None) => warn!(
                    %entity_id,
                    "Home Assistant player not found; using its entity id as the AirPlay name"
                ),
                (_, Some(player)) => {
                    let unsupported = player.unsupported_services();
                    if !unsupported.is_empty() {
                        warn!(%entity_id, ?unsupported, "Home Assistant player does not support all services AirSonos2 calls");
                    }
                }
                _ => {}
            }
            let name = player.map_or_else(
                || object_id(entity_id).to_owned(),
                |player| player.name.clone(),
            );
            info!(%entity_id, %name, "adding Home Assistant player");
            Renderer::HomeAssistant {
                id: ZoneId::new(entity_id.clone()),
                name,
                client: client.clone(),
            }
        })
        .collect())
}

fn build_endpoints(
    renderers: &[Renderer],
    config: &Config,
) -> anyhow::Result<Vec<VirtualAirPlayEndpoint>> {
    renderers
        .iter()
        .enumerate()
        .map(|(index, renderer)| {
            virtual_endpoint_for_zone(
                renderer.id(),
                renderer.name(),
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
    let mut runners: Vec<AirPlayEndpointRunner> = Vec::new();

    for endpoint in endpoints {
        let volume_state = volume_states
            .get(&endpoint.zone_id)
            .cloned()
            .unwrap_or_else(|| ZoneVolumeState::new(0.0));
        let build = AirPlayEndpointRunner::build(
            endpoint,
            &config.airplay,
            config.server.bind,
            events_tx.clone(),
            volume_state,
        );
        let mut runner = match build {
            Ok(runner) => runner,
            Err(error) => {
                for runner in &mut runners {
                    runner.stop().await;
                }
                return Err(error.into());
            }
        };
        if let Err(error) = runner.start().await {
            runner.stop().await;
            for runner in &mut runners {
                runner.stop().await;
            }
            return Err(error.into());
        }
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

/// Reads all volumes at once, so an unreachable renderer delays startup by one request
/// timeout, not one per renderer.
async fn load_volume_states(
    renderers: &HashMap<ZoneId, Renderer>,
) -> HashMap<ZoneId, ZoneVolumeState> {
    let mut reads = tokio::task::JoinSet::new();
    for renderer in renderers.values() {
        let renderer = renderer.clone();
        reads.spawn(async move {
            let volume = renderer.volume().await;
            (renderer, volume)
        });
    }

    let mut volume_states = HashMap::new();
    while let Some(read) = reads.join_next().await {
        let Ok((renderer, volume)) = read else {
            continue;
        };
        let zone_id = renderer.id();
        let volume_percent = match volume {
            Ok(volume_percent) => volume_percent,
            Err(error) => {
                warn!(
                    %zone_id,
                    room = renderer.name(),
                    fallback_percent = FALLBACK_VOLUME_PERCENT,
                    "failed to read renderer volume; using fallback AirPlay volume: {error:#}"
                );
                FALLBACK_VOLUME_PERCENT
            }
        };
        let volume_db = sonos_volume_to_airplay_db(volume_percent);
        info!(
            %zone_id,
            room = renderer.name(),
            volume_percent,
            volume_db,
            "loaded renderer volume for AirPlay reporting"
        );
        volume_states.insert(zone_id.clone(), ZoneVolumeState::new(volume_db));
    }

    volume_states
}

async fn refresh_volume_state(renderer: &Renderer, volume_state: &ZoneVolumeState) {
    let zone_id = renderer.id();
    match renderer.volume().await {
        Ok(volume_percent) => {
            let volume_db = sonos_volume_to_airplay_db(volume_percent);
            volume_state.set_volume_db(volume_db);
            info!(
                %zone_id,
                volume_percent,
                volume_db,
                "refreshed renderer volume for AirPlay reporting"
            );
        }
        Err(error) => {
            warn!(
                %zone_id,
                "failed to refresh renderer volume for AirPlay reporting: {error:#}"
            );
        }
    }
}

struct BridgeRuntime {
    config: Config,
    registry: StreamRegistry,
    renderers: HashMap<ZoneId, Renderer>,
    volume_states: HashMap<ZoneId, ZoneVolumeState>,
    sessions: HashMap<SessionId, SessionRuntime>,
    zone_workers: HashMap<ZoneId, ZoneWorker>,
    next_generation: u64,
    pending_cohorts: VecDeque<SyncCohort>,
    cleanup_tx: mpsc::UnboundedSender<SessionId>,
    downstream_result_tx: mpsc::UnboundedSender<DownstreamStartResult>,
    downstream_result_rx: mpsc::UnboundedReceiver<DownstreamStartResult>,
    downstream_retry_tx: mpsc::UnboundedSender<DownstreamRetry>,
    downstream_retry_rx: mpsc::UnboundedReceiver<DownstreamRetry>,
    prepared_tx: mpsc::UnboundedSender<PreparedDownstream>,
    prepared_rx: mpsc::UnboundedReceiver<PreparedDownstream>,
    cohort_wake_tx: mpsc::UnboundedSender<()>,
    cohort_wake_rx: mpsc::UnboundedReceiver<()>,
    sync_cohort: Option<SyncCohort>,
    tasks: tokio::task::JoinSet<()>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObservedPlayback {
    Unknown,
    Playing,
    Stopped,
}

struct SessionRuntime {
    zone_id: ZoneId,
    format: PcmFormat,
    generation: u64,
    playback_epoch: u64,
    desired_playback: bool,
    observed: ObservedPlayback,
    reset_needed: bool,
    encoder: Option<FfmpegEncoder>,
    prepared: Option<PreparedDownstream>,
    retry_attempts: u32,
    retry_task: Option<JoinHandle<()>>,
    cleanup_task: Option<JoinHandle<()>>,
    /// Paused-session grace deadline. Downstream generations can advance while
    /// paused, so the cleanup timer does not use them.
    cleanup_deadline: Option<tokio::time::Instant>,
}

impl SessionRuntime {
    fn new(zone_id: ZoneId, format: PcmFormat) -> Self {
        Self {
            zone_id,
            format,
            generation: 0,
            playback_epoch: 0,
            desired_playback: true,
            observed: ObservedPlayback::Unknown,
            reset_needed: false,
            encoder: None,
            prepared: None,
            retry_attempts: 0,
            retry_task: None,
            cleanup_task: None,
            cleanup_deadline: None,
        }
    }
}

impl Drop for SessionRuntime {
    fn drop(&mut self) {
        for task in [&self.retry_task, &self.cleanup_task].into_iter().flatten() {
            task.abort();
        }
    }
}

#[derive(Clone)]
struct StreamPrepare {
    session_id: SessionId,
    zone_id: ZoneId,
    generation: u64,
    renderer: Renderer,
    live_stream: LiveStream,
    local_url: Url,
    force_standalone_on_start: bool,
    prepared_tx: mpsc::UnboundedSender<PreparedDownstream>,
    result_tx: mpsc::UnboundedSender<DownstreamStartResult>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DownstreamStartOutcome {
    Started,
    Stopped,
    StopUnknown,
    Unknown,
    Failed,
    PermanentFailure,
}

#[derive(Clone, Debug)]
struct DownstreamStartResult {
    session_id: SessionId,
    zone_id: ZoneId,
    generation: u64,
    outcome: DownstreamStartOutcome,
}

#[derive(Debug)]
struct SyncCohort {
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
    renderer: Renderer,
    live_stream: LiveStream,
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
        renderers: HashMap<ZoneId, Renderer>,
        volume_states: HashMap<ZoneId, ZoneVolumeState>,
        cleanup_tx: mpsc::UnboundedSender<SessionId>,
    ) -> Self {
        let (downstream_result_tx, downstream_result_rx) = mpsc::unbounded_channel();
        let (downstream_retry_tx, downstream_retry_rx) = mpsc::unbounded_channel();
        let (prepared_tx, prepared_rx) = mpsc::unbounded_channel();
        let (cohort_wake_tx, cohort_wake_rx) = mpsc::unbounded_channel();
        Self {
            config,
            registry,
            renderers,
            volume_states,
            sessions: HashMap::new(),
            zone_workers: HashMap::new(),
            next_generation: 0,
            pending_cohorts: VecDeque::new(),
            cleanup_tx,
            downstream_result_tx,
            downstream_result_rx,
            downstream_retry_tx,
            downstream_retry_rx,
            prepared_tx,
            prepared_rx,
            cohort_wake_tx,
            cohort_wake_rx,
            sync_cohort: None,
            tasks: tokio::task::JoinSet::new(),
        }
    }

    async fn run(
        &mut self,
        mut events_rx: mpsc::UnboundedReceiver<AirPlayEvent>,
        mut cleanup_rx: mpsc::UnboundedReceiver<SessionId>,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = self.tasks.join_next(), if !self.tasks.is_empty() => {},
                event = events_rx.recv() => {
                    let Some(event) = event else { break };
                    if let Err(error) = self.handle_event(event).await {
                        warn!("bridge event failed: {error:#}");
                    }
                }
                session_id = cleanup_rx.recv() => {
                    let Some(session_id) = session_id else { continue };
                    // A cancelled timer's message can still be queued, so check the live deadline.
                    if self.sessions.get(&session_id).is_some_and(|session| {
                        !session.desired_playback
                            && session
                                .cleanup_deadline
                                .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
                    }) {
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
                        self.maybe_start_sync_cohort(false).await;
                    }
                }
            }
        }

        Ok(())
    }

    fn worker(&mut self, zone_id: &ZoneId) -> Option<&ZoneWorker> {
        if !self.zone_workers.contains_key(zone_id) {
            let renderer = self.renderers.get(zone_id)?.clone();
            let worker = ZoneWorker::new(
                renderer,
                self.downstream_result_tx.clone(),
                Duration::from_millis(self.config.stream.startup_wait_ms()),
                Duration::from_millis(self.config.stream.prebuffer_ms),
            );
            self.zone_workers.insert(zone_id.clone(), worker);
        }
        self.zone_workers.get(zone_id)
    }

    fn cancel_paused_cleanup(&mut self, session_id: SessionId) {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            session.cleanup_deadline = None;
            if let Some(task) = session.cleanup_task.take() {
                task.abort();
            }
        }
    }

    fn schedule_paused_cleanup(&mut self, session_id: SessionId) {
        self.cancel_paused_cleanup(session_id);
        if let Some(session) = self.sessions.get_mut(&session_id) {
            let tx = self.cleanup_tx.clone();
            let deadline =
                tokio::time::Instant::now() + Duration::from_secs(PAUSED_SESSION_GRACE_SECS);
            session.cleanup_deadline = Some(deadline);
            session.cleanup_task = Some(tokio::spawn(async move {
                tokio::time::sleep_until(deadline).await;
                let _ = tx.send(session_id);
            }));
        }
    }

    fn cancel_downstream_retry(&mut self, session_id: SessionId) {
        if let Some(session) = self.sessions.get_mut(&session_id)
            && let Some(task) = session.retry_task.take()
        {
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
        let Some(session) = self.sessions.get_mut(&session_id) else {
            return;
        };
        if session.retry_attempts >= 6 {
            // A kept prepared stream may never have played; the next Play must rebuild it.
            session.reset_needed = true;
            error!(%session_id, %zone_id, "downstream retry budget exhausted; session requires a new playback request");
            return;
        }
        let delay = downstream_retry_delay(session.retry_attempts);
        session.retry_attempts += 1;
        let tx = self.downstream_retry_tx.clone();
        session.retry_task = Some(tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = tx.send(DownstreamRetry {
                session_id,
                zone_id,
                generation,
            });
        }));
    }

    fn handle_downstream_start_result(&mut self, result: DownstreamStartResult) {
        let Some(session) = self.sessions.get_mut(&result.session_id) else {
            return;
        };
        if session.generation != result.generation {
            return;
        }
        if !session.desired_playback {
            session.observed = if result.outcome == DownstreamStartOutcome::Stopped {
                ObservedPlayback::Stopped
            } else {
                ObservedPlayback::Unknown
            };
            return;
        }
        match result.outcome {
            DownstreamStartOutcome::Stopped | DownstreamStartOutcome::StopUnknown => {}
            DownstreamStartOutcome::Started => {
                session.observed = ObservedPlayback::Playing;
                session.reset_needed = false;
                session.retry_attempts = 0;
                self.cancel_downstream_retry(result.session_id);
            }
            DownstreamStartOutcome::PermanentFailure => {
                session.observed = ObservedPlayback::Unknown;
                session.reset_needed = true;
                session.retry_attempts = 6;
                error!(session_id = %result.session_id, "permanent renderer error; automatic retries stopped");
                self.cancel_downstream_retry(result.session_id);
            }
            _ => {
                session.observed = ObservedPlayback::Unknown;
                session.reset_needed = session.prepared.is_none();
                self.schedule_downstream_retry(
                    result.session_id,
                    result.zone_id,
                    result.generation,
                );
            }
        }
    }

    async fn handle_downstream_retry(&mut self, retry: DownstreamRetry) -> anyhow::Result<()> {
        let Some(session) = self.sessions.get_mut(&retry.session_id) else {
            return Ok(());
        };
        if session.generation != retry.generation || !session.desired_playback {
            return Ok(());
        }
        if let Some(task) = session.retry_task.take() {
            task.abort();
        }
        if let Some(prepared) = session.prepared.clone() {
            if let Some(worker) = self.worker(&retry.zone_id) {
                worker.command(TransportCommand::Play(Box::new(prepared)));
            }
        } else {
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
                session_id, frames, ..
            } => {
                let batch = frames.begin_batch();
                self.registry.record_adapter_drops(frames.take_new_drops());
                for _ in 0..batch {
                    let Some(frame) = frames.pop() else { break };
                    if let Some(session) = self.sessions.get_mut(&session_id)
                        && frame.playback_epoch == session.playback_epoch
                        && let Some(encoder) = &session.encoder
                    {
                        match encoder.try_write_frame(frame) {
                            Ok(true) => {}
                            Ok(false) => {
                                debug!(%session_id, "encoder input full; dropping realtime frame")
                            }
                            Err(error) => {
                                warn!(%session_id, "encoder input closed: {error}");
                                session.prepared = None;
                                session.reset_needed = true;
                                let zone_id = session.zone_id.clone();
                                let desired_playback = session.desired_playback;
                                let encoder = session.encoder.take();
                                let generation = self.next_downstream_generation(session_id);
                                self.registry.remove(&session_id).await;
                                self.retire_encoder(encoder);
                                if let Some(worker) = self.worker(&zone_id) {
                                    worker.command(TransportCommand::Stop {
                                        session_id,
                                        zone_id: zone_id.clone(),
                                        generation,
                                    });
                                }
                                if desired_playback {
                                    self.schedule_downstream_retry(session_id, zone_id, generation);
                                }
                            }
                        }
                    }
                }
                Ok(())
            }
            AirPlayEvent::PlaybackState {
                session_id,
                zone_id,
                playing,
            } => self.set_playback_state(session_id, zone_id, playing).await,
            AirPlayEvent::Flushed {
                session_id,
                zone_id,
                playback_epoch,
            } => {
                let Some(session) = self.sessions.get_mut(&session_id) else {
                    return Ok(());
                };
                if playback_epoch <= session.playback_epoch {
                    return Ok(());
                }
                session.playback_epoch = playback_epoch;
                let playing = session.desired_playback;
                if playing {
                    self.restart_downstream_for_play(session_id, zone_id)
                        .await?;
                } else {
                    let encoder = session.encoder.take();
                    self.registry.remove(&session_id).await;
                    self.retire_encoder(encoder);
                    self.set_playback_state(session_id, zone_id, false).await?;
                }
                Ok(())
            }
            AirPlayEvent::StreamEndedWhilePaused {
                session_id,
                zone_id,
            } => {
                if self.sessions.contains_key(&session_id)
                    && !self
                        .sessions
                        .get(&session_id)
                        .is_some_and(|session| session.desired_playback)
                {
                    debug!(
                        %session_id,
                        %zone_id,
                        "AirPlay audio stream ended while paused; deferring bridge session cleanup"
                    );
                    if let Some(session) = self.sessions.get_mut(&session_id) {
                        session.reset_needed = true;
                    }
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
                volume_percent,
                ..
            } => {
                if let Some(worker) = self.worker(&zone_id) {
                    worker.set_volume(volume_percent);
                }
                Ok(())
            }
            AirPlayEvent::SessionStopped {
                session_id,
                zone_id,
            } => self.stop_session(session_id, Some(zone_id)).await,
            AirPlayEvent::ClientConnected { zone_id, addr } => {
                info!(%zone_id, %addr, "AirPlay client connected");
                if let (Some(renderer), Some(volume_state)) = (
                    self.renderers.get(&zone_id),
                    self.volume_states.get(&zone_id),
                ) {
                    let renderer = renderer.clone();
                    let volume_state = volume_state.clone();
                    self.tasks.spawn(async move {
                        refresh_volume_state(&renderer, &volume_state).await;
                    });
                }
                Ok(())
            }
            AirPlayEvent::ClientDisconnected { zone_id, addr } => {
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
        renderer_ip: Option<IpAddr>,
        format: PcmFormat,
        generation: u64,
    ) -> anyhow::Result<(LiveStream, FfmpegEncoder, Url)> {
        let stream_codec = stream_codec(&self.config.stream.codec)?;
        let local_url = stream_url_for_zone(
            &self.config,
            renderer_ip,
            session_id,
            stream_codec,
            generation,
        )
        .await?;
        let stream_session = StreamSession {
            session_id,
            zone_id,
            codec: stream_codec,
            generation,
            local_url: local_url.clone(),
            encoder_state: EncoderState::Starting,
        };
        let live_stream = LiveStream::new(stream_session);
        live_stream.set_playback_epoch(
            self.sessions
                .get(&session_id)
                .map_or(0, |session| session.playback_epoch),
        );
        let encoder = FfmpegEncoder::spawn(
            FfmpegEncoderConfig {
                ffmpeg_path: self.config.stream.ffmpeg_path.clone(),
                sample_rate: format.sample_rate,
                channels: format.channels,
                mp3_bitrate_kbps: self.config.stream.mp3_bitrate_kbps,
                codec: stream_codec,
                queue_duration: self.config.sync.pcm_queue_duration(),
            },
            live_stream.clone(),
        )?;

        self.registry.insert(live_stream.clone()).await;
        Ok((live_stream, encoder, local_url))
    }

    fn next_downstream_generation(&mut self, session_id: SessionId) -> u64 {
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .expect("generation space exhausted");
        if let Some(session) = self.sessions.get_mut(&session_id) {
            session.generation = self.next_generation;
        }
        self.next_generation
    }

    fn schedule_cohort_wake(&mut self, delay: Duration) {
        let tx = self.cohort_wake_tx.clone();
        self.tasks.spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = tx.send(());
        });
    }

    fn add_session_to_sync_cohort(&mut self, session_id: SessionId) {
        // Remove a prior generation before admitting this session again.
        for cohort in self
            .sync_cohort
            .iter_mut()
            .chain(self.pending_cohorts.iter_mut())
        {
            cohort.sessions.retain(|id| *id != session_id);
            cohort.prepared.remove(&session_id);
        }
        let now = Instant::now();
        let last = self
            .pending_cohorts
            .back_mut()
            .or(self.sync_cohort.as_mut());
        if let Some(cohort) = last
            && now < cohort.window_deadline
            && now < cohort.start_deadline
        {
            cohort.sessions.push(session_id);
            return;
        }
        let window = Duration::from_millis(self.config.sync.multi_select_window_ms);
        let deadline = Duration::from_millis(self.config.sync.start_deadline_ms);
        let cohort = SyncCohort {
            opened_at: now,
            window_deadline: now + window,
            start_deadline: now + deadline,
            sessions: vec![session_id],
            prepared: HashMap::new(),
        };
        if self.sync_cohort.is_none() {
            self.sync_cohort = Some(cohort);
        } else {
            self.pending_cohorts.push_back(cohort);
        }
        self.schedule_cohort_wake(window);
        self.schedule_cohort_wake(deadline);
    }

    async fn handle_prepared_downstream(&mut self, prepared: PreparedDownstream) {
        let Some(session) = self.sessions.get_mut(&prepared.session_id) else {
            return;
        };
        if session.generation != prepared.generation || !session.desired_playback {
            return;
        }
        session.prepared = Some(prepared.clone());
        for cohort in self
            .sync_cohort
            .iter_mut()
            .chain(self.pending_cohorts.iter_mut())
        {
            if cohort.sessions.contains(&prepared.session_id) {
                cohort.prepared.insert(prepared.session_id, prepared);
                self.maybe_start_sync_cohort(false).await;
                return;
            }
        }
        self.play_prepared_downstreams(vec![prepared]).await;
    }

    async fn maybe_start_sync_cohort(&mut self, force: bool) {
        loop {
            if self.sync_cohort.is_none() {
                self.sync_cohort = self.pending_cohorts.pop_front();
            }
            let Some(cohort) = self.sync_cohort.as_mut() else {
                return;
            };
            cohort.sessions.retain(|id| {
                self.sessions
                    .get(id)
                    .is_some_and(|session| session.desired_playback)
            });
            cohort.prepared.retain(|id, prepared| {
                cohort.sessions.contains(id)
                    && self
                        .sessions
                        .get(id)
                        .is_some_and(|session| session.generation == prepared.generation)
            });
            if cohort.sessions.is_empty() {
                self.sync_cohort = None;
                continue;
            }
            if !sync_cohort_should_start(cohort, Instant::now(), force) {
                return;
            }
            let cohort = self.sync_cohort.take().expect("cohort exists");
            let prepared = cohort
                .sessions
                .iter()
                .filter_map(|id| cohort.prepared.get(id).cloned())
                .collect();
            info!(
                age_ms = cohort.opened_at.elapsed().as_millis(),
                "releasing prepared sync cohort"
            );
            self.play_prepared_downstreams(prepared).await;
            // A promoted cohort may already have an expired deadline.
        }
    }

    fn apply_sync_anchors(&self, prepared: &[PreparedDownstream]) {
        let rooms = prepared
            .iter()
            .map(|stream| (stream.zone_id.clone(), stream.renderer.name().to_owned()))
            .collect::<Vec<_>>();
        let delays = configured_delays(
            &rooms,
            &self.config.sync.zone_offsets_ms,
            self.config.sync.default_offset_ms,
        );
        let common_sample = Instant::now() + Duration::from_millis(120);
        for stream in prepared {
            if stream.live_stream.session.codec == StreamCodec::Wav {
                let delay = delays
                    .iter()
                    .find(|delay| delay.zone_id == stream.zone_id)
                    .map_or(0, |delay| delay.delay_ms);
                stream
                    .live_stream
                    .set_playback_plan(common_sample, common_sample + Duration::from_millis(delay));
            } else if prepared.len() > 1
                || self.config.sync.default_offset_ms != 0
                || !self.config.sync.zone_offsets_ms.is_empty()
            {
                warn!(session_id = %stream.session_id, codec = ?stream.live_stream.session.codec,
                    "MP3 does not support sample-aligned WAV offsets; group startup is best effort");
            }
        }
    }

    async fn play_prepared_downstreams(&mut self, prepared: Vec<PreparedDownstream>) {
        self.apply_sync_anchors(&prepared);
        for stream in prepared {
            if let Some(worker) = self.worker(&stream.zone_id) {
                worker.command(TransportCommand::Play(Box::new(stream)));
            }
        }
    }

    async fn start_session(
        &mut self,
        session_id: SessionId,
        zone_id: ZoneId,
        format: PcmFormat,
    ) -> anyhow::Result<()> {
        let old_sessions: Vec<_> = self
            .sessions
            .iter()
            .filter_map(|(id, session)| (session.zone_id == zone_id).then_some(*id))
            .collect();
        for old_id in old_sessions {
            self.stop_session(old_id, None).await?;
        }
        anyhow::ensure!(
            self.renderers.contains_key(&zone_id),
            "no renderer for zone {zone_id}"
        );
        self.sessions
            .insert(session_id, SessionRuntime::new(zone_id.clone(), format));
        self.restart_downstream_for_play(session_id, zone_id).await
    }

    fn retire_encoder(&mut self, encoder: Option<FfmpegEncoder>) {
        if let Some(encoder) = encoder {
            self.tasks.spawn(async move {
                if let Err(error) = encoder.shutdown().await {
                    warn!("encoder shutdown failed: {error}");
                }
            });
        }
    }

    async fn restart_downstream_for_play(
        &mut self,
        session_id: SessionId,
        zone_id: ZoneId,
    ) -> anyhow::Result<()> {
        self.cancel_downstream_retry(session_id);
        let Some(session) = self.sessions.get_mut(&session_id) else {
            return Ok(());
        };
        let format = session.format;
        session.reset_needed = true;
        session.observed = ObservedPlayback::Unknown;
        session.prepared = None;
        let encoder = session.encoder.take();
        debug!(%session_id, playback_epoch = session.playback_epoch, "replacing downstream generation");
        self.registry.remove(&session_id).await;
        self.retire_encoder(encoder);
        let generation = self.next_downstream_generation(session_id);
        let Some(renderer) = self.renderers.get(&zone_id).cloned() else {
            return Ok(());
        };
        // Invalidate queued/in-flight preparation before attempting encoder construction.
        if let Some(worker) = self.worker(&zone_id) {
            worker.command(TransportCommand::Stop {
                session_id,
                zone_id: zone_id.clone(),
                generation,
            });
        }
        let (live_stream, encoder, local_url) = match self
            .create_downstream_stream(
                session_id,
                zone_id.clone(),
                renderer.ip(),
                format,
                generation,
            )
            .await
        {
            Ok(created) => created,
            Err(error) => {
                warn!(%session_id, "encoder construction failed: {error:#}");
                self.schedule_downstream_retry(session_id, zone_id, generation);
                return Ok(());
            }
        };
        self.sessions
            .get_mut(&session_id)
            .expect("session exists")
            .encoder = Some(encoder);
        self.add_session_to_sync_cohort(session_id);
        let start = StreamPrepare {
            session_id,
            zone_id: zone_id.clone(),
            generation,
            force_standalone_on_start: self.config.sonos.force_standalone_on_start
                && matches!(&renderer, Renderer::Sonos { zone, .. } if !zone.is_group_coordinator),
            renderer,
            live_stream,
            local_url,
            prepared_tx: self.prepared_tx.clone(),
            result_tx: self.downstream_result_tx.clone(),
        };
        if let Some(worker) = self.worker(&zone_id) {
            worker.command(TransportCommand::Prepare(Box::new(start)));
        }
        self.maybe_start_sync_cohort(false).await;
        Ok(())
    }

    async fn set_playback_state(
        &mut self,
        session_id: SessionId,
        zone_id: ZoneId,
        playing: bool,
    ) -> anyhow::Result<()> {
        let Some(session) = self.sessions.get_mut(&session_id) else {
            return Ok(());
        };
        let previous = session.desired_playback;
        session.desired_playback = playing;
        if playing {
            let restart = should_restart_downstream_for_play(Some(previous), session.reset_needed);
            session.retry_attempts = 0;
            self.cancel_paused_cleanup(session_id);
            if restart {
                self.restart_downstream_for_play(session_id, zone_id)
                    .await?;
            }
        } else {
            session.reset_needed = true;
            session.observed = ObservedPlayback::Unknown;
            session.prepared = None;
            self.cancel_downstream_retry(session_id);
            let generation = self.next_downstream_generation(session_id);
            if let Some(worker) = self.worker(&zone_id) {
                worker.command(TransportCommand::Stop {
                    session_id,
                    zone_id,
                    generation,
                });
            }
            self.maybe_start_sync_cohort(false).await;
        }
        Ok(())
    }

    async fn stop_session(
        &mut self,
        session_id: SessionId,
        fallback_zone_id: Option<ZoneId>,
    ) -> anyhow::Result<()> {
        let Some(mut session) = self.sessions.remove(&session_id) else {
            return Ok(());
        };
        let zone_id = fallback_zone_id.unwrap_or_else(|| session.zone_id.clone());
        self.registry.remove(&session_id).await;
        self.retire_encoder(session.encoder.take());
        if let Some(worker) = self.worker(&zone_id) {
            worker.command(TransportCommand::Stop {
                session_id,
                zone_id: zone_id.clone(),
                generation: session.generation,
            });
        }
        self.maybe_start_sync_cohort(false).await;
        info!(%session_id, "bridge session stopped");
        Ok(())
    }

    async fn shutdown(&mut self) {
        let session_ids: Vec<_> = self.sessions.keys().copied().collect();
        for session_id in session_ids {
            let _ = self.stop_session(session_id, None).await;
        }
        let mut workers = tokio::task::JoinSet::new();
        for (_, worker) in self.zone_workers.drain() {
            workers.spawn(worker.shutdown());
        }
        while workers.join_next().await.is_some() {}
        self.tasks.shutdown().await;
    }
}

async fn stream_url_for_zone(
    config: &Config,
    renderer_ip: Option<IpAddr>,
    session_id: SessionId,
    codec: StreamCodec,
    generation: u64,
) -> anyhow::Result<Url> {
    let needs_inference =
        config.server.advertise_addr.is_none() && config.server.bind.is_unspecified();
    // Without a renderer IP, use the default route. Inside the Home Assistant app, the
    // route to Home Assistant itself is an internal Docker network LAN players cannot reach.
    let inferred_local_ip = if needs_inference {
        match renderer_ip {
            Some(ip) => local_ip_for_remote(ip).await,
            None => default_route_ip(config.server.bind).await,
        }
    } else {
        None
    };
    let host = resolve_stream_host(&config.server, renderer_ip, inferred_local_ip)?;
    let extension = match codec {
        StreamCodec::Mp3 => "mp3",
        StreamCodec::Aac => "aac",
        StreamCodec::Wav => "wav",
    };
    let stream_addr = SocketAddr::new(host, config.server.http_port);
    let mut url = Url::parse(&format!(
        "http://{stream_addr}/streams/{session_id}.{extension}"
    ))?;
    url.query_pairs_mut()
        .append_pair("gen", &generation.to_string());

    Ok(url)
}

fn should_restart_downstream_for_play(previous_playing: Option<bool>, reset_needed: bool) -> bool {
    previous_playing == Some(false) || reset_needed
}

fn sync_cohort_should_start(cohort: &SyncCohort, now: Instant, force: bool) -> bool {
    let window_closed = now >= cohort.window_deadline;
    let deadline_expired = now >= cohort.start_deadline;
    let all_prepared =
        !cohort.sessions.is_empty() && cohort.sessions.len() == cohort.prepared.len();
    force || all_prepared || (window_closed && deadline_expired)
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

/// Picks the stream URL host. `Config::validate` has already rejected advertise
/// addresses the stream listener cannot accept.
fn resolve_stream_host(
    server: &ServerConfig,
    renderer_ip: Option<IpAddr>,
    inferred_local_ip: Option<IpAddr>,
) -> anyhow::Result<IpAddr> {
    if let Some(advertise_addr) = server.advertise_addr {
        return Ok(advertise_addr);
    }
    if !server.bind.is_unspecified() {
        return Ok(server.bind);
    }
    inferred_local_ip.ok_or_else(|| {
        let target = renderer_ip.map_or_else(
            || "the default route".to_owned(),
            |ip| format!("the renderer at {ip}"),
        );
        anyhow::anyhow!(
            "cannot advertise stream: server.bind is unspecified and no local address routes \
             to {target}; set server.advertise_addr to this host's LAN IP"
        )
    })
}

/// Prefers IPv4. An IPv6 listener (`::`) also tries the IPv6 route, for IPv6-only hosts.
async fn default_route_ip(bind: IpAddr) -> Option<IpAddr> {
    let ipv4 = local_ip_for_remote(DEFAULT_ROUTE_PROBE_V4).await;
    if ipv4.is_some() || bind.is_ipv4() {
        return ipv4;
    }
    local_ip_for_remote(DEFAULT_ROUTE_PROBE_V6).await
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

    pub(crate) fn prepared_downstream(
        session_id: SessionId,
        zone_id: ZoneId,
        codec: StreamCodec,
    ) -> PreparedDownstream {
        PreparedDownstream {
            session_id,
            zone_id: zone_id.clone(),
            generation: 1,
            renderer: sonos(
                zone_id.clone(),
                SonosClient::from_base_url(Url::parse("http://127.0.0.1:1400").expect("sonos url"))
                    .expect("client"),
            ),
            live_stream: live_stream_for(session_id, zone_id, codec),
        }
    }

    pub(crate) fn sonos(id: ZoneId, client: SonosClient) -> Renderer {
        Renderer::Sonos {
            zone: zone(id),
            client,
        }
    }

    pub(crate) fn zone(id: ZoneId) -> SonosZone {
        SonosZone {
            rincon_id: id.to_string(),
            id,
            room_name: "Test".into(),
            ip: "127.0.0.1".parse().unwrap(),
            model: "fake".into(),
            is_visible_room: true,
            is_group_coordinator: true,
        }
    }

    #[tokio::test]
    async fn flush_closes_old_generation_and_rejects_late_old_pcm() {
        let mut runtime = runtime();
        runtime.config.stream.codec = "wav".into();
        runtime.config.server.bind = "127.0.0.1".parse().unwrap();
        let id = SessionId::new();
        let zone = zone_id();
        let mut session = SessionRuntime::new(zone.clone(), format());
        session.desired_playback = false;
        runtime.sessions.insert(id, session);
        let old = live_stream_for(id, zone.clone(), StreamCodec::Wav);
        runtime.registry.insert(old.clone()).await;
        runtime
            .handle_event(AirPlayEvent::Flushed {
                session_id: id,
                zone_id: zone.clone(),
                playback_epoch: 1,
            })
            .await
            .unwrap();
        assert!(old.is_closed());
        assert!(runtime.registry.is_empty().await);
        assert_eq!(runtime.sessions[&id].playback_epoch, 1);
        let (new, encoder, _) = runtime
            .create_downstream_stream(
                id,
                zone.clone(),
                Some("127.0.0.1".parse().unwrap()),
                format(),
                2,
            )
            .await
            .unwrap();
        let (header, mut output) = new.attach_subscriber();
        if header.is_none() {
            assert_eq!(output.recv().await.unwrap().bytes.len(), 44);
        }
        new.arm_playback_anchor_on_next_timed_pcm();
        runtime.sessions.get_mut(&id).unwrap().encoder = Some(encoder);
        for epoch in [0, 1] {
            runtime
                .handle_event(AirPlayEvent::Pcm {
                    session_id: id,
                    zone_id: zone.clone(),
                    frames: {
                        let queue =
                            airsonos2_core::PcmQueue::new(44_100, 2, Duration::from_millis(250));
                        queue.push(airsonos2_core::PcmFrame {
                            buffered_permit: None,
                            playback_epoch: epoch,
                            sample_rate: 44100,
                            channels: 2,
                            samples_f32_interleaved: vec![if epoch == 0 { -1.0 } else { 1.0 }; 2],
                            presentation_time: None,
                        });
                        queue
                    },
                })
                .await
                .unwrap();
        }
        let pcm = tokio::time::timeout(Duration::from_secs(1), output.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pcm.bytes.as_ref(), &[255, 127, 255, 127]);
        assert!(output.try_recv().is_err());
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn delayed_room_transport_does_not_suspend_other_room_pcm() {
        use crate::bridge::tests::FakeSonos;
        let mut fake = FakeSonos::start().await;
        let mut runtime = runtime();
        let a = zone_id();
        let b = ZoneId::new("ROOM_B");
        runtime
            .renderers
            .insert(a.clone(), sonos(a.clone(), fake.client.clone()));
        let session_a = SessionId::new();
        let mut prepared = prepared_downstream(session_a, a.clone(), StreamCodec::Mp3);
        prepared.renderer = sonos(a.clone(), fake.client.clone());
        runtime.play_prepared_downstreams(vec![prepared]).await;
        let (request, hold_play) = fake.request().await;
        assert!(request.contains("#Play"));
        let session_b = SessionId::new();
        let live = live_stream_for(session_b, b.clone(), StreamCodec::Wav);
        live.set_playback_anchor(Instant::now());
        let (_, mut audio) = live.attach_subscriber();
        let encoder = FfmpegEncoder::spawn(
            FfmpegEncoderConfig {
                ffmpeg_path: PathBuf::new(),
                sample_rate: 44_100,
                channels: 2,
                mp3_bitrate_kbps: 192,
                codec: StreamCodec::Wav,
                queue_duration: Duration::from_secs(3),
            },
            live,
        )
        .unwrap();
        let mut session = SessionRuntime::new(b.clone(), format());
        session.encoder = Some(encoder);
        runtime.sessions.insert(session_b, session);
        for stop in [false, true] {
            if stop {
                hold_play.send(()).unwrap();
                runtime.worker(&a).unwrap().command(TransportCommand::Stop {
                    session_id: session_a,
                    zone_id: a.clone(),
                    generation: 1,
                });
                let (request, hold_stop) = fake.request().await;
                assert!(request.contains("#Stop"));
                if let Some(stream) = runtime.registry.get(&session_b).await {
                    stream.arm_playback_anchor_on_next_timed_pcm();
                }
                feed_pcm(&mut runtime, session_b, b.clone()).await;
                let bytes = tokio::time::timeout(Duration::from_secs(1), audio.recv())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(bytes.bytes.len(), 16);
                hold_stop.send(()).unwrap();
                break;
            }
            if let Some(stream) = runtime.registry.get(&session_b).await {
                stream.arm_playback_anchor_on_next_timed_pcm();
            }
            feed_pcm(&mut runtime, session_b, b.clone()).await;
            let header = tokio::time::timeout(Duration::from_secs(1), audio.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(&header.bytes[..4], b"RIFF");
            let pcm = tokio::time::timeout(Duration::from_secs(1), audio.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(pcm.bytes.len(), 16);
        }
        runtime.shutdown().await;
    }

    async fn feed_pcm(runtime: &mut BridgeRuntime, session_id: SessionId, zone_id: ZoneId) {
        tokio::time::timeout(
            Duration::from_millis(100),
            runtime.handle_event(AirPlayEvent::Pcm {
                session_id,
                zone_id,
                frames: {
                    let queue =
                        airsonos2_core::PcmQueue::new(44_100, 2, Duration::from_millis(250));
                    queue.push(airsonos2_core::PcmFrame {
                        buffered_permit: None,
                        playback_epoch: 0,
                        sample_rate: 44_100,
                        channels: 2,
                        samples_f32_interleaved: vec![0.25; 8],
                        presentation_time: None,
                    });
                    queue
                },
            }),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    async fn stopping_active_cohort_promotes_expired_pending_cohort() {
        let mut fake = crate::bridge::tests::FakeSonos::start().await;
        let mut runtime = runtime();
        let a = SessionId::new();
        let b = SessionId::new();
        let room_b = ZoneId::new("ROOM_B");
        runtime
            .renderers
            .insert(room_b.clone(), sonos(room_b.clone(), fake.client.clone()));
        runtime
            .sessions
            .insert(a, SessionRuntime::new(zone_id(), format()));
        let mut session = SessionRuntime::new(room_b.clone(), format());
        session.generation = 1;
        runtime.sessions.insert(b, session);
        runtime.add_session_to_sync_cohort(a);
        runtime.sync_cohort.as_mut().unwrap().window_deadline = Instant::now();
        runtime.add_session_to_sync_cohort(b);
        let mut prepared = prepared_downstream(b, room_b.clone(), StreamCodec::Mp3);
        prepared.renderer = sonos(room_b, fake.client.clone());
        let pending = runtime.pending_cohorts.front_mut().unwrap();
        pending.start_deadline = Instant::now();
        pending.prepared.insert(b, prepared);
        runtime.stop_session(a, None).await.unwrap();
        assert!(runtime.sync_cohort.is_none());
        assert!(runtime.pending_cohorts.is_empty());
        let (request, release) = fake.request().await;
        assert!(request.contains("#Play"));
        release.send(()).unwrap();
    }

    #[tokio::test]
    async fn encoder_construction_failure_retries_and_stale_retry_cannot_revive_stop() {
        let mut runtime = runtime();
        runtime.config.stream.ffmpeg_path = PathBuf::from("/nonexistent/ffmpeg");
        runtime.config.server.bind = "127.0.0.1".parse().unwrap();
        let id = zone_id();
        runtime.renderers.insert(
            id.clone(),
            sonos(
                id.clone(),
                SonosClient::new("127.0.0.1".parse().unwrap()).unwrap(),
            ),
        );
        let session_id = SessionId::new();
        runtime
            .start_session(session_id, id.clone(), format())
            .await
            .unwrap();
        assert!(runtime.registry.is_empty().await);
        assert_eq!(runtime.sessions[&session_id].retry_attempts, 1);
        let generation = runtime.sessions[&session_id].generation;
        runtime.config.stream.codec = "wav".into();
        runtime
            .handle_downstream_retry(DownstreamRetry {
                session_id,
                zone_id: id.clone(),
                generation,
            })
            .await
            .unwrap();
        assert!(runtime.sessions[&session_id].encoder.is_some());
        assert!(runtime.registry.get(&session_id).await.is_some());
        runtime.stop_session(session_id, None).await.unwrap();
        runtime
            .handle_downstream_retry(DownstreamRetry {
                session_id,
                zone_id: id,
                generation,
            })
            .await
            .unwrap();
        assert!(!runtime.sessions.contains_key(&session_id));
        assert!(runtime.registry.is_empty().await);
    }

    #[tokio::test]
    async fn unknown_play_retries_same_generation() {
        let mut runtime = runtime();
        let id = zone_id();
        let session_id = SessionId::new();
        let prepared = prepared_downstream(session_id, id.clone(), StreamCodec::Mp3);
        let mut session = SessionRuntime::new(id.clone(), format());
        session.generation = 7;
        session.prepared = Some(prepared);
        runtime.sessions.insert(session_id, session);
        runtime.handle_downstream_start_result(DownstreamStartResult {
            session_id,
            zone_id: id.clone(),
            generation: 7,
            outcome: DownstreamStartOutcome::Unknown,
        });
        assert_eq!(
            runtime.sessions[&session_id].observed,
            ObservedPlayback::Unknown
        );
        assert!(!runtime.sessions[&session_id].reset_needed);
        runtime
            .handle_downstream_retry(DownstreamRetry {
                session_id,
                zone_id: id,
                generation: 7,
            })
            .await
            .unwrap();
        assert_eq!(runtime.sessions[&session_id].generation, 7);
        assert!(runtime.sessions[&session_id].prepared.is_some());
    }

    #[tokio::test]
    async fn failed_encoder_start_does_not_publish_stream() {
        let mut runtime = runtime();
        runtime.config.stream.ffmpeg_path = PathBuf::from("/nonexistent/ffmpeg");
        runtime.config.server.bind = "127.0.0.1".parse().unwrap();
        let result = runtime
            .create_downstream_stream(
                SessionId::new(),
                ZoneId::new("TEST"),
                Some("127.0.0.1".parse().unwrap()),
                format(),
                1,
            )
            .await;
        assert!(result.is_err());
        assert!(runtime.registry.is_empty().await);
    }

    #[tokio::test]
    async fn pause_marks_session_as_needing_downstream_reset() {
        let mut runtime = runtime();
        let session_id = SessionId::new();
        let zone_id = zone_id();
        runtime
            .sessions
            .insert(session_id, SessionRuntime::new(zone_id.clone(), format()));

        runtime
            .set_playback_state(session_id, zone_id, false)
            .await
            .expect("pause");

        assert!(!runtime.sessions[&session_id].desired_playback);
        assert!(runtime.sessions[&session_id].reset_needed);
    }

    fn ip(value: &str) -> IpAddr {
        value.parse().expect("valid ip")
    }

    #[tokio::test]
    async fn stream_url_formats_ipv4_advertise_addr() {
        let mut config = Config::default();
        config.server.advertise_addr = Some(ip("192.0.2.5"));

        let url = stream_url_for_zone(
            &config,
            Some(ip("192.0.2.50")),
            SessionId::new(),
            StreamCodec::Mp3,
            7,
        )
        .await
        .expect("stream url");

        assert_eq!(url.host_str(), Some("192.0.2.5"));
        assert_eq!(url.port(), Some(7000));
        assert!(url.as_str().starts_with("http://192.0.2.5:7000/streams/"));
        assert_eq!(url.query(), Some("gen=7"));
    }

    #[tokio::test]
    async fn stream_url_brackets_ipv6_advertise_addr() {
        let mut config = Config::default();
        config.server.bind = ip("::");
        config.server.advertise_addr = Some(ip("2001:db8::5"));

        let url = stream_url_for_zone(
            &config,
            Some(ip("2001:db8::50")),
            SessionId::new(),
            StreamCodec::Wav,
            11,
        )
        .await
        .expect("stream url");

        assert_eq!(url.host_str(), Some("[2001:db8::5]"));
        assert_eq!(url.port(), Some(7000));
        assert!(
            url.as_str()
                .starts_with("http://[2001:db8::5]:7000/streams/")
        );
        assert_eq!(url.query(), Some("gen=11"));
    }

    #[test]
    fn resolve_stream_host_prefers_explicit_advertise_addr() {
        let server = ServerConfig {
            advertise_addr: Some(ip("192.0.2.5")),
            bind: ip("192.0.2.99"),
            ..ServerConfig::default()
        };

        let host = resolve_stream_host(&server, Some(ip("192.0.2.50")), Some(ip("192.0.2.1")))
            .expect("host");

        assert_eq!(host, ip("192.0.2.5"));
    }

    #[test]
    fn resolve_stream_host_uses_explicit_bind() {
        let server = ServerConfig {
            bind: ip("192.0.2.7"),
            ..ServerConfig::default()
        };

        let host = resolve_stream_host(&server, Some(ip("192.0.2.50")), None).expect("host");

        assert_eq!(host, ip("192.0.2.7"));
    }

    #[test]
    fn resolve_stream_host_uses_inferred_local_ip_when_bind_is_unspecified() {
        let server = ServerConfig::default();

        let host = resolve_stream_host(&server, Some(ip("192.0.2.50")), Some(ip("192.0.2.1")))
            .expect("host");

        assert_eq!(host, ip("192.0.2.1"));
    }

    #[test]
    fn resolve_stream_host_fails_when_bind_is_unspecified_and_inference_fails() {
        let server = ServerConfig::default();

        let error =
            resolve_stream_host(&server, Some(ip("192.0.2.50")), None).expect_err("must fail");

        let message = error.to_string();
        assert!(message.contains("renderer at 192.0.2.50"));
        assert!(message.contains("server.advertise_addr"));
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

    #[tokio::test]
    async fn cohort_creation_and_joining_within_multi_select_window() {
        let mut runtime = runtime();
        let first = SessionId::new();
        let second = SessionId::new();

        runtime.add_session_to_sync_cohort(first);
        runtime.add_session_to_sync_cohort(second);

        let cohort = runtime.sync_cohort.as_ref().expect("cohort");
        assert_eq!(cohort.sessions, vec![first, second]);
    }

    #[test]
    fn all_prepared_cohort_starts_immediately() {
        let now = Instant::now();
        let session_id = SessionId::new();
        let mut cohort = SyncCohort {
            opened_at: now,
            window_deadline: now + Duration::from_secs(1),
            start_deadline: now + Duration::from_secs(3),
            sessions: vec![session_id],
            prepared: HashMap::new(),
        };
        cohort.prepared.insert(
            session_id,
            prepared_downstream(session_id, zone_id(), StreamCodec::Mp3),
        );

        assert!(sync_cohort_should_start(&cohort, now, false));
    }

    #[test]
    fn cohort_deadline_waits_until_window_and_start_deadline_pass() {
        let now = Instant::now();
        let cohort = SyncCohort {
            opened_at: now,
            window_deadline: now + Duration::from_millis(750),
            start_deadline: now + Duration::from_millis(2_500),
            sessions: vec![SessionId::new()],
            prepared: HashMap::new(),
        };

        assert!(!sync_cohort_should_start(
            &cohort,
            now + Duration::from_millis(800),
            false
        ));
        assert!(sync_cohort_should_start(
            &cohort,
            now + Duration::from_millis(2_600),
            false
        ));
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
    async fn successful_current_downstream_result_clears_reset_marker() {
        let mut runtime = runtime();
        let session_id = SessionId::new();
        let zone_id = zone_id();
        runtime
            .sessions
            .entry(session_id)
            .or_insert_with(|| SessionRuntime::new(zone_id.clone(), format()))
            .generation = 3;
        runtime.sessions.get_mut(&session_id).unwrap().reset_needed = true;
        runtime
            .sessions
            .get_mut(&session_id)
            .unwrap()
            .desired_playback = true;
        runtime
            .sessions
            .get_mut(&session_id)
            .unwrap()
            .retry_attempts = 2;

        runtime.handle_downstream_start_result(DownstreamStartResult {
            session_id,
            zone_id,
            generation: 3,
            outcome: DownstreamStartOutcome::Started,
        });

        assert!(!runtime.sessions[&session_id].reset_needed);
        assert!(runtime.sessions[&session_id].retry_attempts == 0);
    }

    #[tokio::test]
    async fn failed_current_downstream_result_keeps_reset_and_schedules_retry() {
        let mut runtime = runtime();
        let session_id = SessionId::new();
        let zone_id = zone_id();
        runtime
            .sessions
            .entry(session_id)
            .or_insert_with(|| SessionRuntime::new(zone_id.clone(), format()))
            .generation = 4;
        runtime
            .sessions
            .get_mut(&session_id)
            .unwrap()
            .desired_playback = true;

        runtime.handle_downstream_start_result(DownstreamStartResult {
            session_id,
            zone_id,
            generation: 4,
            outcome: DownstreamStartOutcome::Failed,
        });

        assert!(runtime.sessions[&session_id].reset_needed);
        assert!(runtime.sessions[&session_id].retry_task.is_some());
        runtime.cancel_downstream_retry(session_id);
    }

    #[tokio::test]
    async fn stale_downstream_result_is_ignored() {
        let mut runtime = runtime();
        let session_id = SessionId::new();
        let zone_id = zone_id();
        runtime
            .sessions
            .entry(session_id)
            .or_insert_with(|| SessionRuntime::new(zone_id.clone(), format()))
            .generation = 5;
        runtime.sessions.get_mut(&session_id).unwrap().reset_needed = true;
        runtime
            .sessions
            .get_mut(&session_id)
            .unwrap()
            .desired_playback = true;

        runtime.handle_downstream_start_result(DownstreamStartResult {
            session_id,
            zone_id,
            generation: 4,
            outcome: DownstreamStartOutcome::Started,
        });

        assert!(runtime.sessions[&session_id].reset_needed);
    }

    #[tokio::test]
    async fn stale_prepared_downstream_is_ignored() {
        let mut runtime = runtime();
        let session_id = SessionId::new();
        let zone_id = zone_id();
        runtime
            .sessions
            .entry(session_id)
            .or_insert_with(|| SessionRuntime::new(zone_id.clone(), format()))
            .generation = 2;
        runtime.add_session_to_sync_cohort(session_id);

        let mut prepared = prepared_downstream(session_id, zone_id, StreamCodec::Mp3);
        prepared.generation = 1;
        runtime.handle_prepared_downstream(prepared).await;

        assert!(
            runtime
                .sync_cohort
                .as_ref()
                .expect("cohort")
                .prepared
                .is_empty()
        );
    }

    #[tokio::test]
    async fn late_stream_ended_while_desired_playback_true_is_ignored() {
        let mut runtime = runtime();
        let session_id = SessionId::new();
        let zone_id = zone_id();
        runtime
            .sessions
            .insert(session_id, SessionRuntime::new(zone_id.clone(), format()));
        runtime
            .sessions
            .get_mut(&session_id)
            .unwrap()
            .desired_playback = true;

        runtime
            .handle_event(AirPlayEvent::StreamEndedWhilePaused {
                session_id,
                zone_id,
            })
            .await
            .expect("stream ended");

        assert!(!runtime.sessions[&session_id].reset_needed);
        assert!(runtime.sessions[&session_id].cleanup_task.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn duplicate_pause_keeps_paused_cleanup() {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let (cleanup_tx, cleanup_rx) = mpsc::unbounded_channel();
        let mut runtime = BridgeRuntime::new(
            Config::default(),
            StreamRegistry::new(),
            HashMap::new(),
            HashMap::new(),
            cleanup_tx,
        );
        let session_id = SessionId::new();
        let mut session = SessionRuntime::new(zone_id(), format());
        session.desired_playback = false;
        runtime.sessions.insert(session_id, session);
        events_tx
            .send(AirPlayEvent::StreamEndedWhilePaused {
                session_id,
                zone_id: zone_id(),
            })
            .unwrap();
        events_tx
            .send(AirPlayEvent::PlaybackState {
                session_id,
                zone_id: zone_id(),
                playing: false,
            })
            .unwrap();

        tokio::select! {
            _ = runtime.run(events_rx, cleanup_rx) => unreachable!("event sender is alive"),
            _ = tokio::time::sleep(Duration::from_secs(PAUSED_SESSION_GRACE_SECS + 1)) => {}
        }

        assert!(runtime.sessions.is_empty());
    }

    #[tokio::test]
    async fn play_after_exhausted_retries_restarts_downstream() {
        for outcome in [
            DownstreamStartOutcome::Failed,
            DownstreamStartOutcome::PermanentFailure,
        ] {
            let mut runtime = runtime();
            let session_id = SessionId::new();
            let zone_id = zone_id();
            let mut session = SessionRuntime::new(zone_id.clone(), format());
            session.generation = 3;
            session.retry_attempts = 6;
            session.prepared = Some(prepared_downstream(
                session_id,
                zone_id.clone(),
                StreamCodec::Mp3,
            ));
            runtime.sessions.insert(session_id, session);
            runtime.handle_downstream_start_result(DownstreamStartResult {
                session_id,
                zone_id: zone_id.clone(),
                generation: 3,
                outcome,
            });
            assert!(runtime.sessions[&session_id].retry_task.is_none());

            runtime
                .set_playback_state(session_id, zone_id, true)
                .await
                .unwrap();

            assert_ne!(runtime.sessions[&session_id].generation, 3, "{outcome:?}");
        }
    }

    #[tokio::test]
    async fn stop_clears_session_downstream_state_and_tasks() {
        let mut runtime = runtime();
        let session_id = SessionId::new();
        let zone_id = zone_id();
        runtime
            .sessions
            .insert(session_id, SessionRuntime::new(zone_id.clone(), format()));
        runtime.sessions.get_mut(&session_id).unwrap().generation = 8;
        runtime.schedule_paused_cleanup(session_id);

        runtime
            .stop_session(session_id, None)
            .await
            .expect("stop session");

        assert!(!runtime.sessions.contains_key(&session_id));
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
