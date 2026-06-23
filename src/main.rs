use std::{
    collections::{HashMap, HashSet},
    io::SeekFrom,
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    path::{Path, PathBuf},
    pin::Pin,
    process::Command,
    sync::{Arc, OnceLock},
    task::{Context as TaskContext, Poll},
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Context, anyhow};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{ConnectInfo, Path as AxumPath, RawQuery, State},
    http::{
        HeaderMap, HeaderValue, Method, StatusCode,
        header::{
            ACCEPT, ACCEPT_ENCODING, ACCEPT_LANGUAGE, ACCEPT_RANGES, CACHE_CONTROL, CONNECTION,
            CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, DATE, ETAG, HOST,
            IF_RANGE, LAST_MODIFIED, LOCATION, RANGE, SERVER, TRANSFER_ENCODING, USER_AGENT,
        },
    },
    response::{IntoResponse, Response},
    routing::{any, get},
};
use chrono::{SecondsFormat, Utc};
use futures_util::{Stream, TryStreamExt};
use librqbit::{
    AddTorrent, AddTorrentOptions, ConnectionOptions, ListenerOptions, Magnet,
    PeerConnectionOptions, Session, SessionOptions, api::TorrentIdOrHash,
};
use librqbit_core::torrent_metainfo::torrent_from_bytes;
use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str, utf8_percent_encode};
use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::time::{sleep, timeout};
use tokio_util::io::ReaderStream;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::{debug, error, info, warn};
use url::form_urlencoded;

const DEFAULT_HTTP_PORT: u16 = 11470;
const STARTUP_NAME: &str = "stremio-service-rs";
// How long to wait for torrent metadata (DHT + extension protocol handshake).
// Must be shorter than mpv/yt-dlp's 20 s read timeout so we can return a
// proper 503 error before the client gives up and marks the whole URL broken.
const STREAM_INIT_TIMEOUT: Duration = Duration::from_secs(14);
// How long to wait for stream() to open and seek() to complete.
// Both operations can block a tokio OS thread if pieces aren't on disk yet;
// we time them out separately so the runtime stays responsive.
const STREAM_OPEN_TIMEOUT: Duration = Duration::from_secs(10);
const CREATE_METADATA_GRACE: Duration = Duration::from_millis(1500);
const MULTI_USER_ENGINE_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(2 * 60);
const ENGINE_CLEANUP_INTERVAL: Duration = Duration::from_secs(1);
const CACHE_REAPER_INTERVAL: Duration = Duration::from_secs(60);

// ── Streaming window tuning ───────────────────────────────────────────────────
// How far ahead of the read cursor the download scheduler should prioritise.
const STREAM_WINDOW_FORWARD: u64 = 32 * 1024 * 1024; // 32 MB
// How far behind the cursor to keep in the priority window (handles small
// re-reads and codec look-behind).
const STREAM_WINDOW_BACKWARD: u64 = 4 * 1024 * 1024; // 4 MB
// When the read cursor is within this distance of the end of file we extend the
// backward window so the player's internal buffer can re-read freely.
const STREAM_WINDOW_EOF_ZONE: u64 = 64 * 1024 * 1024; // last 64 MB of file
const STREAM_WINDOW_EOF_BACKWARD: u64 = 32 * 1024 * 1024; // 32 MB backward near EOF
// Slide the window every time this many additional bytes have been read.
const STREAM_WINDOW_UPDATE_EVERY: u64 = 4 * 1024 * 1024; // every 4 MB
const OPENSUB_HASH_CHUNK_SIZE: u64 = 64 * 1024;

const PATH_SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}')
    .add(b'/');

const DEFAULT_TRACKERS: &[&str] = &[
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://open.stealth.si:80/announce",
    "https://torrent.tracker.durukanbal.com:443/announce",
    "udp://wepzone.net:6969/announce",
    "udp://tracker.wepzone.net:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://tracker.theoks.net:6969/announce",
    "udp://tracker.t-1.org:6969/announce",
    "udp://tracker.darkness.services:6969/announce",
    "udp://tracker-udp.gbitt.info:80/announce",
    "udp://t.overflow.biz:6969/announce",
    "udp://open.dstud.io:6969/announce",
    "udp://explodie.org:6969/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://bittorrent-tracker.e-n-c-r-y-p-t.net:1337/announce",
    "https://tracker.zhuqiy.com:443/announce",
    "https://tracker.pmman.tech:443/announce",
    "https://tracker.moeblog.cn:443/announce",
    "https://tracker.bt4g.com:443/announce",
];

static LOG_GUARD: OnceLock<tracing_appender::non_blocking::WorkerGuard> = OnceLock::new();

const CINEMETA_BASE_URL: &str = "https://v3-cinemeta.strem.io";
const METAHUB_BASE_URL: &str = "https://images.metahub.space";
const METAHUB_EPISODES_URL: &str = "https://episodes.metahub.space";
const CINEMETA_META_FIELDS: &[&str] = &[
    "imdb_id",
    "name",
    "genre",
    "director",
    "cast",
    "poster",
    "description",
    "trailers",
    "background",
    "logo",
    "imdbRating",
    "runtime",
    "genres",
    "releaseInfo",
];

#[derive(Clone)]
struct AppState {
    torrents: Arc<TorrentService>,
    base_url: Arc<RwLock<String>>,
    client: reqwest::Client,
    proxy_client: reqwest::Client,
    settings: Arc<SettingsStore>,
    local_addon: Arc<LocalAddonIndex>,
    downloads: Arc<DownloadManager>,
}

struct TorrentService {
    session: Arc<Session>,
    multi_user: bool,
    handles: RwLock<HashMap<String, Arc<librqbit::ManagedTorrent>>>,
    last_active: RwLock<HashMap<String, Instant>>,
    active_streams: RwLock<HashMap<String, usize>>,
    pending_magnets: RwLock<HashSet<String>>,
    selected_files: RwLock<HashMap<String, TorrentFileSelections>>,
    owner_torrents: RwLock<HashMap<String, String>>,
    torrent_owners: RwLock<HashMap<String, HashSet<String>>>,
    cache_dir: PathBuf,
}

#[derive(Debug)]
struct CacheEntry {
    key: String,
    path: PathBuf,
    size: u64,
    modified: SystemTime,
}

#[derive(Debug, Default)]
struct TorrentFileSelections {
    anonymous: HashSet<usize>,
    by_owner: HashMap<String, usize>,
}

impl TorrentFileSelections {
    fn all(&self) -> HashSet<usize> {
        self.anonymous
            .iter()
            .copied()
            .chain(self.by_owner.values().copied())
            .collect()
    }
}

struct SettingsStore {
    path: PathBuf,
    values: RwLock<Map<String, Value>>,
}

#[derive(Debug, Default)]
struct LocalAddonIndex {
    entries: RwLock<HashMap<String, LocalAddonEntry>>,
    last_scan: RwLock<Option<Instant>>,
}

#[derive(Clone, Debug)]
struct LocalAddonEntry {
    item_id: String,
    name: String,
    files: Vec<LocalAddonFile>,
    sources: Vec<String>,
    date_modified: Option<SystemTime>,
    source_key: String,
}

#[derive(Clone, Debug)]
struct LocalAddonFile {
    path: String,
    name: String,
    length: u64,
    idx: Option<usize>,
    parsed_name: Option<String>,
    media_type: Option<String>,
    imdb_id: Option<String>,
    season: Option<u32>,
    episode: Option<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ParsedVideoKind {
    Movie,
    Series,
}

impl ParsedVideoKind {
    fn as_cinemeta_type(self) -> &'static str {
        match self {
            Self::Movie => "movie",
            Self::Series => "series",
        }
    }
}

#[derive(Clone, Debug)]
struct ParsedVideoName {
    name: String,
    year: Option<i32>,
    season: Option<u32>,
    episode: Option<u32>,
    kind: ParsedVideoKind,
}

include!("runtime/settings.rs");

type BoxByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>;

/// Wraps the byte stream to:
///   1. Slide the librqbit download-priority window forward as bytes are read, so
///      the scheduler always knows where the player actually is.
///   2. Widen the backward window when the player is near the end of the file,
///      preventing re-fetches of content the player's own buffer may re-read.
///   3. Run an on-drop callback (to decrement the active-stream counter).
struct WindowTrackingStream {
    inner: BoxByteStream,
    on_drop: Option<Box<dyn FnOnce() + Send + 'static>>,
    handle: Arc<librqbit::ManagedTorrent>,
    file_idx: usize,
    file_len: u64,
    /// Absolute byte offset into the file where the current read cursor sits.
    position: u64,
    /// Value of `position` the last time we pushed a window update.
    last_update_at: u64,
}

impl Drop for WindowTrackingStream {
    fn drop(&mut self) {
        if let Some(cb) = self.on_drop.take() {
            cb();
        }
    }
}

impl Stream for WindowTrackingStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let poll = this.inner.as_mut().poll_next(cx);

        if let Poll::Ready(Some(Ok(ref bytes))) = poll {
            this.position += bytes.len() as u64;

            let bytes_since_update = this.position.saturating_sub(this.last_update_at);
            if bytes_since_update >= STREAM_WINDOW_UPDATE_EVERY {
                this.last_update_at = this.position;

                // Near the end of the file the player's internal buffer may
                // re-read content we have already served.  Widen the backward
                // window so those reads hit the disk cache, not the network.
                let near_eof = this.file_len.saturating_sub(this.position) < STREAM_WINDOW_EOF_ZONE;
                let backward = if near_eof {
                    STREAM_WINDOW_EOF_BACKWARD
                } else {
                    STREAM_WINDOW_BACKWARD
                };

                let _ = this.handle.update_streaming_window(
                    this.file_idx,
                    this.position,
                    backward,
                    STREAM_WINDOW_FORWARD,
                );
            }
        }

        poll
    }
}

#[derive(Debug)]
struct AppError(anyhow::Error);

impl From<anyhow::Error> for AppError {
    fn from(value: anyhow::Error) -> Self {
        Self(value)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        error!("{:#}", self.0);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            [(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            )],
            self.0.to_string(),
        )
            .into_response()
    }
}

type AppResult<T> = Result<T, AppError>;

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateTorrentRequest {
    #[serde(default)]
    announce: Vec<String>,
    #[serde(default)]
    file_must_include: Vec<String>,
    guess_file_idx: Option<Value>,
    file_idx: Option<isize>,
    connections: Option<usize>,
    path: Option<String>,
    #[serde(default)]
    initial_peers: Vec<String>,
    #[serde(default)]
    peers: Vec<String>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

#[derive(Debug, Default, Deserialize)]
struct CreateFromTorrentRequest {
    blob: Option<String>,
    from: Option<String>,
}

#[derive(Debug, Serialize)]
struct EngineStats {
    #[serde(rename = "infoHash")]
    info_hash: String,
    name: String,
    peers: usize,
    unchoked: usize,
    queued: usize,
    unique: usize,
    #[serde(rename = "connectionTries")]
    connection_tries: usize,
    #[serde(rename = "swarmPaused")]
    swarm_paused: bool,
    #[serde(rename = "swarmConnections")]
    swarm_connections: usize,
    #[serde(rename = "swarmSize")]
    swarm_size: usize,
    selections: Vec<Value>,
    wires: Option<Vec<Value>>,
    files: Vec<EngineFile>,
    downloaded: u64,
    uploaded: u64,
    #[serde(rename = "downloadSpeed")]
    download_speed: f64,
    #[serde(rename = "uploadSpeed")]
    upload_speed: f64,
    sources: Value,
    #[serde(rename = "peerSearchRunning")]
    peer_search_running: bool,
    opts: Value,
    state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    finished: Option<bool>,
    #[serde(rename = "streamLen", skip_serializing_if = "Option::is_none")]
    stream_len: Option<u64>,
    #[serde(rename = "streamName", skip_serializing_if = "Option::is_none")]
    stream_name: Option<String>,
    #[serde(rename = "streamProgress", skip_serializing_if = "Option::is_none")]
    stream_progress: Option<f64>,
    #[serde(rename = "guessedFileIdx", skip_serializing_if = "Option::is_none")]
    guessed_file_idx: Option<usize>,
}

#[derive(Clone, Debug, Serialize)]
struct EngineFile {
    path: String,
    name: String,
    length: u64,
    offset: u64,
    #[serde(rename = "__cacheEvents")]
    cache_events: bool,
}

#[derive(Debug, Clone)]
struct StreamQuery {
    external: bool,
    download: bool,
    subtitles_sec: Option<String>,
    trackers: Vec<String>,
    filters: Vec<String>,
}

#[derive(Clone, Debug)]
struct SubtitleCue {
    start_ms: i64,
    end_ms: i64,
    text: String,
}

#[derive(Debug, Deserialize)]
struct InfoHashPath {
    info_hash: String,
}

#[derive(Debug, Deserialize)]
struct StatsPath {
    info_hash: String,
    idx: String,
}

#[derive(Debug, Deserialize)]
struct StreamPath {
    info_hash: String,
    idx: String,
}

#[derive(Debug, Deserialize)]
struct StreamNamedPath {
    info_hash: String,
    idx: String,
    filename: String,
}

#[derive(Debug, Deserialize)]
struct SubtitleExt {
    ext: String,
}

#[tokio::main(worker_threads = 32)]
async fn main() -> anyhow::Result<()> {
    init_logging();
    let multi_user = std::env::args().any(|arg| arg == "--multi-user");
    info!(
        mode = if multi_user {
            "multi-user"
        } else {
            "single-user"
        },
        "torrent cleanup mode"
    );

    let app_path = server_app_path();
    let settings_path = server_settings_path(&app_path);
    let settings = Arc::new(SettingsStore {
        path: settings_path,
        values: RwLock::new(default_settings_values(&app_path)),
    });
    settings.load().await;
    {
        let mut values = settings.values.write().await;
        normalize_settings_values(&mut values, &app_path);
    }
    if let Err(err) = settings.save().await {
        warn!(error = %err, "initial settings save failed");
    }

    let cache_dir = {
        let settings_values = settings.values.read().await;
        cache_dir_from_settings(&*settings_values)
    };
    tokio::fs::create_dir_all(&cache_dir)
        .await
        .with_context(|| format!("creating cache dir {}", cache_dir.display()))?;

    let trackers = default_tracker_urls();

    let session = Session::new_with_opts(
        cache_dir.clone(),
        SessionOptions {
            listen: Some(ListenerOptions {
                listen_addr: (Ipv4Addr::UNSPECIFIED, 0).into(),
                enable_upnp_port_forwarding: true,
                ..Default::default()
            }),
            connect: Some(ConnectionOptions {
                peer_opts: Some(PeerConnectionOptions {
                    connect_timeout: Some(Duration::from_secs(8)),
                    read_write_timeout: Some(Duration::from_secs(30)),
                    keep_alive_interval: Some(Duration::from_secs(60)),
                }),
                ..Default::default()
            }),
            // fastresume_folder saves only the have-piece bitfield, not the
            // torrent list.  On restart the session does a fast spot-check
            // instead of a full SHA-1 pass, without auto-resuming any torrent.
            fastresume: true,
            fastresume_folder: Some(cache_dir.join("session")),
            // Lowered from 16: each init SHA-1s in a blocking thread-pool slot.
            concurrent_init_limit: Some(4),
            // Keep librqbit's blocking disk/checksum work below the runtime's
            // worker count so lightweight endpoints can still respond.
            runtime_worker_threads: Some(4),
            trackers,
            ..Default::default()
        },
    )
    .await
    .context("starting librqbit session")?;

    let torrents = Arc::new(TorrentService {
        session,
        multi_user,
        handles: RwLock::new(HashMap::new()),
        last_active: RwLock::new(HashMap::new()),
        active_streams: RwLock::new(HashMap::new()),
        pending_magnets: RwLock::new(HashSet::new()),
        selected_files: RwLock::new(HashMap::new()),
        owner_torrents: RwLock::new(HashMap::new()),
        torrent_owners: RwLock::new(HashMap::new()),
        cache_dir,
    });

    {
        let torrents_cleanup = torrents.clone();
        tokio::spawn(async move {
            loop {
                sleep(ENGINE_CLEANUP_INTERVAL).await;
                torrents_cleanup.cleanup_inactive().await;
            }
        });
    }

    {
        let cache_reaper = torrents.clone();
        let settings_for_reaper = settings.clone();
        tokio::spawn(async move {
            cache_reaper
                .cleanup_cache_to_limit(&settings_for_reaper)
                .await;
            loop {
                sleep(CACHE_REAPER_INTERVAL).await;
                cache_reaper
                    .cleanup_cache_to_limit(&settings_for_reaper)
                    .await;
            }
        });
    }

    let listener = bind_http_listener(http_start_port(), 5).await?;
    let addr = listener.local_addr()?;
    let base_host = server_base_host(multi_user);
    let base_url = format!("http://{}:{}", base_host, addr.port());
    let client = reqwest::Client::builder()
        .user_agent("stremio-service-rs/0.1")
        .http1_only()
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(90))
        .build()
        .context("building HTTP client")?;
    let proxy_client = reqwest::Client::builder()
        .user_agent("stremio-service-rs/0.1")
        .http1_only()
        .redirect(reqwest::redirect::Policy::none())
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(90))
        .build()
        .context("building proxy HTTP client")?;
    let download_client = reqwest::Client::builder()
        .user_agent("stremio-service-rs/0.1")
        .http1_only()
        .connect_timeout(Duration::from_secs(15))
        .build()
        .context("building downloader HTTP client")?;
    let downloads = Arc::new(
        DownloadManager::new(
            app_path.join("downloads"),
            app_path.join("downManager.json"),
            download_client,
            torrents.clone(),
        )
        .await
        .context("starting downloader manager")?,
    );

    let state = AppState {
        torrents,
        base_url: Arc::new(RwLock::new(base_url.clone())),
        client,
        proxy_client,
        settings,
        local_addon: Arc::new(LocalAddonIndex::default()),
        downloads,
    };

    let app = router(state);

    info!("{STARTUP_NAME} listening on {base_url}");
    // this line is needed for the service to work with non-modified Stremio clients, as they rely on it to detect the server and get its URL
    println!("EngineFS server started at {base_url}");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .context("serving HTTP")?;
    Ok(())
}

fn init_logging() {
    let filter = std::env::var("RUST_LOG")
        .unwrap_or_else(|_| "stremio_service_rs=debug,librqbit=info,tower_http=warn".to_string());
    let (non_blocking_writer, guard) = tracing_appender::non_blocking(std::io::stdout());
    let _ = LOG_GUARD.set(guard);
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(non_blocking_writer)
        .with_target(false)
        .compact()
        .init();
}

fn router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
            Method::HEAD,
        ])
        .allow_headers(Any)
        .max_age(Duration::from_secs(1_728_000));

    Router::new()
        .route("/", get(root))
        .route("/favicon.ico", get(favicon))
        .route("/heartbeat", get(heartbeat))
        .route("/settings", get(get_settings).post(post_settings))
        .route("/network-info", get(network_info))
        .route("/casting", get(casting))
        .route("/device-info", get(device_info))
        .route("/local-addon/manifest.json", get(local_addon_manifest))
        .route("/local-addon/{*rest}", get(local_addon_dispatch))
        .route("/hwaccel-profiler", get(hwaccel_profiler))
        .route("/get-https", get(get_https))
        .route("/proxy/{opts}", any(proxy_root))
        .route("/proxy/{opts}/{*pathname}", any(proxy_path))
        .route("/opensubHash", get(opensub_hash))
        .route("/subtitlesTracks", get(subtitles_tracks))
        .route("/subtitles.{ext}", get(subtitles_proxy))
        .route("/probe", any(probe))
        .route("/hlsv2/probe", any(probe))
        .route("/stats.json", get(global_stats))
        .route("/removeAll", get(remove_all))
        .route("/create", any(create_from_torrent))
        .route("/{server_key}/downloader/getAll", get(downloader_get_all))
        .route("/{server_key}/downloader/get", get(downloader_get))
        .route("/{server_key}/downloader/add", get(downloader_add))
        .route("/{server_key}/downloader/pause", get(downloader_pause))
        .route("/{server_key}/downloader/resume", get(downloader_resume))
        .route("/{server_key}/downloader/remove", get(downloader_remove))
        .route("/{server_key}/downloader/stream", get(downloader_stream))
        .route("/{info_hash}/create", any(create_magnet))
        .route("/{info_hash}/stats.json", get(torrent_stats))
        .route("/{info_hash}/remove", get(remove_torrent))
        .route("/{info_hash}/{idx}/stats.json", get(torrent_file_stats))
        .route("/{info_hash}/{idx}", get(stream_short))
        .route("/{info_hash}/{idx}/{*filename}", get(stream_named))
        .fallback(fallback)
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}

async fn bind_http_listener(start_port: u16, attempts: u16) -> anyhow::Result<TcpListener> {
    for port in start_port..start_port + attempts {
        match TcpListener::bind((IpAddr::V4(Ipv4Addr::UNSPECIFIED), port)).await {
            Ok(listener) => return Ok(listener),
            Err(err) => warn!(port, error = %err, "HTTP port unavailable"),
        }
    }
    anyhow::bail!(
        "no free HTTP port in {}..{}",
        start_port,
        start_port + attempts
    );
}

fn server_app_path() -> PathBuf {
    if let Ok(dir) = std::env::var("APP_PATH") {
        let trimmed = dir.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }

    let pkg_name = "stremio-server";

    if cfg!(target_os = "linux") {
        std::env::var("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir())
            .join(format!(".{pkg_name}"))
    } else if cfg!(target_os = "macos") {
        std::env::var("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir())
            .join("Library")
            .join("Application Support")
            .join(pkg_name)
    } else if cfg!(target_os = "windows") {
        std::env::var("APPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir())
            .join("stremio")
            .join(pkg_name)
    } else {
        std::env::temp_dir().join(pkg_name)
    }
}

fn server_settings_path(app_path: &Path) -> PathBuf {
    let settings_dir = std::env::var("SETTINGS_PATH")
        .ok()
        .map(|dir| dir.trim().to_string())
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| app_path.to_path_buf());
    settings_dir.join("server-settings.json")
}

fn default_settings_values(app_path: &Path) -> Map<String, Value> {
    let app_path = app_path.to_string_lossy().to_string();
    let cache_root = app_path.clone();
    let cache_size = if std::env::var("DISABLE_CACHING").is_ok() {
        json!(0)
    } else {
        json!(2147483648u64)
    };

    serde_json::Map::from_iter([
        (
            "serverVersion".to_string(),
            json!(env!("CARGO_PKG_VERSION")),
        ),
        ("appPath".to_string(), json!(app_path)),
        ("cacheRoot".to_string(), json!(cache_root)),
        ("cacheSize".to_string(), cache_size),
        ("btMaxConnections".to_string(), json!(55)),
        ("btHandshakeTimeout".to_string(), json!(20000)),
        ("btRequestTimeout".to_string(), json!(4000)),
        ("btDownloadSpeedSoftLimit".to_string(), json!(2621440)),
        ("btDownloadSpeedHardLimit".to_string(), json!(3670016)),
        ("btMinPeersForStable".to_string(), json!(5)),
        ("remoteHttps".to_string(), json!("")),
        ("localAddonEnabled".to_string(), json!(false)),
        ("transcodeHorsepower".to_string(), json!(0.75)),
        ("transcodeMaxBitRate".to_string(), json!(0)),
        ("transcodeConcurrency".to_string(), json!(1)),
        ("transcodeTrackConcurrency".to_string(), json!(1)),
        ("transcodeHardwareAccel".to_string(), json!(true)),
        ("transcodeProfile".to_string(), Value::Null),
        ("allTranscodeProfiles".to_string(), json!([])),
        ("transcodeMaxWidth".to_string(), json!(1920)),
        ("proxyStreamsEnabled".to_string(), json!(false)),
    ])
}

fn normalize_settings_values(values: &mut Map<String, Value>, app_path: &Path) {
    let app_path_str = app_path.to_string_lossy().to_string();

    values.insert(
        "serverVersion".to_string(),
        Value::String(env!("CARGO_PKG_VERSION").to_string()),
    );

    values
        .entry("appPath".to_string())
        .and_modify(|v| {
            if !v.is_string() {
                *v = Value::String(app_path_str.clone());
            }
        })
        .or_insert_with(|| Value::String(app_path_str.clone()));

    let cache_root_is_valid = values
        .get("cacheRoot")
        .and_then(|v| v.as_str())
        .is_some_and(|s| !s.trim().is_empty());
    if !cache_root_is_valid {
        values.insert("cacheRoot".to_string(), Value::String(app_path_str.clone()));
    }

    if std::env::var("DISABLE_CACHING").is_ok() {
        values.insert("cacheSize".to_string(), json!(0));
    } else {
        let valid = matches!(
            values.get("cacheSize"),
            Some(Value::Number(_)) | Some(Value::Null)
        );
        if !valid {
            values.insert("cacheSize".to_string(), json!(2147483648u64));
        }
    }

    fn ensure_bool(values: &mut Map<String, Value>, key: &str, default: bool) {
        let valid = values.get(key).and_then(|v| v.as_bool()).is_some();
        if !valid {
            values.insert(key.to_string(), json!(default));
        }
    }
    fn ensure_string(values: &mut Map<String, Value>, key: &str, default: &str) {
        let valid = values.get(key).and_then(|v| v.as_str()).is_some();
        if !valid {
            values.insert(key.to_string(), json!(default));
        }
    }
    fn ensure_number(values: &mut Map<String, Value>, key: &str, default: Value) {
        let valid = matches!(values.get(key), Some(Value::Number(_)));
        if !valid {
            values.insert(key.to_string(), default);
        }
    }

    ensure_number(values, "btMaxConnections", json!(55));
    ensure_number(values, "btHandshakeTimeout", json!(20000));
    ensure_number(values, "btRequestTimeout", json!(4000));
    ensure_number(values, "btDownloadSpeedSoftLimit", json!(2621440));
    ensure_number(values, "btDownloadSpeedHardLimit", json!(3670016));
    ensure_number(values, "btMinPeersForStable", json!(5));
    ensure_string(values, "remoteHttps", "");
    ensure_bool(values, "localAddonEnabled", false);
    ensure_number(values, "transcodeHorsepower", json!(0.75));
    ensure_number(values, "transcodeMaxBitRate", json!(0));
    ensure_number(values, "transcodeConcurrency", json!(1));
    ensure_number(values, "transcodeTrackConcurrency", json!(1));
    ensure_bool(values, "transcodeHardwareAccel", true);
    values
        .entry("transcodeProfile".to_string())
        .or_insert(Value::Null);
    values
        .entry("allTranscodeProfiles".to_string())
        .or_insert_with(|| json!([]));
    ensure_number(values, "transcodeMaxWidth", json!(1920));
    ensure_bool(values, "proxyStreamsEnabled", false);
}

fn cache_dir_from_settings(values: &Map<String, Value>) -> PathBuf {
    if let Ok(dir) = std::env::var("STREMIO_SERVICE_CACHE") {
        let trimmed = dir.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }

    let cache_root = values
        .get("cacheRoot")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    if cache_root.is_empty() {
        return std::env::temp_dir().join("stremio-cache");
    }
    PathBuf::from(cache_root).join("stremio-cache")
}

fn cache_size_limit_from_settings(values: &Map<String, Value>) -> Option<u64> {
    match values.get("cacheSize") {
        Some(Value::Null) => None,
        Some(Value::Number(n)) => n.as_u64().or_else(|| {
            n.as_f64()
                .filter(|v| v.is_finite() && *v >= 0.0)
                .map(|v| v as u64)
        }),
        _ => Some(2147483648u64),
    }
}

fn http_start_port() -> u16 {
    std::env::var("STREMIO_SERVICE_PORT")
        .ok()
        .and_then(|port| port.parse().ok())
        .unwrap_or(DEFAULT_HTTP_PORT)
}

fn default_tracker_urls() -> HashSet<url::Url> {
    DEFAULT_TRACKERS
        .iter()
        .filter_map(|tracker| match url::Url::parse(tracker) {
            Ok(url) => Some(url),
            Err(err) => {
                warn!(tracker, error = %err, "ignoring invalid default tracker");
                None
            }
        })
        .collect()
}

fn discover_ipv4_interfaces() -> Vec<String> {
    let mut out = Vec::new();

    if let Ok(socket) = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)) {
        let _ = socket.connect((Ipv4Addr::new(8, 8, 8, 8), 80));
        if let Ok(addr) = socket.local_addr() {
            if let IpAddr::V4(ip) = addr.ip() {
                if !ip.is_loopback() {
                    out.push(ip.to_string());
                }
            }
        }
    }

    if out.is_empty() {
        out.push(Ipv4Addr::LOCALHOST.to_string());
    }
    out
}

fn server_base_host(multi_user: bool) -> String {
    if !multi_user {
        return Ipv4Addr::LOCALHOST.to_string();
    }

    discover_ipv4_interfaces()
        .into_iter()
        .next()
        .unwrap_or_else(|| Ipv4Addr::LOCALHOST.to_string())
}

include!("runtime/local_addon.rs");

include!("runtime/app_routes.rs");

include!("runtime/proxy.rs");

include!("runtime/opensub.rs");

include!("runtime/subtitles.rs");

include!("runtime/downloader.rs");

include!("runtime/torrent_http.rs");

include!("runtime/torrent_service.rs");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_user_server_uses_loopback() {
        assert_eq!(server_base_host(false), "127.0.0.1");
    }

    #[test]
    fn parses_srt_cues() {
        let cues = parse_subtitle_cues(
            "1\n00:00:01,250 --> 00:00:03,500\nHello\nworld\n\n2\n00:00:04,000 --> 00:00:05,000\nBye\n",
        )
        .unwrap();

        assert_eq!(cues.len(), 2);
        assert_eq!(cues[0].start_ms, 1250);
        assert_eq!(cues[0].end_ms, 3500);
        assert_eq!(cues[0].text, "Hello\nworld");
    }

    #[test]
    fn renders_vtt_with_offset() {
        let cues = vec![SubtitleCue {
            start_ms: 1_000,
            end_ms: 2_000,
            text: "A & B".to_string(),
        }];

        let rendered = render_subtitle_cues(&cues, SubtitleRenderFormat::Vtt, 500);
        assert!(rendered.starts_with("WEBVTT\n\n"));
        assert!(rendered.contains("\n0\n00:00:01.500 --> 00:00:02.500"));
        assert!(rendered.contains("00:00:01.500 --> 00:00:02.500"));
        assert!(rendered.contains("A &amp; B"));
    }

    #[test]
    fn parses_vtt_cues_with_ids_and_settings() {
        let cues =
            parse_subtitle_cues("WEBVTT\n\ncue-1\n00:01.000 --> 00:02.250 align:start\nHi\n")
                .unwrap();

        assert_eq!(cues.len(), 1);
        assert_eq!(cues[0].start_ms, 1_000);
        assert_eq!(cues[0].end_ms, 2_250);
        assert_eq!(cues[0].text, "Hi");
    }

    #[test]
    fn computes_opensub_chunk_sum_little_endian() {
        let bytes = [1u8, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(opensub_chunk_sum(&bytes), 3);
    }

    #[test]
    fn validates_playback_owner_header() {
        assert_eq!(
            normalize_playback_owner(" 3b8c8dc0-c8ac-408f-83b7-9f2b57082ada "),
            Some("3b8c8dc0-c8ac-408f-83b7-9f2b57082ada".to_string())
        );
        assert_eq!(normalize_playback_owner("bad owner"), None);
        assert_eq!(normalize_playback_owner("../bad"), None);
    }

    #[test]
    fn uses_header_or_ip_as_playback_owner() {
        let ipv4 = "192.0.2.4:1234".parse().unwrap();
        let ipv6 = "[2001:db8::4]:1234".parse().unwrap();
        let loopback = "127.0.0.1:1234".parse().unwrap();
        assert_eq!(playback_owner(Some("device_1"), None, ipv4), "device_1");
        assert_eq!(
            playback_owner(Some("bad owner"), None, ipv4),
            "ip-192.0.2.4"
        );
        assert_eq!(playback_owner(None, None, ipv6), "ip-2001_db8__4");
        assert_eq!(
            playback_owner(None, Some("198.51.100.7"), loopback),
            "ip-198.51.100.7"
        );
        assert_eq!(
            playback_owner(None, Some("198.51.100.7"), ipv4),
            "ip-192.0.2.4"
        );
        assert_eq!(
            playback_owner(None, Some("not-an-ip"), loopback),
            "ip-127.0.0.1"
        );
    }

    #[test]
    fn combines_file_selections_across_playback_owners() {
        let mut selections = TorrentFileSelections::default();
        selections.anonymous.insert(1);
        selections.by_owner.insert("one".to_string(), 2);
        selections.by_owner.insert("two".to_string(), 3);
        assert_eq!(selections.all(), HashSet::from([1, 2, 3]));

        selections.by_owner.insert("one".to_string(), 4);
        assert_eq!(selections.all(), HashSet::from([1, 3, 4]));
    }

    #[test]
    fn parses_content_range_total() {
        assert_eq!(
            parse_content_range_total("bytes 0-65535/2775200545"),
            Some(2775200545)
        );
        assert_eq!(
            parse_content_range_total("bytes */2775200545"),
            Some(2775200545)
        );
        assert_eq!(parse_content_range_total("bytes 0-1/*"), None);
    }

    #[test]
    fn rewrites_proxy_playlist_urls_like_server_js() {
        let body = "#EXTM3U\nsame/segment.ts\n/absolute/key.key\nhttps://cdn.example.net/video/seg.ts?x=1\n#EXT-X-KEY:METHOD=AES-128,URI=\"keys/file.key\"\n";
        let rewritten = proxy_rewrite_playlist(
            body,
            "/proxy/d=https%3A%2F%2Fexample.com&h=User-Agent%3ATest",
            &url::Url::parse("https://example.com").unwrap(),
            &["User-Agent:Test".to_string()],
        );

        assert!(rewritten.contains("\nsame/segment.ts\n"));
        assert!(rewritten.contains(
            "\n/proxy/d=https%3A%2F%2Fexample.com&h=User-Agent%3ATest/absolute/key.key\n"
        ));
        assert!(rewritten.contains(
            "\n/proxy/d=https%3A%2F%2Fcdn.example.net&h=User-Agent%3ATest/video/seg.ts?x=1\n"
        ));
        assert!(rewritten.contains("URI=\"keys/file.key\""));
    }

    #[test]
    fn parses_common_movie_and_series_release_names() {
        let movie = parse_video_filename(
            "Project.Hail.Mary.2026.iNTERNAL.1080p.10bit.WEBRip.2CH.x265.HEVC-PSA.mkv",
        )
        .unwrap();
        assert_eq!(movie.name, "Project Hail Mary");
        assert_eq!(movie.year, Some(2026));
        assert_eq!(movie.kind, ParsedVideoKind::Movie);

        let dotted = parse_video_filename("From.S04E03.1080p.WEB.H264-GRACE-HI.srt").unwrap();
        assert_eq!(dotted.name, "From");
        assert_eq!(dotted.season, Some(4));
        assert_eq!(dotted.episode, Some(3));
        assert_eq!(dotted.kind, ParsedVideoKind::Series);

        let x_style = parse_video_filename("Some.Show.1x02.HDTV.x264-GROUP.mkv").unwrap();
        assert_eq!(x_style.name, "Some Show");
        assert_eq!(x_style.season, Some(1));
        assert_eq!(x_style.episode, Some(2));

        let words =
            parse_video_filename("The Boys Season 5 Episode 5 One-Shots 720p AMZN WEB-DL.mkv")
                .unwrap();
        assert_eq!(words.name, "The Boys");
        assert_eq!(words.season, Some(5));
        assert_eq!(words.episode, Some(5));
    }

    #[test]
    fn scores_cinemeta_candidates_by_title_alias_and_year() {
        let parsed = parse_video_filename("Project.Hail.Mary.2026.WEBRip.1080p.H264.mkv").unwrap();
        let catalog = json!({
            "metas": [
                {
                    "id": "tt0000001",
                    "name": "Project Hail Mary",
                    "releaseInfo": "2025"
                },
                {
                    "id": "tt12042730",
                    "name": "Hail Mary",
                    "aliases": ["Project Hail Mary"],
                    "releaseInfo": "2026"
                }
            ]
        });

        assert_eq!(
            pick_cinemeta_search_result(&catalog, &parsed).as_deref(),
            Some("tt12042730")
        );
    }
}
