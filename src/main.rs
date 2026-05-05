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
    extract::{Path as AxumPath, RawQuery, State},
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
use futures_util::{TryStreamExt, stream::Stream};
use librqbit::{
    AddTorrent, AddTorrentOptions, ConnectionOptions, ListenerOptions, Magnet,
    PeerConnectionOptions, Session, SessionOptions, api::TorrentIdOrHash,
};
use librqbit_core::torrent_metainfo::torrent_from_bytes;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
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
const ENGINE_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(2 * 60);
const ENGINE_CLEANUP_INTERVAL: Duration = Duration::from_secs(10);
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
}

struct TorrentService {
    session: Arc<Session>,
    handles: RwLock<HashMap<String, Arc<librqbit::ManagedTorrent>>>,
    last_active: RwLock<HashMap<String, Instant>>,
    active_streams: RwLock<HashMap<String, usize>>,
    cache_dir: PathBuf,
}

#[derive(Debug)]
struct CacheEntry {
    key: String,
    path: PathBuf,
    size: u64,
    modified: SystemTime,
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

impl SettingsStore {
    async fn load(&self) {
        let bytes = match tokio::fs::read(&self.path).await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return,
            Err(err) => {
                warn!(path = %self.path.display(), error = %err, "reading settings failed");
                return;
            }
        };

        let loaded = match serde_json::from_slice::<Value>(&bytes) {
            Ok(value) => value,
            Err(err) => {
                warn!(path = %self.path.display(), error = %err, "parsing settings failed");
                return;
            }
        };

        let Some(map) = loaded.as_object() else {
            warn!(path = %self.path.display(), "settings file was not a JSON object");
            return;
        };

        let mut values = self.values.write().await;
        for (k, v) in map {
            values.insert(k.clone(), v.clone());
        }
    }

    async fn save(&self) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating settings dir {}", parent.display()))?;
        }
        let values = self.values.read().await;
        let text = serde_json::to_string_pretty(&Value::Object(values.clone()))
            .context("serializing settings")?;
        tokio::fs::write(&self.path, text)
            .await
            .with_context(|| format!("writing settings {}", self.path.display()))?;
        Ok(())
    }

    async fn update(&self, patch: Map<String, Value>, app_path: &Path) {
        {
            let mut values = self.values.write().await;
            for (k, v) in patch {
                values.insert(k, v);
            }
            normalize_settings_values(&mut values, app_path);
        }

        if let Err(err) = self.save().await {
            warn!(path = %self.path.display(), error = %err, "saving settings failed");
        }
    }

    async fn cache_size_limit(&self) -> Option<u64> {
        let values = self.values.read().await;
        cache_size_limit_from_settings(&values)
    }
}

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

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
            trackers,
            ..Default::default()
        },
    )
    .await
    .context("starting librqbit session")?;

    let torrents = Arc::new(TorrentService {
        session,
        handles: RwLock::new(HashMap::new()),
        last_active: RwLock::new(HashMap::new()),
        active_streams: RwLock::new(HashMap::new()),
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
    let base_host = discover_ipv4_interfaces()
        .into_iter()
        .next()
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let base_url = format!("http://{}:{}", base_host, addr.port());
    let state = AppState {
        torrents,
        base_url: Arc::new(RwLock::new(base_url.clone())),
        client: reqwest::Client::builder()
            .user_agent("stremio-service-rs/0.1")
            .http1_only()
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(90))
            .build()
            .context("building HTTP client")?,
        proxy_client: reqwest::Client::builder()
            .user_agent("stremio-service-rs/0.1")
            .http1_only()
            .redirect(reqwest::redirect::Policy::none())
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(90))
            .build()
            .context("building proxy HTTP client")?,
        settings,
        local_addon: Arc::new(LocalAddonIndex::default()),
    };

    let app = router(state);

    info!("{STARTUP_NAME} listening on {base_url}");
    // this line is needed for the service to work with non-modified Stremio clients, as they rely on it to detect the server and get its URL
    println!("EngineFS server started at {base_url}");
    axum::serve(listener, app).await.context("serving HTTP")?;
    Ok(())
}

fn init_logging() {
    let filter = std::env::var("RUST_LOG")
        .unwrap_or_else(|_| "stremio_service_rs=debug,librqbit=info,tower_http=warn".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(filter)
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

fn local_addon_resource_payload(resource: &str, media_type: &str, id: &str) -> Value {
    match resource {
        "catalog" => json!({ "metas": [] }),
        "meta" => json!({ "meta": null }),
        "stream" => json!({ "streams": [] }),
        "subtitles" => json!({ "subtitles": [] }),
        _ => json!({
            "resource": resource,
            "type": media_type,
            "id": id,
            "result": null
        }),
    }
}

async fn refresh_local_addon_index(state: &AppState) {
    let local_addon_enabled = {
        let values = state.settings.values.read().await;
        values
            .get("localAddonEnabled")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    };
    if !local_addon_enabled {
        return;
    }

    let should_scan = {
        let last = state.local_addon.last_scan.read().await;
        last.map(|instant| instant.elapsed() > Duration::from_secs(60))
            .unwrap_or(true)
    };
    if !should_scan {
        return;
    }

    let app_path = {
        let values = state.settings.values.read().await;
        values
            .get("appPath")
            .and_then(Value::as_str)
            .map(PathBuf::from)
            .unwrap_or_else(server_app_path)
    };

    let client = state.client.clone();
    let scanned = tokio::task::spawn_blocking(move || discover_local_addon_files(&app_path))
        .await
        .unwrap_or_else(|err| {
            warn!(error = %err, "local addon scan task failed");
            Vec::new()
        });

    let mut enriched = Vec::with_capacity(scanned.len());
    for mut entry in scanned {
        let mut found_imdb = false;
        for file in &mut entry.files {
            if let Some(parsed) = parse_video_filename(&file.name) {
                file.parsed_name = Some(parsed.name.clone());
                file.media_type = Some(parsed.kind.as_cinemeta_type().to_string());
                file.season = parsed.season;
                file.episode = parsed.episode;
                if let Ok(Some(meta)) = fetch_cinemeta_for_parsed(&client, &parsed).await {
                    if let Some(imdb_id) = meta
                        .get("imdb_id")
                        .or_else(|| meta.get("id"))
                        .and_then(Value::as_str)
                    {
                        file.imdb_id = Some(imdb_id.to_string());
                        found_imdb = true;
                        if !entry.item_id.starts_with("bt:") {
                            entry.item_id = format!("local:{imdb_id}");
                        }
                    }
                }
            }
        }
        if found_imdb {
            enriched.push(entry);
        }
    }

    let mut entries = state.local_addon.entries.write().await;
    entries.clear();
    for entry in enriched {
        entries.insert(entry.source_key.clone(), entry);
    }
    *state.local_addon.last_scan.write().await = Some(Instant::now());
}

async fn local_addon_bt_meta(state: &AppState, media_type: &str, id: &str) -> AppResult<Value> {
    let Some(info_hash) = id.strip_prefix("bt:") else {
        return Ok(json!({ "meta": null }));
    };
    let info_hash = normalize_info_hash(info_hash)?;
    refresh_local_addon_index(state).await;
    let indexed_entry = {
        let entries = state.local_addon.entries.read().await;
        entries
            .values()
            .find(|entry| entry.item_id == format!("bt:{info_hash}"))
            .cloned()
    };
    if let Some(entry) = indexed_entry {
        let meta = map_local_addon_entry_to_meta(&state.client, &entry, media_type)
            .await
            .unwrap_or_else(|| {
                json!({
                    "id": entry.item_id,
                    "type": media_type,
                    "name": entry.name,
                    "showAsVideos": true
                })
            });
        let now = Utc::now();
        let background = entry
            .files
            .iter()
            .find_map(|file| file.imdb_id.as_ref())
            .map(|imdb_id| format!("{METAHUB_BASE_URL}/background/medium/{imdb_id}/img"));
        let videos = entry
            .files
            .iter()
            .enumerate()
            .map(|(idx, file)| {
                local_addon_file_video(
                    &entry,
                    file,
                    idx,
                    now,
                    file.imdb_id.as_deref(),
                    background.as_deref(),
                )
            })
            .collect::<Vec<_>>();
        let mut meta = meta;
        if let Some(meta) = meta.as_object_mut() {
            meta.insert("id".into(), json!(entry.item_id));
            meta.insert("type".into(), json!(media_type));
            meta.insert("videos".into(), Value::Array(videos));
        }
        return Ok(json!({ "meta": meta }));
    }

    let handle = state
        .torrents
        .get_or_add_magnet(&info_hash, Vec::new(), None)
        .await?;

    let _ = timeout(STREAM_INIT_TIMEOUT, handle.wait_until_initialized()).await;
    let files = files_for_handle(&handle);
    if files.is_empty() {
        return Ok(json!({ "meta": null }));
    }

    let video_files = files
        .iter()
        .enumerate()
        .filter(|(_, file)| is_video_like(&file.name))
        .collect::<Vec<_>>();
    if video_files.is_empty() {
        return Ok(json!({ "meta": null }));
    }

    if let Some((file_idx, _)) = video_files.iter().max_by_key(|(_, file)| file.length) {
        if let Err(err) = state.torrents.select_only_file(&handle, *file_idx).await {
            debug!(info_hash, file_idx, error = %err.0, "local-addon default file selection failed");
        }
    }

    let now = Utc::now();
    let now_text = now.to_rfc3339_opts(SecondsFormat::Millis, true);
    let parsed_files = video_files
        .iter()
        .map(|(file_idx, file)| (*file_idx, *file, parse_video_filename(&file.name)))
        .collect::<Vec<_>>();
    let primary_parsed = parsed_files
        .iter()
        .max_by_key(|(_, file, _)| file.length)
        .and_then(|(_, _, parsed)| parsed.clone());

    let fallback_name = handle.name().unwrap_or_else(|| {
        primary_parsed
            .as_ref()
            .map(|parsed| parsed.name.clone())
            .or_else(|| {
                video_files
                    .iter()
                    .max_by_key(|(_, file)| file.length)
                    .map(|(_, file)| display_name_from_filename(&file.name))
            })
            .unwrap_or_else(|| info_hash.clone())
    });

    let enriched = match primary_parsed.as_ref() {
        Some(parsed) => match fetch_cinemeta_for_parsed(&state.client, parsed).await {
            Ok(meta) => meta,
            Err(err) => {
                debug!(name = %parsed.name, error = %err, "local-addon cinemeta lookup failed");
                None
            }
        },
        None => None,
    };
    let imdb_id = enriched
        .as_ref()
        .and_then(|meta| meta.get("imdb_id").or_else(|| meta.get("id")))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let background = imdb_id
        .as_ref()
        .map(|imdb_id| format!("{METAHUB_BASE_URL}/background/medium/{imdb_id}/img"));

    let videos = parsed_files
        .iter()
        .enumerate()
        .map(|(order, (file_idx, file, parsed))| {
            let released = (now - chrono::Duration::minutes(order as i64))
                .to_rfc3339_opts(SecondsFormat::Millis, true);
            let video_id = local_addon_video_id(imdb_id.as_deref(), parsed.as_ref())
                .unwrap_or_else(|| format!("{info_hash}/{file_idx}"));
            let thumbnail = local_addon_video_thumbnail(
                imdb_id.as_deref(),
                parsed.as_ref(),
                background.as_deref(),
            );

            let mut video = Map::new();
            video.insert("id".into(), json!(video_id));
            video.insert("title".into(), json!(file.name));
            video.insert("publishedAt".into(), json!(now_text));
            video.insert("released".into(), json!(released));
            video.insert(
                "stream".into(),
                json!({
                    "infoHash": info_hash,
                    "fileIdx": file_idx,
                    "title": format!("{info_hash}/{file_idx}"),
                    "sources": Value::Null
                }),
            );
            video.insert("thumbnail".into(), json!(thumbnail));
            if let Some(season) = parsed.as_ref().and_then(|parsed| parsed.season) {
                video.insert("season".into(), json!(season));
            }
            if let Some(episode) = parsed.as_ref().and_then(|parsed| parsed.episode) {
                video.insert("episode".into(), json!(episode));
            }
            Value::Object(video)
        })
        .collect::<Vec<_>>();

    let mut meta = enriched.unwrap_or_else(|| {
        json!({
            "name": fallback_name,
            "showAsVideos": true
        })
    });
    if let Some(meta) = meta.as_object_mut() {
        meta.insert("id".into(), json!(format!("bt:{info_hash}")));
        meta.insert("type".into(), json!(media_type));
        meta.insert("videos".into(), json!(videos));
        meta.entry("name").or_insert_with(|| json!(fallback_name));
        if !meta.contains_key("showAsVideos") && !meta.contains_key("poster") {
            meta.insert("showAsVideos".into(), json!(true));
        }
    }

    Ok(json!({ "meta": meta }))
}

async fn local_addon_catalog(state: &AppState) -> Value {
    refresh_local_addon_index(state).await;
    let entries = state.local_addon.entries.read().await;
    let metas = entries
        .values()
        .filter_map(|entry| {
            let file = entry.files.iter().max_by_key(|file| file.length)?;
            Some(json!({
                "id": entry.item_id,
                "type": "other",
                "name": file.parsed_name.as_deref().unwrap_or(&entry.name),
                "poster": file.imdb_id.as_ref().map(|imdb_id| {
                    format!("{METAHUB_BASE_URL}/poster/medium/{imdb_id}/img")
                })
            }))
        })
        .collect::<Vec<_>>();
    json!({ "metas": metas })
}

async fn local_addon_local_meta(state: &AppState, media_type: &str, id: &str) -> Value {
    refresh_local_addon_index(state).await;
    let entries = state.local_addon.entries.read().await;
    let Some(entry) = entries.values().find(|entry| entry.item_id == id).cloned() else {
        return json!({ "meta": null });
    };
    let mut meta = map_local_addon_entry_to_meta(&state.client, &entry, media_type)
        .await
        .unwrap_or_else(|| {
            json!({
                "id": entry.item_id,
                "type": media_type,
                "name": entry.name,
                "showAsVideos": true
            })
        });
    let now = Utc::now();
    let videos = entry
        .files
        .iter()
        .enumerate()
        .map(|(idx, file)| {
            local_addon_file_video(
                &entry,
                file,
                idx,
                now,
                file.imdb_id.as_deref(),
                file.imdb_id
                    .as_ref()
                    .map(|imdb_id| format!("{METAHUB_BASE_URL}/background/medium/{imdb_id}/img"))
                    .as_deref(),
            )
        })
        .collect::<Vec<_>>();
    if let Some(meta) = meta.as_object_mut() {
        meta.insert("id".into(), json!(entry.item_id));
        meta.insert("type".into(), json!(media_type));
        meta.insert("videos".into(), Value::Array(videos));
    }
    json!({ "meta": meta })
}

async fn local_addon_stream(state: &AppState, media_type: &str, id: &str) -> Value {
    refresh_local_addon_index(state).await;
    let mut streams = Vec::new();

    {
        let entries = state.local_addon.entries.read().await;
        for entry in entries.values() {
            for file in &entry.files {
                if file.media_type.as_deref() == Some(media_type)
                    && local_addon_file_video_id(file).as_deref() == Some(id)
                {
                    if let Some(file_idx) = file.idx {
                        let info_hash = entry.item_id.trim_start_matches("bt:");
                        streams.push(json!({
                            "title": file.name,
                            "infoHash": info_hash,
                            "fileIdx": file_idx,
                            "id": format!("{info_hash}/{file_idx}"),
                            "sources": if entry.sources.is_empty() {
                                Value::Null
                            } else {
                                json!(entry.sources)
                            }
                        }));
                    } else {
                        streams.push(json!({
                            "id": format!("file://{}", file.path),
                            "url": format!("file://{}", file.path),
                            "subtitle": "ADDON_STREAM_LOCALFILE",
                            "title": file.name
                        }));
                    }
                }
            }
        }
    }

    let handles = state.torrents.handles.read().await;
    for handle in handles.values() {
        let info_hash = handle.info_hash().as_string();
        for (file_idx, file) in files_for_handle(handle).into_iter().enumerate() {
            let Some(parsed) = parse_video_filename(&file.name) else {
                continue;
            };
            if parsed.kind.as_cinemeta_type() != media_type {
                continue;
            }
            let imdb_id = match fetch_cinemeta_for_parsed(&state.client, &parsed).await {
                Ok(Some(meta)) => meta
                    .get("imdb_id")
                    .or_else(|| meta.get("id"))
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                _ => None,
            };
            let local_file = LocalAddonFile {
                path: file.path.clone(),
                name: file.name.clone(),
                length: file.length,
                idx: Some(file_idx),
                parsed_name: Some(parsed.name),
                media_type: Some(media_type.to_string()),
                imdb_id,
                season: parsed.season,
                episode: parsed.episode,
            };
            if local_addon_file_video_id(&local_file).as_deref() == Some(id) {
                streams.push(json!({
                    "title": file.name,
                    "infoHash": info_hash,
                    "fileIdx": file_idx,
                    "id": format!("{info_hash}/{file_idx}"),
                    "sources": Value::Null
                }));
            }
        }
    }

    json!({ "streams": streams })
}

async fn fetch_cinemeta_for_parsed(
    client: &reqwest::Client,
    parsed: &ParsedVideoName,
) -> anyhow::Result<Option<Value>> {
    let media_type = parsed.kind.as_cinemeta_type();
    let search = utf8_percent_encode(&parsed.name, PATH_SEGMENT_ENCODE_SET).to_string();
    let url = format!("{CINEMETA_BASE_URL}/catalog/{media_type}/top/search={search}.json");
    let catalog = timeout(Duration::from_secs(6), client.get(url).send())
        .await
        .context("cinemeta search timed out")??
        .error_for_status()
        .context("cinemeta search returned error")?
        .json::<Value>()
        .await
        .context("decoding cinemeta search")?;

    let Some(imdb_id) = pick_cinemeta_search_result(&catalog, parsed) else {
        return Ok(None);
    };

    fetch_cinemeta_meta_by_id(client, media_type, &imdb_id).await
}

fn pick_cinemeta_search_result(catalog: &Value, parsed: &ParsedVideoName) -> Option<String> {
    let metas = catalog.get("metas")?.as_array()?;
    let simplified_query = simplify_video_title(&parsed.name);
    metas
        .iter()
        .filter_map(|meta| {
            let imdb_id = meta
                .get("imdb_id")
                .or_else(|| meta.get("id"))
                .and_then(Value::as_str)?;
            let name = meta.get("name").and_then(Value::as_str).unwrap_or_default();
            let simplified_name = simplify_video_title(name);
            let name_score = if simplified_name == simplified_query {
                100
            } else if simplified_name.contains(&simplified_query)
                || simplified_query.contains(&simplified_name)
            {
                65
            } else {
                0
            };
            if name_score == 0 {
                return None;
            }

            let result_year = meta
                .get("releaseInfo")
                .or_else(|| meta.get("year"))
                .and_then(Value::as_str)
                .and_then(|value| value.get(..4))
                .and_then(|value| value.parse::<i32>().ok());
            let year_score = match (parsed.year, result_year) {
                (Some(expected), Some(actual)) if expected == actual => 50,
                (Some(expected), Some(actual)) if (expected - actual).abs() <= 1 => 20,
                (Some(_), Some(_)) => -50,
                (Some(_), None) => -10,
                _ => 0,
            };

            Some((name_score + year_score, imdb_id.to_owned()))
        })
        .max_by_key(|(score, _)| *score)
        .map(|(_, imdb_id)| imdb_id)
}

fn local_addon_video_id(imdb_id: Option<&str>, parsed: Option<&ParsedVideoName>) -> Option<String> {
    let imdb_id = imdb_id?;
    let parsed = parsed?;
    Some(
        [
            Some(imdb_id.to_owned()),
            parsed.season.map(|value| value.to_string()),
            parsed.episode.map(|value| value.to_string()),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(":"),
    )
}

fn local_addon_video_thumbnail(
    imdb_id: Option<&str>,
    parsed: Option<&ParsedVideoName>,
    background: Option<&str>,
) -> Option<String> {
    let imdb_id = imdb_id?;
    match parsed.and_then(|parsed| parsed.season.zip(parsed.episode)) {
        Some((season, episode)) => Some(format!(
            "{METAHUB_EPISODES_URL}/{imdb_id}/{season}/{episode}/w780.jpg"
        )),
        None => background.map(str::to_owned),
    }
}

async fn map_local_addon_entry_to_meta(
    client: &reqwest::Client,
    entry: &LocalAddonEntry,
    media_type: &str,
) -> Option<Value> {
    let file = entry.files.iter().find(|file| file.imdb_id.is_some())?;
    let imdb_id = file.imdb_id.as_deref()?;
    let cinemeta_type = file.media_type.as_deref().unwrap_or("movie");
    match fetch_cinemeta_meta_by_id(client, cinemeta_type, imdb_id).await {
        Ok(Some(mut meta)) => {
            if let Some(meta) = meta.as_object_mut() {
                meta.insert("id".into(), json!(entry.item_id));
                meta.insert("type".into(), json!(media_type));
            }
            Some(meta)
        }
        _ => Some(json!({
            "id": entry.item_id,
            "type": media_type,
            "name": file.parsed_name.as_deref().unwrap_or(&entry.name),
            "showAsVideos": true,
            "poster": format!("{METAHUB_BASE_URL}/poster/medium/{imdb_id}/img"),
            "background": format!("{METAHUB_BASE_URL}/background/medium/{imdb_id}/img"),
            "logo": format!("{METAHUB_BASE_URL}/logo/medium/{imdb_id}/img")
        })),
    }
}

fn local_addon_file_video(
    entry: &LocalAddonEntry,
    file: &LocalAddonFile,
    idx: usize,
    now: chrono::DateTime<Utc>,
    imdb_id: Option<&str>,
    background: Option<&str>,
) -> Value {
    let released =
        (now - chrono::Duration::minutes(idx as i64)).to_rfc3339_opts(SecondsFormat::Millis, true);
    let published = entry
        .date_modified
        .and_then(system_time_to_rfc3339)
        .unwrap_or_else(|| now.to_rfc3339_opts(SecondsFormat::Millis, true));
    let stream = if let Some(file_idx) = file.idx {
        let info_hash = entry.item_id.trim_start_matches("bt:");
        json!({
            "infoHash": info_hash,
            "fileIdx": file_idx,
            "title": format!("{info_hash}/{file_idx}"),
            "sources": if entry.sources.is_empty() {
                Value::Null
            } else {
                json!(entry.sources)
            }
        })
    } else {
        json!({
            "title": file.path,
            "url": format!("file://{}", file.path),
            "subtitle": "ADDON_STREAM_LOCALFILE"
        })
    };
    let mut video = Map::new();
    video.insert(
        "id".into(),
        json!(local_addon_file_video_id(file).unwrap_or_else(|| {
            file.idx
                .map(|idx| format!("{}/{}", entry.item_id, idx))
                .unwrap_or_else(|| file.path.clone())
        })),
    );
    video.insert("title".into(), json!(file.name));
    video.insert("publishedAt".into(), json!(published));
    video.insert("released".into(), json!(released));
    video.insert("stream".into(), stream);
    video.insert(
        "thumbnail".into(),
        json!(match imdb_id {
            Some(imdb_id) => match file.season.zip(file.episode) {
                Some((season, episode)) => Some(format!(
                    "{METAHUB_EPISODES_URL}/{imdb_id}/{season}/{episode}/w780.jpg"
                )),
                None => background.map(str::to_owned),
            },
            None => None,
        }),
    );
    if let Some(season) = file.season {
        video.insert("season".into(), json!(season));
    }
    if let Some(episode) = file.episode {
        video.insert("episode".into(), json!(episode));
    }
    Value::Object(video)
}

fn local_addon_file_video_id(file: &LocalAddonFile) -> Option<String> {
    let imdb_id = file.imdb_id.as_ref()?;
    Some(
        [
            Some(imdb_id.clone()),
            file.season.map(|value| value.to_string()),
            file.episode.map(|value| value.to_string()),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(":"),
    )
}

fn system_time_to_rfc3339(time: SystemTime) -> Option<String> {
    let duration = time.duration_since(SystemTime::UNIX_EPOCH).ok()?;
    chrono::DateTime::<Utc>::from_timestamp(duration.as_secs() as i64, duration.subsec_nanos())
        .map(|dt| dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

async fn fetch_cinemeta_meta_by_id(
    client: &reqwest::Client,
    media_type: &str,
    imdb_id: &str,
) -> anyhow::Result<Option<Value>> {
    let url = format!("{CINEMETA_BASE_URL}/meta/{media_type}/{imdb_id}.json");
    let full = timeout(Duration::from_secs(6), client.get(url).send())
        .await
        .context("cinemeta meta timed out")??
        .error_for_status()
        .context("cinemeta meta returned error")?
        .json::<Value>()
        .await
        .context("decoding cinemeta meta")?;
    let Some(meta) = full.get("meta").and_then(Value::as_object) else {
        return Ok(None);
    };
    Ok(Some(filter_cinemeta_meta(meta)))
}

fn filter_cinemeta_meta(meta: &Map<String, Value>) -> Value {
    let mut out = Map::new();
    for key in CINEMETA_META_FIELDS {
        if let Some(value) = meta.get(*key) {
            out.insert((*key).to_owned(), value.clone());
        }
    }
    if let Some(imdb_id) = meta
        .get("imdb_id")
        .or_else(|| meta.get("id"))
        .and_then(Value::as_str)
    {
        out.entry("poster")
            .or_insert_with(|| json!(format!("{METAHUB_BASE_URL}/poster/medium/{imdb_id}/img")));
        out.entry("background").or_insert_with(|| {
            json!(format!(
                "{METAHUB_BASE_URL}/background/medium/{imdb_id}/img"
            ))
        });
        out.entry("logo")
            .or_insert_with(|| json!(format!("{METAHUB_BASE_URL}/logo/medium/{imdb_id}/img")));
    }
    Value::Object(out)
}

fn parse_video_filename(name: &str) -> Option<ParsedVideoName> {
    let stem = Path::new(name)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or(name);
    let normalized = stem.replace(['.', '_', '-'], " ");

    let season_episode = RegexBuilder::new(r"(?i)\bS(\d{1,2})\s*E(\d{1,3})\b")
        .build()
        .ok()
        .and_then(|re| {
            re.captures(&normalized).and_then(|captures| {
                let whole = captures.get(0)?;
                let season = captures.get(1)?.as_str().parse::<u32>().ok()?;
                let episode = captures.get(2)?.as_str().parse::<u32>().ok()?;
                Some((whole.start(), season, episode))
            })
        });

    let year_match = RegexBuilder::new(r"\b(19\d{2}|20\d{2})\b")
        .build()
        .ok()
        .and_then(|re| {
            re.find(&normalized)
                .and_then(|m| m.as_str().parse::<i32>().ok().map(|year| (m.start(), year)))
        });

    let cutoff = season_episode
        .map(|(idx, _, _)| idx)
        .or_else(|| year_match.map(|(idx, _)| idx))
        .unwrap_or_else(|| normalized.len());
    let parsed_name = cleanup_video_title(&normalized[..cutoff]);
    if parsed_name.is_empty() {
        return None;
    }

    Some(ParsedVideoName {
        name: parsed_name,
        year: year_match.map(|(_, year)| year),
        season: season_episode.map(|(_, season, _)| season),
        episode: season_episode.map(|(_, _, episode)| episode),
        kind: if season_episode.is_some() {
            ParsedVideoKind::Series
        } else {
            ParsedVideoKind::Movie
        },
    })
}

fn cleanup_video_title(value: &str) -> String {
    value
        .split_whitespace()
        .filter(|part| {
            let lower = part.to_ascii_lowercase();
            !matches!(
                lower.as_str(),
                "1080p"
                    | "720p"
                    | "2160p"
                    | "480p"
                    | "webrip"
                    | "web"
                    | "webdl"
                    | "web-dl"
                    | "bluray"
                    | "brrip"
                    | "hdrip"
                    | "hdtv"
                    | "x264"
                    | "x265"
                    | "h264"
                    | "h265"
                    | "hevc"
            )
        })
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_owned()
}

fn simplify_video_title(value: &str) -> String {
    value
        .chars()
        .flat_map(char::to_lowercase)
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect()
}

fn discover_local_addon_files(app_path: &Path) -> Vec<LocalAddonEntry> {
    let mut paths = Vec::new();
    collect_local_addon_dir(&app_path.join("localFiles"), &mut paths);
    collect_platform_local_addon_paths(&mut paths);

    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for path in paths {
        if out.len() >= 10_000 {
            break;
        }
        let key = path.to_string_lossy().into_owned();
        if !seen.insert(key) {
            continue;
        }
        match local_addon_entry_from_path(&path) {
            Ok(Some(entry)) => out.push(entry),
            Ok(None) => {}
            Err(err) => debug!(path = %path.display(), error = %err, "local addon entry skipped"),
        }
    }
    out
}

fn collect_local_addon_dir(path: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(path) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.is_dir() {
            collect_local_addon_dir(&path, out);
            continue;
        }
        if !metadata.is_file() || !is_interesting_local_addon_file(&path) {
            continue;
        }
        out.push(path);
    }
}

fn collect_platform_local_addon_paths(out: &mut Vec<PathBuf>) {
    #[cfg(target_os = "windows")]
    {
        let query = "SELECT System.ItemUrl FROM SystemIndex WHERE scope='file:' AND (System.Kind IS Null OR System.Kind = 'Video') AND System.FileAttributes <> ALL BITWISE 0x2 AND NOT System.ItemUrl LIKE '%/Program Files%' AND NOT System.ItemUrl LIKE '%/SteamLibrary/%' AND NOT System.ItemUrl LIKE '%/node_modules/%' AND (System.FileExtension = '.torrent' OR System.FileExtension = '.mp4' OR System.FileExtension = '.mkv' OR System.FileExtension = '.avi')";
        let script = format!(
            "$conn = New-Object -ComObject ADODB.Connection; \
             $rs = New-Object -ComObject ADODB.Recordset; \
             $conn.Open('Provider=Search.CollatorDSO;Extended Properties=\"Application=Windows\"'); \
             $rs.Open(\"{}\", $conn); \
             while (-not $rs.EOF) {{ $url = [string]$rs.Fields.Item('System.ItemUrl').Value; if ($url.StartsWith('file:')) {{ ([Uri]$url).LocalPath }}; $rs.MoveNext() }}; \
             $rs.Close(); $conn.Close();",
            query.replace('"', "`\"")
        );
        match Command::new("powershell")
            .args([
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                &script,
            ])
            .output()
        {
            Ok(output) if output.status.success() => {
                for line in String::from_utf8_lossy(&output.stdout).lines() {
                    let trimmed = line.trim();
                    if !trimmed.is_empty() {
                        out.push(PathBuf::from(trimmed));
                    }
                }
            }
            Ok(output) => debug!(
                status = ?output.status.code(),
                stderr = %String::from_utf8_lossy(&output.stderr),
                "windows local addon discovery failed"
            ),
            Err(err) => debug!(error = %err, "windows local addon discovery unavailable"),
        }
    }

    #[cfg(target_os = "macos")]
    {
        match Command::new("mdfind")
            .arg("(kMDItemFSName=*.avi || kMDItemFSName=*.mp4 || kMDItemFSName=*.mkv || kMDItemFSName=*.torrent)")
            .output()
        {
            Ok(output) if output.status.success() => {
                for line in String::from_utf8_lossy(&output.stdout).lines() {
                    let trimmed = line.trim();
                    if !trimmed.is_empty() {
                        out.push(PathBuf::from(trimmed));
                    }
                }
            }
            Ok(output) => debug!(
                status = ?output.status.code(),
                stderr = %String::from_utf8_lossy(&output.stderr),
                "macos local addon discovery failed"
            ),
            Err(err) => debug!(error = %err, "macos local addon discovery unavailable"),
        }
    }

    #[cfg(target_os = "linux")]
    {
        if let Some(home) = std::env::var_os("HOME") {
            let expr =
                "\\( -iname '*.torrent' -o -iname '*.mp4' -o -iname '*.mkv' -o -iname '*.avi' \\)";
            match Command::new("sh")
                .arg("-c")
                .arg(format!(
                    "find \"$1\" -maxdepth 7 -type f {expr} 2>/dev/null"
                ))
                .arg("find")
                .arg(home)
                .output()
            {
                Ok(output) if output.status.success() => {
                    for line in String::from_utf8_lossy(&output.stdout).lines() {
                        let trimmed = line.trim();
                        if !trimmed.is_empty() {
                            out.push(PathBuf::from(trimmed));
                        }
                    }
                }
                Ok(output) => debug!(
                    status = ?output.status.code(),
                    stderr = %String::from_utf8_lossy(&output.stderr),
                    "linux local addon discovery failed"
                ),
                Err(err) => debug!(error = %err, "linux local addon discovery unavailable"),
            }
        }
    }
}

fn local_addon_entry_from_path(path: &Path) -> anyhow::Result<Option<LocalAddonEntry>> {
    let metadata = std::fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    if !metadata.is_file() || !is_interesting_local_addon_file(path) {
        return Ok(None);
    }
    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("torrent"))
    {
        return parse_local_torrent_entry(path, &metadata).map(Some);
    }

    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_owned)
        .unwrap_or_else(|| path.to_string_lossy().into_owned());
    let source_key = path.to_string_lossy().into_owned();
    let item_id = format!(
        "local:{}",
        simplify_video_title(&display_name_from_filename(&name))
    );
    Ok(Some(LocalAddonEntry {
        item_id,
        name: name.clone(),
        files: vec![LocalAddonFile {
            path: source_key.clone(),
            name,
            length: metadata.len(),
            idx: None,
            parsed_name: None,
            media_type: None,
            imdb_id: None,
            season: None,
            episode: None,
        }],
        sources: Vec::new(),
        date_modified: metadata.modified().ok(),
        source_key,
    }))
}

fn parse_local_torrent_entry(
    path: &Path,
    metadata: &std::fs::Metadata,
) -> anyhow::Result<LocalAddonEntry> {
    let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let torrent = torrent_from_bytes(&bytes).context("parse torrent file")?;
    let info_hash = torrent.info_hash.as_string();
    let sources = torrent
        .iter_announce()
        .filter_map(|announce| {
            std::str::from_utf8(announce.as_ref())
                .ok()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| format!("tracker:{value}"))
        })
        .collect::<Vec<_>>();
    let validated = torrent
        .info
        .data
        .validate()
        .context("validate torrent file")?;
    let files = validated
        .iter_file_details_ext()
        .enumerate()
        .filter_map(|(idx, details)| {
            let torrent_path = details.details.filename.to_string();
            if !is_interesting_media_name(&torrent_path) {
                return None;
            }
            let name = Path::new(&torrent_path)
                .file_name()
                .and_then(|name| name.to_str())
                .map(str::to_owned)
                .unwrap_or_else(|| torrent_path.clone());
            Some(LocalAddonFile {
                path: torrent_path,
                name,
                length: details.details.len,
                idx: Some(idx),
                parsed_name: None,
                media_type: None,
                imdb_id: None,
                season: None,
                episode: None,
            })
        })
        .collect::<Vec<_>>();
    if files.is_empty() {
        anyhow::bail!("torrent has no interesting media files");
    }

    let fallback_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_owned)
        .unwrap_or_else(|| path.to_string_lossy().into_owned());
    let name = validated
        .name()
        .map(|name| name.into_owned())
        .unwrap_or(fallback_name);
    Ok(LocalAddonEntry {
        item_id: format!("bt:{info_hash}"),
        name,
        files,
        sources,
        date_modified: metadata.modified().ok(),
        source_key: path.to_string_lossy().into_owned(),
    })
}

fn is_interesting_local_addon_file(path: &Path) -> bool {
    let Some(ext) = path.extension().and_then(|ext| ext.to_str()) else {
        return false;
    };
    is_interesting_media_ext(ext) || ext.eq_ignore_ascii_case("torrent")
}

fn is_interesting_media_name(name: &str) -> bool {
    let Some(ext) = Path::new(name).extension().and_then(|ext| ext.to_str()) else {
        return false;
    };
    is_interesting_media_ext(ext)
}

fn is_interesting_media_ext(ext: &str) -> bool {
    matches!(
        ext.to_ascii_lowercase().as_str(),
        "mkv" | "avi" | "mp4" | "wmv" | "vp8" | "mov" | "mpg" | "mp3" | "flac"
    )
}

async fn root(State(state): State<AppState>) -> Response {
    let base_url = state.base_url.read().await.clone();
    let app_url = format!(
        "https://app.strem.io/shell-v4.4/#?streamingServer={}",
        utf8_percent_encode(&base_url, PATH_SEGMENT_ENCODE_SET)
    );
    redirect(StatusCode::TEMPORARY_REDIRECT, &app_url)
}

async fn favicon() -> Response {
    (StatusCode::NOT_FOUND, Json(json!({}))).into_response()
}

async fn heartbeat() -> Json<Value> {
    Json(json!({ "success": true }))
}

async fn get_settings(State(state): State<AppState>) -> Json<Value> {
    let interfaces = discover_ipv4_interfaces();
    let values = Value::Object(state.settings.values.read().await.clone());
    Json(json!({
        "options": [
            {
                "id": "localAddonEnabled",
                "label": "ENABLE_LOCAL_FILES_ADDON",
                "type": "checkbox"
            },
            {
                "id": "remoteHttps",
                "label": "ENABLE_REMOTE_HTTPS_CONN",
                "type": "select",
                "class": "https",
                "icon": true,
                "selections": std::iter::once(json!({"name": "Disabled", "val": ""}))
                    .chain(interfaces.iter().map(|ip| json!({"name": ip, "val": ip})))
                    .collect::<Vec<Value>>()
            },
            {
                "id": "cacheSize",
                "label": "CACHING",
                "type": "select",
                "class": "caching",
                "icon": true,
                "selections": [
                    {"name": "no caching", "val": 0},
                    {"name": "2GB", "val": 2147483648u64},
                    {"name": "5GB", "val": 5368709120u64},
                    {"name": "10GB", "val": 10737418240u64},
                    {"name": "∞", "val": Value::Null}
                ]
            },
            {
                "id": "cacheRoot",
                "label": "SETTINGS_CACHING_DRIVE",
                "type": "select",
                "class": "caching",
                "selections": [
                    {"val": "C:\\", "name": "C:"},
                    {"val": "D:\\", "name": "D:"},
                    {"val": "E:\\", "name": "E:"}
                ]
            }
        ],
        "values": values,
        "baseUrl": state.base_url.read().await.clone()
    }))
}

async fn post_settings(State(state): State<AppState>, body: Bytes) -> AppResult<Json<Value>> {
    let patch = parse_json_body::<Map<String, Value>>(&body)?;
    let app_path = {
        let values = state.settings.values.read().await;
        values
            .get("appPath")
            .and_then(|v| v.as_str())
            .map(PathBuf::from)
            .unwrap_or_else(server_app_path)
    };
    state.settings.update(patch, &app_path).await;
    {
        let torrents = state.torrents.clone();
        let settings = state.settings.clone();
        tokio::spawn(async move {
            torrents.cleanup_cache_to_limit(&settings).await;
        });
    }
    Ok(Json(json!({ "success": true })))
}

async fn network_info() -> Json<Value> {
    Json(json!({ "availableInterfaces": discover_ipv4_interfaces() }))
}

async fn casting() -> Json<Value> {
    Json(json!([
        {
            "name": "VLC",
            "type": "external",
            "id": "vlc",
            "usePlayerUI": true
        }
    ]))
}

async fn device_info(State(state): State<AppState>) -> Json<Value> {
    let values = state.settings.values.read().await;
    let profiles = values
        .get("allTranscodeProfiles")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let accel: Value = if profiles.is_empty() {
        json!(false)
    } else {
        json!(profiles)
    };
    Json(json!({ "availableHardwareAccelerations": accel }))
}

async fn local_addon_manifest(State(state): State<AppState>) -> Json<Value> {
    let catalog_enabled = state
        .settings
        .values
        .read()
        .await
        .get("localAddonEnabled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let catalogs = if catalog_enabled {
        json!([
            {
                "type": "other",
                "id": "local"
            }
        ])
    } else {
        json!([])
    };
    let resources = if catalog_enabled {
        json!([
            "catalog",
            {
                "name": "meta",
                "types": ["other"],
                "idPrefixes": ["local:", "bt:"]
            },
            {
                "name": "stream",
                "types": ["movie", "series"],
                "idPrefixes": ["tt"]
            }
        ])
    } else {
        json!([
            {
                "name": "meta",
                "types": ["other"],
                "idPrefixes": ["local:", "bt:"]
            },
            {
                "name": "stream",
                "types": ["movie", "series"],
                "idPrefixes": ["tt"]
            }
        ])
    };
    let name = if catalog_enabled {
        "Local Files"
    } else {
        "Local Files (without catalog support)"
    };
    Json(json!({
        "id": "org.stremio.local",
        "version": env!("CARGO_PKG_VERSION"),
        "name": name,
        "description": "Local add-on to find playable files: .torrent, .mp4, .mkv and .avi",
        "resources": resources,
        "types": ["movie", "series", "other"],
        "catalogs": catalogs
    }))
}

async fn local_addon_dispatch(
    State(state): State<AppState>,
    AxumPath(rest): AxumPath<String>,
) -> AppResult<Json<Value>> {
    let rest = rest.trim_start_matches('/');
    let mut parts = rest.split('/').collect::<Vec<_>>();

    if parts.len() < 3 {
        return Ok(Json(json!({ "err": "handler error" })));
    }

    let resource = parts.remove(0);
    let media_type = parts.remove(0);
    let id = parts.remove(0);
    let id = id.strip_suffix(".json").unwrap_or(id);
    let decoded_id = id.replace("%3A", ":").replace("%3a", ":");

    if resource == "meta" && media_type == "other" && decoded_id.starts_with("bt:") {
        return local_addon_bt_meta(&state, media_type, &decoded_id)
            .await
            .map(Json);
    }
    if resource == "catalog" && media_type == "other" && decoded_id == "local" {
        return Ok(Json(local_addon_catalog(&state).await));
    }
    if resource == "meta" && media_type == "other" && decoded_id.starts_with("local:") {
        return Ok(Json(
            local_addon_local_meta(&state, media_type, &decoded_id).await,
        ));
    }
    if resource == "stream" {
        return Ok(Json(
            local_addon_stream(&state, media_type, &decoded_id).await,
        ));
    }

    Ok(Json(local_addon_resource_payload(
        resource,
        media_type,
        &decoded_id,
    )))
}

async fn hwaccel_profiler(State(state): State<AppState>) -> Json<Value> {
    let values = state.settings.values.read().await;
    let profiles = values
        .get("allTranscodeProfiles")
        .cloned()
        .unwrap_or_else(|| json!([]));
    Json(profiles)
}

async fn get_https() -> (StatusCode, &'static str) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "Cannot get valid certificate",
    )
}

#[derive(Debug, Clone)]
struct ProxyOpts {
    destination_origin: String,
    destination_base: url::Url,
    request_headers: Vec<String>,
    response_headers: Vec<String>,
}

fn proxy_opts_parse(raw: &str) -> anyhow::Result<ProxyOpts> {
    let mut destination = None;
    let mut request_headers = Vec::new();
    let mut response_headers = Vec::new();

    for (key, value) in form_urlencoded::parse(raw.as_bytes()) {
        match key.as_ref() {
            "d" => destination = Some(value.into_owned()),
            "h" => request_headers.push(value.into_owned()),
            "r" => response_headers.push(value.into_owned()),
            _ => {}
        }
    }

    let destination = destination.context("missing d")?;
    let destination_url = url::Url::parse(&destination).context("parsing d")?;
    let destination_origin = url_origin(&destination_url)?;
    let destination_base = url::Url::parse(&destination_origin)
        .expect("origin URL should always parse")
        .to_owned();

    Ok(ProxyOpts {
        destination_origin,
        destination_base,
        request_headers,
        response_headers,
    })
}

fn proxy_opts_encode_full(opts: &ProxyOpts) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("d", &opts.destination_origin);
    for header in &opts.request_headers {
        serializer.append_pair("h", header);
    }
    for header in &opts.response_headers {
        serializer.append_pair("r", header);
    }
    serializer.finish()
}

fn proxy_opts_encode_nested(destination_origin: &str, request_headers: &[String]) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("d", destination_origin);
    for header in request_headers {
        serializer.append_pair("h", header);
    }
    serializer.finish()
}

fn url_origin(url: &url::Url) -> anyhow::Result<String> {
    let Some(host) = url.host_str() else {
        anyhow::bail!("URL missing host")
    };
    let mut origin = format!("{}://{}", url.scheme(), host);
    if let Some(port) = url.port() {
        origin.push(':');
        origin.push_str(&port.to_string());
    }
    Ok(origin)
}

fn host_header_value(url: &url::Url) -> anyhow::Result<HeaderValue> {
    let Some(host) = url.host_str() else {
        anyhow::bail!("URL missing host")
    };
    let value = if let Some(port) = url.port() {
        format!("{host}:{port}")
    } else {
        host.to_string()
    };
    HeaderValue::from_str(&value).context("building Host header")
}

fn header_override_parse(raw: &str) -> Option<(http::header::HeaderName, HeaderValue)> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    let mut parts = raw.splitn(2, ':');
    let name = parts.next().unwrap_or("").trim();
    let value = parts.next().unwrap_or("").trim();
    if name.is_empty() {
        return None;
    }

    let name = http::header::HeaderName::from_bytes(name.as_bytes()).ok()?;
    let value = HeaderValue::from_str(value).ok()?;
    Some((name, value))
}

fn proxy_build_upstream_headers(
    request_headers: &HeaderMap,
    dest_url: &url::Url,
    overrides: &[String],
) -> anyhow::Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    for name in [
        ACCEPT,
        ACCEPT_ENCODING,
        ACCEPT_LANGUAGE,
        CONNECTION,
        TRANSFER_ENCODING,
        RANGE,
        IF_RANGE,
        USER_AGENT,
    ] {
        if let Some(value) = request_headers.get(&name) {
            headers.insert(name, value.clone());
        }
    }

    headers.insert(HOST, host_header_value(dest_url)?);

    for raw in overrides {
        if let Some((name, value)) = header_override_parse(raw) {
            headers.insert(name, value);
        }
    }
    Ok(headers)
}

fn proxy_build_downstream_headers(upstream: &HeaderMap, overrides: &[String]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for name in [
        ACCEPT_RANGES,
        CONTENT_TYPE,
        CONTENT_LENGTH,
        CONTENT_RANGE,
        CONNECTION,
        TRANSFER_ENCODING,
        LAST_MODIFIED,
        ETAG,
        SERVER,
        DATE,
    ] {
        if let Some(value) = upstream.get(&name) {
            headers.insert(name, value.clone());
        }
    }

    for raw in overrides {
        if let Some((name, value)) = header_override_parse(raw) {
            headers.insert(name, value);
        }
    }

    headers
}

fn proxy_is_playlist(pathname: &str, response_headers: &HeaderMap) -> bool {
    let ext_is_playlist = Path::new(pathname)
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| matches!(ext.to_ascii_lowercase().as_str(), "m3u" | "m3u8"));

    let content_type_is_playlist = response_headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("mpegurl"));

    ext_is_playlist || content_type_is_playlist
}

fn url_join(segments: &[&str]) -> String {
    let joined = segments.join("/");
    let mut out = String::with_capacity(joined.len());
    let mut prev_slash = false;
    for ch in joined.chars() {
        if ch == '/' {
            if prev_slash {
                continue;
            }
            prev_slash = true;
        } else {
            prev_slash = false;
        }
        out.push(ch);
    }
    out
}

fn proxy_rewrite_playlist(
    body: &str,
    virtual_root: &str,
    dest_origin: &url::Url,
    request_header_overrides: &[String],
) -> String {
    static URI_RE: OnceLock<Regex> = OnceLock::new();
    let uri_re = URI_RE.get_or_init(|| Regex::new(r#"URI=\"([^\"]+)\""#).expect("regex"));

    fn parse_url(
        line: &str,
        virtual_root: &str,
        dest_origin: &url::Url,
        request_header_overrides: &[String],
    ) -> String {
        if line.starts_with("http://") || line.starts_with("https://") {
            if let Ok(line_url) = url::Url::parse(line) {
                let same_origin = line_url.scheme() == dest_origin.scheme()
                    && line_url.host_str() == dest_origin.host_str()
                    && line_url.port() == dest_origin.port();

                if same_origin {
                    let mut out = url_join(&[virtual_root, line_url.path()]);
                    if let Some(query) = line_url.query() {
                        out.push('?');
                        out.push_str(query);
                    }
                    return out;
                }

                if let Ok(line_origin) = url_origin(&line_url) {
                    let opts = proxy_opts_encode_nested(&line_origin, request_header_overrides);
                    let mut out = format!("/proxy/{opts}{}", line_url.path());
                    if let Some(query) = line_url.query() {
                        out.push('?');
                        out.push_str(query);
                    }
                    return out;
                }
            }
        }

        if line.starts_with('/') {
            return url_join(&[virtual_root, line]);
        }

        line.to_string()
    }

    fn parse_line(
        line: &str,
        virtual_root: &str,
        dest_origin: &url::Url,
        request_header_overrides: &[String],
        uri_re: &Regex,
    ) -> String {
        if !line.starts_with('#') && !line.is_empty() {
            return parse_url(line, virtual_root, dest_origin, request_header_overrides);
        }

        if let Some(caps) = uri_re.captures(line) {
            if let Some(m) = caps.get(1) {
                let uri = m.as_str();
                let rewritten = parse_url(uri, virtual_root, dest_origin, request_header_overrides);
                return line.replacen(uri, &rewritten, 1);
            }
        }

        line.to_string()
    }

    let eol = if body.contains("\r\n") {
        "\r\n"
    } else if body.contains('\n') {
        "\n"
    } else if body.contains('\r') {
        "\r"
    } else {
        "\n"
    };

    let mut out = String::with_capacity(body.len().saturating_add(64));
    let mut iter = body.split(eol).peekable();
    while let Some(line) = iter.next() {
        out.push_str(&parse_line(
            line,
            virtual_root,
            dest_origin,
            request_header_overrides,
            uri_re,
        ));
        if iter.peek().is_some() {
            out.push_str(eol);
        }
    }
    out
}

async fn proxy_root(
    State(state): State<AppState>,
    AxumPath(opts): AxumPath<String>,
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    proxy_impl(state, opts, String::new(), method, headers, raw_query).await
}

async fn proxy_path(
    State(state): State<AppState>,
    AxumPath((opts, pathname)): AxumPath<(String, String)>,
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    proxy_impl(state, opts, pathname, method, headers, raw_query).await
}

async fn proxy_impl(
    state: AppState,
    opts_raw: String,
    pathname: String,
    method: Method,
    request_headers: HeaderMap,
    raw_query: Option<String>,
) -> Response {
    let opts = match proxy_opts_parse(&opts_raw) {
        Ok(opts) => opts,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                [(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                )],
                format!("Invalid proxy opts: {err}"),
            )
                .into_response();
        }
    };

    let pathname = pathname.trim_start_matches('/').to_string();
    let mut dest = opts.destination_base.clone();
    if !pathname.is_empty() {
        dest.set_path(&format!("/{pathname}"));
    }
    if let Some(query) = raw_query.as_deref().filter(|q| !q.is_empty()) {
        dest.set_query(Some(query));
    }

    let mut upstream_headers =
        match proxy_build_upstream_headers(&request_headers, &dest, &opts.request_headers) {
            Ok(headers) => headers,
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    [(
                        CONTENT_TYPE,
                        HeaderValue::from_static("text/plain; charset=utf-8"),
                    )],
                    format!("Invalid proxy request: {err}"),
                )
                    .into_response();
            }
        };

    let mut redirect_count = 0usize;
    let mut current = dest.clone();
    let response = loop {
        let result = state
            .proxy_client
            .request(method.clone(), current.clone())
            .headers(upstream_headers.clone())
            .send()
            .await;

        let result = match result {
            Ok(result) => result,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    [(
                        CONTENT_TYPE,
                        HeaderValue::from_static("text/plain; charset=utf-8"),
                    )],
                    format!("Proxy upstream request failed: {err}"),
                )
                    .into_response();
            }
        };

        let is_redirect =
            result.status().is_redirection() && result.headers().get(LOCATION).is_some();
        if !is_redirect {
            break result;
        }

        if redirect_count >= 5 {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                )],
                "Proxy upstream request failed: Too many redirects".to_string(),
            )
                .into_response();
        }

        let Some(location) = result
            .headers()
            .get(LOCATION)
            .and_then(|value| value.to_str().ok())
        else {
            break result;
        };

        let base_origin = match url_origin(&current) {
            Ok(origin) => origin,
            Err(_) => opts.destination_origin.clone(),
        };

        let base = match url::Url::parse(&base_origin) {
            Ok(base) => base,
            Err(_) => opts.destination_base.clone(),
        };

        let next = match base.join(location) {
            Ok(url) => url,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    [(
                        CONTENT_TYPE,
                        HeaderValue::from_static("text/plain; charset=utf-8"),
                    )],
                    format!("Proxy upstream redirect invalid: {err}"),
                )
                    .into_response();
            }
        };

        current = next;
        if let Ok(host) = host_header_value(&current) {
            upstream_headers.insert(HOST, host);
        }
        for raw in &opts.request_headers {
            if let Some((name, value)) = header_override_parse(raw) {
                upstream_headers.insert(name, value);
            }
        }
        redirect_count += 1;
    };

    let mut response_headers =
        proxy_build_downstream_headers(response.headers(), &opts.response_headers);
    let is_playlist = proxy_is_playlist(&pathname, &response_headers);
    if is_playlist {
        response_headers.remove(CONTENT_LENGTH);
        response_headers.insert(ACCEPT_RANGES, HeaderValue::from_static("none"));

        let transfer_value = response_headers
            .get(TRANSFER_ENCODING)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();
        if !transfer_value.to_ascii_lowercase().contains("chunked") {
            if transfer_value.is_empty() {
                response_headers.insert(TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
            } else {
                let merged = format!("{transfer_value}, chunked");
                if let Ok(value) = HeaderValue::from_str(&merged) {
                    response_headers.insert(TRANSFER_ENCODING, value);
                }
            }
        }
    }

    let status = response.status();
    if is_playlist {
        let bytes = match response.bytes().await {
            Ok(bytes) => bytes,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    [(
                        CONTENT_TYPE,
                        HeaderValue::from_static("text/plain; charset=utf-8"),
                    )],
                    format!("Proxy upstream read failed: {err}"),
                )
                    .into_response();
            }
        };

        let opts_encoded_full = proxy_opts_encode_full(&opts);
        let virtual_root = format!("/proxy/{opts_encoded_full}");
        let rewritten = proxy_rewrite_playlist(
            &String::from_utf8_lossy(&bytes),
            &virtual_root,
            &opts.destination_base,
            &opts.request_headers,
        );

        let rewritten_bytes = Bytes::from(rewritten.into_bytes());
        let body = Body::from_stream(futures_util::stream::once(async move {
            Ok::<Bytes, std::io::Error>(rewritten_bytes)
        }));

        let mut resp = Response::builder()
            .status(status)
            .body(body)
            .expect("response builder failed");
        *resp.headers_mut() = response_headers;
        resp
    } else {
        let stream = response
            .bytes_stream()
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err));
        let body = Body::from_stream(stream);

        let mut resp = Response::builder()
            .status(status)
            .body(body)
            .expect("response builder failed");
        *resp.headers_mut() = response_headers;
        resp
    }
}

async fn opensub_hash(State(state): State<AppState>, RawQuery(raw_query): RawQuery) -> Response {
    let Some(url) = query_first(raw_query.as_deref(), "videoUrl") else {
        return opensub_hash_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            Some("missing videoUrl".to_string()),
            None,
        );
    };

    match compute_opensub_hash(&state.client, &url).await {
        Ok(result) => opensub_hash_response(StatusCode::OK, None, Some(result)),
        Err(err) => {
            warn!(url, error = %err, "opensubHash failed");
            opensub_hash_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                Some(err.to_string()),
                None,
            )
        }
    }
}

fn opensub_hash_response(
    status: StatusCode,
    error: Option<String>,
    result: Option<OpenSubHashResult>,
) -> Response {
    let body = json!({
        "error": error,
        "result": result
    });
    (status, Json(body)).into_response()
}

#[derive(Debug, Serialize)]
struct OpenSubHashResult {
    size: u64,
    hash: String,
}

async fn compute_opensub_hash(
    client: &reqwest::Client,
    url: &str,
) -> anyhow::Result<OpenSubHashResult> {
    let first = fetch_byte_range(client, url, 0, OPENSUB_HASH_CHUNK_SIZE - 1).await?;
    let size = first
        .total
        .or_else(|| first.content_length)
        .context("could not determine video size")?;

    if size < OPENSUB_HASH_CHUNK_SIZE {
        return Err(anyhow!(
            "video is too small for OpenSubtitles hash: {size} bytes"
        ));
    }

    let tail_start = size.saturating_sub(OPENSUB_HASH_CHUNK_SIZE);
    let last = if tail_start == 0 {
        first.bytes.clone()
    } else {
        fetch_byte_range(client, url, tail_start, size - 1)
            .await?
            .bytes
    };

    let mut hash = size;
    hash = hash.wrapping_add(opensub_chunk_sum(&first.bytes));
    hash = hash.wrapping_add(opensub_chunk_sum(&last));

    Ok(OpenSubHashResult {
        size,
        hash: format!("{hash:016x}"),
    })
}

struct ByteRangeResponse {
    bytes: Bytes,
    total: Option<u64>,
    content_length: Option<u64>,
}

async fn fetch_byte_range(
    client: &reqwest::Client,
    url: &str,
    start: u64,
    end: u64,
) -> anyhow::Result<ByteRangeResponse> {
    let response = client
        .get(url)
        .header(RANGE, format!("bytes={start}-{end}"))
        .send()
        .await
        .with_context(|| format!("requesting byte range {start}-{end}"))?
        .error_for_status()
        .with_context(|| format!("byte range request failed for {start}-{end}"))?;

    let status = response.status();
    let headers = response.headers().clone();
    let total = headers
        .get(CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_content_range_total);
    let content_length = headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let expected_len = end.saturating_sub(start).saturating_add(1);
    if status != StatusCode::PARTIAL_CONTENT && content_length.is_some_and(|len| len > expected_len)
    {
        return Err(anyhow!(
            "server ignored range request {start}-{end} and returned {content_length:?} bytes"
        ));
    }

    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("reading byte range {start}-{end}"))?;
    if bytes.len() as u64 > expected_len {
        return Err(anyhow!(
            "server returned {} bytes for range {start}-{end}",
            bytes.len()
        ));
    }

    Ok(ByteRangeResponse {
        bytes,
        total,
        content_length,
    })
}

fn parse_content_range_total(value: &str) -> Option<u64> {
    value
        .rsplit_once('/')
        .and_then(|(_, total)| total.parse::<u64>().ok())
}

fn opensub_chunk_sum(bytes: &[u8]) -> u64 {
    bytes.chunks_exact(8).fold(0u64, |sum, chunk| {
        let mut word = [0u8; 8];
        word.copy_from_slice(chunk);
        sum.wrapping_add(u64::from_le_bytes(word))
    })
}

async fn subtitles_tracks(
    State(state): State<AppState>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let Some(url) = query_first(raw_query.as_deref(), "subsUrl") else {
        return subtitles_tracks_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            Some("missing subsUrl".to_string()),
            None,
        );
    };

    match fetch_subtitle_cues(&state.client, &url).await {
        Ok(cues) => subtitles_tracks_response(StatusCode::OK, None, Some(cues)),
        Err(err) => {
            warn!(url, error = %err, "subtitlesTracks failed");
            subtitles_tracks_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                Some(err.to_string()),
                None,
            )
        }
    }
}

async fn subtitles_proxy(
    State(state): State<AppState>,
    AxumPath(SubtitleExt { ext }): AxumPath<SubtitleExt>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let Some(url) = query_first(raw_query.as_deref(), "from") else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let offset_ms = query_first(raw_query.as_deref(), "offset")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);

    match fetch_subtitle(&state.client, &url).await {
        Ok(response) => match response.bytes().await {
            Ok(bytes) if !bytes.is_empty() => {
                let (content_type, body) = subtitle_response_body(&ext, &bytes, offset_ms);
                let mut response = Response::builder().status(StatusCode::OK);
                {
                    let headers = response.headers_mut().expect("headers exist");
                    headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
                    headers.insert(
                        CACHE_CONTROL,
                        HeaderValue::from_static("public, max-age=86400"),
                    );
                }
                response.body(Body::from(body)).unwrap_or_else(|err| {
                    warn!(error = %err, "building subtitle response failed");
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                })
            }
            Ok(_) => StatusCode::NO_CONTENT.into_response(),
            Err(err) => {
                warn!(url, error = %err, "subtitle body read failed");
                StatusCode::BAD_GATEWAY.into_response()
            }
        },
        Err(err) => {
            warn!(url, error = %err, "subtitle proxy failed");
            StatusCode::BAD_GATEWAY.into_response()
        }
    }
}

async fn fetch_subtitle_cues(
    client: &reqwest::Client,
    url: &str,
) -> anyhow::Result<Vec<SubtitleCue>> {
    let response = fetch_subtitle(client, url).await?;
    let bytes = response.bytes().await.context("reading subtitle body")?;
    parse_subtitle_cues(&String::from_utf8_lossy(&bytes)).context("parsing subtitle cues")
}

fn subtitles_tracks_response(
    status: StatusCode,
    error: Option<String>,
    cues: Option<Vec<SubtitleCue>>,
) -> Response {
    let tracks = cues
        .unwrap_or_default()
        .into_iter()
        .map(|cue| {
            json!({
                "startTime": subtitle_date_string(cue.start_ms),
                "endTime": subtitle_date_string(cue.end_ms),
                "text": cue.text
            })
        })
        .collect::<Vec<_>>();
    let body = json!({
        "error": error,
        "result": if status.is_success() {
            json!({ "tracks": tracks })
        } else {
            Value::Null
        }
    });
    (status, Json(body)).into_response()
}

async fn fetch_subtitle(client: &reqwest::Client, url: &str) -> anyhow::Result<reqwest::Response> {
    let mut last_error = None;

    for attempt in 1..=3 {
        match client
            .get(url)
            .header("accept", "text/vtt,application/x-subrip,text/plain,*/*")
            .timeout(Duration::from_secs(45))
            .send()
            .await
        {
            Ok(response) => {
                return response
                    .error_for_status()
                    .context("subtitle upstream returned an error status");
            }
            Err(err) => {
                last_error = Some(err);
                if attempt < 3 {
                    sleep(Duration::from_millis(250 * attempt)).await;
                }
            }
        }
    }

    Err(last_error
        .map(anyhow::Error::from)
        .unwrap_or_else(|| anyhow!("subtitle request failed before it was sent")))
}

fn subtitle_response_body(ext: &str, bytes: &Bytes, offset_ms: i64) -> (&'static str, Bytes) {
    let text = String::from_utf8_lossy(bytes);
    let cues = parse_subtitle_cues(&text).unwrap_or_default();
    match ext.to_ascii_lowercase().as_str() {
        "vtt" => (
            "text/vtt; charset=utf-8",
            Bytes::from(render_subtitle_cues(
                &cues,
                SubtitleRenderFormat::Vtt,
                offset_ms,
            )),
        ),
        "srt" => (
            "application/x-subrip; charset=utf-8",
            Bytes::from(render_subtitle_cues(
                &cues,
                SubtitleRenderFormat::Srt,
                offset_ms,
            )),
        ),
        _ => ("text/plain; charset=utf-8", bytes.clone()),
    }
}

enum SubtitleRenderFormat {
    Srt,
    Vtt,
}

fn parse_subtitle_cues(input: &str) -> anyhow::Result<Vec<SubtitleCue>> {
    let normalized = input.replace("\r\n", "\n").replace('\r', "\n");
    let mut cues = Vec::new();

    for block in normalized.split("\n\n") {
        let mut lines = block
            .lines()
            .map(str::trim_end)
            .filter(|line| !line.trim().is_empty())
            .collect::<Vec<_>>();

        if lines.is_empty() {
            continue;
        }

        if lines[0]
            .trim_start_matches('\u{feff}')
            .starts_with("WEBVTT")
            || lines[0].starts_with("NOTE")
            || lines[0].starts_with("STYLE")
            || lines[0].starts_with("REGION")
        {
            continue;
        }

        let Some(timing_idx) = lines.iter().position(|line| line.contains("-->")) else {
            continue;
        };
        let timing = lines[timing_idx];
        let Some((start, end)) = parse_subtitle_timing_line(timing) else {
            continue;
        };

        lines.drain(..=timing_idx);
        let text = lines.join("\n");
        if text.trim().is_empty() {
            continue;
        }
        cues.push(SubtitleCue {
            start_ms: start,
            end_ms: end,
            text,
        });
    }

    Ok(cues)
}

fn parse_subtitle_timing_line(line: &str) -> Option<(i64, i64)> {
    let (start, rest) = line.split_once("-->")?;
    let end = rest.split_whitespace().next().unwrap_or(rest);
    Some((
        parse_subtitle_timestamp(start.trim())?,
        parse_subtitle_timestamp(end.trim())?,
    ))
}

fn parse_subtitle_timestamp(value: &str) -> Option<i64> {
    let normalized = value.replace(',', ".");
    let mut parts = normalized.split(':').collect::<Vec<_>>();
    if !(2..=3).contains(&parts.len()) {
        return None;
    }

    let seconds = parts.pop()?;
    let minutes = parts.pop()?.parse::<i64>().ok()?;
    let hours = parts
        .pop()
        .map(|value| value.parse::<i64>().ok())
        .unwrap_or(Some(0))?;
    let (seconds, millis) = match seconds.split_once('.') {
        Some((seconds, millis)) => {
            let millis = millis
                .chars()
                .take(3)
                .collect::<String>()
                .parse::<i64>()
                .ok()
                .map(|value| match millis.len() {
                    1 => value * 100,
                    2 => value * 10,
                    _ => value,
                })?;
            (seconds.parse::<i64>().ok()?, millis)
        }
        None => (seconds.parse::<i64>().ok()?, 0),
    };

    Some((((hours * 60) + minutes) * 60 + seconds) * 1000 + millis)
}

fn render_subtitle_cues(
    cues: &[SubtitleCue],
    format: SubtitleRenderFormat,
    offset_ms: i64,
) -> String {
    let mut out = String::new();
    if matches!(format, SubtitleRenderFormat::Vtt) {
        out.push_str("WEBVTT\n\n");
    }

    for (idx, cue) in cues.iter().enumerate() {
        let start = cue.start_ms.saturating_add(offset_ms).max(0);
        let end = cue.end_ms.saturating_add(offset_ms).max(0);
        out.push_str(&idx.to_string());
        out.push('\n');
        out.push_str(&format_subtitle_timestamp(start, &format));
        out.push_str(" --> ");
        out.push_str(&format_subtitle_timestamp(end, &format));
        out.push('\n');
        out.push_str(&cue.text.replace('&', "&amp;"));
        out.push_str("\n\n");
    }

    out
}

fn format_subtitle_timestamp(ms: i64, format: &SubtitleRenderFormat) -> String {
    let ms = ms.max(0);
    let hours = ms / 3_600_000;
    let minutes = (ms / 60_000) % 60;
    let seconds = (ms / 1_000) % 60;
    let millis = ms % 1_000;
    let separator = match format {
        SubtitleRenderFormat::Srt => ',',
        SubtitleRenderFormat::Vtt => '.',
    };
    format!("{hours:02}:{minutes:02}:{seconds:02}{separator}{millis:03}")
}

fn subtitle_date_string(ms: i64) -> String {
    let ms = ms.max(0);
    if let Some(dt) = chrono::DateTime::<Utc>::from_timestamp_millis(ms) {
        dt.to_rfc3339_opts(SecondsFormat::Millis, true)
    } else {
        "1970-01-01T00:00:00.000Z".to_string()
    }
}

async fn probe() -> Json<Value> {
    Json(json!({
        "error": null,
        "result": null
    }))
}

async fn global_stats(State(state): State<AppState>) -> Json<Value> {
    let handles = state.torrents.handles.read().await;
    let mut out = Map::new();
    for (hash, handle) in handles.iter() {
        state.torrents.touch(hash).await;
        out.insert(
            hash.clone(),
            stats_for_handle(handle, &state.torrents.cache_dir, None, None),
        );
    }
    Json(Value::Object(out))
}

async fn create_magnet(
    State(state): State<AppState>,
    AxumPath(InfoHashPath { info_hash }): AxumPath<InfoHashPath>,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> AppResult<Json<Value>> {
    let request = parse_json_body::<CreateTorrentRequest>(&body)?;
    let query_trackers = query_values(raw_query.as_deref(), "tr");
    let query_filters = query_values(raw_query.as_deref(), "f");
    let handle = state
        .torrents
        .add_magnet(&info_hash, &request, query_trackers)
        .await?;

    state.torrents.touch(&handle.info_hash().as_string()).await;

    let _ = timeout(CREATE_METADATA_GRACE, handle.wait_until_initialized()).await;
    let filters = if query_filters.is_empty() {
        request.file_must_include.clone()
    } else {
        query_filters
    };
    let guessed = guess_index_for_handle(
        &handle,
        &filters,
        request.guess_file_idx.as_ref(),
        request.file_idx,
    );
    Ok(Json(stats_for_handle(
        &handle,
        &state.torrents.cache_dir,
        guessed,
        guessed,
    )))
}

async fn create_from_torrent(State(state): State<AppState>, body: Bytes) -> AppResult<Json<Value>> {
    let request = parse_json_body::<CreateFromTorrentRequest>(&body)?;
    let bytes = if let Some(blob) = request.blob {
        decode_hex(&blob)?
    } else if let Some(from) = request.from {
        read_torrent_source(&state, &from).await?
    } else if !body.is_empty() {
        body
    } else {
        return Err(anyhow!("expected blob, from, or raw torrent bytes").into());
    };

    let handle = state.torrents.add_torrent_bytes(bytes).await?;
    let _ = timeout(CREATE_METADATA_GRACE, handle.wait_until_initialized()).await;
    Ok(Json(stats_for_handle(
        &handle,
        &state.torrents.cache_dir,
        None,
        None,
    )))
}

async fn torrent_stats(
    State(state): State<AppState>,
    AxumPath(InfoHashPath { info_hash }): AxumPath<InfoHashPath>,
) -> Json<Option<Value>> {
    let Some(handle) = state.torrents.get(&info_hash).await else {
        return Json(None);
    };
    state.torrents.touch(&handle.info_hash().as_string()).await;
    Json(Some(stats_for_handle(
        &handle,
        &state.torrents.cache_dir,
        None,
        None,
    )))
}

async fn torrent_file_stats(
    State(state): State<AppState>,
    AxumPath(StatsPath { info_hash, idx }): AxumPath<StatsPath>,
) -> Json<Option<Value>> {
    let Some(handle) = state.torrents.get(&info_hash).await else {
        return Json(None);
    };
    state.torrents.touch(&handle.info_hash().as_string()).await;

    // Stats consumers expect per-file stats to include stream* fields.
    // If metadata isn't ready or we can't resolve the file index, return null (matches official server behavior).
    let _ = timeout(CREATE_METADATA_GRACE, handle.wait_until_initialized()).await;
    let Ok(file_idx) = resolve_file_index(&handle, &idx, &[]) else {
        return Json(None);
    };

    Json(Some(stats_for_handle(
        &handle,
        &state.torrents.cache_dir,
        Some(file_idx),
        None,
    )))
}

async fn remove_torrent(
    State(state): State<AppState>,
    AxumPath(InfoHashPath { info_hash }): AxumPath<InfoHashPath>,
) -> Json<Value> {
    if let Err(err) = state.torrents.remove(&info_hash).await {
        warn!(info_hash, error = %err, "torrent remove failed");
    }
    Json(json!({}))
}

async fn remove_all(State(state): State<AppState>) -> Json<Value> {
    if let Err(err) = state.torrents.remove_all().await {
        warn!(error = %err, "removeAll failed");
    }
    Json(json!({}))
}

async fn stream_short(
    State(state): State<AppState>,
    method: Method,
    AxumPath(StreamPath { info_hash, idx }): AxumPath<StreamPath>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> AppResult<Response> {
    stream_common(state, method, info_hash, idx, raw_query, headers).await
}

async fn stream_named(
    State(state): State<AppState>,
    method: Method,
    AxumPath(StreamNamedPath {
        info_hash,
        idx,
        filename,
    }): AxumPath<StreamNamedPath>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> AppResult<Response> {
    debug!(
        info_hash,
        idx, filename, "stream request with filename path"
    );
    stream_common(state, method, info_hash, idx, raw_query, headers).await
}

async fn stream_common(
    state: AppState,
    method: Method,
    info_hash: String,
    idx: String,
    raw_query: Option<String>,
    headers: HeaderMap,
) -> AppResult<Response> {
    let t0 = std::time::Instant::now();
    let query = parse_stream_query(raw_query.as_deref());
    let normalized_info_hash = match normalize_info_hash(&info_hash) {
        Ok(hash) => hash,
        Err(err) => {
            warn!(info_hash, idx, error = %err, "non-torrent stream route hit");
            return Ok(StatusCode::NOT_FOUND.into_response());
        }
    };

    let range_header = headers
        .get(RANGE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("none")
        .to_owned();

    info!(
        info_hash,
        idx,
        range = %range_header,
        "[TIMING] request received",
    );

    let initial_file_idx = idx.parse::<isize>().ok().and_then(valid_idx);
    state.torrents.stop_others(&normalized_info_hash);

    let handle = state
        .torrents
        .get_or_add_magnet(
            &normalized_info_hash,
            query.trackers.clone(),
            initial_file_idx,
        )
        .await?;

    info!(
        info_hash,
        elapsed_ms = t0.elapsed().as_millis(),
        "[TIMING] torrent handle acquired"
    );

    // ── Wait for metadata / initial check ─────────────────────────────────────
    match timeout(STREAM_INIT_TIMEOUT, handle.wait_until_initialized()).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            warn!(
                info_hash,
                "[TIMING] torrent initialization failed after {}ms: {err:#}",
                t0.elapsed().as_millis()
            );
            return Ok((
                StatusCode::SERVICE_UNAVAILABLE,
                [("Retry-After", "5")],
                "torrent initialization failed",
            )
                .into_response());
        }
        Err(_elapsed) => {
            warn!(
                info_hash,
                "[TIMING] timed out waiting for torrent metadata after {}ms",
                t0.elapsed().as_millis()
            );
            return Ok((
                StatusCode::SERVICE_UNAVAILABLE,
                [("Retry-After", "5")],
                "waiting for torrent metadata",
            )
                .into_response());
        }
    }

    info!(
        info_hash,
        elapsed_ms = t0.elapsed().as_millis(),
        "[TIMING] wait_until_initialized done"
    );

    let file_idx = resolve_file_index(&handle, &idx, &query.filters)?;
    let file = file_for_handle(&handle, file_idx)?.context("torrent file not found")?;

    {
        let torrents = state.torrents.clone();
        let handle2 = handle.clone();
        tokio::spawn(async move {
            if let Err(e) = torrents.select_only_file(&handle2, file_idx).await {
                warn!(file_idx, "select_only_file failed (non-fatal): {:?}", e.0);
            }
        });
    }

    if query.external {
        let location = format!(
            "/{}/{}/{}{}",
            normalized_info_hash,
            file_idx,
            utf8_percent_encode(&file.name, PATH_SEGMENT_ENCODE_SET),
            if query.download { "?download=1" } else { "" }
        );
        return Ok(redirect(StatusCode::TEMPORARY_REDIRECT, &location));
    }

    let total_len = file.length;
    let range = headers
        .get(RANGE)
        .and_then(|header| header.to_str().ok())
        .and_then(|range| parse_range(range, total_len));

    let (status, start, end) = match range {
        Some(range) => (StatusCode::PARTIAL_CONTENT, range.0, range.1),
        None => (StatusCode::OK, 0, total_len.saturating_sub(1)),
    };

    info!(
        info_hash,
        elapsed_ms = t0.elapsed().as_millis(),
        start,
        total_len,
        "[TIMING] range parsed, setting streaming window",
    );

    let initial_near_eof =
        total_len > 0 && total_len.saturating_sub(start) < STREAM_WINDOW_EOF_ZONE;
    let initial_backward = if initial_near_eof {
        STREAM_WINDOW_EOF_BACKWARD
    } else {
        STREAM_WINDOW_BACKWARD
    };

    let _ =
        handle.update_streaming_window(file_idx, start, initial_backward, STREAM_WINDOW_FORWARD);

    info!(
        info_hash,
        elapsed_ms = t0.elapsed().as_millis(),
        "[TIMING] calling stream()"
    );

    let mut stream = match timeout(STREAM_OPEN_TIMEOUT, handle.clone().stream(file_idx)).await {
        Ok(Ok(s)) => {
            info!(
                info_hash,
                elapsed_ms = t0.elapsed().as_millis(),
                "[TIMING] stream() opened"
            );
            s
        }
        Ok(Err(err)) => {
            warn!(
                info_hash,
                file_idx,
                elapsed_ms = t0.elapsed().as_millis(),
                "[TIMING] stream() failed: {err:#}"
            );
            return Ok((
                StatusCode::SERVICE_UNAVAILABLE,
                [("Retry-After", "2")],
                "opening torrent stream failed",
            )
                .into_response());
        }
        Err(_elapsed) => {
            warn!(
                info_hash,
                file_idx,
                start,
                elapsed_ms = t0.elapsed().as_millis(),
                "[TIMING] stream() timed out — pieces not yet available"
            );
            return Ok((
                StatusCode::SERVICE_UNAVAILABLE,
                [("Retry-After", "3")],
                "waiting for torrent pieces",
            )
                .into_response());
        }
    };

    if start > 0 {
        info!(
            info_hash,
            start,
            elapsed_ms = t0.elapsed().as_millis(),
            "[TIMING] calling seek()"
        );
        match timeout(STREAM_OPEN_TIMEOUT, stream.seek(SeekFrom::Start(start))).await {
            Ok(Ok(_)) => {
                info!(
                    info_hash,
                    start,
                    elapsed_ms = t0.elapsed().as_millis(),
                    "[TIMING] seek() done"
                );
            }
            Ok(Err(err)) => {
                warn!(
                    info_hash,
                    file_idx,
                    start,
                    elapsed_ms = t0.elapsed().as_millis(),
                    "[TIMING] seek() failed: {err:#}"
                );
                return Ok((
                    StatusCode::SERVICE_UNAVAILABLE,
                    [("Retry-After", "2")],
                    "torrent stream seek failed",
                )
                    .into_response());
            }
            Err(_elapsed) => {
                warn!(
                    info_hash,
                    file_idx,
                    start,
                    elapsed_ms = t0.elapsed().as_millis(),
                    "[TIMING] seek() timed out — pieces at offset not available"
                );
                return Ok((
                    StatusCode::SERVICE_UNAVAILABLE,
                    [("Retry-After", "5")],
                    "waiting for pieces at seek offset",
                )
                    .into_response());
            }
        }
    }

    let body_len = if total_len == 0 { 0 } else { end - start + 1 };
    let mime = mime_guess::from_path(&file.path)
        .first_or_octet_stream()
        .to_string();

    let mut response = Response::builder().status(status);
    {
        let headers = response
            .headers_mut()
            .context("creating response headers")?;
        headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        headers.insert(CONTENT_TYPE, header_value(&mime)?);
        headers.insert(CONTENT_LENGTH, header_value(&body_len.to_string())?);
        headers.insert(
            "transferMode.dlna.org",
            HeaderValue::from_static("Streaming"),
        );
        headers.insert(
            "contentFeatures.dlna.org",
            HeaderValue::from_static("DLNA.ORG_OP=01"),
        );

        if status == StatusCode::PARTIAL_CONTENT {
            headers.insert(
                CONTENT_RANGE,
                header_value(&format!("bytes {}-{}/{}", start, end, total_len))?,
            );
        }
        if query.download {
            headers.insert(
                CONTENT_DISPOSITION,
                header_value(&format!(
                    "attachment; filename=\"{}\"",
                    file.name.replace('"', "")
                ))?,
            );
        }
        if let Some(sec) = query.subtitles_sec {
            headers.insert("CaptionInfo.sec", header_value(&sec)?);
        }
    }

    if method == Method::HEAD || body_len == 0 {
        return Ok(response
            .body(Body::empty())
            .context("building HEAD response")?);
    }

    state.torrents.stream_started(&normalized_info_hash).await;

    let torrents = state.torrents.clone();
    let hash = normalized_info_hash.clone();
    let on_drop = move || {
        tokio::spawn(async move {
            torrents.stream_finished(&hash).await;
        });
    };

    let inner = ReaderStream::with_capacity(stream.take(body_len), 512 * 1024);
    let body_stream = WindowTrackingStream {
        inner: Box::pin(inner),
        on_drop: Some(Box::new(on_drop)),
        handle: handle.clone(),
        file_idx,
        file_len: total_len,
        position: start,
        last_update_at: start,
    };

    let body = Body::from_stream(body_stream);
    Ok(response.body(body).context("building stream response")?)
}

async fn fallback(method: Method, uri: axum::http::Uri) -> Response {
    warn!(%method, %uri, "unimplemented route");
    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "error": "not implemented",
            "path": uri.path()
        })),
    )
        .into_response()
}

impl TorrentService {
    async fn add_magnet(
        &self,
        info_hash: &str,
        request: &CreateTorrentRequest,
        query_trackers: Vec<String>,
    ) -> AppResult<Arc<librqbit::ManagedTorrent>> {
        let info_hash = normalize_info_hash(info_hash)?;
        if let Some(handle) = self.get(&info_hash).await {
            self.touch(&info_hash).await;
            return Ok(handle);
        }

        let mut magnet = Magnet::parse(&info_hash).context("parsing info hash as magnet")?;
        let trackers = merge_trackers(request.announce.iter().cloned().chain(query_trackers));
        magnet.trackers = trackers.clone();

        let mut initial_peers = Vec::new();
        initial_peers.extend(parse_peer_addrs(&request.initial_peers));
        initial_peers.extend(parse_peer_addrs(&request.peers));

        let default_output_folder = self.cache_dir.join(&info_hash);
        let output_folder = request
            .path
            .as_ref()
            .filter(|p| !p.trim().is_empty())
            .cloned()
            .or_else(|| Some(default_output_folder.to_string_lossy().into_owned()));

        if let Some(connections) = request.connections {
            debug!(
                connections,
                "requested connection cap is currently advisory"
            );
        }
        if !request.extra.is_empty() {
            debug!(keys = ?request.extra.keys().collect::<Vec<_>>(), "create request had extra options");
        }

        let handle = self
            .session
            .add_torrent(
                AddTorrent::from_url(magnet.to_string()),
                Some(AddTorrentOptions {
                    overwrite: true,
                    output_folder,
                    peer_opts: Some(PeerConnectionOptions {
                        connect_timeout: Some(Duration::from_secs(8)),
                        read_write_timeout: Some(Duration::from_secs(30)),
                        keep_alive_interval: Some(Duration::from_secs(60)),
                    }),
                    force_tracker_interval: Some(Duration::from_secs(120)),
                    only_files: request
                        .file_idx
                        .and_then(valid_idx)
                        .map(|file_idx| vec![file_idx]),
                    initial_peers: if initial_peers.is_empty() {
                        None
                    } else {
                        Some(initial_peers)
                    },
                    trackers: Some(trackers),
                    ..Default::default()
                }),
            )
            .await
            .context("adding magnet to librqbit")?
            .into_handle()
            .context("torrent was not started")?;

        let ih = handle.info_hash().as_string();
        self.handles
            .write()
            .await
            .insert(ih.clone(), handle.clone());
        self.touch(&ih).await;
        Ok(handle)
    }

    /// Remove every torrent except `current_info_hash` in the background.
    ///
    /// Spawned as a detached task so the caller returns immediately — session.delete()
    /// can block waiting for internal cleanup and must not hold up the new request.
    fn stop_others(self: &Arc<Self>, current_info_hash: &str) {
        let others: Vec<String> = self
            .handles
            // Use try_read so we never block the async task; if the lock is
            // contended we just skip — the inactivity timer is the safety net.
            .try_read()
            .map(|g| {
                g.keys()
                    .filter(|h| h.as_str() != current_info_hash)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        if others.is_empty() {
            return;
        }

        let service = Arc::clone(self);
        tokio::spawn(async move {
            for hash in others {
                info!(info_hash = %hash, "stopping torrent (new stream started)");
                if let Err(err) = service.remove(&hash).await {
                    warn!(info_hash = %hash, error = %err, "failed to stop old torrent");
                }
            }
        });
    }

    async fn get_or_add_magnet(
        &self,
        info_hash: &str,
        query_trackers: Vec<String>,
        preferred_file_idx: Option<usize>,
    ) -> AppResult<Arc<librqbit::ManagedTorrent>> {
        let info_hash = normalize_info_hash(info_hash)?;
        if let Some(handle) = self.get(&info_hash).await {
            return Ok(handle);
        }
        self.add_magnet(
            &info_hash,
            &CreateTorrentRequest {
                announce: Vec::new(),
                file_must_include: Vec::new(),
                guess_file_idx: None,
                file_idx: preferred_file_idx.map(|idx| idx as isize),
                connections: None,
                path: None,
                initial_peers: Vec::new(),
                peers: Vec::new(),
                extra: Map::new(),
            },
            query_trackers,
        )
        .await
    }

    async fn select_only_file(
        &self,
        handle: &Arc<librqbit::ManagedTorrent>,
        file_idx: usize,
    ) -> AppResult<()> {
        self.session
            .update_only_files(handle, &HashSet::from([file_idx]))
            .await
            .with_context(|| format!("selecting only torrent file {file_idx}"))
            .map_err(AppError::from)
    }

    async fn add_torrent_bytes(&self, bytes: Bytes) -> AppResult<Arc<librqbit::ManagedTorrent>> {
        let handle = self
            .session
            .add_torrent(
                AddTorrent::from_bytes(bytes),
                Some(AddTorrentOptions {
                    overwrite: true,
                    output_folder: Some(self.cache_dir.to_string_lossy().into_owned()),
                    trackers: Some(merge_trackers(std::iter::empty())),
                    ..Default::default()
                }),
            )
            .await
            .context("adding torrent file to librqbit")?
            .into_handle()
            .context("torrent was not started")?;
        let ih = handle.info_hash().as_string();
        self.handles
            .write()
            .await
            .insert(ih.clone(), handle.clone());
        self.touch(&ih).await;
        Ok(handle)
    }

    async fn get(&self, info_hash: &str) -> Option<Arc<librqbit::ManagedTorrent>> {
        let Ok(info_hash) = normalize_info_hash(info_hash) else {
            return None;
        };
        self.handles.read().await.get(&info_hash).cloned()
    }

    async fn remove(&self, info_hash: &str) -> anyhow::Result<()> {
        let info_hash = normalize_info_hash(info_hash)?;
        self.handles.write().await.remove(&info_hash);
        self.last_active.write().await.remove(&info_hash);
        self.active_streams.write().await.remove(&info_hash);
        self.session
            .delete(TorrentIdOrHash::parse(&info_hash)?, false)
            .await
            .with_context(|| format!("deleting torrent {info_hash}"))?;
        Ok(())
    }

    async fn remove_all(&self) -> anyhow::Result<()> {
        let keys = self
            .handles
            .read()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            if let Err(err) = self.remove(&key).await {
                warn!(info_hash = key, error = %err, "failed to remove torrent");
            }
        }
        Ok(())
    }

    async fn touch(&self, info_hash: &str) {
        self.last_active
            .write()
            .await
            .insert(info_hash.to_string(), Instant::now());
    }

    async fn stream_started(&self, info_hash: &str) {
        self.touch(info_hash).await;
        let mut active = self.active_streams.write().await;
        *active.entry(info_hash.to_string()).or_insert(0) += 1;
    }

    async fn stream_finished(&self, info_hash: &str) {
        self.touch(info_hash).await;
        let mut active = self.active_streams.write().await;
        match active.get_mut(info_hash) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                active.remove(info_hash);
            }
            None => {}
        }
    }

    async fn cleanup_inactive(&self) {
        let keys = self
            .handles
            .read()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();

        for hash in keys {
            let active = self
                .active_streams
                .read()
                .await
                .get(&hash)
                .copied()
                .unwrap_or(0);
            if active > 0 {
                continue;
            }

            let last = self.last_active.read().await.get(&hash).copied();
            let Some(last) = last else {
                continue;
            };

            if last.elapsed() <= ENGINE_INACTIVITY_TIMEOUT {
                continue;
            }

            info!(info_hash = %hash, "engine inactive, destroying it");
            if let Err(err) = self.remove(&hash).await {
                warn!(info_hash = %hash, error = %err, "cleanup remove failed");
                continue;
            }
            info!(info_hash = %hash, "engine destroyed");
        }
    }

    async fn cleanup_cache_to_limit(&self, settings: &SettingsStore) {
        let Some(limit) = settings.cache_size_limit().await else {
            debug!("cache reaper skipped because cacheSize is unlimited");
            return;
        };

        let active_hashes = self.active_cache_keys().await;
        let (mut total, mut candidates) = match collect_cache_entries(
            &self.cache_dir,
            &active_hashes,
        )
        .await
        {
            Ok(entries) => entries,
            Err(err) => {
                warn!(cache_dir = %self.cache_dir.display(), error = %err, "cache reaper scan failed");
                return;
            }
        };

        if total <= limit {
            debug!(
                cache_dir = %self.cache_dir.display(),
                total,
                limit,
                "cache reaper skipped; cache is within limit"
            );
            return;
        }

        candidates.sort_by(|a, b| {
            a.modified
                .cmp(&b.modified)
                .then_with(|| a.path.cmp(&b.path))
        });

        info!(
            cache_dir = %self.cache_dir.display(),
            total,
            limit,
            candidates = candidates.len(),
            "cache is over limit; pruning inactive entries"
        );

        for entry in candidates {
            if total <= limit {
                break;
            }

            match remove_cache_entry(&entry, &self.cache_dir).await {
                Ok(()) => {
                    total = total.saturating_sub(entry.size);
                    info!(
                        key = %entry.key,
                        path = %entry.path.display(),
                        freed = entry.size,
                        remaining = total,
                        limit,
                        "cache entry removed"
                    );
                }
                Err(err) => {
                    warn!(
                        key = %entry.key,
                        path = %entry.path.display(),
                        error = %err,
                        "cache entry removal failed"
                    );
                }
            }
        }

        if total > limit {
            warn!(
                cache_dir = %self.cache_dir.display(),
                total,
                limit,
                active = active_hashes.len(),
                "cache remains over limit; remaining data is active or could not be removed"
            );
        }
    }

    async fn active_cache_keys(&self) -> HashSet<String> {
        let mut active = self
            .handles
            .read()
            .await
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        active.extend(self.active_streams.read().await.keys().cloned());
        active
    }
}

async fn collect_cache_entries(
    cache_dir: &Path,
    active_keys: &HashSet<String>,
) -> anyhow::Result<(u64, Vec<CacheEntry>)> {
    let mut total = 0u64;
    let mut candidates = Vec::new();

    let mut entries = match tokio::fs::read_dir(cache_dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok((0, Vec::new())),
        Err(err) => return Err(err).with_context(|| format!("reading {}", cache_dir.display())),
    };

    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("reading {}", cache_dir.display()))?
    {
        let path = entry.path();
        let key = entry.file_name().to_string_lossy().to_string();

        // librqbit stores fastresume bitfields here.  We delete per-torrent
        // bitfields together with the torrent cache entry, but never prune the
        // session directory as a standalone cache object.
        if key.eq_ignore_ascii_case("session") {
            continue;
        }

        let (size, modified) = cache_entry_stats(&path)
            .await
            .with_context(|| format!("scanning cache entry {}", path.display()))?;
        total = total.saturating_add(size);

        if !active_keys.contains(&key) {
            candidates.push(CacheEntry {
                key,
                path,
                size,
                modified,
            });
        }
    }

    Ok((total, candidates))
}

async fn cache_entry_stats(path: &Path) -> anyhow::Result<(u64, SystemTime)> {
    let mut size = 0u64;
    let mut modified = SystemTime::UNIX_EPOCH;
    let mut stack = vec![path.to_path_buf()];

    while let Some(path) = stack.pop() {
        let metadata = match tokio::fs::symlink_metadata(&path).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err).with_context(|| format!("stat {}", path.display())),
        };

        if let Ok(mtime) = metadata.modified() {
            if mtime > modified {
                modified = mtime;
            }
        }

        if metadata.is_dir() {
            let mut entries = match tokio::fs::read_dir(&path).await {
                Ok(entries) => entries,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err).with_context(|| format!("reading {}", path.display())),
            };

            while let Some(entry) = entries
                .next_entry()
                .await
                .with_context(|| format!("reading {}", path.display()))?
            {
                stack.push(entry.path());
            }
        } else if metadata.is_file() {
            size = size.saturating_add(metadata.len());
        }
    }

    Ok((size, modified))
}

async fn remove_cache_entry(entry: &CacheEntry, cache_dir: &Path) -> anyhow::Result<()> {
    let canonical_cache = tokio::fs::canonicalize(cache_dir)
        .await
        .with_context(|| format!("canonicalizing {}", cache_dir.display()))?;
    let parent = entry.path.parent().context("cache entry has no parent")?;
    let canonical_parent = tokio::fs::canonicalize(parent)
        .await
        .with_context(|| format!("canonicalizing {}", parent.display()))?;

    if canonical_parent != canonical_cache {
        anyhow::bail!(
            "refusing to remove cache entry outside cache root: {}",
            entry.path.display()
        );
    }

    let metadata = match tokio::fs::symlink_metadata(&entry.path).await {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err).with_context(|| format!("stat {}", entry.path.display())),
    };

    if metadata.is_dir() {
        tokio::fs::remove_dir_all(&entry.path)
            .await
            .with_context(|| format!("removing {}", entry.path.display()))?;
    } else {
        tokio::fs::remove_file(&entry.path)
            .await
            .with_context(|| format!("removing {}", entry.path.display()))?;
    }

    remove_fastresume_for_cache_key(cache_dir, &entry.key).await;
    Ok(())
}

async fn remove_fastresume_for_cache_key(cache_dir: &Path, key: &str) {
    if key.len() != 40 || !key.chars().all(|c| c.is_ascii_hexdigit()) {
        return;
    }

    let bitv_path = cache_dir.join("session").join(format!("{key}.bitv"));
    match tokio::fs::remove_file(&bitv_path).await {
        Ok(()) => debug!(path = %bitv_path.display(), "removed stale fastresume bitfield"),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            warn!(path = %bitv_path.display(), error = %err, "failed to remove fastresume bitfield")
        }
    }
}

fn stats_for_handle(
    handle: &Arc<librqbit::ManagedTorrent>,
    cache_dir: &Path,
    stream_idx: Option<usize>,
    guessed_idx: Option<usize>,
) -> Value {
    let stats = handle.stats();
    let files = files_for_handle(handle);
    let live = stats.live.as_ref();
    let peer_stats = live.map(|live| &live.snapshot.peer_stats);
    let live_peers = peer_stats.map(|peers| peers.live).unwrap_or(0);
    let queued_peers = peer_stats.map(|peers| peers.queued).unwrap_or(0);
    let connecting_peers = peer_stats.map(|peers| peers.connecting).unwrap_or(0);
    let discovered_peers = peer_stats.map(|peers| peers.seen).unwrap_or(live_peers);
    let dead_peers = peer_stats.map(|peers| peers.dead).unwrap_or(0);
    let unique_peers = discovered_peers;
    let download_speed = live
        .map(|live| mib_per_sec_to_bytes_per_sec(live.download_speed.mbps))
        .unwrap_or(0.0);
    let upload_speed = live
        .map(|live| mib_per_sec_to_bytes_per_sec(live.upload_speed.mbps))
        .unwrap_or(0.0);

    // Keep completion based on verified torrent/file bytes. `fetched_bytes` includes duplicate/raw peer
    // traffic and can exceed the file length, which makes the player show impossible percentages.
    let downloaded = stats.progress_bytes;
    let display_peers = if live_peers > 0 {
        live_peers
    } else if connecting_peers > 0 {
        connecting_peers.min(40)
    } else if unique_peers > 0 {
        unique_peers.min(3)
    } else if downloaded > 0 && !stats.finished {
        1
    } else {
        0
    };
    let info_hash = handle.info_hash().as_string();
    let source_urls = source_urls(&info_hash);

    let wires = stream_idx
        .is_none()
        .then(|| {
            handle
                .live()
                .map(|live| live.per_peer_stats_snapshot(Default::default()))
                .map(|snapshot| {
                    let mut addrs = snapshot.peers.keys().cloned().collect::<Vec<_>>();
                    addrs.sort();

                    let n = addrs.len() as f64;
                    let (base_down, mut rem_down) = if addrs.is_empty() {
                        (0.0, 0.0)
                    } else {
                        ((download_speed / n).floor(), download_speed % n)
                    };
                    let (base_up, mut rem_up) = if addrs.is_empty() {
                        (0.0, 0.0)
                    } else {
                        ((upload_speed / n).floor(), upload_speed % n)
                    };

                    addrs
                        .into_iter()
                        .map(|addr| {
                            let mut down = base_down;
                            if rem_down >= 1.0 {
                                down += 1.0;
                                rem_down -= 1.0;
                            }

                            let mut up = base_up;
                            if rem_up >= 1.0 {
                                up += 1.0;
                                rem_up -= 1.0;
                            }

                            json!({
                                "requests": 0,
                                "address": addr,
                                "amInterested": false,
                                "isSeeder": false,
                                "downSpeed": down,
                                "upSpeed": up
                            })
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        })
        .unwrap_or_default();

    let mut out = EngineStats {
        info_hash: info_hash.clone(),
        name: handle.name().unwrap_or_default(),
        peers: if wires.is_empty() {
            display_peers as usize
        } else {
            wires.len()
        },
        unchoked: if live_peers > 1 {
            (live_peers / 2) as usize
        } else {
            live_peers as usize
        },
        queued: (queued_peers + connecting_peers) as usize,
        unique: unique_peers as usize,
        connection_tries: dead_peers as usize,
        swarm_paused: matches!(stats.state, librqbit::TorrentStatsState::Paused),
        swarm_connections: display_peers as usize,
        swarm_size: 400,
        selections: Vec::new(),
        wires: Some(wires),
        files: files.clone(),
        downloaded,
        uploaded: stats.uploaded_bytes,
        download_speed,
        upload_speed: download_speed.max(upload_speed),
        sources: official_sources(&source_urls, unique_peers.min(400) as usize),
        peer_search_running: !stats.finished,
        opts: official_stats_opts(&source_urls, &info_hash, cache_dir),
        state: stats.state.to_string(),
        error: stats.error,
        finished: None,
        stream_len: None,
        stream_name: None,
        stream_progress: None,
        guessed_file_idx: guessed_idx,
    };

    if let Some(idx) = stream_idx {
        if let Some(file) = files.get(idx) {
            out.wires = None;
            out.stream_len = Some(file.length);
            out.stream_name = Some(file.name.clone());
            out.stream_progress = Some({
                let done = stats
                    .file_progress
                    .get(idx)
                    .copied()
                    .unwrap_or(0)
                    .min(file.length);
                if stats.finished {
                    1.0
                } else if file.length == 0 {
                    0.0
                } else {
                    done as f64 / file.length as f64
                }
            });
        }
    }

    serde_json::to_value(out).unwrap_or_else(|_| json!(null))
}

fn source_urls(info_hash: &str) -> Vec<String> {
    DEFAULT_TRACKERS
        .iter()
        .map(|tracker| format!("tracker:{tracker}"))
        .chain(std::iter::once(format!("dht:{info_hash}")))
        .collect()
}

fn official_sources(source_urls: &[String], discovered_peers: usize) -> Value {
    let last_started = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let mut remaining = discovered_peers;
    let tracker_count = source_urls.len().saturating_sub(1).max(1);

    Value::Array(
        source_urls
            .iter()
            .enumerate()
            .map(|(_idx, url)| {
                let is_dht = url.starts_with("dht:");
                let found = if is_dht {
                    remaining
                } else {
                    let share = remaining.div_ceil(tracker_count);
                    remaining = remaining.saturating_sub(share);
                    share
                };

                json!({
                    "numFound": found,
                    "numFoundUniq": found,
                    "numRequests": if is_dht { 0 } else { 1 },
                    "url": url,
                    "lastStarted": last_started
                })
            })
            .collect(),
    )
}

fn official_stats_opts(source_urls: &[String], info_hash: &str, cache_dir: &Path) -> Value {
    let path = cache_dir.join(info_hash).to_string_lossy().to_string();
    json!({
        "peerSearch": {
            "min": 40,
            "max": 150,
            "sources": source_urls
        },
        "dht": false,
        "tracker": false,
        "connections": 400,
        "handshakeTimeout": 25000,
        "timeout": 6000,
        "virtual": true,
        "swarmCap": {
            "minPeers": 10,
            "maxSpeed": 8388608u64
        },
        "growler": {
            "flood": 0,
            "pulse": 78643200u64
        },
        "path": path
    })
}

fn files_for_handle(handle: &Arc<librqbit::ManagedTorrent>) -> Vec<EngineFile> {
    handle
        .with_metadata(|metadata| {
            metadata
                .file_infos
                .iter()
                .map(|file| {
                    let path = file.relative_filename.to_string_lossy().replace('\\', "/");
                    let name = file
                        .relative_filename
                        .file_name()
                        .map(|name| name.to_string_lossy().into_owned())
                        .unwrap_or_else(|| path.clone());
                    EngineFile {
                        path,
                        name,
                        length: file.len,
                        offset: file.offset_in_torrent,
                        cache_events: true,
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

fn file_for_handle(
    handle: &Arc<librqbit::ManagedTorrent>,
    idx: usize,
) -> anyhow::Result<Option<EngineFile>> {
    Ok(files_for_handle(handle).get(idx).cloned())
}

fn guess_index_for_handle(
    handle: &Arc<librqbit::ManagedTorrent>,
    filters: &[String],
    guess: Option<&Value>,
    explicit_idx: Option<isize>,
) -> Option<usize> {
    if let Some(idx) = explicit_idx.and_then(valid_idx) {
        return Some(idx);
    }
    let files = files_for_handle(handle);
    guess_index(&files, filters, guess)
}

fn resolve_file_index(
    handle: &Arc<librqbit::ManagedTorrent>,
    idx: &str,
    filters: &[String],
) -> anyhow::Result<usize> {
    if let Ok(value) = idx.parse::<isize>() {
        if let Some(idx) = valid_idx(value) {
            return Ok(idx);
        }
    }

    let files = files_for_handle(handle);
    if let Some(idx) = guess_index(&files, filters, Some(&Value::String(idx.to_string()))) {
        return Ok(idx);
    }

    guess_index(&files, filters, None).context("could not resolve torrent file index")
}

fn valid_idx(idx: isize) -> Option<usize> {
    if idx >= 0 { Some(idx as usize) } else { None }
}

fn guess_index(files: &[EngineFile], filters: &[String], guess: Option<&Value>) -> Option<usize> {
    if !filters.is_empty() {
        if let Some(idx) = files.iter().position(|file| {
            filters
                .iter()
                .all(|filter| filter_matches_file(filter, &file.path, &file.name))
        }) {
            return Some(idx);
        }
    }

    if let Some(guess) = guess.and_then(value_to_guess_string) {
        let needle = guess.to_ascii_lowercase();
        if let Some(idx) = files.iter().position(|file| {
            file.path.to_ascii_lowercase().contains(&needle)
                || file.name.to_ascii_lowercase().contains(&needle)
        }) {
            return Some(idx);
        }
    }

    files
        .iter()
        .enumerate()
        .filter(|(_, file)| is_video_like(&file.name))
        .max_by_key(|(_, file)| file.length)
        .map(|(idx, _)| idx)
        .or_else(|| {
            files
                .iter()
                .enumerate()
                .max_by_key(|(_, file)| file.length)
                .map(|(idx, _)| idx)
        })
}

fn value_to_guess_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) if !s.trim().is_empty() => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn filter_matches_file(filter: &str, path: &str, name: &str) -> bool {
    let haystack = format!("{path}\n{name}");
    if let Some((pattern, flags)) = parse_regex_filter(filter) {
        let mut builder = RegexBuilder::new(pattern);
        builder.case_insensitive(flags.contains('i'));
        return builder
            .build()
            .map(|regex| regex.is_match(&haystack))
            .unwrap_or(false);
    }
    haystack
        .to_ascii_lowercase()
        .contains(&filter.to_ascii_lowercase())
}

fn parse_regex_filter(filter: &str) -> Option<(&str, &str)> {
    if !filter.starts_with('/') {
        return None;
    }
    let last = filter.rfind('/')?;
    if last == 0 {
        return None;
    }
    Some((&filter[1..last], &filter[last + 1..]))
}

fn is_video_like(name: &str) -> bool {
    let Some(ext) = Path::new(name).extension().and_then(|ext| ext.to_str()) else {
        return false;
    };
    matches!(
        ext.to_ascii_lowercase().as_str(),
        "mkv" | "mp4" | "avi" | "mov" | "m4v" | "webm" | "ts" | "m2ts" | "wmv"
    )
}

fn display_name_from_filename(name: &str) -> String {
    let stem = Path::new(name)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or(name);
    stem.replace(['.', '_'], " ")
}

fn parse_json_body<T>(body: &Bytes) -> AppResult<T>
where
    T: Default + for<'de> Deserialize<'de>,
{
    if body.is_empty() {
        return Ok(T::default());
    }
    serde_json::from_slice(body)
        .or_else(|_| serde_json::from_slice::<T>(b"{}"))
        .context("parsing JSON body")
        .map_err(AppError::from)
}

async fn read_torrent_source(state: &AppState, from: &str) -> AppResult<Bytes> {
    if from.starts_with("http://") || from.starts_with("https://") {
        return state
            .client
            .get(from)
            .send()
            .await
            .context("fetching torrent file")?
            .error_for_status()
            .context("torrent file HTTP status")?
            .bytes()
            .await
            .context("reading torrent file HTTP body")
            .map_err(AppError::from);
    }
    tokio::fs::read(from)
        .await
        .with_context(|| format!("reading torrent file {from}"))
        .map(Bytes::from)
        .map_err(AppError::from)
}

fn decode_hex(input: &str) -> AppResult<Bytes> {
    let mut normalized = input.trim();
    if let Some(stripped) = normalized.strip_prefix("0x") {
        normalized = stripped;
    }
    let mut out = Vec::with_capacity(normalized.len() / 2);
    let mut chars = normalized.as_bytes().chunks_exact(2);
    if !chars.remainder().is_empty() {
        return Err(anyhow!("hex blob has odd length").into());
    }
    for chunk in &mut chars {
        let text = std::str::from_utf8(chunk).context("hex blob is not utf8")?;
        out.push(u8::from_str_radix(text, 16).context("hex blob contains invalid digits")?);
    }
    Ok(Bytes::from(out))
}

fn normalize_info_hash(input: &str) -> anyhow::Result<String> {
    let trimmed = input.trim();
    if trimmed.len() == 40 && trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
        return Ok(trimmed.to_ascii_lowercase());
    }
    let magnet = Magnet::parse(trimmed)?;
    magnet
        .as_id20()
        .map(|id| id.as_string())
        .context("magnet did not contain a v1 BTIH hash")
}

fn merge_trackers<I>(extra: I) -> Vec<String>
where
    I: IntoIterator<Item = String>,
{
    let mut seen = HashSet::new();
    let mut trackers = Vec::new();
    for tracker in DEFAULT_TRACKERS
        .iter()
        .map(|tracker| tracker.to_string())
        .chain(extra)
    {
        let trimmed = tracker.trim();
        if trimmed.is_empty() || !seen.insert(trimmed.to_ascii_lowercase()) {
            continue;
        }
        trackers.push(trimmed.to_string());
    }
    trackers
}

fn parse_peer_addrs(values: &[String]) -> Vec<SocketAddr> {
    values
        .iter()
        .filter_map(|value| match value.parse::<SocketAddr>() {
            Ok(addr) => Some(addr),
            Err(err) => {
                warn!(peer = value, error = %err, "ignoring invalid initial peer");
                None
            }
        })
        .collect()
}

fn parse_stream_query(raw_query: Option<&str>) -> StreamQuery {
    StreamQuery {
        external: query_flag(raw_query, "external"),
        download: query_flag(raw_query, "download"),
        subtitles_sec: query_first(raw_query, "subtitles"),
        trackers: query_values(raw_query, "tr"),
        filters: query_values(raw_query, "f"),
    }
}

fn query_pairs(raw_query: Option<&str>) -> Vec<(String, String)> {
    raw_query
        .map(|raw| {
            form_urlencoded::parse(raw.as_bytes())
                .map(|(key, value)| (key.into_owned(), value.into_owned()))
                .collect()
        })
        .unwrap_or_default()
}

fn query_values(raw_query: Option<&str>, key: &str) -> Vec<String> {
    query_pairs(raw_query)
        .into_iter()
        .filter_map(|(k, v)| if k == key { Some(v) } else { None })
        .collect()
}

fn query_first(raw_query: Option<&str>, key: &str) -> Option<String> {
    query_values(raw_query, key).into_iter().next()
}

fn query_flag(raw_query: Option<&str>, key: &str) -> bool {
    query_pairs(raw_query).into_iter().any(|(k, v)| {
        k == key && (v.is_empty() || !matches!(v.as_str(), "0" | "false" | "False" | "FALSE"))
    })
}

fn parse_range(header: &str, len: u64) -> Option<(u64, u64)> {
    let range = header.strip_prefix("bytes=")?.split(',').next()?.trim();
    let (start, end) = range.split_once('-')?;

    if start.is_empty() {
        let suffix = end.parse::<u64>().ok()?;
        if suffix == 0 {
            return None;
        }
        let start = len.saturating_sub(suffix);
        return Some((start, len.saturating_sub(1)));
    }

    let start = start.parse::<u64>().ok()?;
    let end = if end.is_empty() {
        len.saturating_sub(1)
    } else {
        end.parse::<u64>().ok()?.min(len.saturating_sub(1))
    };

    if len == 0 || start > end || start >= len {
        return None;
    }
    Some((start, end))
}

fn mib_per_sec_to_bytes_per_sec(speed: f64) -> f64 {
    if speed.is_finite() && speed > 0.0 {
        speed * 1024.0 * 1024.0
    } else {
        0.0
    }
}

fn header_value(value: &str) -> anyhow::Result<HeaderValue> {
    HeaderValue::from_str(value).with_context(|| format!("invalid header value {value:?}"))
}

fn redirect(status: StatusCode, location: &str) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    if let Ok(location) = HeaderValue::from_str(location) {
        response.headers_mut().insert(LOCATION, location);
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
