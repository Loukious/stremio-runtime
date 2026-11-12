//! Stremio HLSv2 VOD conversion. Segments are generated on demand: seeking does
//! not require converting or downloading all preceding media.
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{
    Json,
    body::Body,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::sync::{Mutex, Notify, OnceCell, Semaphore, watch};
use url::{Url, form_urlencoded};

use crate::media_probe::{
    HlsProbe, HlsSamples, HlsStream, probe_media, probe_samples, resolve_media_url,
};

#[path = "hls_mp4.rs"]
mod mp4;
#[path = "hls_process.rs"]
mod process;

const SEGMENT_SECONDS: f64 = 4.0;
const SESSION_LIMIT: usize = 2;
const PROCESS_LIMIT: usize = 2;
const CACHE_LIMIT: usize = 64 * 1024 * 1024;
const SEGMENT_LIMIT: usize = 64 * 1024 * 1024;
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug)]
enum HlsError {
    Invalid(&'static str),
    Missing,
    Busy,
    Cancelled,
    Failed(&'static str),
    Process(process::ProcessError),
}

impl fmt::Display for HlsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Invalid(message) | Self::Failed(message) => message,
            Self::Missing => "Track or segment not found",
            Self::Busy => "HLS conversion is busy; try again later",
            Self::Cancelled => "HLS converter is destroyed",
            Self::Process(process::ProcessError::Unavailable) => "FFmpeg is unavailable",
            Self::Process(process::ProcessError::Failed) => "FFmpeg could not convert the media",
            Self::Process(process::ProcessError::TooLarge) => {
                "Converted media exceeded the size limit"
            }
            Self::Process(process::ProcessError::Timeout) => "FFmpeg conversion timed out",
            Self::Process(process::ProcessError::Cancelled) => "HLS converter is destroyed",
        })
    }
}

fn error_response(code: u32, operation: &str, error: HlsError) -> Response {
    let status = match error {
        HlsError::Missing => return StatusCode::NOT_FOUND.into_response(),
        HlsError::Busy => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(json!({ "error": { "code": code, "message": format!("Failed to {operation}: {error}") } }))).into_response()
}

fn data_response(content_type: &'static str, bytes: impl Into<Bytes>) -> Response {
    let bytes = bytes.into();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, bytes.len())
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(bytes))
        .unwrap()
}

#[derive(Clone)]
struct Options {
    media_url: String,
    query: String,
    max_audio_channels: u32,
    max_width: Option<u32>,
    video_codecs: Vec<String>,
    audio_codecs: Vec<String>,
    force: bool,
}

impl Options {
    fn parse(query: Option<&str>, base: &str) -> Result<Self, HlsError> {
        let pairs: Vec<_> = form_urlencoded::parse(query.unwrap_or_default().as_bytes())
            .into_owned()
            .collect();
        let values = |name: &str| {
            pairs
                .iter()
                .filter(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
                .collect::<Vec<_>>()
        };
        let urls = values("mediaURL");
        let [input] = urls.as_slice() else {
            return Err(HlsError::Invalid("mediaURL is required"));
        };
        let resolved = resolve_media_url(input, base)
            .map_err(|_| HlsError::Invalid("Invalid HTTP(S) media URL"))?;
        let media_url = if input.to_ascii_lowercase().starts_with("http://")
            || input.to_ascii_lowercase().starts_with("https://")
        {
            input.clone()
        } else {
            resolved.to_string()
        };
        let positive = |name: &str, max: u32| -> Result<Option<u32>, HlsError> {
            let values = values(name);
            match values.as_slice() {
                [] => Ok(None),
                [value] => value
                    .parse::<u32>()
                    .ok()
                    .filter(|value| *value > 0 && *value <= max)
                    .map(Some)
                    .ok_or(HlsError::Invalid("Invalid conversion limit")),
                _ => Err(HlsError::Invalid("Duplicate conversion limit")),
            }
        };
        let max_audio_channels = positive("maxAudioChannels", 32)?.unwrap_or(2);
        let max_width = positive("maxWidth", 7680)?.map(|width| (width / 2 * 2).max(2));
        if values("profile")
            .iter()
            .any(|profile| !matches!(profile.as_str(), "" | "none" | "software" | "cpu"))
        {
            return Err(HlsError::Invalid(
                "Hardware conversion profiles are not supported",
            ));
        }
        let mut canonical = form_urlencoded::Serializer::new(String::new());
        canonical.append_pair("mediaURL", &media_url);
        for (key, value) in &pairs {
            if key != "mediaURL" {
                canonical.append_pair(key, value);
            }
        }
        Ok(Self {
            media_url,
            query: canonical.finish(),
            max_audio_channels,
            max_width,
            video_codecs: values("videoCodecs"),
            audio_codecs: values("audioCodecs"),
            force: values("forceTranscoding")
                .iter()
                .any(|value| !value.is_empty()),
        })
    }
}

struct ServiceInner {
    sessions: Mutex<HashMap<String, Arc<Converter>>>,
    slots: Arc<Semaphore>,
    stopping: AtomicBool,
}

#[derive(Clone)]
pub(super) struct HlsService {
    inner: Arc<ServiceInner>,
}

impl HlsService {
    pub(super) fn new() -> Self {
        let inner = Arc::new(ServiceInner {
            sessions: Mutex::new(HashMap::new()),
            slots: Arc::new(Semaphore::new(PROCESS_LIMIT)),
            stopping: AtomicBool::new(false),
        });
        let weak = Arc::downgrade(&inner);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(15));
            loop {
                tick.tick().await;
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                if inner.stopping.load(Ordering::Acquire) {
                    break;
                }
                let expired = {
                    let mut sessions = inner.sessions.lock().await;
                    let ids = sessions
                        .iter()
                        .filter(|(_, session)| {
                            session.idle() > IDLE_TIMEOUT
                                && session.active.load(Ordering::Acquire) == 0
                        })
                        .map(|(id, _)| id.clone())
                        .collect::<Vec<_>>();
                    ids.into_iter()
                        .filter_map(|id| sessions.remove(&id))
                        .collect::<Vec<_>>()
                };
                for session in expired {
                    session.shutdown().await;
                }
            }
        });
        Self { inner }
    }

    async fn converter(
        &self,
        id: &str,
        query: Option<&str>,
        base: &str,
    ) -> Result<Arc<Converter>, HlsError> {
        if self.inner.stopping.load(Ordering::Acquire) {
            return Err(HlsError::Cancelled);
        }
        if id.is_empty()
            || id.len() > 128
            || !id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(HlsError::Invalid("Invalid converter id"));
        }
        let mut sessions = self.inner.sessions.lock().await;
        if let Some(converter) = sessions.get(id) {
            converter.touch();
            return Ok(converter.clone());
        }
        let options = Options::parse(query, base)?;
        if sessions.len() >= SESSION_LIMIT {
            let oldest = sessions
                .iter()
                .filter(|(_, converter)| converter.active.load(Ordering::Acquire) == 0)
                .max_by_key(|(_, converter)| converter.idle())
                .map(|(id, _)| id.clone());
            let Some(oldest) = oldest else {
                return Err(HlsError::Busy);
            };
            if let Some(old) = sessions.remove(&oldest) {
                old.cancel();
            }
        }
        let converter = Arc::new(Converter::new(
            options,
            base.to_string(),
            self.inner.slots.clone(),
        ));
        sessions.insert(id.to_string(), converter.clone());
        Ok(converter)
    }

    pub(super) async fn handle(
        &self,
        id: &str,
        rest: &str,
        query: Option<&str>,
        base: &str,
    ) -> Response {
        let converter = match self.converter(id, query, base).await {
            Ok(converter) => converter,
            Err(error) => return error_response(1, "create hls converter", error),
        };
        if rest == "burn" {
            return match Box::pin(converter.burn(query)).await {
                Ok(()) => StatusCode::OK.into_response(),
                Err(error) => error_response(40, "burn subtitles", error),
            };
        }
        if let Some(track) = rest
            .strip_suffix(".m3u8")
            .filter(|track| !track.contains('/'))
        {
            return match converter.playlist(track).await {
                Ok(playlist) => data_response("application/vnd.apple.mpegurl", playlist),
                Err(error) => error_response(10, "read hls playlist", error),
            };
        }
        if let Some(track) = rest.strip_suffix("/init.mp4") {
            return match converter.init_segment(track).await {
                Ok(bytes) => data_response("video/mp4", bytes),
                Err(error) => error_response(20, "read init segment", error),
            };
        }
        if let Some((track, file)) = rest.split_once('/')
            && let Some(sequence) = file
                .strip_prefix("segment")
                .and_then(|value| value.split_once('.'))
            && let Ok(number) = sequence.0.parse::<usize>()
        {
            let valid = ((track.starts_with("video") || track.starts_with("audio"))
                && sequence.1 == "m4s")
                || (track.starts_with("subtitle") && sequence.1 == "vtt");
            if valid {
                return match converter.segment(track, number).await {
                    Ok(bytes) => data_response(
                        if sequence.1 == "vtt" {
                            "text/vtt"
                        } else {
                            "video/mp4"
                        },
                        bytes,
                    ),
                    Err(error) => error_response(30, "read media segment", error),
                };
            }
        }
        if let Some(value) = rest
            .strip_prefix("source/subtitle/")
            .and_then(|value| value.strip_suffix(".ass"))
        {
            return match value.parse::<usize>() {
                Ok(id) => match converter.source("subtitle", id).await {
                    Ok((bytes, _)) => data_response("text/plain; charset=utf-8", bytes),
                    Err(error) => error_response(50, "read subtitle source", error),
                },
                Err(_) => StatusCode::NOT_FOUND.into_response(),
            };
        }
        if let Some(value) = rest.strip_prefix("source/attachment/") {
            return match value.parse::<usize>() {
                Ok(id) => match converter.source("attachment", id).await {
                    Ok((bytes, mime)) => {
                        let mut response = data_response("application/octet-stream", bytes);
                        if let Some(mime) =
                            mime.and_then(|mime| mime.parse::<axum::http::HeaderValue>().ok())
                        {
                            response.headers_mut().insert(header::CONTENT_TYPE, mime);
                        }
                        response
                    }
                    Err(error) => error_response(60, "read attachment source", error),
                },
                Err(_) => StatusCode::NOT_FOUND.into_response(),
            };
        }
        StatusCode::NOT_FOUND.into_response()
    }

    pub(super) async fn status(&self) -> Value {
        let sessions = self
            .inner
            .sessions
            .lock()
            .await
            .iter()
            .map(|(id, converter)| (id.clone(), converter.clone()))
            .collect::<Vec<_>>();
        let mut result = serde_json::Map::new();
        for (id, converter) in sessions {
            result.insert(id, converter.status().await);
        }
        Value::Object(result)
    }

    pub(super) async fn destroy(&self, id: &str) -> Response {
        let converter = { self.inner.sessions.lock().await.remove(id) };
        if let Some(converter) = converter {
            converter.shutdown().await;
        }
        StatusCode::OK.into_response()
    }

    pub(super) async fn shutdown(&self) {
        self.inner.stopping.store(true, Ordering::Release);
        let converters = self
            .inner
            .sessions
            .lock()
            .await
            .drain()
            .map(|(_, converter)| converter)
            .collect::<Vec<_>>();
        for converter in converters {
            converter.shutdown().await;
        }
    }
}

struct Converter {
    options: Options,
    base: String,
    probe: OnceCell<HlsProbe>,
    tracks: Mutex<HashMap<String, Arc<Track>>>,
    slots: Arc<Semaphore>,
    cancelled: watch::Sender<bool>,
    touched: StdMutex<Instant>,
    touched_epoch: AtomicU64,
    active: AtomicUsize,
    idle_notify: Notify,
    cache_bytes: AtomicUsize,
}

impl Converter {
    fn new(options: Options, base: String, slots: Arc<Semaphore>) -> Self {
        let (cancelled, _) = watch::channel(false);
        Self {
            options,
            base,
            slots,
            cancelled,
            probe: OnceCell::new(),
            tracks: Mutex::new(HashMap::new()),
            touched: StdMutex::new(Instant::now()),
            touched_epoch: AtomicU64::new(epoch_ms()),
            active: AtomicUsize::new(0),
            idle_notify: Notify::new(),
            cache_bytes: AtomicUsize::new(0),
        }
    }
    fn touch(&self) {
        *self.touched.lock().unwrap() = Instant::now();
        self.touched_epoch.store(epoch_ms(), Ordering::Release);
    }
    fn idle(&self) -> Duration {
        self.touched.lock().unwrap().elapsed()
    }
    fn cancel(&self) {
        self.cancelled.send_replace(true);
    }
    async fn shutdown(&self) {
        self.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let notified = self.idle_notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.active.load(Ordering::Acquire) == 0 {
                    break;
                }
                notified.await;
            }
        })
        .await;
        self.tracks.lock().await.clear();
        self.cache_bytes.store(0, Ordering::Release);
    }
    async fn metadata(&self) -> Result<&HlsProbe, HlsError> {
        let mut cancelled = self.cancelled.subscribe();
        if *cancelled.borrow() {
            return Err(HlsError::Cancelled);
        }
        tokio::select! {
        result = self.probe
            .get_or_try_init(|| async {
                Box::pin(probe_media(&self.options.media_url, &self.base))
                    .await
                    .map_err(|_| HlsError::Failed("Could not probe the media"))
            })
            => if *cancelled.borrow() { Err(HlsError::Cancelled) } else { result },
        _ = cancelled.changed() => Err(HlsError::Cancelled),
        }
    }
    async fn track(&self, name: &str) -> Result<Arc<Track>, HlsError> {
        let metadata = self.metadata().await?;
        let duration = metadata
            .format
            .duration
            .filter(|duration| duration.is_finite() && *duration > 0.0)
            .ok_or(HlsError::Invalid("Live media is not supported"))?;
        if duration > 7.0 * 86400.0 {
            return Err(HlsError::Invalid(
                "Media duration exceeds the conversion limit",
            ));
        }
        if let Some(track) = self.tracks.lock().await.get(name) {
            return Ok(track.clone());
        }
        let stream = metadata
            .streams
            .iter()
            .find(|stream| format!("{}{}", stream.track, stream.id) == name)
            .filter(|stream| matches!(stream.track.as_str(), "video" | "audio" | "subtitle"))
            .ok_or(HlsError::Missing)?
            .clone();
        if name != format!("{}{}", stream.track, stream.id)
            || (stream.track == "video" && stream.id != 0)
        {
            return Err(HlsError::Missing);
        }
        let mut segments = uniform_segments(duration);
        let mut copy = false;
        if stream.track == "video"
            && !self.options.force
            && matches!(stream.codec.as_str(), "h264" | "hevc")
            && (stream.codec != "hevc" || metadata.format.name.contains("mp4"))
            && self
                .options
                .video_codecs
                .iter()
                .any(|codec| codec == &stream.codec)
            && self
                .options
                .max_width
                .is_none_or(|max| stream.width().is_some_and(|width| max >= width))
        {
            if let Ok(url) = Url::parse(&self.options.media_url)
                && let Ok(Ok(samples)) =
                    tokio::time::timeout(Duration::from_secs(5), probe_samples(&url, metadata))
                        .await
                && let Some(timeline) = samples
                    .get(&stream.index.to_string())
                    .and_then(|samples| copy_segments(samples, duration))
            {
                segments = timeline;
                copy = true;
            }
            if copy && metadata.format.name.contains("matroska") {
                // Cues identify random-access packets but do not prove GOP
                // independence. Check a later GOP's first VCL NAL; open-GOP
                // recovery pictures need references outside a seeked segment.
                let mut check = *segments.get(1).unwrap_or(&segments[0]);
                check.frames = Some(1);
                copy = tokio::time::timeout(
                    Duration::from_secs(5),
                    self.execute(
                        &clip_arguments(&self.options, &stream, &check, true, None),
                        SEGMENT_LIMIT,
                    ),
                )
                .await
                .ok()
                .and_then(Result::ok)
                .is_some_and(|bytes| mp4::independent_video_start(&bytes, &stream.codec));
                if !copy {
                    segments = uniform_segments(duration);
                }
            }
        } else if stream.track == "audio" {
            copy = !self.options.force
                && stream.codec == "aac"
                && self.options.audio_codecs.iter().any(|codec| codec == "aac")
                && stream
                    .channels()
                    .is_some_and(|channels| channels <= self.options.max_audio_channels);
            if stream.start_time.is_some_and(|start| start.abs() > 0.025) {
                copy = false;
            }
            if copy {
                // A copied track cannot synthesize silence before/after its
                // source packets. Only copy when the packet table proves it
                // covers the advertised VOD timeline.
                copy = if let Ok(url) = Url::parse(&self.options.media_url) {
                    if let Ok(Ok(samples)) =
                        tokio::time::timeout(Duration::from_secs(5), probe_samples(&url, metadata))
                            .await
                    {
                        samples
                            .get(&stream.index.to_string())
                            .is_some_and(|samples| {
                                samples.samples.last().is_some_and(|last| {
                                    last.duration.is_some_and(|length| {
                                        (last.pts as f64 + length as f64) / samples.timescale
                                            >= duration - 0.05
                                    })
                                })
                            })
                    } else {
                        false
                    }
                } else {
                    false
                };
            }
        }
        let mut tracks = self.tracks.lock().await;
        if let Some(track) = tracks.get(name) {
            return Ok(track.clone());
        }
        if tracks.len() >= 12 {
            return Err(HlsError::Busy);
        }
        let track = Arc::new(Track {
            stream,
            segments,
            copy,
            cache: Mutex::new(TrackCache::default()),
            cancelled: watch::channel(false).0,
        });
        tracks.insert(name.to_string(), track.clone());
        Ok(track)
    }
    async fn playlist(&self, name: &str) -> Result<String, HlsError> {
        if name == "master" {
            let probe = self.metadata().await?;
            if probe
                .format
                .duration
                .filter(|duration| duration.is_finite() && *duration > 0.0)
                .is_none()
            {
                return Err(HlsError::Invalid("Live media is not supported"));
            }
            return master_playlist(probe, &self.options.query);
        }
        let track = self.track(name).await?;
        Ok(track_playlist(name, &track.segments, &self.options.query))
    }
    async fn init_segment(&self, name: &str) -> Result<Bytes, HlsError> {
        let track = self.track(name).await?;
        if !matches!(track.stream.track.as_str(), "video" | "audio") {
            return Err(HlsError::Missing);
        }
        let mut cache = track.cache.lock().await;
        if let Some(init) = &cache.init {
            return Ok(init.clone());
        }
        self.generate(&track, 1, &mut cache).await?;
        cache
            .init
            .clone()
            .ok_or(HlsError::Failed("Init segment is missing"))
    }
    async fn segment(&self, name: &str, sequence: usize) -> Result<Bytes, HlsError> {
        let track = self.track(name).await?;
        if sequence == 0 || sequence > track.segments.len() {
            return Err(HlsError::Missing);
        }
        let mut cache = track.cache.lock().await;
        if let Some(bytes) = cache.media.get(&sequence) {
            return Ok(bytes.clone());
        }
        self.generate(&track, sequence, &mut cache).await
    }
    async fn execute(&self, args: &[String], limit: usize) -> Result<Bytes, HlsError> {
        self.execute_for(args, limit, None).await
    }
    async fn execute_for(
        &self,
        args: &[String],
        limit: usize,
        track_cancel: Option<watch::Receiver<bool>>,
    ) -> Result<Bytes, HlsError> {
        let mut cancelled = self.cancelled.subscribe();
        if *cancelled.borrow() {
            return Err(HlsError::Cancelled);
        }
        let permit = tokio::select! {
            permit = tokio::time::timeout(Duration::from_secs(30), self.slots.clone().acquire_owned()) =>
                permit.map_err(|_| HlsError::Busy)?.map_err(|_| HlsError::Cancelled)?,
            _ = cancelled.changed() => return Err(HlsError::Cancelled),
        };
        self.active.fetch_add(1, Ordering::AcqRel);
        struct Active<'a>(&'a Converter);
        impl Drop for Active<'_> {
            fn drop(&mut self) {
                self.0.active.fetch_sub(1, Ordering::AcqRel);
                self.0.idle_notify.notify_waiters();
            }
        }
        let _active = Active(self);
        let _permit = permit;
        if let Some(mut track_cancel) = track_cancel {
            if *track_cancel.borrow() {
                return Err(HlsError::Cancelled);
            }
            tokio::select! {
                result = Box::pin(process::run(args, limit, cancelled)) => result.map_err(HlsError::Process),
                _ = track_cancel.changed() => Err(HlsError::Cancelled),
            }
        } else {
            Box::pin(process::run(args, limit, cancelled))
                .await
                .map_err(HlsError::Process)
        }
    }
    async fn generate(
        &self,
        track: &Track,
        sequence: usize,
        cache: &mut TrackCache,
    ) -> Result<Bytes, HlsError> {
        // Keep a single initialization header across arbitrary seeks. A copied
        // VFR GOP can have a different reorder delay from the first GOP.
        if track.copy && track.stream.track == "video" && sequence != 1 && cache.init.is_none() {
            Box::pin(self.generate(track, 1, cache)).await?;
        }
        let segment = track.segments.get(sequence - 1).ok_or(HlsError::Missing)?;
        let output = self
            .execute_for(
                &clip_arguments(
                    &self.options,
                    &track.stream,
                    segment,
                    track.copy,
                    cache.burn.as_deref(),
                ),
                SEGMENT_LIMIT,
                Some(track.cancelled.subscribe()),
            )
            .await?;
        let bytes = if track.stream.track == "subtitle" {
            normalize_vtt(&output, segment.start, segment.duration)?
        } else {
            if track.copy
                && track.stream.track == "video"
                && !mp4::independent_video_start(&output, &track.stream.codec)
            {
                return Err(HlsError::Failed(
                    "Video GOP is not independently seekable; force video transcoding",
                ));
            }
            let target = if track.copy && track.stream.track == "video" {
                let local = mp4::edit_offset(&output).map_err(HlsError::Failed)?;
                let canonical = cache
                    .init
                    .as_ref()
                    .map(|init| mp4::edit_offset(init))
                    .transpose()
                    .map_err(HlsError::Failed)?
                    .unwrap_or(local);
                segment.start + canonical - local
            } else {
                segment.start
            };
            let fragment = mp4::split(output, target).map_err(HlsError::Failed)?;
            if fragment.init.len() > 1024 * 1024 {
                return Err(HlsError::Failed("Init segment exceeded the size limit"));
            }
            if cache.init.is_none() {
                cache.init = Some(fragment.init);
            }
            fragment.media
        };
        while cache.order.len() >= 8
            || self
                .cache_bytes
                .load(Ordering::Acquire)
                .saturating_add(bytes.len())
                > CACHE_LIMIT
        {
            let Some(oldest) = cache.order.pop_front() else {
                break;
            };
            if let Some(old) = cache.media.remove(&oldest) {
                self.cache_bytes.fetch_sub(old.len(), Ordering::AcqRel);
            }
        }
        if reserve_cache(&self.cache_bytes, bytes.len()) {
            cache.media.insert(sequence, bytes.clone());
            cache.order.push_back(sequence);
        }
        Ok(bytes)
    }
    async fn burn(&self, query: Option<&str>) -> Result<(), HlsError> {
        let values = form_urlencoded::parse(query.unwrap_or_default().as_bytes())
            .into_owned()
            .collect::<Vec<_>>();
        let parameter = |name: &str| -> Result<Option<&str>, HlsError> {
            let found = values
                .iter()
                .filter(|(key, _)| key == name)
                .collect::<Vec<_>>();
            match found.as_slice() {
                [] => Ok(None),
                [(_, value)] => Ok(Some(value.as_str())),
                _ => Err(HlsError::Invalid("Duplicate subtitle parameter")),
            }
        };
        let embedded = parameter("id")?.filter(|value| !value.is_empty());
        let external = parameter("url")?.filter(|value| !value.is_empty());
        if embedded.is_some() && external.is_some() {
            return Err(HlsError::Invalid("Choose one subtitle source"));
        }
        let track = self.track("video0").await?;
        if track.copy {
            return Err(HlsError::Invalid(
                "Burning subtitles requires a transcoded video track",
            ));
        }
        // Prepare the new source before invalidating the previous playable track.
        let burn = if let Some(url) = external {
            let url = resolve_media_url(url, &self.base)
                .map_err(|_| HlsError::Invalid("Invalid HTTP(S) subtitle URL"))?;
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|_| HlsError::Failed("Could not load subtitle source"))?;
            let mut cancelled = self.cancelled.subscribe();
            let bytes = tokio::select! {
                result = download_subtitle(&client, url) => result?,
                _ = cancelled.changed() => return Err(HlsError::Cancelled),
            };
            let extension = if bytes.starts_with(b"WEBVTT") {
                "vtt"
            } else if bytes.windows(13).any(|window| window == b"[Script Info]") {
                "ass"
            } else {
                "srt"
            };
            Some(Arc::new(SubtitleFiles::create(&bytes, extension).await?))
        } else if let Some(id) = embedded {
            let id = id
                .parse::<usize>()
                .map_err(|_| HlsError::Invalid("Invalid subtitle id"))?;
            let metadata = self.metadata().await?;
            let stream = metadata
                .streams
                .iter()
                .find(|stream| stream.track == "subtitle" && stream.id == id)
                .ok_or(HlsError::Missing)?;
            if matches!(
                stream.codec.as_str(),
                "dvb_subtitle" | "dvd_subtitle" | "hdmv_pgs_subtitle" | "xsub"
            ) {
                return Err(HlsError::Invalid(
                    "Bitmap subtitle burning is not supported",
                ));
            }
            let ass = matches!(stream.codec.as_str(), "ass" | "ssa");
            let mut arguments = input_arguments(&self.options.media_url, None);
            arguments.extend(strings(&[
                "-map",
                &format!("0:{}", stream.index),
                "-c:s",
                if ass { "copy" } else { "webvtt" },
                "-f",
                if ass { "ass" } else { "webvtt" },
                "pipe:1",
            ]));
            let bytes = self.execute(&arguments, 4 * 1024 * 1024).await?;
            let mut files = SubtitleFiles::create(&bytes, if ass { "ass" } else { "vtt" }).await?;
            if ass {
                let fonts = metadata
                    .streams
                    .iter()
                    .filter(|stream| {
                        stream.track == "attachment"
                            && stream
                                .extra
                                .get("filename")
                                .and_then(Value::as_str)
                                .is_some_and(|name| {
                                    name.to_ascii_lowercase().ends_with(".ttf")
                                        || name.to_ascii_lowercase().ends_with(".otf")
                                })
                    })
                    .take(8)
                    .collect::<Vec<_>>();
                let mut total = 0;
                for (ordinal, font) in fonts.iter().enumerate() {
                    let (bytes, _) = self.source("attachment", font.id).await?;
                    total += bytes.len();
                    if total > 32 * 1024 * 1024 {
                        return Err(HlsError::Failed("Subtitle fonts exceeded the size limit"));
                    }
                    files.write(&format!("font{ordinal}.ttf"), &bytes).await?;
                }
            }
            Some(Arc::new(files))
        } else {
            None
        };
        if *self.cancelled.borrow() {
            return Err(HlsError::Cancelled);
        }
        track.cancelled.send_replace(true);
        let mut cache = track.cache.lock().await;
        let removed = cache.media.values().map(Bytes::len).sum::<usize>();
        self.cache_bytes.fetch_sub(removed, Ordering::AcqRel);
        *cache = TrackCache {
            burn,
            ..Default::default()
        };
        track.cancelled.send_replace(false);
        Ok(())
    }
    async fn source(&self, kind: &str, id: usize) -> Result<(Bytes, Option<String>), HlsError> {
        let stream = self
            .metadata()
            .await?
            .streams
            .iter()
            .find(|stream| stream.track == kind && stream.id == id)
            .ok_or(HlsError::Missing)?;
        let mut arguments = input_arguments(&self.options.media_url, None);
        if kind == "subtitle" {
            if !matches!(stream.codec.as_str(), "ass" | "ssa") {
                return Err(HlsError::Missing);
            }
            arguments.extend(strings(&[
                "-map",
                &format!("0:{}", stream.index),
                "-c:s",
                "copy",
                "-f",
                "ass",
                "pipe:1",
            ]));
        } else {
            let input = arguments
                .iter()
                .position(|argument| argument == "-i")
                .unwrap();
            arguments.splice(
                input..input,
                strings(&[&format!("-dump_attachment:{}", stream.index), "pipe:1"]),
            );
            arguments.extend(strings(&["-t", "0", "-f", "null", "pipe:2"]));
        }
        let bytes = self.execute(&arguments, 16 * 1024 * 1024).await?;
        Ok((
            bytes,
            stream
                .extra
                .get("mimeType")
                .and_then(Value::as_str)
                .filter(|mime| {
                    mime.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'+' | b'.')
                    })
                })
                .map(str::to_owned),
        ))
    }
    async fn status(&self) -> Value {
        let tracks = self
            .tracks
            .lock()
            .await
            .iter()
            .map(|(name, track)| (name.clone(), track.clone()))
            .collect::<Vec<_>>();
        let mut converters = serde_json::Map::new();
        for (name, track) in tracks {
            let cached = track
                .cache
                .try_lock()
                .ok()
                .map(|cache| cache.media.keys().copied().collect::<Vec<_>>());
            converters.insert(
                name,
                json!({"copy":track.copy,"segments":track.segments.len(),"cachedSegments":cached }),
            );
        }
        json!({"status":{"query":self.options.query,"probe":{"value":self.probe.get(),"error":null},"converters":converters,"activeProcesses":self.active.load(Ordering::Acquire),"cacheBytes":self.cache_bytes.load(Ordering::Acquire)},
            "touched": chrono::DateTime::<chrono::Utc>::from_timestamp_millis(self.touched_epoch.load(Ordering::Acquire) as i64).map(|time|time.to_rfc3339())})
    }
}

struct Track {
    stream: HlsStream,
    segments: Vec<Segment>,
    copy: bool,
    cache: Mutex<TrackCache>,
    cancelled: watch::Sender<bool>,
}
#[derive(Default)]
struct TrackCache {
    init: Option<Bytes>,
    media: HashMap<usize, Bytes>,
    order: VecDeque<usize>,
    burn: Option<Arc<SubtitleFiles>>,
}
#[derive(Debug, Clone, Copy)]
struct Segment {
    start: f64,
    duration: f64,
    frames: Option<usize>,
}

struct SubtitleFiles {
    directory: PathBuf,
    subtitle: PathBuf,
    files: Vec<PathBuf>,
}
impl SubtitleFiles {
    async fn create(bytes: &[u8], extension: &str) -> Result<Self, HlsError> {
        static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "stremio-hls-{}-{}-{}",
            std::process::id(),
            epoch_ms(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        tokio::fs::create_dir(&directory)
            .await
            .map_err(|_| HlsError::Failed("Could not prepare subtitle source"))?;
        let mut files = Self {
            subtitle: directory.join(format!("subtitle.{extension}")),
            directory,
            files: Vec::new(),
        };
        files.write(&format!("subtitle.{extension}"), bytes).await?;
        if let Some(directory) = std::env::current_exe()
            .ok()
            .and_then(|exe| exe.parent().map(|parent| parent.join("fonts")))
            .filter(|directory| directory.is_dir())
        {
            let mut entries = tokio::fs::read_dir(directory)
                .await
                .map_err(|_| HlsError::Failed("Could not load bundled subtitle fonts"))?;
            let mut count = 0;
            let mut total = 0u64;
            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|_| HlsError::Failed("Could not load bundled subtitle fonts"))?
            {
                let source = entry.path();
                if !source
                    .extension()
                    .and_then(|extension| extension.to_str())
                    .is_some_and(|extension| {
                        matches!(extension.to_ascii_lowercase().as_str(), "ttf" | "otf")
                    })
                {
                    continue;
                }
                let metadata = entry
                    .metadata()
                    .await
                    .map_err(|_| HlsError::Failed("Could not load bundled subtitle fonts"))?;
                if !metadata.is_file() {
                    continue;
                }
                total += metadata.len();
                count += 1;
                if count > 32 || total > 32 * 1024 * 1024 {
                    return Err(HlsError::Failed(
                        "Bundled subtitle fonts exceeded the size limit",
                    ));
                }
                let destination = files.directory.join(format!("bundled-font{count}.ttf"));
                files.files.push(destination.clone());
                if tokio::fs::hard_link(&source, &destination).await.is_err() {
                    tokio::fs::copy(&source, destination)
                        .await
                        .map_err(|_| HlsError::Failed("Could not load bundled subtitle fonts"))?;
                }
            }
        }
        Ok(files)
    }
    async fn write(&mut self, name: &str, bytes: &[u8]) -> Result<(), HlsError> {
        let path = self.directory.join(name);
        self.files.push(path.clone());
        tokio::fs::write(path, bytes)
            .await
            .map_err(|_| HlsError::Failed("Could not prepare subtitle source"))
    }
}
impl Drop for SubtitleFiles {
    fn drop(&mut self) {
        // Only files created by this converter are removed; never recurse through
        // a configurable or remotely supplied directory.
        for file in &self.files {
            let _ = std::fs::remove_file(file);
        }
        let _ = std::fs::remove_dir(&self.directory);
    }
}
async fn download_subtitle(client: &reqwest::Client, url: Url) -> Result<Vec<u8>, HlsError> {
    let response = client
        .get(url)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|_| HlsError::Failed("Could not load subtitle source"))?;
    if response
        .content_length()
        .is_some_and(|length| length > 4 * 1024 * 1024)
    {
        return Err(HlsError::Failed("Subtitle source exceeded the size limit"));
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| HlsError::Failed("Could not load subtitle source"))?;
        if bytes.len().saturating_add(chunk.len()) > 4 * 1024 * 1024 {
            return Err(HlsError::Failed("Subtitle source exceeded the size limit"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
fn reserve_cache(counter: &AtomicUsize, length: usize) -> bool {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        let Some(next) = current
            .checked_add(length)
            .filter(|next| *next <= CACHE_LIMIT)
        else {
            return false;
        };
        match counter.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(updated) => current = updated,
        }
    }
}
fn filter_path(path: &Path) -> String {
    // The directory and filenames are generated locally. Escape both libavfilter
    // parsing layers so a Windows drive colon or apostrophe remains a filename.
    path.to_string_lossy()
        .replace('\\', "/")
        .replace(':', "\\:")
        .replace('\'', "'\\''")
}
fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}
fn uniform_segments(duration: f64) -> Vec<Segment> {
    (0..(duration / SEGMENT_SECONDS).ceil() as usize)
        .map(|index| {
            let start = index as f64 * SEGMENT_SECONDS;
            Segment {
                start,
                duration: (duration - start).min(SEGMENT_SECONDS),
                frames: None,
            }
        })
        .collect()
}

fn copy_segments(samples: &HlsSamples, duration: f64) -> Option<Vec<Segment>> {
    if !samples.timescale.is_finite() || samples.timescale <= 0.0 {
        return None;
    }
    let mut keys = samples
        .samples
        .iter()
        .enumerate()
        .filter(|(_, sample)| sample.key)
        .map(|(index, sample)| (sample.pts as f64 / samples.timescale, index))
        .filter(|(time, _)| time.is_finite() && *time >= 0.0 && *time < duration)
        .collect::<Vec<_>>();
    keys.sort_by(|a, b| f64::total_cmp(&a.0, &b.0));
    keys.dedup_by(|a, b| (a.0 - b.0).abs() < 0.000_001);
    if keys.first().is_none_or(|(time, _)| *time > 0.25) {
        return None;
    }
    let mut segments = Vec::new();
    let mut start = 0.0;
    let mut first_sample = keys.first()?.1;
    let full_samples = samples
        .samples
        .iter()
        .all(|sample| sample.duration.is_some() && sample.dts.is_some());
    loop {
        let next = keys
            .iter()
            .copied()
            .find(|(time, _)| *time >= start + SEGMENT_SECONDS - 0.001);
        let end = next.map(|(time, _)| time).unwrap_or(duration);
        let last_sample = next
            .map(|(_, index)| index)
            .unwrap_or(samples.samples.len());
        if end <= start || end - start > 12.0 {
            return None;
        }
        let frames = if full_samples {
            let slice = samples.samples.get(first_sample..last_sample)?;
            // Closed GOPs must not contain leading pictures from a prior segment.
            let key_pts = samples.samples.get(first_sample)?.pts as f64 / samples.timescale;
            if slice.iter().any(|sample| {
                sample.pts as f64 / samples.timescale < key_pts - 0.001
                    || sample.pts as f64 / samples.timescale >= end + 0.001
            }) {
                return None;
            }
            Some(slice.len())
        } else {
            None
        };
        segments.push(Segment {
            start,
            duration: end - start,
            frames,
        });
        start = end;
        first_sample = last_sample;
        if start >= duration - 0.000_001 {
            return Some(segments);
        }
    }
}
fn quoted(value: &str) -> String {
    value
        .chars()
        .filter(|ch| !ch.is_control())
        .map(|ch| if ch == '"' { '\'' } else { ch })
        .collect()
}
fn master_playlist(probe: &HlsProbe, query: &str) -> Result<String, HlsError> {
    let video = probe.streams.iter().any(|stream| stream.track == "video");
    let audio = probe
        .streams
        .iter()
        .filter(|stream| stream.track == "audio")
        .collect::<Vec<_>>();
    let subtitles = probe
        .streams
        .iter()
        .filter(|stream| {
            stream.track == "subtitle"
                && !matches!(
                    stream.codec.as_str(),
                    "dvb_subtitle" | "dvd_subtitle" | "hdmv_pgs_subtitle" | "xsub"
                )
        })
        .collect::<Vec<_>>();
    if !video && audio.is_empty() {
        return Err(HlsError::Invalid("No video or audio streams found"));
    }
    let mut lines = vec!["#EXTM3U".to_string(), "#EXT-X-VERSION:7".to_string()];
    for (kind, streams) in [("AUDIO", &audio), ("SUBTITLES", &subtitles)] {
        for stream in streams {
            let name = stream
                .extra
                .get("title")
                .or_else(|| stream.extra.get("language"))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{} {}", stream.track, stream.id + 1));
            let lang = stream
                .extra
                .get("language")
                .and_then(Value::as_str)
                .map(|lang| format!(",LANGUAGE=\"{}\"", quoted(lang)))
                .unwrap_or_default();
            let default = if kind == "AUDIO" && stream.id == 0 {
                "YES"
            } else {
                "NO"
            };
            let forced = if kind == "SUBTITLES" {
                ",FORCED=NO"
            } else {
                ""
            };
            let uri = if kind == "AUDIO" && !video {
                String::new()
            } else {
                format!(",URI=\"{}{}.m3u8?{query}\"", stream.track, stream.id)
            };
            lines.push(format!("#EXT-X-MEDIA:TYPE={kind},GROUP-ID=\"{}\",NAME=\"{}\"{lang},AUTOSELECT={},DEFAULT={default}{forced}{uri}",if kind=="AUDIO"{"audio"}else{"subtitles"},quoted(&name),if kind=="AUDIO"{"YES"}else{"NO"}));
        }
    }
    lines.push(format!(
        "#EXT-X-STREAM-INF:BANDWIDTH={}{}{}",
        if video { 4_000_000 } else { 192_000 },
        if !audio.is_empty() {
            ",AUDIO=\"audio\""
        } else {
            ""
        },
        if !subtitles.is_empty() {
            ",SUBTITLES=\"subtitles\""
        } else {
            ""
        }
    ));
    lines.push(format!(
        "{}0.m3u8?{query}",
        if video { "video" } else { "audio" }
    ));
    Ok(lines.join("\n") + "\n")
}
fn track_playlist(name: &str, segments: &[Segment], query: &str) -> String {
    let target = segments
        .iter()
        .map(|segment| segment.duration.ceil() as u64)
        .max()
        .unwrap_or(1);
    let mut lines = vec![
        "#EXTM3U".to_string(),
        "#EXT-X-VERSION:7".to_string(),
        format!("#EXT-X-TARGETDURATION:{target}"),
        "#EXT-X-MEDIA-SEQUENCE:1".to_string(),
        "#EXT-X-PLAYLIST-TYPE:VOD".to_string(),
    ];
    let subtitles = name.starts_with("subtitle");
    if !subtitles {
        lines.push(format!("#EXT-X-MAP:URI=\"{name}/init.mp4?{query}\""));
    }
    for (index, segment) in segments.iter().enumerate() {
        lines.push(format!("#EXTINF:{:.6},", segment.duration));
        lines.push(format!(
            "{name}/segment{}.{}?{query}",
            index + 1,
            if subtitles { "vtt" } else { "m4s" }
        ));
    }
    lines.push("#EXT-X-ENDLIST".to_string());
    lines.join("\n") + "\n"
}
fn input_arguments(url: &str, start: Option<f64>) -> Vec<String> {
    let mut arguments = strings(&[
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-y",
        "-threads",
        "2",
        "-protocol_whitelist",
        "http,https,tcp,tls,crypto",
        "-rw_timeout",
        "30000000",
    ]);
    if let Some(start) = start {
        arguments.extend(strings(&["-ss", &format!("{start:.6}")]));
    }
    arguments.extend(strings(&["-i", url]));
    arguments
}
fn clip_arguments(
    options: &Options,
    stream: &HlsStream,
    segment: &Segment,
    copy_video: bool,
    burn: Option<&SubtitleFiles>,
) -> Vec<String> {
    let lookback = if stream.track == "subtitle" {
        segment.start.min(30.0)
    } else {
        0.0
    };
    let mut arguments = input_arguments(&options.media_url, Some(segment.start - lookback));
    arguments.extend(strings(&[
        "-map",
        &format!("0:{}", stream.index),
        "-t",
        &format!("{:.6}", segment.duration + lookback),
        "-map_metadata",
        "-1",
        "-map_chapters",
        "-1",
    ]));
    match stream.track.as_str() {
        "video" => {
            arguments.push("-an".to_string());
            if copy_video {
                arguments.extend(strings(&["-c:v", "copy"]));
                arguments.extend(strings(&[
                    "-bsf:v",
                    &format!(
                        "noise=amount=0:drop='lt(pts*tb,0)+gte(pts*tb,{:.9})'",
                        segment.duration
                    ),
                ]));
                if let Some(frames) = segment.frames {
                    arguments.extend(strings(&["-frames:v", &frames.to_string()]));
                }
                if stream.codec == "hevc" {
                    arguments.extend(strings(&["-tag:v", "hvc1"]));
                }
            } else {
                let fps = stream
                    .frame_rate()
                    .filter(|fps| fps.is_finite() && *fps > 0.0)
                    .unwrap_or(30.0)
                    .min(60.0)
                    .round()
                    .max(1.0);
                arguments.extend(strings(&[
                    "-c:v",
                    "libx264",
                    "-preset",
                    "veryfast",
                    "-crf",
                    "20",
                    "-profile:v",
                    "baseline",
                    "-level:v",
                    "5.1",
                    "-pix_fmt",
                    "yuv420p",
                    "-bf",
                    "0",
                    "-sc_threshold",
                    "0",
                    "-r",
                    &format!("{fps:.6}"),
                    "-g",
                    &((fps * SEGMENT_SECONDS).round() as u32).max(1).to_string(),
                    "-threads",
                    "2",
                ]));
                let mut filters = Vec::new();
                if let Some(burn) = burn {
                    filters.push(format!("setpts=PTS+{:.6}/TB", segment.start));
                    filters.push(format!(
                        "subtitles=filename='{}':fontsdir='{}'",
                        filter_path(&burn.subtitle),
                        filter_path(&burn.directory)
                    ));
                    filters.push(format!("setpts=PTS-{:.6}/TB", segment.start));
                }
                if let (Some(width), Some(height)) = (stream.width(), stream.height()) {
                    let out_width = options.max_width.unwrap_or(width).min(width) / 2 * 2;
                    let out_height = ((f64::from(height) * f64::from(out_width) / f64::from(width))
                        .round() as u32
                        / 2
                        * 2)
                    .max(2);
                    if out_width != width || out_height != height {
                        filters.push(format!("scale={out_width}:{out_height}"));
                    }
                }
                if !filters.is_empty() {
                    arguments.extend(strings(&["-vf", &filters.join(",")]));
                }
            }
            arguments.extend(strings(&["-video_track_timescale", "90000"]));
        }
        "audio" => {
            let compatible = copy_video;
            if compatible {
                arguments.extend(strings(&["-c:a", "copy"]));
            } else {
                arguments.extend(strings(&[
                    "-af",
                    &format!(
                        "aresample=async=1:first_pts=0,apad=whole_dur={:.9}",
                        segment.duration
                    ),
                    "-c:a",
                    "aac",
                    "-b:a",
                    "192k",
                    "-ar",
                    "48000",
                    "-ac",
                    &stream
                        .channels()
                        .unwrap_or(2)
                        .min(options.max_audio_channels)
                        .max(1)
                        .to_string(),
                ]));
            }
            arguments.push("-vn".to_string());
        }
        "subtitle" => {
            arguments.extend(strings(&["-c:s", "webvtt", "-f", "webvtt", "pipe:1"]));
            return arguments;
        }
        _ => {}
    }
    arguments.extend(strings(&[
        "-movflags",
        if copy_video && stream.track == "video" {
            "delay_moov+frag_keyframe+default_base_moof+skip_sidx"
        } else {
            "empty_moov+frag_keyframe+default_base_moof+skip_sidx"
        },
        "-frag_duration",
        "1000000",
        "-min_frag_duration",
        "1000000",
        "-f",
        "mp4",
        "pipe:1",
    ]));
    arguments
}

fn parse_vtt_time(value: &str) -> Option<f64> {
    let parts = value.split(':').collect::<Vec<_>>();
    match parts.as_slice() {
        [minutes, seconds] => {
            Some(minutes.parse::<f64>().ok()? * 60.0 + seconds.parse::<f64>().ok()?)
        }
        [hours, minutes, seconds] => Some(
            hours.parse::<f64>().ok()? * 3600.0
                + minutes.parse::<f64>().ok()? * 60.0
                + seconds.parse::<f64>().ok()?,
        ),
        _ => None,
    }
}
fn vtt_time(time: f64) -> String {
    let ms = (time.max(0.0) * 1000.0).round() as u64;
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        ms / 3_600_000,
        (ms / 60_000) % 60,
        (ms / 1000) % 60,
        ms % 1000
    )
}
fn normalize_vtt(bytes: &[u8], start: f64, duration: f64) -> Result<Bytes, HlsError> {
    let text =
        std::str::from_utf8(bytes).map_err(|_| HlsError::Failed("Invalid subtitle encoding"))?;
    let offset = start - start.min(30.0);
    let mut result = "WEBVTT\nX-TIMESTAMP-MAP=LOCAL:00:00:00.000,MPEGTS:0\n\n".to_string();
    for block in text.replace("\r\n", "\n").split("\n\n") {
        let lines = block.lines().collect::<Vec<_>>();
        let Some(timing) = lines.iter().position(|line| line.contains(" --> ")) else {
            continue;
        };
        let Some((begin, end)) = lines[timing].split_once(" --> ") else {
            continue;
        };
        let Some(begin) = parse_vtt_time(begin) else {
            continue;
        };
        let Some(end) = end.split_ascii_whitespace().next().and_then(parse_vtt_time) else {
            continue;
        };
        let begin = (begin + offset).max(start);
        let end = (end + offset).min(start + duration);
        if end <= begin {
            continue;
        }
        result.push_str(&format!(
            "{} --> {}\n{}\n\n",
            vtt_time(begin),
            vtt_time(end),
            lines[timing + 1..].join("\n")
        ));
    }
    Ok(Bytes::from(result))
}

#[cfg(test)]
#[path = "hls_tests.rs"]
mod integration_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn vod_playlist_has_seekable_timeline_and_short_final_segment() {
        let segments = uniform_segments(9.25);
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[2].duration, 1.25);
        let playlist = track_playlist(
            "audio1",
            &segments,
            "mediaURL=https%3A%2F%2Fexample.test%2Fmovie",
        );
        assert!(playlist.contains("#EXT-X-MEDIA-SEQUENCE:1"));
        assert!(playlist.contains("#EXTINF:1.250000,"));
        assert!(playlist.contains("audio1/segment3.m4s?"));
        assert!(playlist.ends_with("#EXT-X-ENDLIST\n"));
    }
    #[test]
    fn options_preserve_signed_source_and_repeated_codec_preferences() {
        let source = "https://CDN.test/movie?x=a%2fb+z&x=1&_reload=signed";
        let query = form_urlencoded::Serializer::new(String::new())
            .append_pair("mediaURL", source)
            .append_pair("videoCodecs", "h264")
            .append_pair("videoCodecs", "hevc")
            .finish();
        let options = Options::parse(Some(&query), "http://localhost:11470").unwrap();
        assert_eq!(options.media_url, source);
        assert_eq!(options.video_codecs, vec!["h264", "hevc"]);
        assert!(
            Options::parse(
                Some("mediaURL=file%3A%2F%2FC%3A%2Fmovie.mp4"),
                "http://localhost"
            )
            .is_err()
        );
    }
    #[test]
    fn subtitle_segments_clip_overlapping_cues_on_the_absolute_video_timeline() {
        let result = normalize_vtt(
            b"WEBVTT\n\n00:00:02.000 --> 00:00:06.000\nHello\n\n",
            4.0,
            4.0,
        )
        .unwrap();
        assert!(
            std::str::from_utf8(&result)
                .unwrap()
                .contains("00:00:04.000 --> 00:00:06.000")
        );
        assert!(std::str::from_utf8(&result).unwrap().contains("MPEGTS:0"));
    }
}
