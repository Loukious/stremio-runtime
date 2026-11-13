use std::{collections::BTreeMap, time::Instant};

use axum::{
    Json,
    extract::{RawQuery, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use serde_json::{Map, Value, json};
use tokio::{process::Command, sync::Mutex};

use super::{
    AppState, Limits, PROBE_SLOTS, ProbeError, ffprobe_executable, resolve_media_url, run_command,
};

const HLS_PROBE_LIMITS: Limits = Limits {
    timeout: std::time::Duration::from_secs(60),
    stdout: 4 * 1024 * 1024,
    stderr: 64 * 1024,
};
const CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(300);
static CACHE: Mutex<Vec<(String, Instant, HlsProbe)>> = Mutex::const_new(Vec::new());

#[derive(Clone, Debug, Serialize)]
pub(crate) struct HlsProbe {
    pub format: HlsFormat,
    pub streams: Vec<HlsStream>,
    pub samples: BTreeMap<String, HlsSamples>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct HlsFormat {
    pub name: String,
    pub duration: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HlsStream {
    /// Ordinal among streams of the same type, used in video0/audio0/subtitle0 routes.
    pub id: usize,
    /// FFprobe's global input index, used in FFmpeg's 0:<index> mapping.
    pub index: i64,
    pub track: String,
    pub codec: String,
    pub stream_bit_rate: Option<u64>,
    pub stream_max_bit_rate: Option<u64>,
    pub start_time: Option<f64>,
    pub start_time_ts: Option<i64>,
    pub timescale: u64,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl HlsStream {
    fn unsigned(&self, name: &str) -> Option<u32> {
        self.extra.get(name)?.as_u64()?.try_into().ok()
    }

    pub(crate) fn width(&self) -> Option<u32> {
        self.unsigned("width")
    }
    pub(crate) fn height(&self) -> Option<u32> {
        self.unsigned("height")
    }
    pub(crate) fn channels(&self) -> Option<u32> {
        self.unsigned("channels")
    }
    pub(crate) fn frame_rate(&self) -> Option<f64> {
        self.extra.get("frameRate")?.as_f64()
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HlsSamples {
    #[serde(rename = "type")]
    pub kind: String,
    pub timescale: f64,
    pub sample_duration: Option<f64>,
    pub samples: Vec<HlsSample>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct HlsSample {
    pub key: bool,
    pub pts: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dts: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<u64>,
}

pub(crate) fn error_response(error: ProbeError) -> Response {
    let status = match error {
        ProbeError::InvalidInput => StatusCode::BAD_REQUEST,
        ProbeError::Busy => StatusCode::SERVICE_UNAVAILABLE,
        ProbeError::Timeout => StatusCode::GATEWAY_TIMEOUT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    // Upstream names code 100 PROBE_MEDIA_FAILED, but its handler accidentally
    // references PROBE_FAILED and drops the code. Preserve the intended identifier.
    (
        status,
        Json(json!({"error": {"code": 100, "message": format!("Failed to probe media: {error}")}})),
    )
        .into_response()
}

pub(crate) async fn hls_probe(
    State(state): State<AppState>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let urls = crate::query_values(raw_query.as_deref(), "mediaURL");
    let [input] = urls.as_slice() else {
        return error_response(ProbeError::InvalidInput);
    };
    let base = state.base_url.read().await.clone();
    let mut result = match probe_media(input, &base).await {
        Ok(result) => result,
        Err(error) => return error_response(error),
    };
    if crate::query_values(raw_query.as_deref(), "samples")
        .iter()
        .any(|value| !value.is_empty())
    {
        let url = match resolve_media_url(input, &base) {
            Ok(url) => url,
            Err(error) => return error_response(error),
        };
        result.samples = match super::samples::probe_samples(&url, &result).await {
            Ok(samples) => samples,
            Err(error) => return error_response(error),
        };
    }
    Json(result).into_response()
}

pub(crate) async fn probe_media(input: &str, base: &str) -> Result<HlsProbe, ProbeError> {
    let url = resolve_media_url(input, base)?;
    {
        let mut cache = CACHE.lock().await;
        cache.retain(|(_, inserted, _)| inserted.elapsed() < CACHE_TTL);
        if let Some((_, _, result)) = cache.iter().find(|(key, _, _)| key == url.as_str()) {
            return Ok(result.clone());
        }
    }
    let _permit = PROBE_SLOTS.try_acquire().map_err(|_| ProbeError::Busy)?;
    let mut command = Command::new(ffprobe_executable());
    command.args([
        "-v", "error", "-hide_banner", "-of", "json",
        "-show_entries", "stream=index,bit_rate,max_bit_rate,codec_type,codec_name,start_time,start_pts,r_frame_rate,sample_rate,channels,channel_layout,time_base,has_b_frames,nb_frames,width,height,color_space,color_transfer,color_primaries,codec_tag_string,extradata_size:stream_tags=title,language,duration,bps,number_of_bytes,filename,mimetype:format=format_name,duration,bit_rate,max_bit_rate",
        "-protocol_whitelist", "http,https,tcp,tls,crypto",
        "-rw_timeout", "10000000", "-analyzeduration", "10000000", "-probesize", "10000000",
        "-i", url.as_str(),
    ]);
    #[cfg(windows)]
    command.creation_flags(0x0800_0000);
    let output = run_command(&mut command, HLS_PROBE_LIMITS).await?;
    let result = map_hls_metadata(&output)?;
    let mut cache = CACHE.lock().await;
    cache.retain(|(key, _, _)| key != url.as_str());
    cache.insert(0, (url.to_string(), Instant::now(), result.clone()));
    cache.truncate(3);
    Ok(result)
}

fn number(value: Option<&Value>) -> Option<f64> {
    let value = value?;
    let number = value.as_f64().or_else(|| value.as_str()?.parse().ok())?;
    number.is_finite().then_some(number)
}

fn unsigned(value: Option<&Value>) -> Option<u64> {
    super::integer(value)
}

fn signed(value: Option<&Value>) -> Option<i64> {
    let value = value?;
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn bitrate(value: Option<&Value>) -> Option<u64> {
    if value.is_none() || value == Some(&Value::Null) || value.and_then(Value::as_str) == Some("") {
        Some(0)
    } else {
        unsigned(value)
    }
}

fn hls_frame_rate(value: Option<&Value>) -> Option<f64> {
    let (numerator, denominator) = value?.as_str()?.split_once('/')?;
    let rate = numerator.parse::<f64>().ok()? / denominator.parse::<f64>().ok()?;
    rate.is_finite().then_some(rate)
}

fn text(value: Option<&Value>) -> Option<&str> {
    value.and_then(Value::as_str)
}

fn tag<'a>(stream: &'a Value, name: &str) -> Option<&'a Value> {
    let tags = stream.get("tags")?.as_object()?;
    tags.iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value)
}

fn map_hls_metadata(output: &[u8]) -> Result<HlsProbe, ProbeError> {
    let metadata: Value = serde_json::from_slice(output).map_err(|_| ProbeError::InvalidOutput)?;
    let object = metadata.as_object().ok_or(ProbeError::InvalidOutput)?;
    if object.get("format").is_some_and(|value| !value.is_object())
        || object.get("streams").is_some_and(|value| !value.is_array())
        || (!object.contains_key("format") && !object.contains_key("streams"))
    {
        return Err(ProbeError::InvalidOutput);
    }
    let format = object.get("format");
    let duration = number(format.and_then(|value| value.get("duration")));
    let mut counts = BTreeMap::<String, usize>::new();
    let mut streams = Vec::new();
    for stream in object
        .get("streams")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if !stream.is_object() {
            return Err(ProbeError::InvalidOutput);
        }
        let track = text(stream.get("codec_type"))
            .unwrap_or("unknown")
            .to_string();
        let id = counts.entry(track.clone()).or_default();
        let mut extra = Map::new();
        match track.as_str() {
            "video" => {
                extra.extend([
                    ("width".into(), json!(unsigned(stream.get("width")))),
                    ("height".into(), json!(unsigned(stream.get("height")))),
                    (
                        "frameRate".into(),
                        json!(hls_frame_rate(stream.get("r_frame_rate"))),
                    ),
                    (
                        "numberOfFrames".into(),
                        json!(unsigned(stream.get("nb_frames"))),
                    ),
                    (
                        "isHdr".into(),
                        json!(
                            text(stream.get("color_space")) == Some("bt2020nc")
                                && text(stream.get("color_transfer")) == Some("smpte2084")
                                && text(stream.get("color_primaries")) == Some("bt2020")
                        ),
                    ),
                    (
                        "isDoVi".into(),
                        json!(text(stream.get("codec_tag_string")) == Some("dvhe")),
                    ),
                    (
                        "hasBFrames".into(),
                        json!(unsigned(stream.get("has_b_frames")).is_some_and(|value| value > 0)),
                    ),
                    (
                        "formatBitRate".into(),
                        json!(bitrate(format.and_then(|value| value.get("bit_rate")))),
                    ),
                    (
                        "formatMaxBitRate".into(),
                        json!(bitrate(format.and_then(|value| value.get("max_bit_rate")))),
                    ),
                    ("bps".into(), json!(bitrate(tag(stream, "BPS")))),
                    (
                        "numberOfBytes".into(),
                        json!(bitrate(tag(stream, "NUMBER_OF_BYTES"))),
                    ),
                    ("formatDuration".into(), json!(duration)),
                ]);
            }
            "audio" => {
                extra.extend([
                    (
                        "sampleRate".into(),
                        json!(unsigned(stream.get("sample_rate"))),
                    ),
                    ("channels".into(), json!(unsigned(stream.get("channels")))),
                    (
                        "channelLayout".into(),
                        json!(text(stream.get("channel_layout")).unwrap_or("unknown")),
                    ),
                    ("title".into(), json!(text(tag(stream, "title")))),
                    ("language".into(), json!(text(tag(stream, "language")))),
                ]);
            }
            "subtitle" => {
                extra.extend([
                    ("title".into(), json!(text(tag(stream, "title")))),
                    ("language".into(), json!(text(tag(stream, "language")))),
                ]);
            }
            "attachment" => {
                extra.extend([
                    ("filename".into(), json!(text(tag(stream, "filename")))),
                    ("mimeType".into(), json!(text(tag(stream, "mimetype")))),
                    ("size".into(), json!(unsigned(stream.get("extradata_size")))),
                ]);
            }
            _ => {}
        }
        let timescale = text(stream.get("time_base"))
            .and_then(|value| value.split_once('/'))
            .and_then(|(_, denominator)| denominator.parse::<u64>().ok())
            .unwrap_or(1);
        streams.push(HlsStream {
            id: *id,
            index: signed(stream.get("index")).unwrap_or(-1),
            track: track.clone(),
            codec: text(stream.get("codec_name"))
                .unwrap_or("unknown")
                .to_string(),
            stream_bit_rate: bitrate(stream.get("bit_rate")),
            stream_max_bit_rate: bitrate(stream.get("max_bit_rate")),
            start_time: number(stream.get("start_time")),
            start_time_ts: signed(stream.get("start_pts")),
            timescale,
            extra,
        });
        *id += 1;
    }
    Ok(HlsProbe {
        format: HlsFormat {
            name: text(format.and_then(|value| value.get("format_name")))
                .unwrap_or("unknown")
                .to_string(),
            duration,
        },
        streams,
        samples: BTreeMap::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hls_metadata_uses_upstream_names_type_ordinals_and_rich_track_fields() {
        let metadata = json!({
            "format": {"format_name":"matroska,webm","duration":"12.345","bit_rate":"8000000"},
            "streams": [
                {"index":0,"codec_type":"video","codec_name":"hevc","width":3840,"height":2160,"r_frame_rate":"24000/1001","time_base":"1/1000","has_b_frames":2,"color_space":"bt2020nc","color_transfer":"smpte2084","color_primaries":"bt2020","codec_tag_string":"dvhe","tags":{"BPS":"7000000","NUMBER_OF_BYTES":"9000000000"}},
                {"index":3,"codec_type":"audio","codec_name":"aac","sample_rate":"48000","channels":6,"channel_layout":"5.1","start_time":"-0.021","start_pts":-1024,"time_base":"1/48000","tags":{"language":"eng","title":"Surround"}},
                {"index":5,"codec_type":"subtitle","codec_name":"ass","tags":{"language":"spa","title":"Spanish"}},
                {"index":8,"codec_type":"audio","codec_name":"ac3","channels":2},
                {"index":9,"codec_type":"attachment","codec_name":"ttf","extradata_size":1234,"tags":{"filename":"font.ttf","mimetype":"font/ttf"}}
            ]
        });
        let result = map_hls_metadata(&serde_json::to_vec(&metadata).unwrap()).unwrap();
        assert_eq!(result.format.duration, Some(12.345));
        assert_eq!(
            result
                .streams
                .iter()
                .map(|stream| (stream.id, stream.index))
                .collect::<Vec<_>>(),
            [(0, 0), (0, 3), (0, 5), (1, 8), (0, 9)]
        );
        assert_eq!(result.streams[0].width(), Some(3840));
        assert_eq!(result.streams[0].height(), Some(2160));
        assert!((result.streams[0].frame_rate().unwrap() - 24000.0 / 1001.0).abs() < 1e-8);
        assert_eq!(result.streams[1].channels(), Some(6));
        assert_eq!(result.streams[1].extra["sampleRate"], 48000);
        let body = serde_json::to_value(result).unwrap();
        assert_eq!(body["streams"][0]["numberOfBytes"], 9_000_000_000u64);
        assert_eq!(body["streams"][0]["isHdr"], true);
        assert_eq!(body["streams"][0]["isDoVi"], true);
        assert_eq!(body["streams"][1]["startTimeTs"], -1024);
        assert_eq!(body["streams"][1]["startTime"], -0.021);
        assert_eq!(body["streams"][1]["language"], "eng");
        assert_eq!(body["streams"][2]["title"], "Spanish");
        assert_eq!(body["streams"][4]["mimeType"], "font/ttf");
        assert!(body["streams"][0].get("channels").is_none());
        assert!(body["streams"][1].get("width").is_none());
        assert_eq!(body["samples"], json!({}));
    }

    #[test]
    fn sparse_hls_metadata_has_explicit_nulls_and_preserves_unknown_streams() {
        let result = map_hls_metadata(br#"{"streams":[{"index":7,"codec_type":"video","r_frame_rate":"0/0","bit_rate":"N/A"},{"index":9,"codec_type":"data"}]}"#).unwrap();
        let body = serde_json::to_value(result).unwrap();
        assert_eq!(body["format"], json!({"name":"unknown","duration":null}));
        assert_eq!(body["streams"][0]["frameRate"], Value::Null);
        assert_eq!(body["streams"][0]["streamBitRate"], Value::Null);
        assert_eq!(body["streams"][0]["timescale"], 1);
        assert_eq!(body["streams"][1]["track"], "data");
        assert_eq!(body["streams"][1]["codec"], "unknown");
        for invalid in [
            b"null".as_slice(),
            b"{}",
            br#"{"streams":{}}"#,
            br#"{"streams":[null]}"#,
        ] {
            assert_eq!(
                map_hls_metadata(invalid).unwrap_err(),
                ProbeError::InvalidOutput
            );
        }
    }

    #[tokio::test]
    async fn hls_probe_errors_use_the_browser_error_shape() {
        let response = error_response(ProbeError::ProcessFailed);
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let bytes = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap(),
            json!({"error":{"code":100,"message":"Failed to probe media: ffprobe could not probe the media"}})
        );
        assert_eq!(
            error_response(ProbeError::InvalidInput).status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            error_response(ProbeError::Timeout).status(),
            StatusCode::GATEWAY_TIMEOUT
        );
    }
}
