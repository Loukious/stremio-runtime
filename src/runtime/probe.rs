use std::{ffi::OsString, fmt, process::Stdio, time::Duration};

use axum::{
    Json,
    extract::{RawQuery, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
    sync::Semaphore,
};
use url::Url;

use super::AppState;

#[path = "probe_hls.rs"]
mod hls;
#[path = "probe_samples.rs"]
mod samples;

pub(super) use hls::{HlsProbe, HlsSamples, HlsStream, hls_probe, probe_media};
pub(super) use samples::probe_samples;

static PROBE_SLOTS: Semaphore = Semaphore::const_new(2);
const PROBE_LIMITS: Limits = Limits {
    timeout: Duration::from_secs(15),
    stdout: 1024 * 1024,
    stderr: 64 * 1024,
};

#[derive(Clone, Copy)]
struct Limits {
    timeout: Duration,
    stdout: usize,
    stderr: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum ProbeError {
    InvalidInput,
    Busy,
    Unavailable,
    ProcessFailed,
    OutputTooLarge,
    InvalidOutput,
    Timeout,
    SamplesUnavailable,
}

impl fmt::Display for ProbeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidInput => "invalid HTTP(S) media URL or relative media path",
            Self::Busy => "media probing is busy; try again later",
            Self::Unavailable => "ffprobe is unavailable",
            Self::ProcessFailed => "ffprobe could not probe the media",
            Self::OutputTooLarge => "media metadata exceeded the limit",
            Self::InvalidOutput => "ffprobe returned invalid metadata",
            Self::Timeout => "media probing timed out",
            Self::SamplesUnavailable => "media sample timing metadata is unavailable",
        })
    }
}

impl std::error::Error for ProbeError {}

impl IntoResponse for ProbeError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::InvalidInput => (
                StatusCode::BAD_REQUEST,
                "url must be an HTTP(S) media URL or a relative media path without credentials",
            ),
            Self::Busy => (
                StatusCode::SERVICE_UNAVAILABLE,
                "media probing is busy; try again later",
            ),
            Self::Timeout => (StatusCode::GATEWAY_TIMEOUT, "media probing timed out"),
            Self::Unavailable => (StatusCode::INTERNAL_SERVER_ERROR, "ffprobe is unavailable"),
            Self::ProcessFailed => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "ffprobe could not probe the media",
            ),
            Self::OutputTooLarge => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "ffprobe output exceeded the limit",
            ),
            Self::InvalidOutput => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "ffprobe returned invalid metadata",
            ),
            Self::SamplesUnavailable => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "media sample timing metadata is unavailable",
            ),
        };
        (status, Json(json!({"error": message, "result": null}))).into_response()
    }
}

pub(super) async fn probe(
    State(state): State<AppState>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let urls = super::query_values(raw_query.as_deref(), "url");
    let [input] = urls.as_slice() else {
        return ProbeError::InvalidInput.into_response();
    };
    let base = state.base_url.read().await.clone();
    let url = match resolve_media_url(input, &base) {
        Ok(url) => url,
        Err(error) => return error.into_response(),
    };
    let Ok(_permit) = PROBE_SLOTS.try_acquire() else {
        return ProbeError::Busy.into_response();
    };
    let mut command = probe_command(ffprobe_executable(), &url);
    match run_command(&mut command, PROBE_LIMITS)
        .await
        .and_then(|output| map_metadata(&output))
    {
        Ok(result) => Json(result).into_response(),
        Err(error) => error.into_response(),
    }
}

pub(super) fn resolve_media_url(input: &str, base: &str) -> Result<Url, ProbeError> {
    if input.is_empty()
        || input.len() > 8192
        || input
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte == 0 || byte == b'\\')
        || input.starts_with("//")
    {
        return Err(ProbeError::InvalidInput);
    }
    let url = match Url::parse(input) {
        Ok(url) => {
            // Require a complete absolute HTTP URL; the URL parser also accepts http:/host.
            if !(input.to_ascii_lowercase().starts_with("http://")
                || input.to_ascii_lowercase().starts_with("https://"))
            {
                return Err(ProbeError::InvalidInput);
            }
            url
        }
        Err(url::ParseError::RelativeUrlWithoutBase) => Url::parse(base)
            .and_then(|base| base.join(input))
            .map_err(|_| ProbeError::InvalidInput)?,
        Err(_) => return Err(ProbeError::InvalidInput),
    };
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(ProbeError::InvalidInput);
    }
    Ok(url)
}

pub(super) fn ffprobe_executable() -> OsString {
    if let Some(path) = std::env::var_os("STREMIO_FFPROBE_PATH").filter(|path| !path.is_empty()) {
        return path;
    }
    let filename = if cfg!(windows) {
        "ffprobe.exe"
    } else {
        "ffprobe"
    };
    if let Some(adjacent) = std::env::current_exe()
        .ok()
        .and_then(|executable| executable.parent().map(|parent| parent.join(filename)))
        .filter(|path| path.is_file())
    {
        return adjacent.into_os_string();
    }
    OsString::from("ffprobe")
}

fn probe_command(executable: OsString, url: &Url) -> Command {
    let mut command = Command::new(executable);
    command.args([
        "-v", "error",
        "-hide_banner",
        "-of", "json",
        "-show_entries", "format=format_name,duration,bit_rate:stream=index,codec_type,codec_name,width,height,bit_rate,avg_frame_rate,r_frame_rate:stream_tags=language:stream_disposition=default",
        "-protocol_whitelist", "http,https,tcp,tls,crypto",
        "-rw_timeout", "10000000",
        "-analyzeduration", "5000000",
        "-probesize", "5000000",
        "-i", url.as_str(),
    ]);
    #[cfg(windows)]
    command.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    command
}

// Keep ownership while waiting and reading pipes. Cancellation drops this guard,
// requests termination immediately, then reaps the child on the active runtime.
struct ProbeChild(Option<Child>);

impl ProbeChild {
    async fn kill_and_wait(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
        self.0.take();
    }
}

impl Drop for ProbeChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.start_kill();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = child.wait().await;
                });
            }
            // kill_on_drop is also enabled, including during runtime shutdown.
        }
    }
}

async fn read_limited(
    mut pipe: impl AsyncRead + Unpin,
    limit: usize,
) -> Result<Vec<u8>, ProbeError> {
    let mut output = Vec::new();
    let mut buffer = [0; 8192];
    loop {
        let read = pipe
            .read(&mut buffer)
            .await
            .map_err(|_| ProbeError::ProcessFailed)?;
        if read == 0 {
            return Ok(output);
        }
        if read > limit.saturating_sub(output.len()) {
            return Err(ProbeError::OutputTooLarge);
        }
        output.extend_from_slice(&buffer[..read]);
    }
}

async fn run_command(command: &mut Command, limits: Limits) -> Result<Vec<u8>, ProbeError> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let child = command.spawn().map_err(|_| ProbeError::Unavailable)?;
    let mut guard = ProbeChild(Some(child));
    let child = guard.0.as_mut().expect("child is owned");
    let stdout = child.stdout.take().expect("stdout is piped");
    let stderr = child.stderr.take().expect("stderr is piped");
    let outcome = tokio::time::timeout(limits.timeout, async {
        tokio::try_join!(
            read_limited(stdout, limits.stdout),
            read_limited(stderr, limits.stderr),
            async { child.wait().await.map_err(|_| ProbeError::ProcessFailed) },
        )
    })
    .await;
    match outcome {
        Ok(Ok((stdout, _stderr, status))) => {
            guard.0.take(); // wait has already reaped the child.
            if status.success() {
                Ok(stdout)
            } else {
                Err(ProbeError::ProcessFailed)
            }
        }
        failed => {
            guard.kill_and_wait().await;
            match failed {
                Ok(Err(error)) => Err(error),
                Err(_) => Err(ProbeError::Timeout),
                _ => unreachable!(),
            }
        }
    }
}

fn finite_number(value: Option<&Value>) -> Option<f64> {
    let value = value?;
    let number = value.as_f64().or_else(|| value.as_str()?.parse().ok())?;
    (number.is_finite() && number >= 0.0).then_some(number)
}

fn integer(value: Option<&Value>) -> Option<u64> {
    let value = value?;
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

fn frame_rate(value: Option<&Value>) -> Option<f64> {
    let value = value?;
    if let Some((numerator, denominator)) = value.as_str().and_then(|text| text.split_once('/')) {
        let rate = numerator.parse::<f64>().ok()? / denominator.parse::<f64>().ok()?;
        return (rate.is_finite() && rate > 0.0).then_some(rate);
    }
    finite_number(Some(value)).filter(|rate| *rate > 0.0)
}

fn map_metadata(output: &[u8]) -> Result<Value, ProbeError> {
    let metadata: Value = serde_json::from_slice(output).map_err(|_| ProbeError::InvalidOutput)?;
    if !metadata.is_object() {
        return Err(ProbeError::InvalidOutput);
    }
    if (metadata.get("format").is_none() && metadata.get("streams").is_none())
        || metadata
            .get("format")
            .is_some_and(|format| !format.is_object())
        || metadata
            .get("streams")
            .is_some_and(|streams| !streams.is_array())
    {
        return Err(ProbeError::InvalidOutput);
    }
    let format = metadata.get("format");
    let format_name = format
        .and_then(|format| format.get("format_name"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let container = if format_name.split(',').any(|name| name == "matroska") {
        "mkv"
    } else if format_name.split(',').any(|name| name == "mp4") {
        "mp4"
    } else {
        format_name.split(',').next().unwrap_or_default()
    };
    let streams: Vec<Value> = metadata.get("streams").and_then(Value::as_array).into_iter().flatten().enumerate().filter_map(|(index, stream)| {
        let kind = stream.get("codec_type").and_then(Value::as_str)?;
        if !matches!(kind, "video" | "audio" | "subtitle") { return None; }
        let size = if kind == "video" {
            integer(stream.get("width")).zip(integer(stream.get("height"))).map(|(width, height)| [width, height])
        } else { None };
        let fps = if kind == "video" {
            frame_rate(stream.get("avg_frame_rate")).or_else(|| frame_rate(stream.get("r_frame_rate")))
        } else { None };
        let disposition = stream.get("disposition").and_then(|value| value.get("default"));
        let default = disposition.and_then(Value::as_bool).unwrap_or_else(|| integer(disposition).is_some_and(|flag| flag != 0));
        Some(json!({
            "codec_type": kind,
            "codec_name": stream.get("codec_name").and_then(Value::as_str).unwrap_or_default(),
            "stream": integer(stream.get("index")).unwrap_or(index as u64),
            "default": default,
            "bitrate": integer(stream.get("bit_rate")).unwrap_or(0),
            "fps": fps,
            "size": size,
            "lang": stream.get("tags").and_then(|value| value.get("language")).and_then(Value::as_str),
        }))
    }).collect();
    Ok(json!({
        "container": container,
        "duration": finite_number(format.and_then(|format| format.get("duration"))).unwrap_or(0.0),
        "bitrate": integer(format.and_then(|format| format.get("bit_rate"))).unwrap_or(0),
        "streams": streams,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{self, Write},
        path::PathBuf,
    };

    #[test]
    fn metadata_preserves_fractional_values_and_all_media_streams() {
        let metadata = json!({
            "format": {"format_name": "matroska,webm", "duration": "12.345", "bit_rate": "500000"},
            "streams": [
                {"index":0,"codec_type":"video","codec_name":"h264","width":1920,"height":1080,"avg_frame_rate":"24000/1001","disposition":{"default":1},"bit_rate":"400000","tags":{"language":"eng"}},
                {"index":2,"codec_type":"audio","codec_name":"aac","disposition":{"default":0}},
                {"index":3,"codec_type":"subtitle","codec_name":"ass","disposition":{"default":true},"tags":{"language":"spa"}},
                {"index":4,"codec_type":"attachment","codec_name":"ttf"}
            ]
        });
        let result = map_metadata(&serde_json::to_vec(&metadata).unwrap()).unwrap();
        assert_eq!(result["container"], "mkv");
        assert_eq!(result["duration"], 12.345);
        assert_eq!(result["bitrate"], 500000);
        assert!(result.get("result").is_none());
        assert_eq!(result["streams"].as_array().unwrap().len(), 3);
        assert_eq!(result["streams"][0]["size"], json!([1920, 1080]));
        assert_eq!(result["streams"][0]["default"], true);
        assert!((result["streams"][0]["fps"].as_f64().unwrap() - 24000.0 / 1001.0).abs() < 1e-10);
        assert_eq!(result["streams"][1]["default"], false);
        assert_eq!(result["streams"][2]["codec_type"], "subtitle");
        assert_eq!(result["streams"][2]["lang"], "spa");
    }

    #[test]
    fn sparse_metadata_and_na_fields_are_tolerated() {
        let metadata = json!({"format":{"format_name":"mov,mp4,m4a,3gp,3g2,mj2","duration":"N/A","bit_rate":"N/A"},"streams":[{"codec_type":"video","avg_frame_rate":"0/0","r_frame_rate":"30000/1001"},{"codec_type":"audio"}]});
        let result = map_metadata(&serde_json::to_vec(&metadata).unwrap()).unwrap();
        assert_eq!(result["container"], "mp4");
        assert_eq!(result["duration"], 0.0);
        assert_eq!(result["bitrate"], 0);
        assert!(result["streams"][0]["size"].is_null());
        assert_eq!(result["streams"][0]["default"], false);
        assert!((result["streams"][0]["fps"].as_f64().unwrap() - 30000.0 / 1001.0).abs() < 1e-10);
        assert!(result["streams"][1]["fps"].is_null());
        assert!(map_metadata(b"{\"streams\":[]}").is_ok());
        assert_eq!(map_metadata(b"invalid"), Err(ProbeError::InvalidOutput));
        assert_eq!(map_metadata(b"[]"), Err(ProbeError::InvalidOutput));
        for invalid in [b"{}".as_slice(), b"{\"format\":[]}", b"{\"streams\":{}}"] {
            assert_eq!(map_metadata(invalid), Err(ProbeError::InvalidOutput));
        }
    }

    #[test]
    fn urls_resolve_with_the_advertised_random_port_and_reject_unsafe_inputs() {
        let base = "http://127.0.0.1:48321";
        for relative in ["/abcd/0/file.mkv", "abcd/0/file.mkv"] {
            assert_eq!(
                resolve_media_url(relative, base).unwrap().as_str(),
                "http://127.0.0.1:48321/abcd/0/file.mkv"
            );
        }
        assert_eq!(
            resolve_media_url("https://example.com/a%20b?token=value", base)
                .unwrap()
                .as_str(),
            "https://example.com/a%20b?token=value"
        );
        for input in [
            "",
            "file:///C:/movie.mkv",
            "C:\\movie.mkv",
            "C:/movie.mkv",
            "\\\\host\\movie",
            "//host/movie",
            "ftp://host/movie",
            "http:/host/movie",
            "https://user:pass@host/movie",
            "http://",
            "http://host/movie\r\nheader",
            "http://host/movie bad",
            "http://host/movie\0",
            "javascript:alert(1)",
        ] {
            assert_eq!(
                resolve_media_url(input, base),
                Err(ProbeError::InvalidInput),
                "{input:?}"
            );
        }
    }

    #[test]
    fn command_uses_separate_input_and_restricts_nested_protocols() {
        let url = Url::parse("https://example.com/video?arg=-show_entries").unwrap();
        let command = probe_command(OsString::from("configured-ffprobe"), &url);
        let args: Vec<_> = command
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert_eq!(&args[args.len() - 2..], &["-i", url.as_str()]);
        let whitelist = args
            .iter()
            .position(|arg| arg == "-protocol_whitelist")
            .unwrap();
        assert_eq!(args[whitelist + 1], "http,https,tcp,tls,crypto");
        assert!(!args.iter().any(|arg| arg == "-show_packets"));
    }

    #[tokio::test]
    async fn failures_have_json_bodies_and_real_status_codes() {
        for (error, expected) in [
            (ProbeError::InvalidInput, 400),
            (ProbeError::Unavailable, 500),
            (ProbeError::ProcessFailed, 500),
            (ProbeError::InvalidOutput, 500),
            (ProbeError::Busy, 503),
            (ProbeError::Timeout, 504),
        ] {
            let response = error.into_response();
            assert_eq!(response.status().as_u16(), expected);
            assert_eq!(response.headers()["content-type"], "application/json");
            let body = axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap();
            let error: Value = serde_json::from_slice(&body).unwrap();
            assert!(error["error"].is_string());
            assert_eq!(error.get("result"), Some(&Value::Null));
        }
    }

    // Run this test in a subprocess to exercise real pipe/process ownership without ffprobe or media.
    #[test]
    fn subprocess_fixture() {
        let Ok(mode) = std::env::var("STREMIO_PROBE_FIXTURE") else {
            return;
        };
        if let Ok(pid_file) = std::env::var("STREMIO_PROBE_FIXTURE_PID") {
            std::fs::write(pid_file, std::process::id().to_string()).unwrap();
        }
        match mode.as_str() {
            "success" => {
                print!("fixture-success");
                io::stdout().flush().unwrap();
            }
            "failure" => {
                eprint!("fixture-failure");
                std::process::exit(42);
            }
            "stdout-limit" => {
                io::stdout().write_all(&[b'x'; 4096]).unwrap();
                io::stdout().flush().unwrap();
                std::thread::sleep(Duration::from_secs(30));
            }
            "stderr-limit" => {
                io::stderr().write_all(&[b'x'; 4096]).unwrap();
                io::stderr().flush().unwrap();
                std::thread::sleep(Duration::from_secs(30));
            }
            "wait" => std::thread::sleep(Duration::from_secs(30)),
            _ => panic!("unknown fixture"),
        }
    }

    fn fixture_command(mode: &str, pid_file: &std::path::Path) -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "media_probe::tests::subprocess_fixture",
                "--nocapture",
            ])
            .env("STREMIO_PROBE_FIXTURE", mode)
            .env("STREMIO_PROBE_FIXTURE_PID", pid_file);
        command
    }

    fn pid_file() -> PathBuf {
        std::env::temp_dir().join(format!(
            "stremio-probe-{}-{}.pid",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[cfg(windows)]
    fn process_alive(pid: u32) -> bool {
        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn OpenProcess(access: u32, inherit: i32, pid: u32) -> *mut std::ffi::c_void;
            fn GetExitCodeProcess(handle: *mut std::ffi::c_void, code: *mut u32) -> i32;
            fn CloseHandle(handle: *mut std::ffi::c_void) -> i32;
        }
        unsafe {
            let handle = OpenProcess(0x1000, 0, pid);
            if handle.is_null() {
                return false;
            }
            let mut code = 0;
            let alive = GetExitCodeProcess(handle, &mut code) != 0 && code == 259;
            CloseHandle(handle);
            alive
        }
    }

    #[cfg(unix)]
    fn process_alive(pid: u32) -> bool {
        unsafe extern "C" {
            fn kill(pid: i32, signal: i32) -> i32;
        }
        unsafe { kill(pid as i32, 0) == 0 }
    }

    async fn assert_child_stopped(path: &std::path::Path) {
        if let Ok(pid) = std::fs::read_to_string(path) {
            let pid: u32 = pid.parse().unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                while process_alive(pid) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("probe child must be terminated/reaped");
        } else {
            panic!("fixture child did not start");
        }
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn runner_handles_success_failure_limits_and_timeout_and_reaps_children() {
        let limits = Limits {
            timeout: Duration::from_secs(3),
            stdout: 1024,
            stderr: 1024,
        };
        for (mode, expected) in [
            ("success", None),
            ("failure", Some(ProbeError::ProcessFailed)),
            ("stdout-limit", Some(ProbeError::OutputTooLarge)),
            ("stderr-limit", Some(ProbeError::OutputTooLarge)),
            ("wait", Some(ProbeError::Timeout)),
        ] {
            let path = pid_file();
            let mut command = fixture_command(mode, &path);
            let limits = if mode == "wait" {
                Limits {
                    timeout: Duration::from_millis(500),
                    ..limits
                }
            } else {
                limits
            };
            let result = run_command(&mut command, limits).await;
            if let Some(error) = expected {
                assert_eq!(result, Err(error), "{mode}");
            } else {
                assert!(
                    String::from_utf8(result.unwrap())
                        .unwrap()
                        .contains("fixture-success")
                );
            }
            assert_child_stopped(&path).await;
        }
        let mut unavailable = Command::new("stremio-ffprobe-nonexistent-123456789");
        assert_eq!(
            run_command(&mut unavailable, limits).await,
            Err(ProbeError::Unavailable)
        );
    }

    #[tokio::test]
    async fn cancelling_runner_terminates_and_reaps_the_child() {
        let path = pid_file();
        let mut command = fixture_command("wait", &path);
        let task = tokio::spawn(async move { run_command(&mut command, PROBE_LIMITS).await });
        tokio::time::timeout(Duration::from_secs(3), async {
            while !path.is_file() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("fixture must start");
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_child_stopped(&path).await;
    }
}
