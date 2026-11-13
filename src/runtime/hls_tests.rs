use super::*;
use axum::{Router, body::to_bytes, extract::State, http::HeaderMap, routing::get};
use std::{process::Command, sync::atomic::AtomicUsize};

struct Fixture {
    directory: PathBuf,
    files: Vec<PathBuf>,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let directory = std::env::temp_dir().join(format!(
            "stremio-hls-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        Self {
            directory,
            files: Vec::new(),
        }
    }
    fn path(&mut self, name: &str) -> PathBuf {
        let path = self.directory.join(name);
        self.files.push(path.clone());
        path
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for path in &self.files {
            let _ = std::fs::remove_file(path);
        }
        let _ = std::fs::remove_dir(&self.directory);
    }
}
fn ffmpeg(arguments: &[&str]) {
    let output = Command::new(process::executable())
        .args(arguments)
        .output()
        .expect("FFmpeg is needed for the HLS integration tests");
    assert!(
        output.status.success(),
        "FFmpeg fixture failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
fn fixture_mp4(fixture: &mut Fixture) -> PathBuf {
    fixture_video(fixture, "baseline", "0")
}
fn fixture_video(fixture: &mut Fixture, profile: &str, b_frames: &str) -> PathBuf {
    let path = fixture.path("fixture.mp4");
    ffmpeg(&[
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-y",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=size=160x90:rate=24",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=440:sample_rate=48000",
        "-t",
        "9.25",
        "-c:v",
        "libx264",
        "-profile:v",
        profile,
        "-bf",
        b_frames,
        "-g",
        "96",
        "-sc_threshold",
        "0",
        "-c:a",
        "aac",
        "-ac",
        "2",
        "-movflags",
        "+faststart",
        path.to_str().unwrap(),
    ]);
    path
}
#[derive(Clone)]
struct Media {
    bytes: Bytes,
    subtitle: Bytes,
}
async fn range_media(State(media): State<Media>, headers: HeaderMap) -> Response {
    let length = media.bytes.len();
    let range = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("bytes="))
        .and_then(|value| value.split_once('-'));
    if let Some((start, end)) = range {
        let start = start.parse::<usize>().unwrap_or(0);
        let end = end.parse::<usize>().unwrap_or(length - 1).min(length - 1);
        if start > end {
            return StatusCode::RANGE_NOT_SATISFIABLE.into_response();
        }
        return Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header(header::ACCEPT_RANGES, "bytes")
            .header(
                header::CONTENT_RANGE,
                format!("bytes {start}-{end}/{length}"),
            )
            .header(header::CONTENT_LENGTH, end - start + 1)
            .body(Body::from(media.bytes.slice(start..=end)))
            .unwrap();
    }
    Response::builder()
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, length)
        .body(Body::from(media.bytes))
        .unwrap()
}
async fn subtitle(State(media): State<Media>) -> Response {
    data_response("text/plain", media.subtitle)
}
async fn serve(bytes: Bytes, subtitle_bytes: Bytes) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new()
        .route("/fixture", get(range_media))
        .route("/subtitle", get(subtitle))
        .with_state(Media {
            bytes,
            subtitle: subtitle_bytes,
        });
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (base, server)
}
async fn request(service: &HlsService, id: &str, rest: &str, query: &str, base: &str) -> Bytes {
    let response = service.handle(id, rest, Some(query), base).await;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), SEGMENT_LIMIT).await.unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "{rest}: {}",
        String::from_utf8_lossy(&bytes)
    );
    bytes
}
fn probe_fragment(
    fixture: &mut Fixture,
    name: &str,
    init: &[u8],
    media: &[u8],
    kind: &str,
) -> Value {
    let path = fixture.path(name);
    let mut bytes = init.to_vec();
    bytes.extend_from_slice(media);
    std::fs::write(&path, bytes).unwrap();
    let output = Command::new(crate::media_probe::ffprobe_executable())
        .args([
            "-v",
            "error",
            "-select_streams",
            kind,
            "-show_packets",
            "-show_streams",
            "-of",
            "json",
            path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    let packets = value["packets"].as_array().unwrap();
    assert!(!packets.is_empty(), "No decoded packets: {value}");
    let decode = Command::new(process::executable())
        .args([
            "-v",
            "error",
            "-i",
            path.to_str().unwrap(),
            "-map",
            if kind == "v" { "0:v" } else { "0:a" },
            "-f",
            "null",
            "-",
        ])
        .output()
        .unwrap();
    assert!(
        decode.status.success() && decode.stderr.is_empty(),
        "Fragment did not decode: {}",
        String::from_utf8_lossy(&decode.stderr)
    );
    value
}
fn packet_start(value: &Value) -> f64 {
    value["packets"][0]["pts_time"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}
fn packet_end(value: &Value) -> f64 {
    let packet = value["packets"].as_array().unwrap().last().unwrap();
    packet["pts_time"].as_str().unwrap().parse::<f64>().unwrap()
        + packet["duration_time"]
            .as_str()
            .unwrap()
            .parse::<f64>()
            .unwrap()
}

#[tokio::test]
#[ignore = "requires FFmpeg and FFprobe; run with --include-ignored using packaged media tools"]
async fn real_http_video_audio_segments_decode_after_out_of_order_seek() {
    let mut fixture = Fixture::new();
    let path = fixture_mp4(&mut fixture);
    let (base, server) = serve(Bytes::from(std::fs::read(path).unwrap()), Bytes::new()).await;
    let service = HlsService::new();
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair("mediaURL", &format!("{base}/fixture"))
        .append_pair("forceTranscoding", "1")
        .finish();
    let master = request(&service, "decode", "master.m3u8", &query, &base).await;
    assert!(String::from_utf8_lossy(&master).contains("audio0.m3u8?"));
    for (track, kind) in [("video0", "v"), ("audio0", "a")] {
        let playlist = request(&service, "decode", &format!("{track}.m3u8"), &query, &base).await;
        assert!(String::from_utf8_lossy(&playlist).contains("#EXTINF:1.250000,"));
        let final_segment = request(
            &service,
            "decode",
            &format!("{track}/segment3.m4s"),
            &query,
            &base,
        )
        .await;
        let init = request(
            &service,
            "decode",
            &format!("{track}/init.mp4"),
            &query,
            &base,
        )
        .await;
        let info = probe_fragment(
            &mut fixture,
            &format!("{track}-3.mp4"),
            &init,
            &final_segment,
            kind,
        );
        assert!(
            (packet_start(&info) - 8.0).abs() < 0.002,
            "{track} seek timestamp: {info}"
        );
        assert!(
            (packet_end(&info) - 9.25).abs() < 0.07,
            "{track} final duration: {info}"
        );
        let mut all = Vec::new();
        for sequence in 1..=3 {
            let media = request(
                &service,
                "decode",
                &format!("{track}/segment{sequence}.m4s"),
                &query,
                &base,
            )
            .await;
            let value = probe_fragment(
                &mut fixture,
                &format!("{track}-{sequence}-decode.mp4"),
                &init,
                &media,
                kind,
            );
            assert!((packet_start(&value) - (sequence - 1) as f64 * 4.0).abs() < 0.002);
            all.extend_from_slice(&media);
        }
        assert_eq!(
            init,
            request(
                &service,
                "decode",
                &format!("{track}/init.mp4"),
                &query,
                &base
            )
            .await
        );
        let decoded = probe_fragment(&mut fixture, &format!("{track}-all.mp4"), &init, &all, kind);
        assert!((packet_end(&decoded) - 9.25).abs() < 0.07);
    }
    assert_eq!(service.destroy("decode").await.status(), StatusCode::OK);
    assert!(service.status().await.as_object().unwrap().is_empty());
    service.shutdown().await;
    server.abort();
}

#[tokio::test]
#[ignore = "requires FFmpeg, libass and FFprobe; run with --include-ignored using packaged media tools"]
async fn real_subtitle_burn_uses_absolute_seek_time_and_resets_cache() {
    let mut fixture = Fixture::new();
    let path = fixture_mp4(&mut fixture);
    let (base, server) = serve(
        Bytes::from(std::fs::read(path).unwrap()),
        Bytes::from_static(b"1\n00:00:08,000 --> 00:00:09,250\nSEEK SUBTITLE\n\n"),
    )
    .await;
    let service = HlsService::new();
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair("mediaURL", &format!("{base}/fixture"))
        .append_pair("forceTranscoding", "1")
        .finish();
    let clear = request(&service, "burn", "video0/segment3.m4s", &query, &base).await;
    let burn_query = form_urlencoded::Serializer::new(String::new())
        .append_pair("url", &format!("{base}/subtitle"))
        .finish();
    request(&service, "burn", "burn", &burn_query, &base).await;
    let burned = request(&service, "burn", "video0/segment3.m4s", &query, &base).await;
    assert_ne!(
        clear, burned,
        "Subtitle at 8 seconds was not burned into a seeked segment"
    );
    let init = request(&service, "burn", "video0/init.mp4", &query, &base).await;
    probe_fragment(&mut fixture, "burned.mp4", &init, &burned, "v");
    request(&service, "burn", "burn", "", &base).await;
    assert_eq!(
        clear,
        request(&service, "burn", "video0/segment3.m4s", &query, &base).await
    );
    service.shutdown().await;
    server.abort();
}

#[tokio::test]
#[ignore = "requires FFmpeg and FFprobe; run with --include-ignored using packaged media tools"]
async fn real_keyframe_video_and_aac_copy_segments_decode_and_keep_source_codec() {
    let mut fixture = Fixture::new();
    let path = fixture_mp4(&mut fixture);
    let (base, server) = serve(Bytes::from(std::fs::read(path).unwrap()), Bytes::new()).await;
    let service = HlsService::new();
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair("mediaURL", &format!("{base}/fixture"))
        .append_pair("videoCodecs", "h264")
        .append_pair("audioCodecs", "aac")
        .finish();
    for (track, kind) in [("video0", "v"), ("audio0", "a")] {
        let final_segment = request(
            &service,
            "copy",
            &format!("{track}/segment3.m4s"),
            &query,
            &base,
        )
        .await;
        let init = request(
            &service,
            "copy",
            &format!("{track}/init.mp4"),
            &query,
            &base,
        )
        .await;
        let value = probe_fragment(
            &mut fixture,
            &format!("{track}-copy.mp4"),
            &init,
            &final_segment,
            kind,
        );
        assert!((packet_start(&value) - 8.0).abs() < 0.002);
        assert!((packet_end(&value) - 9.25).abs() < 0.07);
        if kind == "v" {
            assert!(value["packets"][0]["flags"].as_str().unwrap().contains('K'));
        }
    }
    let status = service.status().await;
    for track in ["video0", "audio0"] {
        assert_eq!(
            status["copy"]["status"]["converters"][track]["copy"], true,
            "{status}"
        );
    }
    let response = service.handle("copy", "burn", Some("id=0"), &base).await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let error: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1024).await.unwrap()).unwrap();
    assert_eq!(error["error"]["code"], 40);
    service.shutdown().await;
    server.abort();
}

#[tokio::test]
#[ignore = "requires FFmpeg, libass and FFprobe; run with --include-ignored using packaged media tools"]
async fn real_embedded_ass_vtt_segments_font_sources_and_burn_are_available() {
    let mut fixture = Fixture::new();
    let mp4 = fixture_mp4(&mut fixture);
    let ass = fixture.path("embedded.ass");
    std::fs::write(&ass, "[Script Info]\nScriptType: v4.00+\nPlayResX: 160\nPlayResY: 90\n\n[V4+ Styles]\nFormat: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\nStyle: Default,Arial,12,&H00FFFFFF,&H000000FF,&H00000000,&H00000000,0,0,0,0,100,100,0,0,1,1,0,2,5,5,5,1\n\n[Events]\nFormat: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\nDialogue: 0,0:00:02.00,0:00:06.00,Default,,0,0,0,,Overlap cue\nDialogue: 0,0:00:08.00,0:00:09.25,Default,,0,0,0,,Embedded seek cue\n").unwrap();
    let mkv = fixture.path("fixture.mkv");
    let font = Path::new("C:/Windows/Fonts/arial.ttf");
    let mut arguments = vec![
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-y",
        "-i",
        mp4.to_str().unwrap(),
        "-i",
        ass.to_str().unwrap(),
        "-map",
        "0",
        "-map",
        "1",
        "-c",
        "copy",
    ];
    if font.is_file() {
        arguments.extend([
            "-attach",
            font.to_str().unwrap(),
            "-metadata:s:t:0",
            "mimetype=application/x-truetype-font",
            "-metadata:s:t:0",
            "filename=Arial.ttf",
        ]);
    }
    arguments.push(mkv.to_str().unwrap());
    ffmpeg(&arguments);
    let (base, server) = serve(Bytes::from(std::fs::read(mkv).unwrap()), Bytes::new()).await;
    let service = HlsService::new();
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair("mediaURL", &format!("{base}/fixture"))
        .append_pair("forceTranscoding", "1")
        .finish();
    let master = request(&service, "embedded", "master.m3u8", &query, &base).await;
    assert!(String::from_utf8_lossy(&master).contains("subtitle0.m3u8?"));
    let source = request(&service, "embedded", "source/subtitle/0.ass", &query, &base).await;
    assert!(String::from_utf8_lossy(&source).contains("Embedded seek cue"));
    if font.is_file() {
        let bytes = request(&service, "embedded", "source/attachment/0", &query, &base).await;
        assert_eq!(bytes.as_ref(), std::fs::read(font).unwrap());
    }
    let vtt = request(
        &service,
        "embedded",
        "subtitle0/segment2.vtt",
        &query,
        &base,
    )
    .await;
    let text = String::from_utf8_lossy(&vtt);
    assert!(text.contains("Overlap cue"), "{text}");
    assert!(text.contains("00:00:04.000 --> 00:00:06.000"), "{text}");
    let clear = request(&service, "embedded", "video0/segment3.m4s", &query, &base).await;
    request(&service, "embedded", "burn", "id=0", &base).await;
    let burned = request(&service, "embedded", "video0/segment3.m4s", &query, &base).await;
    assert_ne!(clear, burned);
    let init = request(&service, "embedded", "video0/init.mp4", &query, &base).await;
    probe_fragment(&mut fixture, "embedded-burn.mp4", &init, &burned, "v");
    service.shutdown().await;
    server.abort();
}

#[tokio::test]
#[ignore = "requires FFmpeg and FFprobe; run with --include-ignored using packaged media tools"]
async fn real_b_frame_video_copy_with_audio_conversion_preserves_presentation_timeline() {
    let mut fixture = Fixture::new();
    let path = fixture_video(&mut fixture, "high", "3");
    let (base, server) = serve(Bytes::from(std::fs::read(path).unwrap()), Bytes::new()).await;
    let service = HlsService::new();
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair("mediaURL", &format!("{base}/fixture"))
        .append_pair("videoCodecs", "h264")
        .finish();
    let final_segment = request(&service, "bcopy", "video0/segment3.m4s", &query, &base).await;
    let init = request(&service, "bcopy", "video0/init.mp4", &query, &base).await;
    let value = probe_fragment(&mut fixture, "bcopy-final.mp4", &init, &final_segment, "v");
    assert!((packet_start(&value) - 8.0).abs() < 0.002, "{value}");
    let mut all = Vec::new();
    for sequence in 1..=3 {
        let segment = request(
            &service,
            "bcopy",
            &format!("video0/segment{sequence}.m4s"),
            &query,
            &base,
        )
        .await;
        let value = probe_fragment(
            &mut fixture,
            &format!("bcopy-{sequence}.mp4"),
            &init,
            &segment,
            "v",
        );
        let packets = value["packets"].as_array().unwrap();
        assert_eq!(packets.len(), if sequence < 3 { 96 } else { 30 }, "{value}");
        let min = packets
            .iter()
            .map(|packet| packet["pts_time"].as_str().unwrap().parse::<f64>().unwrap())
            .min_by(f64::total_cmp)
            .unwrap();
        let max = packets
            .iter()
            .map(|packet| packet["pts_time"].as_str().unwrap().parse::<f64>().unwrap())
            .max_by(f64::total_cmp)
            .unwrap()
            + 1.0 / 24.0;
        assert!((min - (sequence - 1) as f64 * 4.0).abs() < 0.002, "{value}");
        assert!(
            (max - if sequence < 3 {
                sequence as f64 * 4.0
            } else {
                9.25
            })
            .abs()
                < 0.002,
            "{value}"
        );
        all.extend_from_slice(&segment);
    }
    probe_fragment(&mut fixture, "bcopy-all.mp4", &init, &all, "v");
    request(&service, "bcopy", "audio0/segment2.m4s", &query, &base).await;
    let status = service.status().await;
    assert_eq!(
        status["bcopy"]["status"]["converters"]["video0"]["copy"], true,
        "{status}"
    );
    assert_eq!(
        status["bcopy"]["status"]["converters"]["audio0"]["copy"], false,
        "{status}"
    );
    service.shutdown().await;
    server.abort();
}

#[tokio::test]
#[ignore = "requires FFmpeg and FFprobe; run with --include-ignored using packaged media tools"]
async fn real_delayed_and_short_audio_is_padded_on_the_video_timeline() {
    let mut fixture = Fixture::new();
    let path = fixture.path("delayed.mkv");
    ffmpeg(&[
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-y",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=size=160x90:rate=24:duration=12",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=880:sample_rate=48000:duration=3",
        "-filter_complex",
        "[1:a]asetpts=PTS+3/TB[a]",
        "-map",
        "0:v",
        "-map",
        "[a]",
        "-c:v",
        "libx264",
        "-bf",
        "3",
        "-g",
        "96",
        "-sc_threshold",
        "0",
        "-c:a",
        "pcm_s16le",
        path.to_str().unwrap(),
    ]);
    let (base, server) = serve(Bytes::from(std::fs::read(path).unwrap()), Bytes::new()).await;
    let service = HlsService::new();
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair("mediaURL", &format!("{base}/fixture"))
        .append_pair("videoCodecs", "h264")
        .finish();
    let first = request(&service, "delayed", "audio0/segment1.m4s", &query, &base).await;
    let init = request(&service, "delayed", "audio0/init.mp4", &query, &base).await;
    let info = probe_fragment(&mut fixture, "delayed-audio-first.mp4", &init, &first, "a");
    assert!((packet_end(&info) - 4.0).abs() < 0.07, "{info}");
    let input = fixture.path("delayed-audio-pcm.mp4");
    let mut bytes = init.to_vec();
    bytes.extend_from_slice(&first);
    std::fs::write(&input, bytes).unwrap();
    let output = Command::new(process::executable())
        .args([
            "-v",
            "error",
            "-i",
            input.to_str().unwrap(),
            "-f",
            "s16le",
            "-ac",
            "1",
            "pipe:1",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let samples = output
        .stdout
        .as_chunks::<2>()
        .0
        .iter()
        .map(|bytes| i16::from_le_bytes(*bytes))
        .collect::<Vec<_>>();
    let rms = |range: std::ops::Range<usize>| -> f64 {
        (samples[range.clone()]
            .iter()
            .map(|sample| f64::from(*sample).powi(2))
            .sum::<f64>()
            / range.len() as f64)
            .sqrt()
    };
    assert!(
        rms(48000..2 * 48000) < 2.0,
        "Audio played before its source start time"
    );
    assert!(
        rms(170000..180000) > 100.0,
        "Source audio missing after3 seconds"
    );
    for sequence in [2, 3] {
        let segment = request(
            &service,
            "delayed",
            &format!("audio0/segment{sequence}.m4s"),
            &query,
            &base,
        )
        .await;
        let info = probe_fragment(
            &mut fixture,
            &format!("delayed-audio-{sequence}.mp4"),
            &init,
            &segment,
            "a",
        );
        assert!((packet_start(&info) - (sequence - 1) as f64 * 4.0).abs() < 0.002);
        assert!((packet_end(&info) - sequence as f64 * 4.0).abs() < 0.07);
    }
    service.shutdown().await;
    server.abort();
}

#[tokio::test]
#[ignore = "requires FFmpeg and FFprobe; run with --include-ignored using packaged media tools"]
async fn real_vfr_matroska_video_copy_uses_packet_timestamps_instead_of_inferred_frame_counts() {
    let mut fixture = Fixture::new();
    let path = fixture.path("vfr.mkv");
    ffmpeg(&[
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-y",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=size=160x90:rate=24:duration=9.25",
        "-vf",
        "select='if(lt(t,4),1,not(mod(n,2)))'",
        "-fps_mode",
        "vfr",
        "-force_key_frames",
        "expr:gte(t,n_forced*4)",
        "-c:v",
        "libx264",
        "-bf",
        "3",
        "-g",
        "96",
        "-sc_threshold",
        "0",
        "-x264-params",
        "open-gop=0",
        path.to_str().unwrap(),
    ]);
    let (base, server) = serve(Bytes::from(std::fs::read(path).unwrap()), Bytes::new()).await;
    let service = HlsService::new();
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair("mediaURL", &format!("{base}/fixture"))
        .append_pair("videoCodecs", "h264")
        .finish();
    request(&service, "vfr", "video0/segment3.m4s", &query, &base).await;
    let init = request(&service, "vfr", "video0/init.mp4", &query, &base).await;
    let mut all = Vec::new();
    for sequence in 1..=3 {
        let segment = request(
            &service,
            "vfr",
            &format!("video0/segment{sequence}.m4s"),
            &query,
            &base,
        )
        .await;
        let info = probe_fragment(
            &mut fixture,
            &format!("vfr-{sequence}.mp4"),
            &init,
            &segment,
            "v",
        );
        let packets = info["packets"].as_array().unwrap();
        assert_eq!(
            packets.len(),
            match sequence {
                1 => 96,
                2 => 48,
                _ => 15,
            },
            "{info}"
        );
        let minimum = packets
            .iter()
            .map(|packet| packet["pts_time"].as_str().unwrap().parse::<f64>().unwrap())
            .min_by(f64::total_cmp)
            .unwrap();
        assert!(
            (minimum - (sequence - 1) as f64 * 4.0).abs() < 0.003,
            "{info}"
        );
        all.extend_from_slice(&segment);
    }
    probe_fragment(&mut fixture, "vfr-all.mp4", &init, &all, "v");
    let status = service.status().await;
    assert_eq!(
        status["vfr"]["status"]["converters"]["video0"]["copy"], true,
        "{status}"
    );
    service.shutdown().await;
    server.abort();
}

#[tokio::test]
#[ignore = "requires FFmpeg HEVC decoder and FFprobe; run with --include-ignored using packaged media tools"]
async fn real_hevc_closed_gop_video_is_copied_while_ac3_audio_is_converted() {
    let mut fixture = Fixture::new();
    let (base, server) = serve(
        Bytes::from_static(include_bytes!(
            "../../tests/fixtures/hls-hevc-closed-gop.mp4"
        )),
        Bytes::new(),
    )
    .await;
    let service = HlsService::new();
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair("mediaURL", &format!("{base}/fixture"))
        .append_pair("videoCodecs", "hevc")
        .finish();
    request(&service, "hevc", "video0/segment3.m4s", &query, &base).await;
    let init = request(&service, "hevc", "video0/init.mp4", &query, &base).await;
    let mut all = Vec::new();
    for sequence in 1..=3 {
        let segment = request(
            &service,
            "hevc",
            &format!("video0/segment{sequence}.m4s"),
            &query,
            &base,
        )
        .await;
        let info = probe_fragment(
            &mut fixture,
            &format!("hevc-{sequence}.mp4"),
            &init,
            &segment,
            "v",
        );
        assert_eq!(info["streams"][0]["codec_name"], "hevc");
        assert_eq!(info["streams"][0]["codec_tag_string"], "hvc1");
        assert!(
            (packet_start(&info) - (sequence - 1) as f64 * 4.0).abs() < 0.003,
            "{info}"
        );
        all.extend_from_slice(&segment);
    }
    probe_fragment(&mut fixture, "hevc-all.mp4", &init, &all, "v");
    let audio = request(&service, "hevc", "audio0/segment2.m4s", &query, &base).await;
    let audio_init = request(&service, "hevc", "audio0/init.mp4", &query, &base).await;
    let info = probe_fragment(&mut fixture, "hevc-audio.mp4", &audio_init, &audio, "a");
    assert_eq!(info["streams"][0]["codec_name"], "aac");
    let status = service.status().await;
    assert_eq!(
        status["hevc"]["status"]["converters"]["video0"]["copy"], true,
        "{status}"
    );
    assert_eq!(
        status["hevc"]["status"]["converters"]["audio0"]["copy"], false,
        "{status}"
    );
    service.shutdown().await;
    server.abort();
}

#[tokio::test]
#[ignore = "requires FFmpeg and FFprobe; run with --include-ignored using packaged media tools"]
async fn real_open_gop_matroska_video_falls_back_to_independent_transcoded_segments() {
    let mut fixture = Fixture::new();
    let path = fixture.path("open-gop.mkv");
    ffmpeg(&[
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-y",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=size=160x90:rate=24:duration=9.25",
        "-c:v",
        "libx264",
        "-bf",
        "3",
        "-g",
        "96",
        "-sc_threshold",
        "0",
        "-x264-params",
        "open-gop=1",
        path.to_str().unwrap(),
    ]);
    let (base, server) = serve(Bytes::from(std::fs::read(path).unwrap()), Bytes::new()).await;
    let service = HlsService::new();
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair("mediaURL", &format!("{base}/fixture"))
        .append_pair("videoCodecs", "h264")
        .finish();
    let segment = request(&service, "open", "video0/segment2.m4s", &query, &base).await;
    let init = request(&service, "open", "video0/init.mp4", &query, &base).await;
    probe_fragment(&mut fixture, "open-transcoded.mp4", &init, &segment, "v");
    let status = service.status().await;
    assert_eq!(
        status["open"]["status"]["converters"]["video0"]["copy"], false,
        "{status}"
    );
    service.shutdown().await;
    server.abort();
}
