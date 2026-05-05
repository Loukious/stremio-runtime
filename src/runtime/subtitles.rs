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
