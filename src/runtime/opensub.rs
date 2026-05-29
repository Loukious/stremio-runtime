async fn opensub_hash(State(state): State<AppState>, RawQuery(raw_query): RawQuery) -> Response {
    let Some(url) = query_first(raw_query.as_deref(), "videoUrl") else {
        return opensub_hash_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            Some("missing videoUrl".to_string()),
            None,
        );
    };

    if let Some((info_hash, idx)) = parse_local_torrent_media_url(&url) {
        match compute_local_opensub_hash(&state, &info_hash, &idx).await {
            Ok(result) => return opensub_hash_response(StatusCode::OK, None, Some(result)),
            Err(err) => {
                debug!(
                    url,
                    error = %err,
                    "local opensubHash unavailable without touching stream priority"
                );
                return opensub_hash_response(StatusCode::OK, None, None);
            }
        }
    }

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
    hash: Option<String>,
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
        hash: Some(format!("{hash:016x}")),
    })
}

async fn compute_local_opensub_hash(
    state: &AppState,
    info_hash: &str,
    idx: &str,
) -> anyhow::Result<OpenSubHashResult> {
    let handle = state
        .torrents
        .get(info_hash)
        .await
        .context("torrent is not active")?;

    let _ = timeout(CREATE_METADATA_GRACE, handle.wait_until_initialized()).await;
    let file_idx = resolve_file_index(&handle, idx, &[])?;
    let file = file_for_handle(&handle, file_idx)?.context("torrent file not found")?;
    let size = file.length;

    if size < OPENSUB_HASH_CHUNK_SIZE {
        return Ok(OpenSubHashResult { size, hash: None });
    }

    let stats = handle.stats();
    let verified = stats
        .file_progress
        .get(file_idx)
        .copied()
        .unwrap_or(0)
        .min(size);
    if verified < size {
        return Ok(OpenSubHashResult { size, hash: None });
    }

    let path = state.torrents.cache_dir.join(info_hash).join(&file.path);
    let hash = compute_opensub_hash_from_file(&path, size).await?;
    Ok(OpenSubHashResult {
        size,
        hash: Some(hash),
    })
}

async fn compute_opensub_hash_from_file(path: &Path, size: u64) -> anyhow::Result<String> {
    let first = read_file_range(path, 0, OPENSUB_HASH_CHUNK_SIZE).await?;
    let tail_start = size.saturating_sub(OPENSUB_HASH_CHUNK_SIZE);
    let last = if tail_start == 0 {
        first.clone()
    } else {
        read_file_range(path, tail_start, OPENSUB_HASH_CHUNK_SIZE).await?
    };

    let mut hash = size;
    hash = hash.wrapping_add(opensub_chunk_sum(&first));
    hash = hash.wrapping_add(opensub_chunk_sum(&last));
    Ok(format!("{hash:016x}"))
}

async fn read_file_range(path: &Path, start: u64, len: u64) -> anyhow::Result<Vec<u8>> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("opening {}", path.display()))?;
    file.seek(SeekFrom::Start(start))
        .await
        .with_context(|| format!("seeking {} to {start}", path.display()))?;
    let mut bytes = vec![0u8; len as usize];
    file.read_exact(&mut bytes)
        .await
        .with_context(|| format!("reading {} bytes from {}", len, path.display()))?;
    Ok(bytes)
}

fn parse_local_torrent_media_url(media_url: &str) -> Option<(String, String)> {
    let parsed = url::Url::parse(media_url).ok()?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return None;
    }

    let mut segments = parsed.path_segments()?;
    let info_hash = normalize_info_hash(segments.next()?).ok()?;
    let idx = segments.next()?.to_string();
    Some((info_hash, idx))
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
