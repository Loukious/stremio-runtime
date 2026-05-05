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
