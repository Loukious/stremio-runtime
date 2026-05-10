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

    if let Err(e) = state.torrents.select_only_file(&handle, file_idx).await {
        warn!(file_idx, "select_only_file failed (non-fatal): {:?}", e.0);
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
