#[derive(Clone, Debug, Deserialize, Serialize)]
struct DownloadRecord {
    id: String,
    url: String,
    status: String,
    #[serde(default, rename = "fileName")]
    file_name: String,
    #[serde(default, rename = "filePath")]
    file_path: String,
    #[serde(default)]
    total: u64,
    #[serde(default)]
    downloaded: u64,
    #[serde(default)]
    progress: f64,
    #[serde(default, deserialize_with = "deserialize_u64_lossy")]
    speed: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

struct DownloadManager {
    dir: PathBuf,
    stats_file: PathBuf,
    client: reqwest::Client,
    torrents: Arc<TorrentService>,
    downloads: RwLock<HashMap<String, DownloadRecord>>,
    tasks: RwLock<HashMap<String, tokio::task::JoinHandle<()>>>,
}

impl DownloadManager {
    async fn new(
        dir: PathBuf,
        stats_file: PathBuf,
        client: reqwest::Client,
        torrents: Arc<TorrentService>,
    ) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("creating downloads dir {}", dir.display()))?;

        let downloads = match tokio::fs::read(&stats_file).await {
            Ok(bytes) => serde_json::from_slice::<HashMap<String, DownloadRecord>>(&bytes)
                .unwrap_or_default(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(err) => return Err(err).with_context(|| format!("reading {}", stats_file.display())),
        };

        Ok(Self {
            dir,
            stats_file,
            client,
            torrents,
            downloads: RwLock::new(downloads),
            tasks: RwLock::new(HashMap::new()),
        })
    }

    async fn all(&self) -> Vec<DownloadRecord> {
        self.downloads.read().await.values().cloned().collect()
    }

    async fn get(&self, id: &str) -> Option<DownloadRecord> {
        self.downloads.read().await.get(id).cloned()
    }

    async fn is_running(&self, id: &str) -> bool {
        self.downloads
            .read()
            .await
            .get(id)
            .is_some_and(|record| record.status == "running")
    }

    async fn add(self: &Arc<Self>, url: String) -> anyhow::Result<DownloadRecord> {
        let id = self.generate_id().await;
        let file_name = guess_download_file_name(&url, &id);
        let record = DownloadRecord {
            id: id.clone(),
            url,
            status: "running".to_string(),
            file_path: self.dir.join(&file_name).to_string_lossy().to_string(),
            file_name,
            total: 0,
            downloaded: 0,
            progress: 0.0,
            speed: 0,
            error: None,
        };

        self.downloads
            .write()
            .await
            .insert(id.clone(), record.clone());
        self.persist().await;
        self.spawn_download(id).await;
        Ok(record)
    }

    async fn pause(self: &Arc<Self>, id: &str) -> anyhow::Result<()> {
        let mut downloads = self.downloads.write().await;
        let record = downloads
            .get_mut(id)
            .ok_or_else(|| anyhow!("there is no download handler with this id"))?;
        if record.status != "running" {
            anyhow::bail!("download is not running");
        }
        record.status = "paused".to_string();
        record.speed = 0;
        drop(downloads);

        if let Some(task) = self.tasks.write().await.remove(id) {
            task.abort();
        }
        self.release_download_owner(id).await;
        self.persist().await;
        Ok(())
    }

    async fn resume(self: &Arc<Self>, id: &str) -> anyhow::Result<()> {
        let mut downloads = self.downloads.write().await;
        let record = downloads
            .get_mut(id)
            .ok_or_else(|| anyhow!("there is no download with this id"))?;
        if record.status != "paused" && record.status != "error" {
            anyhow::bail!("download is not pause or errored");
        }
        record.status = "running".to_string();
        record.error = None;
        drop(downloads);

        self.persist().await;
        self.spawn_download(id.to_string()).await;
        Ok(())
    }

    async fn remove(self: &Arc<Self>, id: &str) -> anyhow::Result<()> {
        if let Some(task) = self.tasks.write().await.remove(id) {
            task.abort();
        }
        self.release_download_owner(id).await;

        let record = self
            .downloads
            .write()
            .await
            .remove(id)
            .ok_or_else(|| anyhow!("there is no download handler with this id"))?;
        if !record.file_name.is_empty() {
            let path = self.download_path(&record.file_name);
            match tokio::fs::remove_file(path).await {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err).context("removing downloaded file"),
            }
        }
        self.persist().await;
        Ok(())
    }

    async fn stream_file(&self, id: &str, headers: HeaderMap) -> AppResult<Response> {
        let record = self
            .get(id)
            .await
            .ok_or_else(|| anyhow!("there is no download handler with this id"))?;
        if record.status != "finished" {
            return Ok(downloader_error(StatusCode::INTERNAL_SERVER_ERROR, "stream for this id not finished"));
        }
        if record.file_name.is_empty() {
            return Ok(downloader_error(StatusCode::NOT_FOUND, "file not found"));
        }

        let path = self.download_path(&record.file_name);
        let metadata = match tokio::fs::metadata(&path).await {
            Ok(metadata) if metadata.is_file() => metadata,
            _ => return Ok(downloader_error(StatusCode::NOT_FOUND, "file not found")),
        };
        let total_len = metadata.len();
        let range = headers
            .get(RANGE)
            .and_then(|header| header.to_str().ok())
            .and_then(|range| parse_range(range, total_len));
        let (status, start, end) = match range {
            Some((start, end)) => (StatusCode::PARTIAL_CONTENT, start, end),
            None => (StatusCode::OK, 0, total_len.saturating_sub(1)),
        };
        let body_len = if total_len == 0 { 0 } else { end - start + 1 };

        let mut file = tokio::fs::File::open(&path)
            .await
            .with_context(|| format!("opening {}", path.display()))?;
        if start > 0 {
            file.seek(SeekFrom::Start(start))
                .await
                .with_context(|| format!("seeking {}", path.display()))?;
        }

        let mime = mime_guess::from_path(&path)
            .first_or_octet_stream()
            .to_string();
        let mut response = Response::builder().status(status);
        {
            let headers = response
                .headers_mut()
                .context("creating downloader stream headers")?;
            headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
            headers.insert(CONTENT_TYPE, header_value(&mime)?);
            headers.insert(CONTENT_LENGTH, header_value(&body_len.to_string())?);
            if status == StatusCode::PARTIAL_CONTENT {
                headers.insert(
                    CONTENT_RANGE,
                    header_value(&format!("bytes {}-{}/{}", start, end, total_len))?,
                );
            }
        }

        let body = Body::from_stream(ReaderStream::with_capacity(
            file.take(body_len),
            512 * 1024,
        ));
        Ok(response
            .body(body)
            .context("building downloader stream response")?)
    }

    async fn spawn_download(self: &Arc<Self>, id: String) {
        if let Some(task) = self.tasks.write().await.remove(&id) {
            task.abort();
        }

        let manager = Arc::clone(self);
        let task_id = id.clone();
        let task = tokio::spawn(async move {
            let result = manager.run_download(&task_id).await;
            manager.release_download_owner(&task_id).await;
            if let Err(err) = result {
                warn!(download_id = %task_id, error = %err, "download failed");
                let mut downloads = manager.downloads.write().await;
                if let Some(record) = downloads.get_mut(&task_id) {
                    if record.status == "running" {
                        record.status = "error".to_string();
                        record.error = Some(err.to_string());
                        record.speed = 0;
                    }
                }
                drop(downloads);
                manager.persist().await;
            }
            manager.tasks.write().await.remove(&task_id);
        });
        self.tasks.write().await.insert(id, task);
    }

    async fn run_download(self: &Arc<Self>, id: &str) -> anyhow::Result<()> {
        let record = self
            .get(id)
            .await
            .ok_or_else(|| anyhow!("download disappeared"))?;
        let mut downloaded = existing_download_len(&record.file_path).await;

        let source_url = downloader_source_url(&record.url);
        let mut request = self
            .client
            .get(&source_url)
            .header("x-stremio-playback-owner", downloader_owner(id));
        if downloaded > 0 {
            request = request.header(RANGE, format!("bytes={downloaded}-"));
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("starting download {source_url}"))?
            .error_for_status()
            .with_context(|| format!("download HTTP status {source_url}"))?;

        let file_name = response
            .headers()
            .get(CONTENT_DISPOSITION)
            .and_then(|value| value.to_str().ok())
            .and_then(content_disposition_filename)
            .unwrap_or_else(|| record.file_name.clone());
        let file_name = sanitize_download_file_name(&file_name, id);
        let path = self.download_path(&file_name);

        if file_name != record.file_name {
            downloaded = 0;
        }

        let total = response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .map(|len| len.saturating_add(downloaded))
            .unwrap_or(record.total);

        {
            let mut downloads = self.downloads.write().await;
            let record = downloads
                .get_mut(id)
                .ok_or_else(|| anyhow!("download disappeared"))?;
            record.file_name = file_name.clone();
            record.file_path = path.to_string_lossy().to_string();
            record.total = total;
            record.downloaded = downloaded;
            record.status = "running".to_string();
            record.error = None;
            record.progress = progress_percent(downloaded, total);
        }
        self.persist().await;

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(downloaded > 0)
            .truncate(downloaded == 0)
            .open(&path)
            .await
            .with_context(|| format!("opening {}", path.display()))?;

        let mut stream = response.bytes_stream();
        let mut last_speed_at = Instant::now();
        let mut last_speed_downloaded = downloaded;
        let mut last_speed = 0;
        let mut last_persist = Instant::now();

        while let Some(chunk) = stream
            .try_next()
            .await
            .with_context(|| format!("reading {}", record.url))?
        {
            file.write_all(&chunk)
                .await
                .with_context(|| format!("writing {}", path.display()))?;
            downloaded = downloaded.saturating_add(chunk.len() as u64);

            let speed_elapsed = last_speed_at.elapsed();
            if speed_elapsed >= Duration::from_millis(250) || total > 0 && downloaded >= total {
                let elapsed = speed_elapsed.as_secs_f64().max(0.001);
                last_speed =
                    ((downloaded.saturating_sub(last_speed_downloaded)) as f64 / elapsed).floor()
                        as u64;
                last_speed_at = Instant::now();
                last_speed_downloaded = downloaded;
            }

            let should_persist = last_persist.elapsed() >= Duration::from_secs(2);
            let mut downloads = self.downloads.write().await;
            if let Some(record) = downloads.get_mut(id) {
                if record.status != "running" {
                    return Ok(());
                }
                record.downloaded = downloaded;
                record.total = total;
                record.progress = progress_percent(downloaded, total);
                record.speed = last_speed;
            }
            drop(downloads);

            if should_persist {
                last_persist = Instant::now();
                self.persist().await;
            }
        }

        file.flush()
            .await
            .with_context(|| format!("flushing {}", path.display()))?;

        let mut downloads = self.downloads.write().await;
        if let Some(record) = downloads.get_mut(id) {
            record.downloaded = downloaded;
            record.total = total.max(downloaded);
            record.progress = 100.0;
            record.speed = 0;
            record.status = "finished".to_string();
            record.error = None;
        }
        drop(downloads);
        self.persist().await;
        Ok(())
    }

    async fn release_download_owner(&self, id: &str) {
        self.torrents.release_owner(&downloader_owner(id)).await;
    }

    fn download_path(&self, file_name: &str) -> PathBuf {
        self.dir.join(sanitize_download_file_name(file_name, "download"))
    }

    async fn generate_id(&self) -> String {
        loop {
            let millis = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis();
            let id = format!("{millis:x}");
            if !self.downloads.read().await.contains_key(&id) {
                return id;
            }
            sleep(Duration::from_millis(1)).await;
        }
    }

    async fn persist(&self) {
        let downloads = self.downloads.read().await.clone();
        let bytes = match serde_json::to_vec(&downloads) {
            Ok(bytes) => bytes,
            Err(err) => {
                warn!(error = %err, "failed to serialize downloader state");
                return;
            }
        };
        if let Err(err) = tokio::fs::write(&self.stats_file, bytes).await {
            warn!(path = %self.stats_file.display(), error = %err, "failed to persist downloader state");
        }
    }
}

async fn downloader_get_all(
    State(state): State<AppState>,
    AxumPath(_server_key): AxumPath<String>,
) -> Json<Value> {
    Json(json!(state.downloads.all().await))
}

async fn downloader_get(
    State(state): State<AppState>,
    AxumPath(_server_key): AxumPath<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let Some(id) = query_first(raw_query.as_deref(), "id") else {
        return downloader_error(StatusCode::INTERNAL_SERVER_ERROR, "missing id");
    };
    match state.downloads.get(&id).await {
        Some(record) => downloader_json(StatusCode::OK, json!(record)),
        None => downloader_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "there is no download with this id",
        ),
    }
}

async fn downloader_add(
    State(state): State<AppState>,
    AxumPath(_server_key): AxumPath<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let Some(url) = parse_downloader_add_url(raw_query.as_deref()) else {
        return downloader_error_with_code(StatusCode::INTERNAL_SERVER_ERROR, 1, "missing url");
    };
    match state.downloads.add(url).await {
        Ok(record) => downloader_json(StatusCode::OK, json!(record)),
        Err(err) => downloader_error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string()),
    }
}

async fn downloader_pause(
    State(state): State<AppState>,
    AxumPath(_server_key): AxumPath<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let Some(id) = query_first(raw_query.as_deref(), "id") else {
        return downloader_error(StatusCode::INTERNAL_SERVER_ERROR, "missing id");
    };
    match state.downloads.pause(&id).await {
        Ok(()) => downloader_json(StatusCode::OK, json!({ "success": true })),
        Err(err) => downloader_error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string()),
    }
}

async fn downloader_resume(
    State(state): State<AppState>,
    AxumPath(_server_key): AxumPath<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let Some(id) = query_first(raw_query.as_deref(), "id") else {
        return downloader_error(StatusCode::INTERNAL_SERVER_ERROR, "missing id");
    };
    match state.downloads.resume(&id).await {
        Ok(()) => downloader_json(StatusCode::OK, json!({ "success": true })),
        Err(err) => downloader_error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string()),
    }
}

async fn downloader_remove(
    State(state): State<AppState>,
    AxumPath(_server_key): AxumPath<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let Some(id) = query_first(raw_query.as_deref(), "id") else {
        return downloader_error(StatusCode::INTERNAL_SERVER_ERROR, "missing id");
    };
    match state.downloads.remove(&id).await {
        Ok(()) => downloader_json(StatusCode::OK, json!({ "success": true })),
        Err(err) => downloader_error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string()),
    }
}

async fn downloader_stream(
    State(state): State<AppState>,
    AxumPath(_server_key): AxumPath<String>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> AppResult<Response> {
    let Some(id) = query_first(raw_query.as_deref(), "id") else {
        return Ok(downloader_error(StatusCode::INTERNAL_SERVER_ERROR, "missing id"));
    };
    state.downloads.stream_file(&id, headers).await
}

fn downloader_json(status: StatusCode, value: Value) -> Response {
    (
        status,
        [(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
        Json(value),
    )
        .into_response()
}

fn downloader_error(status: StatusCode, message: &str) -> Response {
    downloader_json(status, json!({ "error": { "message": message } }))
}

fn downloader_error_with_code(status: StatusCode, code: i64, message: &str) -> Response {
    downloader_json(status, json!({ "error": { "code": code, "message": message } }))
}

fn parse_downloader_add_url(raw_query: Option<&str>) -> Option<String> {
    let raw_query = raw_query?;
    let value = raw_query
        .find("url=")
        .map(|idx| &raw_query[idx + "url=".len()..])?;
    let plus_decoded = value.replace('+', " ");
    let decoded = percent_decode_str(&plus_decoded)
        .decode_utf8_lossy()
        .into_owned();
    let trimmed = decoded.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn content_disposition_filename(header: &str) -> Option<String> {
    for part in header.split(';').map(str::trim) {
        if let Some(value) = part.strip_prefix("filename*=") {
            let value = value.trim_matches('"');
            if let Some((_, encoded)) = value.split_once("''") {
                return form_urlencoded::parse(encoded.as_bytes())
                    .next()
                    .map(|(value, _)| value.into_owned());
            }
        }
        if let Some(value) = part.strip_prefix("filename=") {
            return Some(value.trim_matches('"').to_string());
        }
    }
    None
}

fn guess_download_file_name(url: &str, fallback: &str) -> String {
    let name = url::Url::parse(url)
        .ok()
        .and_then(|url| {
            url.path_segments()
                .and_then(|mut segments| segments.next_back())
                .map(str::to_string)
        })
        .unwrap_or_default();
    sanitize_download_file_name(&name, fallback)
}

fn downloader_source_url(url: &str) -> String {
    let Ok(mut parsed) = url::Url::parse(url) else {
        return url.to_string();
    };
    let is_torrent_stream = parsed
        .path_segments()
        .and_then(|mut segments| segments.next().map(str::to_string))
        .is_some_and(|segment| {
            segment.len() == 40 && segment.chars().all(|ch| ch.is_ascii_hexdigit())
        });
    if !is_torrent_stream {
        return url.to_string();
    }

    let mut removed_external = false;
    let pairs = parsed
        .query_pairs()
        .filter_map(|(key, value)| {
            if key == "external" {
                removed_external = true;
                None
            } else {
                Some((key.into_owned(), value.into_owned()))
            }
        })
        .collect::<Vec<_>>();
    if !removed_external {
        return url.to_string();
    }

    parsed.set_query(None);
    if !pairs.is_empty() {
        let mut serializer = form_urlencoded::Serializer::new(String::new());
        serializer.extend_pairs(pairs.iter().map(|(key, value)| (key.as_str(), value.as_str())));
        parsed.set_query(Some(&serializer.finish()));
    }
    parsed.to_string()
}

fn sanitize_download_file_name(file_name: &str, fallback: &str) -> String {
    let mut sanitized = file_name
        .chars()
        .map(|ch| match ch {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            ch if ch.is_control() => '_',
            ch => ch,
        })
        .collect::<String>();
    sanitized = sanitized.trim_matches([' ', '.']).to_string();
    if sanitized.is_empty() {
        fallback.to_string()
    } else {
        sanitized
    }
}

async fn existing_download_len(path: &str) -> u64 {
    tokio::fs::metadata(path)
        .await
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn progress_percent(downloaded: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        ((downloaded as f64 / total as f64) * 100.0).clamp(0.0, 100.0)
    }
}

fn downloader_owner(id: &str) -> String {
    format!("download-{id}")
}

fn downloader_id_from_owner(owner: &str) -> Option<&str> {
    owner.strip_prefix("download-").filter(|id| !id.is_empty())
}

fn deserialize_u64_lossy<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Number(number) => number
            .as_u64()
            .or_else(|| number.as_f64().map(|value| value.max(0.0).floor() as u64))
            .ok_or_else(|| serde::de::Error::custom("invalid number")),
        Value::String(value) => value
            .parse::<u64>()
            .map_err(|err| serde::de::Error::custom(err.to_string())),
        Value::Null => Ok(0),
        _ => Err(serde::de::Error::custom("expected number")),
    }
}
