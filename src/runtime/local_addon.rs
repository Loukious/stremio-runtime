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
            let mut candidate_titles = vec![name];
            for key in ["aliases", "aka", "videos"] {
                if let Some(values) = meta.get(key).and_then(Value::as_array) {
                    for value in values {
                        if let Some(title) = value.as_str() {
                            candidate_titles.push(title);
                        } else if let Some(title) = value.get("title").and_then(Value::as_str) {
                            candidate_titles.push(title);
                        }
                    }
                }
            }

            let name_score = candidate_titles
                .iter()
                .map(|title| score_title_match(&simplified_query, &simplify_video_title(title)))
                .max()
                .unwrap_or(0);
            if name_score == 0 {
                return None;
            }

            let result_year = meta_year(meta);
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

fn score_title_match(query: &str, candidate: &str) -> i32 {
    if query.is_empty() || candidate.is_empty() {
        0
    } else if candidate == query {
        100
    } else if candidate.contains(query) || query.contains(candidate) {
        70
    } else {
        let query_tokens = title_tokens(query);
        let candidate_tokens = title_tokens(candidate);
        if query_tokens.is_empty() || candidate_tokens.is_empty() {
            return 0;
        }
        let overlap = query_tokens
            .iter()
            .filter(|token| candidate_tokens.contains(token))
            .count();
        let min_len = query_tokens.len().min(candidate_tokens.len());
        if overlap == min_len && min_len >= 2 {
            60
        } else if overlap >= 2 && overlap * 2 >= query_tokens.len().max(candidate_tokens.len()) {
            45
        } else {
            0
        }
    }
}

fn title_tokens(value: &str) -> Vec<String> {
    static TOKEN_RE: OnceLock<Regex> = OnceLock::new();
    let token_re = TOKEN_RE.get_or_init(|| Regex::new(r"[a-z0-9]+").expect("regex"));
    token_re
        .find_iter(value)
        .map(|m| m.as_str().to_owned())
        .filter(|token| !matches!(token.as_str(), "the" | "a" | "an"))
        .collect()
}

fn meta_year(meta: &Value) -> Option<i32> {
    [
        meta.get("releaseInfo"),
        meta.get("year"),
        meta.get("released"),
        meta.get("publishedAt"),
    ]
    .into_iter()
    .flatten()
    .filter_map(Value::as_str)
    .find_map(|value| {
        Regex::new(r"\b(19\d{2}|20\d{2})\b")
            .ok()?
            .find(value)?
            .as_str()
            .parse::<i32>()
            .ok()
    })
}

fn parse_video_filename(name: &str) -> Option<ParsedVideoName> {
    static SEASON_EPISODE_PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    static YEAR_RE: OnceLock<Regex> = OnceLock::new();
    static QUALITY_RE: OnceLock<Regex> = OnceLock::new();

    let stem = Path::new(name)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or(name);
    let normalized = normalize_release_name(stem);

    let season_episode_patterns = SEASON_EPISODE_PATTERNS.get_or_init(|| {
        [
            r"(?i)\bS(\d{1,2})\s*E(\d{1,3})\b",
            r"(?i)\b(\d{1,2})x(\d{1,3})\b",
            r"(?i)\bSeason\s*(\d{1,2})\s*Episode\s*(\d{1,3})\b",
            r"(?i)\bS(\d{1,2})\b.*?\bE(\d{1,3})\b",
        ]
        .into_iter()
        .filter_map(|pattern| Regex::new(pattern).ok())
        .collect()
    });

    let season_episode = season_episode_patterns.iter().find_map(|re| {
        re.captures(&normalized).and_then(|captures| {
            let whole = captures.get(0)?;
            let season = captures.get(1)?.as_str().parse::<u32>().ok()?;
            let episode = captures.get(2)?.as_str().parse::<u32>().ok()?;
            Some((whole.start(), season, episode))
        })
    });

    let year_re = YEAR_RE.get_or_init(|| Regex::new(r"\b(19\d{2}|20\d{2})\b").expect("regex"));
    let year_match = year_re
        .find(&normalized)
        .and_then(|m| m.as_str().parse::<i32>().ok().map(|year| (m.start(), year)));

    let quality_re = QUALITY_RE.get_or_init(|| {
        Regex::new(
            r"(?i)\b(2160p|1080p|720p|480p|4k|8k|web[- ]?dl|webrip|web|bluray|blu[- ]?ray|brrip|hdrip|hdtv|dvdrip|remux|xvid|x264|x265|h\.?264|h\.?265|hevc|av1|aac|ddp?5?\.?1|atmos|proper|repack|internal|limited|extended|remastered|10bit|8bit)\b",
        )
        .expect("regex")
    });
    let quality_match = quality_re.find(&normalized).map(|m| m.start());

    let cutoff = [
        season_episode.map(|(idx, _, _)| idx),
        year_match.map(|(idx, _)| idx),
        quality_match,
    ]
    .into_iter()
    .flatten()
    .min()
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

fn normalize_release_name(value: &str) -> String {
    static BRACKET_RE: OnceLock<Regex> = OnceLock::new();
    static SEP_RE: OnceLock<Regex> = OnceLock::new();
    static SPACES_RE: OnceLock<Regex> = OnceLock::new();

    let bracket_re =
        BRACKET_RE.get_or_init(|| Regex::new(r"[\[\(\{][^\]\)\}]*[\]\)\}]").expect("regex"));
    let sep_re = SEP_RE.get_or_init(|| Regex::new(r"[._+\-]+").expect("regex"));
    let spaces_re = SPACES_RE.get_or_init(|| Regex::new(r"\s+").expect("regex"));

    let no_brackets = bracket_re.replace_all(value, " ");
    let separated = sep_re.replace_all(&no_brackets, " ");
    spaces_re.replace_all(separated.trim(), " ").to_string()
}

fn cleanup_video_title(value: &str) -> String {
    value
        .split_whitespace()
        .take_while(|part| !is_release_noise_token(part))
        .filter(|part| !is_release_noise_token(part))
        .collect::<Vec<_>>()
        .join(" ")
        .trim_matches(|ch: char| !ch.is_ascii_alphanumeric())
        .trim()
        .to_owned()
}

fn is_release_noise_token(part: &str) -> bool {
    let lower = part
        .trim_matches(|ch: char| !ch.is_ascii_alphanumeric())
        .to_ascii_lowercase();
    lower.is_empty()
        || matches!(
            lower.as_str(),
            "1080p"
                | "720p"
                | "2160p"
                | "480p"
                | "4k"
                | "8k"
                | "webrip"
                | "web"
                | "webdl"
                | "web-dl"
                | "bluray"
                | "blu-ray"
                | "brrip"
                | "hdrip"
                | "hdtv"
                | "dvdrip"
                | "remux"
                | "xvid"
                | "x264"
                | "x265"
                | "h264"
                | "h265"
                | "hevc"
                | "av1"
                | "aac"
                | "dd"
                | "ddp"
                | "dd5"
                | "ddp5"
                | "atmos"
                | "proper"
                | "repack"
                | "internal"
                | "limited"
                | "extended"
                | "remastered"
                | "10bit"
                | "8bit"
                | "yify"
                | "rarbg"
                | "eztv"
                | "psa"
        )
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
