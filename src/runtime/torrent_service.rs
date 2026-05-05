impl TorrentService {
    async fn add_magnet(
        &self,
        info_hash: &str,
        request: &CreateTorrentRequest,
        query_trackers: Vec<String>,
    ) -> AppResult<Arc<librqbit::ManagedTorrent>> {
        let info_hash = normalize_info_hash(info_hash)?;
        if let Some(handle) = self.get(&info_hash).await {
            self.touch(&info_hash).await;
            return Ok(handle);
        }

        let mut magnet = Magnet::parse(&info_hash).context("parsing info hash as magnet")?;
        let trackers = merge_trackers(request.announce.iter().cloned().chain(query_trackers));
        magnet.trackers = trackers.clone();

        let mut initial_peers = Vec::new();
        initial_peers.extend(parse_peer_addrs(&request.initial_peers));
        initial_peers.extend(parse_peer_addrs(&request.peers));

        let default_output_folder = self.cache_dir.join(&info_hash);
        let output_folder = request
            .path
            .as_ref()
            .filter(|p| !p.trim().is_empty())
            .cloned()
            .or_else(|| Some(default_output_folder.to_string_lossy().into_owned()));

        if let Some(connections) = request.connections {
            debug!(
                connections,
                "requested connection cap is currently advisory"
            );
        }
        if !request.extra.is_empty() {
            debug!(keys = ?request.extra.keys().collect::<Vec<_>>(), "create request had extra options");
        }

        let handle = self
            .session
            .add_torrent(
                AddTorrent::from_url(magnet.to_string()),
                Some(AddTorrentOptions {
                    overwrite: true,
                    output_folder,
                    peer_opts: Some(PeerConnectionOptions {
                        connect_timeout: Some(Duration::from_secs(8)),
                        read_write_timeout: Some(Duration::from_secs(30)),
                        keep_alive_interval: Some(Duration::from_secs(60)),
                    }),
                    force_tracker_interval: Some(Duration::from_secs(120)),
                    only_files: request
                        .file_idx
                        .and_then(valid_idx)
                        .map(|file_idx| vec![file_idx]),
                    initial_peers: if initial_peers.is_empty() {
                        None
                    } else {
                        Some(initial_peers)
                    },
                    trackers: Some(trackers),
                    ..Default::default()
                }),
            )
            .await
            .context("adding magnet to librqbit")?
            .into_handle()
            .context("torrent was not started")?;

        let ih = handle.info_hash().as_string();
        self.handles
            .write()
            .await
            .insert(ih.clone(), handle.clone());
        self.touch(&ih).await;
        Ok(handle)
    }

    /// Remove every torrent except `current_info_hash` in the background.
    ///
    /// Spawned as a detached task so the caller returns immediately — session.delete()
    /// can block waiting for internal cleanup and must not hold up the new request.
    fn stop_others(self: &Arc<Self>, current_info_hash: &str) {
        let others: Vec<String> = self
            .handles
            // Use try_read so we never block the async task; if the lock is
            // contended we just skip — the inactivity timer is the safety net.
            .try_read()
            .map(|g| {
                g.keys()
                    .filter(|h| h.as_str() != current_info_hash)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        if others.is_empty() {
            return;
        }

        let service = Arc::clone(self);
        tokio::spawn(async move {
            for hash in others {
                info!(info_hash = %hash, "stopping torrent (new stream started)");
                if let Err(err) = service.remove(&hash).await {
                    warn!(info_hash = %hash, error = %err, "failed to stop old torrent");
                }
            }
        });
    }

    async fn get_or_add_magnet(
        &self,
        info_hash: &str,
        query_trackers: Vec<String>,
        preferred_file_idx: Option<usize>,
    ) -> AppResult<Arc<librqbit::ManagedTorrent>> {
        let info_hash = normalize_info_hash(info_hash)?;
        if let Some(handle) = self.get(&info_hash).await {
            return Ok(handle);
        }
        self.add_magnet(
            &info_hash,
            &CreateTorrentRequest {
                announce: Vec::new(),
                file_must_include: Vec::new(),
                guess_file_idx: None,
                file_idx: preferred_file_idx.map(|idx| idx as isize),
                connections: None,
                path: None,
                initial_peers: Vec::new(),
                peers: Vec::new(),
                extra: Map::new(),
            },
            query_trackers,
        )
        .await
    }

    async fn select_only_file(
        &self,
        handle: &Arc<librqbit::ManagedTorrent>,
        file_idx: usize,
    ) -> AppResult<()> {
        self.session
            .update_only_files(handle, &HashSet::from([file_idx]))
            .await
            .with_context(|| format!("selecting only torrent file {file_idx}"))
            .map_err(AppError::from)
    }

    async fn add_torrent_bytes(&self, bytes: Bytes) -> AppResult<Arc<librqbit::ManagedTorrent>> {
        let handle = self
            .session
            .add_torrent(
                AddTorrent::from_bytes(bytes),
                Some(AddTorrentOptions {
                    overwrite: true,
                    output_folder: Some(self.cache_dir.to_string_lossy().into_owned()),
                    trackers: Some(merge_trackers(std::iter::empty())),
                    ..Default::default()
                }),
            )
            .await
            .context("adding torrent file to librqbit")?
            .into_handle()
            .context("torrent was not started")?;
        let ih = handle.info_hash().as_string();
        self.handles
            .write()
            .await
            .insert(ih.clone(), handle.clone());
        self.touch(&ih).await;
        Ok(handle)
    }

    async fn get(&self, info_hash: &str) -> Option<Arc<librqbit::ManagedTorrent>> {
        let Ok(info_hash) = normalize_info_hash(info_hash) else {
            return None;
        };
        self.handles.read().await.get(&info_hash).cloned()
    }

    async fn remove(&self, info_hash: &str) -> anyhow::Result<()> {
        let info_hash = normalize_info_hash(info_hash)?;
        self.handles.write().await.remove(&info_hash);
        self.last_active.write().await.remove(&info_hash);
        self.active_streams.write().await.remove(&info_hash);
        self.session
            .delete(TorrentIdOrHash::parse(&info_hash)?, false)
            .await
            .with_context(|| format!("deleting torrent {info_hash}"))?;
        Ok(())
    }

    async fn remove_all(&self) -> anyhow::Result<()> {
        let keys = self
            .handles
            .read()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            if let Err(err) = self.remove(&key).await {
                warn!(info_hash = key, error = %err, "failed to remove torrent");
            }
        }
        Ok(())
    }

    async fn touch(&self, info_hash: &str) {
        self.last_active
            .write()
            .await
            .insert(info_hash.to_string(), Instant::now());
    }

    async fn stream_started(&self, info_hash: &str) {
        self.touch(info_hash).await;
        let mut active = self.active_streams.write().await;
        *active.entry(info_hash.to_string()).or_insert(0) += 1;
    }

    async fn stream_finished(&self, info_hash: &str) {
        self.touch(info_hash).await;
        let mut active = self.active_streams.write().await;
        match active.get_mut(info_hash) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                active.remove(info_hash);
            }
            None => {}
        }
    }

    async fn cleanup_inactive(&self) {
        let keys = self
            .handles
            .read()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();

        for hash in keys {
            let active = self
                .active_streams
                .read()
                .await
                .get(&hash)
                .copied()
                .unwrap_or(0);
            if active > 0 {
                continue;
            }

            let last = self.last_active.read().await.get(&hash).copied();
            let Some(last) = last else {
                continue;
            };

            if last.elapsed() <= ENGINE_INACTIVITY_TIMEOUT {
                continue;
            }

            info!(info_hash = %hash, "engine inactive, destroying it");
            if let Err(err) = self.remove(&hash).await {
                warn!(info_hash = %hash, error = %err, "cleanup remove failed");
                continue;
            }
            info!(info_hash = %hash, "engine destroyed");
        }
    }

    async fn cleanup_cache_to_limit(&self, settings: &SettingsStore) {
        let Some(limit) = settings.cache_size_limit().await else {
            debug!("cache reaper skipped because cacheSize is unlimited");
            return;
        };

        let active_hashes = self.active_cache_keys().await;
        let (mut total, mut candidates) = match collect_cache_entries(
            &self.cache_dir,
            &active_hashes,
        )
        .await
        {
            Ok(entries) => entries,
            Err(err) => {
                warn!(cache_dir = %self.cache_dir.display(), error = %err, "cache reaper scan failed");
                return;
            }
        };

        if total <= limit {
            debug!(
                cache_dir = %self.cache_dir.display(),
                total,
                limit,
                "cache reaper skipped; cache is within limit"
            );
            return;
        }

        candidates.sort_by(|a, b| {
            a.modified
                .cmp(&b.modified)
                .then_with(|| a.path.cmp(&b.path))
        });

        info!(
            cache_dir = %self.cache_dir.display(),
            total,
            limit,
            candidates = candidates.len(),
            "cache is over limit; pruning inactive entries"
        );

        for entry in candidates {
            if total <= limit {
                break;
            }

            match remove_cache_entry(&entry, &self.cache_dir).await {
                Ok(()) => {
                    total = total.saturating_sub(entry.size);
                    info!(
                        key = %entry.key,
                        path = %entry.path.display(),
                        freed = entry.size,
                        remaining = total,
                        limit,
                        "cache entry removed"
                    );
                }
                Err(err) => {
                    warn!(
                        key = %entry.key,
                        path = %entry.path.display(),
                        error = %err,
                        "cache entry removal failed"
                    );
                }
            }
        }

        if total > limit {
            warn!(
                cache_dir = %self.cache_dir.display(),
                total,
                limit,
                active = active_hashes.len(),
                "cache remains over limit; remaining data is active or could not be removed"
            );
        }
    }

    async fn active_cache_keys(&self) -> HashSet<String> {
        let mut active = self
            .handles
            .read()
            .await
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        active.extend(self.active_streams.read().await.keys().cloned());
        active
    }
}

async fn collect_cache_entries(
    cache_dir: &Path,
    active_keys: &HashSet<String>,
) -> anyhow::Result<(u64, Vec<CacheEntry>)> {
    let mut total = 0u64;
    let mut candidates = Vec::new();

    let mut entries = match tokio::fs::read_dir(cache_dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok((0, Vec::new())),
        Err(err) => return Err(err).with_context(|| format!("reading {}", cache_dir.display())),
    };

    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("reading {}", cache_dir.display()))?
    {
        let path = entry.path();
        let key = entry.file_name().to_string_lossy().to_string();

        // librqbit stores fastresume bitfields here.  We delete per-torrent
        // bitfields together with the torrent cache entry, but never prune the
        // session directory as a standalone cache object.
        if key.eq_ignore_ascii_case("session") {
            continue;
        }

        let (size, modified) = cache_entry_stats(&path)
            .await
            .with_context(|| format!("scanning cache entry {}", path.display()))?;
        total = total.saturating_add(size);

        if !active_keys.contains(&key) {
            candidates.push(CacheEntry {
                key,
                path,
                size,
                modified,
            });
        }
    }

    Ok((total, candidates))
}

async fn cache_entry_stats(path: &Path) -> anyhow::Result<(u64, SystemTime)> {
    let mut size = 0u64;
    let mut modified = SystemTime::UNIX_EPOCH;
    let mut stack = vec![path.to_path_buf()];

    while let Some(path) = stack.pop() {
        let metadata = match tokio::fs::symlink_metadata(&path).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err).with_context(|| format!("stat {}", path.display())),
        };

        if let Ok(mtime) = metadata.modified() {
            if mtime > modified {
                modified = mtime;
            }
        }

        if metadata.is_dir() {
            let mut entries = match tokio::fs::read_dir(&path).await {
                Ok(entries) => entries,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err).with_context(|| format!("reading {}", path.display())),
            };

            while let Some(entry) = entries
                .next_entry()
                .await
                .with_context(|| format!("reading {}", path.display()))?
            {
                stack.push(entry.path());
            }
        } else if metadata.is_file() {
            size = size.saturating_add(metadata.len());
        }
    }

    Ok((size, modified))
}

async fn remove_cache_entry(entry: &CacheEntry, cache_dir: &Path) -> anyhow::Result<()> {
    let canonical_cache = tokio::fs::canonicalize(cache_dir)
        .await
        .with_context(|| format!("canonicalizing {}", cache_dir.display()))?;
    let parent = entry.path.parent().context("cache entry has no parent")?;
    let canonical_parent = tokio::fs::canonicalize(parent)
        .await
        .with_context(|| format!("canonicalizing {}", parent.display()))?;

    if canonical_parent != canonical_cache {
        anyhow::bail!(
            "refusing to remove cache entry outside cache root: {}",
            entry.path.display()
        );
    }

    let metadata = match tokio::fs::symlink_metadata(&entry.path).await {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err).with_context(|| format!("stat {}", entry.path.display())),
    };

    if metadata.is_dir() {
        tokio::fs::remove_dir_all(&entry.path)
            .await
            .with_context(|| format!("removing {}", entry.path.display()))?;
    } else {
        tokio::fs::remove_file(&entry.path)
            .await
            .with_context(|| format!("removing {}", entry.path.display()))?;
    }

    remove_fastresume_for_cache_key(cache_dir, &entry.key).await;
    Ok(())
}

async fn remove_fastresume_for_cache_key(cache_dir: &Path, key: &str) {
    if key.len() != 40 || !key.chars().all(|c| c.is_ascii_hexdigit()) {
        return;
    }

    let bitv_path = cache_dir.join("session").join(format!("{key}.bitv"));
    match tokio::fs::remove_file(&bitv_path).await {
        Ok(()) => debug!(path = %bitv_path.display(), "removed stale fastresume bitfield"),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            warn!(path = %bitv_path.display(), error = %err, "failed to remove fastresume bitfield")
        }
    }
}

fn stats_for_handle(
    handle: &Arc<librqbit::ManagedTorrent>,
    cache_dir: &Path,
    stream_idx: Option<usize>,
    guessed_idx: Option<usize>,
) -> Value {
    let stats = handle.stats();
    let files = files_for_handle(handle);
    let live = stats.live.as_ref();
    let peer_stats = live.map(|live| &live.snapshot.peer_stats);
    let live_peers = peer_stats.map(|peers| peers.live).unwrap_or(0);
    let queued_peers = peer_stats.map(|peers| peers.queued).unwrap_or(0);
    let connecting_peers = peer_stats.map(|peers| peers.connecting).unwrap_or(0);
    let discovered_peers = peer_stats.map(|peers| peers.seen).unwrap_or(live_peers);
    let dead_peers = peer_stats.map(|peers| peers.dead).unwrap_or(0);
    let unique_peers = discovered_peers;
    let download_speed = live
        .map(|live| mib_per_sec_to_bytes_per_sec(live.download_speed.mbps))
        .unwrap_or(0.0);
    let upload_speed = live
        .map(|live| mib_per_sec_to_bytes_per_sec(live.upload_speed.mbps))
        .unwrap_or(0.0);

    // Keep completion based on verified torrent/file bytes. `fetched_bytes` includes duplicate/raw peer
    // traffic and can exceed the file length, which makes the player show impossible percentages.
    let downloaded = stats.progress_bytes;
    let display_peers = if live_peers > 0 {
        live_peers
    } else if connecting_peers > 0 {
        connecting_peers.min(40)
    } else if unique_peers > 0 {
        unique_peers.min(3)
    } else if downloaded > 0 && !stats.finished {
        1
    } else {
        0
    };
    let info_hash = handle.info_hash().as_string();
    let source_urls = source_urls(&info_hash);

    let wires = stream_idx
        .is_none()
        .then(|| {
            handle
                .live()
                .map(|live| live.per_peer_stats_snapshot(Default::default()))
                .map(|snapshot| {
                    let mut addrs = snapshot.peers.keys().cloned().collect::<Vec<_>>();
                    addrs.sort();

                    let n = addrs.len() as f64;
                    let (base_down, mut rem_down) = if addrs.is_empty() {
                        (0.0, 0.0)
                    } else {
                        ((download_speed / n).floor(), download_speed % n)
                    };
                    let (base_up, mut rem_up) = if addrs.is_empty() {
                        (0.0, 0.0)
                    } else {
                        ((upload_speed / n).floor(), upload_speed % n)
                    };

                    addrs
                        .into_iter()
                        .map(|addr| {
                            let mut down = base_down;
                            if rem_down >= 1.0 {
                                down += 1.0;
                                rem_down -= 1.0;
                            }

                            let mut up = base_up;
                            if rem_up >= 1.0 {
                                up += 1.0;
                                rem_up -= 1.0;
                            }

                            json!({
                                "requests": 0,
                                "address": addr,
                                "amInterested": false,
                                "isSeeder": false,
                                "downSpeed": down,
                                "upSpeed": up
                            })
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        })
        .unwrap_or_default();

    let mut out = EngineStats {
        info_hash: info_hash.clone(),
        name: handle.name().unwrap_or_default(),
        peers: if wires.is_empty() {
            display_peers as usize
        } else {
            wires.len()
        },
        unchoked: if live_peers > 1 {
            (live_peers / 2) as usize
        } else {
            live_peers as usize
        },
        queued: (queued_peers + connecting_peers) as usize,
        unique: unique_peers as usize,
        connection_tries: dead_peers as usize,
        swarm_paused: matches!(stats.state, librqbit::TorrentStatsState::Paused),
        swarm_connections: display_peers as usize,
        swarm_size: 400,
        selections: Vec::new(),
        wires: Some(wires),
        files: files.clone(),
        downloaded,
        uploaded: stats.uploaded_bytes,
        download_speed,
        upload_speed: download_speed.max(upload_speed),
        sources: official_sources(&source_urls, unique_peers.min(400) as usize),
        peer_search_running: !stats.finished,
        opts: official_stats_opts(&source_urls, &info_hash, cache_dir),
        state: stats.state.to_string(),
        error: stats.error,
        finished: None,
        stream_len: None,
        stream_name: None,
        stream_progress: None,
        guessed_file_idx: guessed_idx,
    };

    if let Some(idx) = stream_idx {
        if let Some(file) = files.get(idx) {
            out.wires = None;
            out.stream_len = Some(file.length);
            out.stream_name = Some(file.name.clone());
            out.stream_progress = Some({
                let done = stats
                    .file_progress
                    .get(idx)
                    .copied()
                    .unwrap_or(0)
                    .min(file.length);
                if stats.finished {
                    1.0
                } else if file.length == 0 {
                    0.0
                } else {
                    done as f64 / file.length as f64
                }
            });
        }
    }

    serde_json::to_value(out).unwrap_or_else(|_| json!(null))
}

fn source_urls(info_hash: &str) -> Vec<String> {
    DEFAULT_TRACKERS
        .iter()
        .map(|tracker| format!("tracker:{tracker}"))
        .chain(std::iter::once(format!("dht:{info_hash}")))
        .collect()
}

fn official_sources(source_urls: &[String], discovered_peers: usize) -> Value {
    let last_started = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let mut remaining = discovered_peers;
    let tracker_count = source_urls.len().saturating_sub(1).max(1);

    Value::Array(
        source_urls
            .iter()
            .enumerate()
            .map(|(_idx, url)| {
                let is_dht = url.starts_with("dht:");
                let found = if is_dht {
                    remaining
                } else {
                    let share = remaining.div_ceil(tracker_count);
                    remaining = remaining.saturating_sub(share);
                    share
                };

                json!({
                    "numFound": found,
                    "numFoundUniq": found,
                    "numRequests": if is_dht { 0 } else { 1 },
                    "url": url,
                    "lastStarted": last_started
                })
            })
            .collect(),
    )
}

fn official_stats_opts(source_urls: &[String], info_hash: &str, cache_dir: &Path) -> Value {
    let path = cache_dir.join(info_hash).to_string_lossy().to_string();
    json!({
        "peerSearch": {
            "min": 40,
            "max": 150,
            "sources": source_urls
        },
        "dht": false,
        "tracker": false,
        "connections": 400,
        "handshakeTimeout": 25000,
        "timeout": 6000,
        "virtual": true,
        "swarmCap": {
            "minPeers": 10,
            "maxSpeed": 8388608u64
        },
        "growler": {
            "flood": 0,
            "pulse": 78643200u64
        },
        "path": path
    })
}

fn files_for_handle(handle: &Arc<librqbit::ManagedTorrent>) -> Vec<EngineFile> {
    handle
        .with_metadata(|metadata| {
            metadata
                .file_infos
                .iter()
                .map(|file| {
                    let path = file.relative_filename.to_string_lossy().replace('\\', "/");
                    let name = file
                        .relative_filename
                        .file_name()
                        .map(|name| name.to_string_lossy().into_owned())
                        .unwrap_or_else(|| path.clone());
                    EngineFile {
                        path,
                        name,
                        length: file.len,
                        offset: file.offset_in_torrent,
                        cache_events: true,
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

fn file_for_handle(
    handle: &Arc<librqbit::ManagedTorrent>,
    idx: usize,
) -> anyhow::Result<Option<EngineFile>> {
    Ok(files_for_handle(handle).get(idx).cloned())
}

fn guess_index_for_handle(
    handle: &Arc<librqbit::ManagedTorrent>,
    filters: &[String],
    guess: Option<&Value>,
    explicit_idx: Option<isize>,
) -> Option<usize> {
    if let Some(idx) = explicit_idx.and_then(valid_idx) {
        return Some(idx);
    }
    let files = files_for_handle(handle);
    guess_index(&files, filters, guess)
}

fn resolve_file_index(
    handle: &Arc<librqbit::ManagedTorrent>,
    idx: &str,
    filters: &[String],
) -> anyhow::Result<usize> {
    if let Ok(value) = idx.parse::<isize>() {
        if let Some(idx) = valid_idx(value) {
            return Ok(idx);
        }
    }

    let files = files_for_handle(handle);
    if let Some(idx) = guess_index(&files, filters, Some(&Value::String(idx.to_string()))) {
        return Ok(idx);
    }

    guess_index(&files, filters, None).context("could not resolve torrent file index")
}

fn valid_idx(idx: isize) -> Option<usize> {
    if idx >= 0 { Some(idx as usize) } else { None }
}

fn guess_index(files: &[EngineFile], filters: &[String], guess: Option<&Value>) -> Option<usize> {
    if !filters.is_empty() {
        if let Some(idx) = files.iter().position(|file| {
            filters
                .iter()
                .all(|filter| filter_matches_file(filter, &file.path, &file.name))
        }) {
            return Some(idx);
        }
    }

    if let Some(guess) = guess.and_then(value_to_guess_string) {
        let needle = guess.to_ascii_lowercase();
        if let Some(idx) = files.iter().position(|file| {
            file.path.to_ascii_lowercase().contains(&needle)
                || file.name.to_ascii_lowercase().contains(&needle)
        }) {
            return Some(idx);
        }
    }

    files
        .iter()
        .enumerate()
        .filter(|(_, file)| is_video_like(&file.name))
        .max_by_key(|(_, file)| file.length)
        .map(|(idx, _)| idx)
        .or_else(|| {
            files
                .iter()
                .enumerate()
                .max_by_key(|(_, file)| file.length)
                .map(|(idx, _)| idx)
        })
}

fn value_to_guess_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) if !s.trim().is_empty() => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn filter_matches_file(filter: &str, path: &str, name: &str) -> bool {
    let haystack = format!("{path}\n{name}");
    if let Some((pattern, flags)) = parse_regex_filter(filter) {
        let mut builder = RegexBuilder::new(pattern);
        builder.case_insensitive(flags.contains('i'));
        return builder
            .build()
            .map(|regex| regex.is_match(&haystack))
            .unwrap_or(false);
    }
    haystack
        .to_ascii_lowercase()
        .contains(&filter.to_ascii_lowercase())
}

fn parse_regex_filter(filter: &str) -> Option<(&str, &str)> {
    if !filter.starts_with('/') {
        return None;
    }
    let last = filter.rfind('/')?;
    if last == 0 {
        return None;
    }
    Some((&filter[1..last], &filter[last + 1..]))
}

fn is_video_like(name: &str) -> bool {
    let Some(ext) = Path::new(name).extension().and_then(|ext| ext.to_str()) else {
        return false;
    };
    matches!(
        ext.to_ascii_lowercase().as_str(),
        "mkv" | "mp4" | "avi" | "mov" | "m4v" | "webm" | "ts" | "m2ts" | "wmv"
    )
}

fn display_name_from_filename(name: &str) -> String {
    let stem = Path::new(name)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or(name);
    stem.replace(['.', '_'], " ")
}

fn parse_json_body<T>(body: &Bytes) -> AppResult<T>
where
    T: Default + for<'de> Deserialize<'de>,
{
    if body.is_empty() {
        return Ok(T::default());
    }
    serde_json::from_slice(body)
        .or_else(|_| serde_json::from_slice::<T>(b"{}"))
        .context("parsing JSON body")
        .map_err(AppError::from)
}

async fn read_torrent_source(state: &AppState, from: &str) -> AppResult<Bytes> {
    if from.starts_with("http://") || from.starts_with("https://") {
        return state
            .client
            .get(from)
            .send()
            .await
            .context("fetching torrent file")?
            .error_for_status()
            .context("torrent file HTTP status")?
            .bytes()
            .await
            .context("reading torrent file HTTP body")
            .map_err(AppError::from);
    }
    tokio::fs::read(from)
        .await
        .with_context(|| format!("reading torrent file {from}"))
        .map(Bytes::from)
        .map_err(AppError::from)
}

fn decode_hex(input: &str) -> AppResult<Bytes> {
    let mut normalized = input.trim();
    if let Some(stripped) = normalized.strip_prefix("0x") {
        normalized = stripped;
    }
    let mut out = Vec::with_capacity(normalized.len() / 2);
    let mut chars = normalized.as_bytes().chunks_exact(2);
    if !chars.remainder().is_empty() {
        return Err(anyhow!("hex blob has odd length").into());
    }
    for chunk in &mut chars {
        let text = std::str::from_utf8(chunk).context("hex blob is not utf8")?;
        out.push(u8::from_str_radix(text, 16).context("hex blob contains invalid digits")?);
    }
    Ok(Bytes::from(out))
}

fn normalize_info_hash(input: &str) -> anyhow::Result<String> {
    let trimmed = input.trim();
    if trimmed.len() == 40 && trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
        return Ok(trimmed.to_ascii_lowercase());
    }
    let magnet = Magnet::parse(trimmed)?;
    magnet
        .as_id20()
        .map(|id| id.as_string())
        .context("magnet did not contain a v1 BTIH hash")
}

fn merge_trackers<I>(extra: I) -> Vec<String>
where
    I: IntoIterator<Item = String>,
{
    let mut seen = HashSet::new();
    let mut trackers = Vec::new();
    for tracker in DEFAULT_TRACKERS
        .iter()
        .map(|tracker| tracker.to_string())
        .chain(extra)
    {
        let trimmed = tracker.trim();
        if trimmed.is_empty() || !seen.insert(trimmed.to_ascii_lowercase()) {
            continue;
        }
        trackers.push(trimmed.to_string());
    }
    trackers
}

fn parse_peer_addrs(values: &[String]) -> Vec<SocketAddr> {
    values
        .iter()
        .filter_map(|value| match value.parse::<SocketAddr>() {
            Ok(addr) => Some(addr),
            Err(err) => {
                warn!(peer = value, error = %err, "ignoring invalid initial peer");
                None
            }
        })
        .collect()
}

fn parse_stream_query(raw_query: Option<&str>) -> StreamQuery {
    StreamQuery {
        external: query_flag(raw_query, "external"),
        download: query_flag(raw_query, "download"),
        subtitles_sec: query_first(raw_query, "subtitles"),
        trackers: query_values(raw_query, "tr"),
        filters: query_values(raw_query, "f"),
    }
}

fn query_pairs(raw_query: Option<&str>) -> Vec<(String, String)> {
    raw_query
        .map(|raw| {
            form_urlencoded::parse(raw.as_bytes())
                .map(|(key, value)| (key.into_owned(), value.into_owned()))
                .collect()
        })
        .unwrap_or_default()
}

fn query_values(raw_query: Option<&str>, key: &str) -> Vec<String> {
    query_pairs(raw_query)
        .into_iter()
        .filter_map(|(k, v)| if k == key { Some(v) } else { None })
        .collect()
}

fn query_first(raw_query: Option<&str>, key: &str) -> Option<String> {
    query_values(raw_query, key).into_iter().next()
}

fn query_flag(raw_query: Option<&str>, key: &str) -> bool {
    query_pairs(raw_query).into_iter().any(|(k, v)| {
        k == key && (v.is_empty() || !matches!(v.as_str(), "0" | "false" | "False" | "FALSE"))
    })
}

fn parse_range(header: &str, len: u64) -> Option<(u64, u64)> {
    let range = header.strip_prefix("bytes=")?.split(',').next()?.trim();
    let (start, end) = range.split_once('-')?;

    if start.is_empty() {
        let suffix = end.parse::<u64>().ok()?;
        if suffix == 0 {
            return None;
        }
        let start = len.saturating_sub(suffix);
        return Some((start, len.saturating_sub(1)));
    }

    let start = start.parse::<u64>().ok()?;
    let end = if end.is_empty() {
        len.saturating_sub(1)
    } else {
        end.parse::<u64>().ok()?.min(len.saturating_sub(1))
    };

    if len == 0 || start > end || start >= len {
        return None;
    }
    Some((start, end))
}

fn mib_per_sec_to_bytes_per_sec(speed: f64) -> f64 {
    if speed.is_finite() && speed > 0.0 {
        speed * 1024.0 * 1024.0
    } else {
        0.0
    }
}

fn header_value(value: &str) -> anyhow::Result<HeaderValue> {
    HeaderValue::from_str(value).with_context(|| format!("invalid header value {value:?}"))
}

fn redirect(status: StatusCode, location: &str) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    if let Ok(location) = HeaderValue::from_str(location) {
        response.headers_mut().insert(LOCATION, location);
    }
    response
}
