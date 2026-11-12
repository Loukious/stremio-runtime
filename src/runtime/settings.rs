impl SettingsStore {
    async fn load(&self) {
        let bytes = match tokio::fs::read(&self.path).await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return,
            Err(err) => {
                warn!(path = %self.path.display(), error = %err, "reading settings failed");
                return;
            }
        };

        let loaded = match serde_json::from_slice::<Value>(&bytes) {
            Ok(value) => value,
            Err(err) => {
                warn!(path = %self.path.display(), error = %err, "parsing settings failed");
                return;
            }
        };

        let Some(map) = loaded.as_object() else {
            warn!(path = %self.path.display(), "settings file was not a JSON object");
            return;
        };

        let mut values = self.values.write().await;
        for (k, v) in map {
            values.insert(k.clone(), v.clone());
        }
    }

    async fn save(&self) -> anyhow::Result<()> {
        // Exclude updates while the snapshot is written so saves cannot overwrite a newer value.
        let values = self.values.write().await;
        self.save_values(&values).await
    }

    async fn save_values(&self, values: &Map<String, Value>) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating settings dir {}", parent.display()))?;
        }
        let text = serde_json::to_string_pretty(&Value::Object(values.clone()))
            .context("serializing settings")?;
        static NEXT_SAVE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let temporary = self.path.with_extension(format!(
            "{}.{}.tmp",
            std::process::id(),
            NEXT_SAVE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let result = async {
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .await?;
            file.write_all(text.as_bytes()).await?;
            file.sync_all().await?;
            drop(file);
            tokio::fs::rename(&temporary, &self.path).await
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        }
        result.with_context(|| format!("writing settings {}", self.path.display()))
    }

    async fn update(
        &self,
        mut patch: Map<String, Value>,
        app_path: &Path,
    ) -> Result<(), (StatusCode, anyhow::Error)> {
        let mut values = self.values.write().await;
        let mut next = values.clone();
        if let Some(root) = patch.get("cacheRoot") {
            let root = root
                .as_str()
                .context("The cache location must be an absolute folder path.")
                .map_err(|err| (StatusCode::BAD_REQUEST, err))?;
            let root = validate_cache_root(Path::new(root))
                .await
                .map_err(|err| (StatusCode::BAD_REQUEST, err))?;
            let previous = values
                .get("cacheRoot")
                .and_then(Value::as_str)
                .unwrap_or("");
            if Path::new(previous) != root {
                let mut old_roots = values
                    .get("cacheRootsToClean")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                if !previous.is_empty() && !old_roots.iter().any(|v| v.as_str() == Some(previous)) {
                    old_roots.push(json!(previous));
                }
                old_roots.retain(|v| v.as_str().is_some_and(|v| Path::new(v) != root));
                next.insert("cacheRootsToClean".into(), json!(old_roots));
            }
            patch.insert("cacheRoot".into(), json!(root.to_string_lossy()));
        }
        for (k, v) in patch {
            // Old roots are tracked by the runtime, rather than accepted from a settings POST.
            if k != "cacheRootsToClean" {
                next.insert(k, v);
            }
        }
        normalize_settings_values(&mut next, app_path);
        self.save_values(&next)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err))?;
        *values = next;
        Ok(())
    }

    async fn cache_size_limit(&self) -> Option<u64> {
        let values = self.values.read().await;
        cache_size_limit_from_settings(&values)
    }
}

async fn validate_cache_root(root: &Path) -> anyhow::Result<PathBuf> {
    anyhow::ensure!(
        root.is_absolute(),
        "The cache location must be an absolute folder path."
    );
    let root = tokio::fs::canonicalize(root)
        .await
        .context("The cache location must be an existing folder.")?;
    anyhow::ensure!(
        tokio::fs::metadata(&root).await?.is_dir(),
        "The cache location must be a folder."
    );
    let cache = root.join("stremio-cache");
    match tokio::fs::create_dir(&cache).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(err) => return Err(err).context("The cache location is not writable."),
    }
    let metadata = tokio::fs::symlink_metadata(&cache).await?;
    anyhow::ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "The cache directory must be a regular folder."
    );
    static NEXT_PROBE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let probe = cache.join(format!(
        ".write-check-{}-{}",
        std::process::id(),
        NEXT_PROBE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
        .await
        .context("The cache location is not writable.")?;
    drop(file);
    tokio::fs::remove_file(probe).await?;
    Ok(root)
}
