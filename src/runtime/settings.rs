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
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating settings dir {}", parent.display()))?;
        }
        let values = self.values.read().await;
        let text = serde_json::to_string_pretty(&Value::Object(values.clone()))
            .context("serializing settings")?;
        tokio::fs::write(&self.path, text)
            .await
            .with_context(|| format!("writing settings {}", self.path.display()))?;
        Ok(())
    }

    async fn update(&self, patch: Map<String, Value>, app_path: &Path) {
        {
            let mut values = self.values.write().await;
            for (k, v) in patch {
                values.insert(k, v);
            }
            normalize_settings_values(&mut values, app_path);
        }

        if let Err(err) = self.save().await {
            warn!(path = %self.path.display(), error = %err, "saving settings failed");
        }
    }

    async fn cache_size_limit(&self) -> Option<u64> {
        let values = self.values.read().await;
        cache_size_limit_from_settings(&values)
    }
}
