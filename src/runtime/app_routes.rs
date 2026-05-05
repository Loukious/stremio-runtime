async fn root(State(state): State<AppState>) -> Response {
    let base_url = state.base_url.read().await.clone();
    let app_url = format!(
        "https://app.strem.io/shell-v4.4/#?streamingServer={}",
        utf8_percent_encode(&base_url, PATH_SEGMENT_ENCODE_SET)
    );
    redirect(StatusCode::TEMPORARY_REDIRECT, &app_url)
}

async fn favicon() -> Response {
    (StatusCode::NOT_FOUND, Json(json!({}))).into_response()
}

async fn heartbeat() -> Json<Value> {
    Json(json!({ "success": true }))
}

async fn get_settings(State(state): State<AppState>) -> Json<Value> {
    let interfaces = discover_ipv4_interfaces();
    let values = Value::Object(state.settings.values.read().await.clone());
    Json(json!({
        "options": [
            {
                "id": "localAddonEnabled",
                "label": "ENABLE_LOCAL_FILES_ADDON",
                "type": "checkbox"
            },
            {
                "id": "remoteHttps",
                "label": "ENABLE_REMOTE_HTTPS_CONN",
                "type": "select",
                "class": "https",
                "icon": true,
                "selections": std::iter::once(json!({"name": "Disabled", "val": ""}))
                    .chain(interfaces.iter().map(|ip| json!({"name": ip, "val": ip})))
                    .collect::<Vec<Value>>()
            },
            {
                "id": "cacheSize",
                "label": "CACHING",
                "type": "select",
                "class": "caching",
                "icon": true,
                "selections": [
                    {"name": "no caching", "val": 0},
                    {"name": "2GB", "val": 2147483648u64},
                    {"name": "5GB", "val": 5368709120u64},
                    {"name": "10GB", "val": 10737418240u64},
                    {"name": "∞", "val": Value::Null}
                ]
            },
            {
                "id": "cacheRoot",
                "label": "SETTINGS_CACHING_DRIVE",
                "type": "select",
                "class": "caching",
                "selections": [
                    {"val": "C:\\", "name": "C:"},
                    {"val": "D:\\", "name": "D:"},
                    {"val": "E:\\", "name": "E:"}
                ]
            }
        ],
        "values": values,
        "baseUrl": state.base_url.read().await.clone()
    }))
}

async fn post_settings(State(state): State<AppState>, body: Bytes) -> AppResult<Json<Value>> {
    let patch = parse_json_body::<Map<String, Value>>(&body)?;
    let app_path = {
        let values = state.settings.values.read().await;
        values
            .get("appPath")
            .and_then(|v| v.as_str())
            .map(PathBuf::from)
            .unwrap_or_else(server_app_path)
    };
    state.settings.update(patch, &app_path).await;
    {
        let torrents = state.torrents.clone();
        let settings = state.settings.clone();
        tokio::spawn(async move {
            torrents.cleanup_cache_to_limit(&settings).await;
        });
    }
    Ok(Json(json!({ "success": true })))
}

async fn network_info() -> Json<Value> {
    Json(json!({ "availableInterfaces": discover_ipv4_interfaces() }))
}

async fn casting() -> Json<Value> {
    Json(json!([
        {
            "name": "VLC",
            "type": "external",
            "id": "vlc",
            "usePlayerUI": true
        }
    ]))
}

async fn device_info(State(state): State<AppState>) -> Json<Value> {
    let values = state.settings.values.read().await;
    let profiles = values
        .get("allTranscodeProfiles")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let accel: Value = if profiles.is_empty() {
        json!(false)
    } else {
        json!(profiles)
    };
    Json(json!({ "availableHardwareAccelerations": accel }))
}

async fn local_addon_manifest(State(state): State<AppState>) -> Json<Value> {
    let catalog_enabled = state
        .settings
        .values
        .read()
        .await
        .get("localAddonEnabled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Json(local_addon_manifest_payload(catalog_enabled))
}

fn local_addon_manifest_payload(catalog_enabled: bool) -> Value {
    let catalogs = if catalog_enabled {
        json!([
            {
                "type": "other",
                "id": "local"
            }
        ])
    } else {
        json!([])
    };
    let resources = if catalog_enabled {
        json!([
            "catalog",
            {
                "name": "meta",
                "types": ["other"],
                "idPrefixes": ["local:", "bt:"]
            },
            {
                "name": "stream",
                "types": ["movie", "series"],
                "idPrefixes": ["tt"]
            }
        ])
    } else {
        json!([
            {
                "name": "meta",
                "types": ["other"],
                "idPrefixes": ["local:", "bt:"]
            },
            {
                "name": "stream",
                "types": ["movie", "series"],
                "idPrefixes": ["tt"]
            }
        ])
    };
    let name = if catalog_enabled {
        "Local Files"
    } else {
        "Local Files (without catalog support)"
    };
    json!({
        "id": "org.stremio.local",
        "version": env!("CARGO_PKG_VERSION"),
        "name": name,
        "description": "Local add-on to find playable files: .torrent, .mp4, .mkv and .avi",
        "resources": resources,
        "types": ["movie", "series", "other"],
        "catalogs": catalogs
    })
}

async fn local_addon_dispatch(
    State(state): State<AppState>,
    AxumPath(rest): AxumPath<String>,
) -> AppResult<Json<Value>> {
    let rest = rest.trim_start_matches('/');
    let mut parts = rest.split('/').collect::<Vec<_>>();

    if parts.len() < 3 {
        return Ok(Json(json!({ "err": "handler error" })));
    }

    let resource = parts.remove(0);
    let media_type = parts.remove(0);
    let id = parts.remove(0);
    let id = id.strip_suffix(".json").unwrap_or(id);
    let decoded_id = id.replace("%3A", ":").replace("%3a", ":");

    if resource == "meta" && media_type == "other" && decoded_id.starts_with("bt:") {
        return local_addon_bt_meta(&state, media_type, &decoded_id)
            .await
            .map(Json);
    }
    if resource == "catalog" && media_type == "other" && decoded_id == "local" {
        return Ok(Json(local_addon_catalog(&state).await));
    }
    if resource == "meta" && media_type == "other" && decoded_id.starts_with("local:") {
        return Ok(Json(
            local_addon_local_meta(&state, media_type, &decoded_id).await,
        ));
    }
    if resource == "stream" {
        return Ok(Json(
            local_addon_stream(&state, media_type, &decoded_id).await,
        ));
    }

    Ok(Json(local_addon_resource_payload(
        resource,
        media_type,
        &decoded_id,
    )))
}

async fn hwaccel_profiler(State(state): State<AppState>) -> Json<Value> {
    let values = state.settings.values.read().await;
    let profiles = values
        .get("allTranscodeProfiles")
        .cloned()
        .unwrap_or_else(|| json!([]));
    Json(profiles)
}

async fn get_https() -> (StatusCode, &'static str) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "Cannot get valid certificate",
    )
}
