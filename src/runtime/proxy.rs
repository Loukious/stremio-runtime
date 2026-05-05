#[derive(Debug, Clone)]
struct ProxyOpts {
    destination_origin: String,
    destination_base: url::Url,
    request_headers: Vec<String>,
    response_headers: Vec<String>,
}

fn proxy_opts_parse(raw: &str) -> anyhow::Result<ProxyOpts> {
    let mut destination = None;
    let mut request_headers = Vec::new();
    let mut response_headers = Vec::new();

    for (key, value) in form_urlencoded::parse(raw.as_bytes()) {
        match key.as_ref() {
            "d" => destination = Some(value.into_owned()),
            "h" => request_headers.push(value.into_owned()),
            "r" => response_headers.push(value.into_owned()),
            _ => {}
        }
    }

    let destination = destination.context("missing d")?;
    let destination_url = url::Url::parse(&destination).context("parsing d")?;
    let destination_origin = url_origin(&destination_url)?;
    let destination_base = url::Url::parse(&destination_origin)
        .expect("origin URL should always parse")
        .to_owned();

    Ok(ProxyOpts {
        destination_origin,
        destination_base,
        request_headers,
        response_headers,
    })
}

fn proxy_opts_encode_full(opts: &ProxyOpts) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("d", &opts.destination_origin);
    for header in &opts.request_headers {
        serializer.append_pair("h", header);
    }
    for header in &opts.response_headers {
        serializer.append_pair("r", header);
    }
    serializer.finish()
}

fn proxy_opts_encode_nested(destination_origin: &str, request_headers: &[String]) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("d", destination_origin);
    for header in request_headers {
        serializer.append_pair("h", header);
    }
    serializer.finish()
}

fn url_origin(url: &url::Url) -> anyhow::Result<String> {
    let Some(host) = url.host_str() else {
        anyhow::bail!("URL missing host")
    };
    let mut origin = format!("{}://{}", url.scheme(), host);
    if let Some(port) = url.port() {
        origin.push(':');
        origin.push_str(&port.to_string());
    }
    Ok(origin)
}

fn host_header_value(url: &url::Url) -> anyhow::Result<HeaderValue> {
    let Some(host) = url.host_str() else {
        anyhow::bail!("URL missing host")
    };
    let value = if let Some(port) = url.port() {
        format!("{host}:{port}")
    } else {
        host.to_string()
    };
    HeaderValue::from_str(&value).context("building Host header")
}

fn header_override_parse(raw: &str) -> Option<(http::header::HeaderName, HeaderValue)> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    let mut parts = raw.splitn(2, ':');
    let name = parts.next().unwrap_or("").trim();
    let value = parts.next().unwrap_or("").trim();
    if name.is_empty() {
        return None;
    }

    let name = http::header::HeaderName::from_bytes(name.as_bytes()).ok()?;
    let value = HeaderValue::from_str(value).ok()?;
    Some((name, value))
}

fn proxy_build_upstream_headers(
    request_headers: &HeaderMap,
    dest_url: &url::Url,
    overrides: &[String],
) -> anyhow::Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    for name in [
        ACCEPT,
        ACCEPT_ENCODING,
        ACCEPT_LANGUAGE,
        CONNECTION,
        TRANSFER_ENCODING,
        RANGE,
        IF_RANGE,
        USER_AGENT,
    ] {
        if let Some(value) = request_headers.get(&name) {
            headers.insert(name, value.clone());
        }
    }

    headers.insert(HOST, host_header_value(dest_url)?);

    for raw in overrides {
        if let Some((name, value)) = header_override_parse(raw) {
            headers.insert(name, value);
        }
    }
    Ok(headers)
}

fn proxy_build_downstream_headers(upstream: &HeaderMap, overrides: &[String]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for name in [
        ACCEPT_RANGES,
        CONTENT_TYPE,
        CONTENT_LENGTH,
        CONTENT_RANGE,
        CONNECTION,
        TRANSFER_ENCODING,
        LAST_MODIFIED,
        ETAG,
        SERVER,
        DATE,
    ] {
        if let Some(value) = upstream.get(&name) {
            headers.insert(name, value.clone());
        }
    }

    for raw in overrides {
        if let Some((name, value)) = header_override_parse(raw) {
            headers.insert(name, value);
        }
    }

    headers
}

fn proxy_is_playlist(pathname: &str, response_headers: &HeaderMap) -> bool {
    let ext_is_playlist = Path::new(pathname)
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| matches!(ext.to_ascii_lowercase().as_str(), "m3u" | "m3u8"));

    let content_type_is_playlist = response_headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("mpegurl"));

    ext_is_playlist || content_type_is_playlist
}

fn url_join(segments: &[&str]) -> String {
    let joined = segments.join("/");
    let mut out = String::with_capacity(joined.len());
    let mut prev_slash = false;
    for ch in joined.chars() {
        if ch == '/' {
            if prev_slash {
                continue;
            }
            prev_slash = true;
        } else {
            prev_slash = false;
        }
        out.push(ch);
    }
    out
}

fn proxy_rewrite_playlist(
    body: &str,
    virtual_root: &str,
    dest_origin: &url::Url,
    request_header_overrides: &[String],
) -> String {
    static URI_RE: OnceLock<Regex> = OnceLock::new();
    let uri_re = URI_RE.get_or_init(|| Regex::new(r#"URI=\"([^\"]+)\""#).expect("regex"));

    fn parse_url(
        line: &str,
        virtual_root: &str,
        dest_origin: &url::Url,
        request_header_overrides: &[String],
    ) -> String {
        if line.starts_with("http://") || line.starts_with("https://") {
            if let Ok(line_url) = url::Url::parse(line) {
                let same_origin = line_url.scheme() == dest_origin.scheme()
                    && line_url.host_str() == dest_origin.host_str()
                    && line_url.port() == dest_origin.port();

                if same_origin {
                    let mut out = url_join(&[virtual_root, line_url.path()]);
                    if let Some(query) = line_url.query() {
                        out.push('?');
                        out.push_str(query);
                    }
                    return out;
                }

                if let Ok(line_origin) = url_origin(&line_url) {
                    let opts = proxy_opts_encode_nested(&line_origin, request_header_overrides);
                    let mut out = format!("/proxy/{opts}{}", line_url.path());
                    if let Some(query) = line_url.query() {
                        out.push('?');
                        out.push_str(query);
                    }
                    return out;
                }
            }
        }

        if line.starts_with('/') {
            return url_join(&[virtual_root, line]);
        }

        line.to_string()
    }

    fn parse_line(
        line: &str,
        virtual_root: &str,
        dest_origin: &url::Url,
        request_header_overrides: &[String],
        uri_re: &Regex,
    ) -> String {
        if !line.starts_with('#') && !line.is_empty() {
            return parse_url(line, virtual_root, dest_origin, request_header_overrides);
        }

        if let Some(caps) = uri_re.captures(line) {
            if let Some(m) = caps.get(1) {
                let uri = m.as_str();
                let rewritten = parse_url(uri, virtual_root, dest_origin, request_header_overrides);
                return line.replacen(uri, &rewritten, 1);
            }
        }

        line.to_string()
    }

    let eol = if body.contains("\r\n") {
        "\r\n"
    } else if body.contains('\n') {
        "\n"
    } else if body.contains('\r') {
        "\r"
    } else {
        "\n"
    };

    let mut out = String::with_capacity(body.len().saturating_add(64));
    let mut iter = body.split(eol).peekable();
    while let Some(line) = iter.next() {
        out.push_str(&parse_line(
            line,
            virtual_root,
            dest_origin,
            request_header_overrides,
            uri_re,
        ));
        if iter.peek().is_some() {
            out.push_str(eol);
        }
    }
    out
}

async fn proxy_root(
    State(state): State<AppState>,
    AxumPath(opts): AxumPath<String>,
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    proxy_impl(state, opts, String::new(), method, headers, raw_query).await
}

async fn proxy_path(
    State(state): State<AppState>,
    AxumPath((opts, pathname)): AxumPath<(String, String)>,
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    proxy_impl(state, opts, pathname, method, headers, raw_query).await
}

async fn proxy_impl(
    state: AppState,
    opts_raw: String,
    pathname: String,
    method: Method,
    request_headers: HeaderMap,
    raw_query: Option<String>,
) -> Response {
    let opts = match proxy_opts_parse(&opts_raw) {
        Ok(opts) => opts,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                [(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                )],
                format!("Invalid proxy opts: {err}"),
            )
                .into_response();
        }
    };

    let pathname = pathname.trim_start_matches('/').to_string();
    let mut dest = opts.destination_base.clone();
    if !pathname.is_empty() {
        dest.set_path(&format!("/{pathname}"));
    }
    if let Some(query) = raw_query.as_deref().filter(|q| !q.is_empty()) {
        dest.set_query(Some(query));
    }

    let mut upstream_headers =
        match proxy_build_upstream_headers(&request_headers, &dest, &opts.request_headers) {
            Ok(headers) => headers,
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    [(
                        CONTENT_TYPE,
                        HeaderValue::from_static("text/plain; charset=utf-8"),
                    )],
                    format!("Invalid proxy request: {err}"),
                )
                    .into_response();
            }
        };

    let mut redirect_count = 0usize;
    let mut current = dest.clone();
    let response = loop {
        let result = state
            .proxy_client
            .request(method.clone(), current.clone())
            .headers(upstream_headers.clone())
            .send()
            .await;

        let result = match result {
            Ok(result) => result,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    [(
                        CONTENT_TYPE,
                        HeaderValue::from_static("text/plain; charset=utf-8"),
                    )],
                    format!("Proxy upstream request failed: {err}"),
                )
                    .into_response();
            }
        };

        let is_redirect =
            result.status().is_redirection() && result.headers().get(LOCATION).is_some();
        if !is_redirect {
            break result;
        }

        if redirect_count >= 5 {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                )],
                "Proxy upstream request failed: Too many redirects".to_string(),
            )
                .into_response();
        }

        let Some(location) = result
            .headers()
            .get(LOCATION)
            .and_then(|value| value.to_str().ok())
        else {
            break result;
        };

        let base_origin = match url_origin(&current) {
            Ok(origin) => origin,
            Err(_) => opts.destination_origin.clone(),
        };

        let base = match url::Url::parse(&base_origin) {
            Ok(base) => base,
            Err(_) => opts.destination_base.clone(),
        };

        let next = match base.join(location) {
            Ok(url) => url,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    [(
                        CONTENT_TYPE,
                        HeaderValue::from_static("text/plain; charset=utf-8"),
                    )],
                    format!("Proxy upstream redirect invalid: {err}"),
                )
                    .into_response();
            }
        };

        current = next;
        if let Ok(host) = host_header_value(&current) {
            upstream_headers.insert(HOST, host);
        }
        for raw in &opts.request_headers {
            if let Some((name, value)) = header_override_parse(raw) {
                upstream_headers.insert(name, value);
            }
        }
        redirect_count += 1;
    };

    let mut response_headers =
        proxy_build_downstream_headers(response.headers(), &opts.response_headers);
    let is_playlist = proxy_is_playlist(&pathname, &response_headers);
    if is_playlist {
        response_headers.remove(CONTENT_LENGTH);
        response_headers.insert(ACCEPT_RANGES, HeaderValue::from_static("none"));

        let transfer_value = response_headers
            .get(TRANSFER_ENCODING)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();
        if !transfer_value.to_ascii_lowercase().contains("chunked") {
            if transfer_value.is_empty() {
                response_headers.insert(TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
            } else {
                let merged = format!("{transfer_value}, chunked");
                if let Ok(value) = HeaderValue::from_str(&merged) {
                    response_headers.insert(TRANSFER_ENCODING, value);
                }
            }
        }
    }

    let status = response.status();
    if is_playlist {
        let bytes = match response.bytes().await {
            Ok(bytes) => bytes,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    [(
                        CONTENT_TYPE,
                        HeaderValue::from_static("text/plain; charset=utf-8"),
                    )],
                    format!("Proxy upstream read failed: {err}"),
                )
                    .into_response();
            }
        };

        let opts_encoded_full = proxy_opts_encode_full(&opts);
        let virtual_root = format!("/proxy/{opts_encoded_full}");
        let rewritten = proxy_rewrite_playlist(
            &String::from_utf8_lossy(&bytes),
            &virtual_root,
            &opts.destination_base,
            &opts.request_headers,
        );

        let rewritten_bytes = Bytes::from(rewritten.into_bytes());
        let body = Body::from_stream(futures_util::stream::once(async move {
            Ok::<Bytes, std::io::Error>(rewritten_bytes)
        }));

        let mut resp = Response::builder()
            .status(status)
            .body(body)
            .expect("response builder failed");
        *resp.headers_mut() = response_headers;
        resp
    } else {
        let stream = response
            .bytes_stream()
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err));
        let body = Body::from_stream(stream);

        let mut resp = Response::builder()
            .status(status)
            .body(body)
            .expect("response builder failed");
        *resp.headers_mut() = response_headers;
        resp
    }
}
