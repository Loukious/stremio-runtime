use std::future::Future;

use tokio::{net::TcpStream, sync::oneshot, task::JoinHandle};

use super::*;

async fn upstream_fixture<F, Fut>(serve: F) -> (url::Url, JoinHandle<anyhow::Result<()>>)
where
    F: FnOnce(TcpStream) -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<()>> + Send,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = url::Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        serve(socket).await
    });
    (url, task)
}

async fn read_request(socket: &mut TcpStream) -> anyhow::Result<String> {
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        socket.read_exact(&mut byte).await?;
        request.push(byte[0]);
        anyhow::ensure!(request.len() <= 8192, "oversized fixture request");
    }
    Ok(String::from_utf8(request)?)
}

async fn request_to_upstream(
    upstream: &url::Url,
    pathname: &str,
    method: Method,
    headers: HeaderMap,
    timeouts: ProxyTimeouts,
) -> Response {
    let mut opts = form_urlencoded::Serializer::new(String::new());
    opts.append_pair("d", upstream.as_str());
    proxy_response(
        &build_proxy_http_client().unwrap(),
        opts.finish(),
        pathname.to_string(),
        method,
        headers,
        None,
        timeouts,
    )
    .await
}

#[tokio::test]
async fn media_body_survives_the_former_total_request_deadline() -> anyhow::Result<()> {
    let (release_tx, release_rx) = oneshot::channel();
    let (upstream, server) = upstream_fixture(move |mut socket| async move {
        read_request(&mut socket).await?;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n01234")
            .await?;
        release_rx.await?;
        socket.write_all(b"56789").await?;
        socket.shutdown().await?;
        Ok(())
    })
    .await;
    let response = request_to_upstream(
        &upstream,
        "video.mkv",
        Method::GET,
        HeaderMap::new(),
        ProxyTimeouts::default(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[CONTENT_LENGTH], "10");
    let mut body = response.into_body().into_data_stream();
    let first = body.try_next().await?.expect("initial media bytes");
    assert_eq!(first.as_ref(), b"01234");

    // Establish the real HTTP connection before pausing time. Advancing beyond
    // the former production deadline makes the old total-timeout client fail
    // without waiting 90 seconds or relying on paused-time network scheduling.
    tokio::time::pause();
    tokio::time::advance(PROXY_RESPONSE_HEADERS_TIMEOUT + Duration::from_secs(1)).await;
    tokio::time::resume();
    release_tx.send(()).unwrap();
    let remaining: Vec<Bytes> = timeout(Duration::from_secs(3), body.try_collect()).await??;
    assert_eq!(remaining.concat(), b"56789");
    server.await??;
    Ok(())
}

#[tokio::test]
async fn truncated_advertised_media_body_remains_an_error() -> anyhow::Result<()> {
    let (upstream, server) = upstream_fixture(|mut socket| async move {
        read_request(&mut socket).await?;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 64\r\nConnection: close\r\n\r\nshort")
            .await?;
        socket.shutdown().await?;
        Ok(())
    })
    .await;
    let response = request_to_upstream(
        &upstream,
        "video.mkv",
        Method::GET,
        HeaderMap::new(),
        ProxyTimeouts::default(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[CONTENT_LENGTH], "64");
    let body = axum::body::to_bytes(response.into_body(), 1024).await;
    assert!(body.is_err(), "truncated media must not become a clean EOF");
    server.await??;
    Ok(())
}

#[tokio::test]
async fn waiting_for_upstream_headers_remains_bounded() -> anyhow::Result<()> {
    let (release_tx, release_rx) = oneshot::channel();
    let (upstream, server) = upstream_fixture(move |mut socket| async move {
        read_request(&mut socket).await?;
        release_rx.await?;
        socket.shutdown().await?;
        Ok(())
    })
    .await;
    let response = request_to_upstream(
        &upstream,
        "video.mkv",
        Method::GET,
        HeaderMap::new(),
        ProxyTimeouts {
            response_headers: Duration::from_millis(50),
            ..ProxyTimeouts::default()
        },
    )
    .await;
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    release_tx.send(()).unwrap();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn buffered_playlist_body_remains_bounded() -> anyhow::Result<()> {
    let (release_tx, release_rx) = oneshot::channel();
    let (upstream, server) = upstream_fixture(move |mut socket| async move {
        read_request(&mut socket).await?;
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/x-mpegurl\r\nContent-Length: 10\r\n\r\n#EXTM3U\n",
            )
            .await?;
        release_rx.await?;
        socket.write_all(b"x\n").await?;
        socket.shutdown().await?;
        Ok(())
    })
    .await;
    let response = request_to_upstream(
        &upstream,
        "video.m3u8",
        Method::GET,
        HeaderMap::new(),
        ProxyTimeouts {
            playlist_body: Duration::from_millis(50),
            ..ProxyTimeouts::default()
        },
    )
    .await;
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    release_tx.send(()).unwrap();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn media_range_offsets_above_four_gib_remain_exact() -> anyhow::Result<()> {
    let (upstream, server) = upstream_fixture(|mut socket| async move {
        let request = read_request(&mut socket).await?.to_ascii_lowercase();
        assert!(request.contains("range: bytes=4294967297-4294967306\r\n"));
        socket
            .write_all(
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 10\r\nContent-Range: bytes 4294967297-4294967306/9000000000\r\nAccept-Ranges: bytes\r\nContent-Encoding: x-fixture-opaque\r\n\r\n0123456789",
            )
            .await?;
        socket.shutdown().await?;
        Ok(())
    })
    .await;
    let mut headers = HeaderMap::new();
    headers.insert(
        RANGE,
        HeaderValue::from_static("bytes=4294967297-4294967306"),
    );
    let response = request_to_upstream(
        &upstream,
        "video.mkv",
        Method::GET,
        headers,
        ProxyTimeouts::default(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()[CONTENT_LENGTH], "10");
    assert_eq!(
        response.headers()[CONTENT_RANGE],
        "bytes 4294967297-4294967306/9000000000"
    );
    assert_eq!(response.headers()[ACCEPT_RANGES], "bytes");
    assert_eq!(
        response.headers()[http::header::CONTENT_ENCODING],
        "x-fixture-opaque"
    );
    let bytes = axum::body::to_bytes(response.into_body(), 1024).await?;
    assert_eq!(bytes.as_ref(), b"0123456789");
    server.await??;
    Ok(())
}

#[tokio::test]
async fn head_preserves_resource_length_without_expecting_media_bytes() -> anyhow::Result<()> {
    let (upstream, server) = upstream_fixture(|mut socket| async move {
        let request = read_request(&mut socket).await?;
        assert!(request.starts_with("HEAD /video.mkv HTTP/1.1\r\n"));
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 9000000000\r\n\r\n")
            .await?;
        socket.shutdown().await?;
        Ok(())
    })
    .await;
    let response = request_to_upstream(
        &upstream,
        "video.mkv",
        Method::HEAD,
        HeaderMap::new(),
        ProxyTimeouts::default(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[CONTENT_LENGTH], "9000000000");
    assert!(
        axum::body::to_bytes(response.into_body(), 1024)
            .await?
            .is_empty()
    );
    server.await??;
    Ok(())
}
