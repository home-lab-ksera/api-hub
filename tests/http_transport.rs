use api_hub::{
    config::{settings::Settings, state::AppState},
    http_client, server,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::{Duration, timeout},
};

fn state(endpoint: Option<String>) -> AppState {
    AppState {
        database: None,
        client: http_client::new(),
        settings: Settings {
            slack_bot_token: "fixture".into(),
            slack_api_base_url: "http://127.0.0.1:1".into(),
            s3_access_key_id: "fixture".into(),
            s3_secret_access_key: "fixture".into(),
            s3_region: "us-east-1".into(),
            s3_endpoint: endpoint,
            s3_browser_endpoint: None,
            s3_use_path_style: true,
            s3_session_token: None,
        },
    }
}
async fn head(socket: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        bytes.push(socket.read_u8().await.unwrap());
    }
    String::from_utf8(bytes).unwrap()
}

#[tokio::test]
async fn preserves_signed_target_redirect_and_encoded_body_without_replay() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/bucket/a%20%2B.bin?partNumber=1&uploadId=a%2Fb%2B",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = head(&mut socket).await;
        assert!(
            request
                .starts_with("GET /bucket/a%20%2B.bin?partNumber=1&uploadId=a%2Fb%2B HTTP/1.1\r\n")
        );
        assert!(
            request
                .to_lowercase()
                .contains("x-amz-content-sha256: fixture")
        );
        socket.write_all(b"HTTP/1.1 302 Found\r\nLocation: /other\r\nContent-Encoding: gzip\r\nContent-Length: 4\r\n\r\nraw!").await.unwrap();
        assert!(head(&mut socket).await.starts_with("POST "));
        drop(socket); // Slack等の副作用後に応答を失った場合、再送しない。
        assert!(
            timeout(Duration::from_millis(150), listener.accept())
                .await
                .is_err()
        );
    });
    let client = http_client::new();
    let response = http_client::request(
        &client,
        "GET".into(),
        url.clone(),
        vec![("x-amz-content-sha256".into(), "fixture".into())],
        vec![],
        false,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), 302);
    assert_eq!(response.headers()["content-encoding"], "gzip");
    assert_eq!(response.bytes().await.unwrap().as_ref(), b"raw!");
    assert!(
        http_client::request(&client, "POST".into(), url, vec![], vec![], false)
            .send()
            .await
            .is_err()
    );
    server.await.unwrap();
}

#[tokio::test]
async fn shares_client_pool_and_does_not_read_head_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/object", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        for _ in 0..3 {
            assert!(head(&mut socket).await.starts_with("HEAD "));
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\n\r\n")
                .await
                .unwrap();
        }
    });
    let client = http_client::new();
    timeout(Duration::from_secs(3), async {
        for _ in 0..3 {
            let response = client.clone().head(&url).send().await.unwrap();
            assert!(response.bytes().await.unwrap().is_empty());
        }
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn truncated_body_and_normalized_signed_path_are_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/object", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        head(&mut socket).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort")
            .await
            .unwrap();
    });
    let client = http_client::new();
    assert!(
        client
            .get(&url)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .is_err()
    );
    assert!(
        http_client::request(
            &client,
            "GET".into(),
            format!("{url}/../other"),
            vec![],
            vec![],
            false
        )
        .send()
        .await
        .is_err()
    );
    server.await.unwrap();
}

#[tokio::test]
async fn axum_preserves_api_errors_cors_request_id_and_limits_json() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = oneshot::channel();
    let server = tokio::spawn(server::serve(listener, state(None), async {
        let _ = stopped.await;
    }));
    let client = http_client::new();
    for (method, path, status) in [
        ("GET", "/health", 200),
        ("POST", "/health", 405),
        ("GET", "/missing", 404),
        ("POST", "/notifications", 503),
    ] {
        let response = client
            .request(method.parse().unwrap(), format!("{base}{path}"))
            .header("x-request-id", "fixture-id")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()["x-request-id"], "fixture-id");
        if status >= 400 {
            assert_eq!(
                response.headers()["content-type"],
                "application/problem+json"
            );
        }
        response.bytes().await.unwrap();
    }
    let response = client
        .request(
            reqwest::Method::OPTIONS,
            format!("{base}/s3/browser_get_object"),
        )
        .header(
            "origin",
            "https://hitomi-upload-viewer.internal.qroksera.com",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 204);
    assert_eq!(
        response.headers()["access-control-allow-origin"],
        "https://hitomi-upload-viewer.internal.qroksera.com"
    );
    let response = client
        .post(format!("{base}/notifications"))
        .body(vec![0; 1024 * 1024 + 1])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 413);
    stop.send(()).unwrap();
    timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn graceful_shutdown_stops_accepting_and_drains_preview() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", upstream.local_addr().unwrap());
    let (release, wait) = oneshot::channel();
    let source = tokio::spawn(async move {
        let (mut socket, _) = upstream.accept().await.unwrap();
        head(&mut socket).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nabc")
            .await
            .unwrap();
        wait.await.unwrap();
        socket.write_all(b"def").await.unwrap();
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let server = tokio::spawn(server::serve(listener, state(Some(endpoint)), async {
        let _ = stopped.await;
    }));
    let mut response = http_client::new()
        .get(format!("http://{address}/s3/preview/b/file.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.chunk().await.unwrap().unwrap().as_ref(), b"abc");
    stop.send(()).unwrap();
    timeout(Duration::from_secs(3), async {
        loop {
            if TcpStream::connect(address).await.is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!server.is_finished());
    release.send(()).unwrap();
    assert_eq!(response.bytes().await.unwrap().as_ref(), b"def");
    source.await.unwrap();
    timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn preview_preserves_unsatisfiable_range_status_headers_and_body() {
    let source = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", source.local_addr().unwrap());
    let fixture = tokio::spawn(async move {
        let (mut socket, _) = source.accept().await.unwrap();
        assert!(
            head(&mut socket)
                .await
                .to_lowercase()
                .contains("range: bytes=100-")
        );
        socket.write_all(b"HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 5\r\nContent-Type: text/plain\r\nContent-Range: bytes */6\r\n\r\nrange").await.unwrap();
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/s3/preview/b/file.bin",
        listener.local_addr().unwrap()
    );
    let (stop, stopped) = oneshot::channel();
    let api = tokio::spawn(server::serve(listener, state(Some(endpoint)), async {
        let _ = stopped.await;
    }));
    let response = http_client::new()
        .get(url)
        .header("range", "bytes=100-")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 416);
    assert_eq!(response.headers()["content-range"], "bytes */6");
    assert_eq!(
        response
            .headers()
            .get_all("access-control-allow-origin")
            .iter()
            .count(),
        1
    );
    assert_eq!(response.bytes().await.unwrap().as_ref(), b"range");
    stop.send(()).unwrap();
    fixture.await.unwrap();
    api.await.unwrap().unwrap();
}
