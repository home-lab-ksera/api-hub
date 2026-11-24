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
            s3_endpoint: endpoint,
            ..Settings::for_test("http://127.0.0.1:1")
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

/// URL の正規化で署名と別のオブジェクトを指す要求は、送らずにエラーにする。
#[tokio::test]
async fn normalized_signed_path_is_not_sent() {
    // 届けば 200 を返す宛先。エラーが送信前の確認によるものだと分かるようにする。
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/object/../other", listener.local_addr().unwrap());
    let client = http_client::new();
    let send = http_client::request(&client, "GET".into(), url, vec![], vec![], false).send();
    assert!(send.await.is_err());
    assert!(
        timeout(Duration::from_millis(150), listener.accept())
            .await
            .is_err(),
        "the request reached the server"
    );
}

/// JSON API の本文は 1MiB まで。実際の TCP 接続で、上限を超えた本文を 413 で断る。
#[tokio::test]
async fn json_body_over_limit_is_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = oneshot::channel();
    let server = tokio::spawn(server::serve(listener, state(None), async {
        let _ = stopped.await;
    }));
    let response = http_client::new()
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

#[tokio::test]
async fn unavailable_database_keeps_integrated_http_running() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(3)
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy("postgres://fixture@127.0.0.1:1/unavailable")
        .unwrap();
    let mut app = state(None);
    app.database = Some(pool.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = oneshot::channel();
    let process = tokio::spawn(server::serve_integrated(listener, app, async {
        let _ = stopped.await;
    }));
    let client = http_client::new();
    assert_eq!(
        client
            .get(format!("{base}/metrics"))
            .send()
            .await
            .unwrap()
            .status(),
        503
    );
    assert_eq!(
        client
            .get(format!("{base}/health"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert!(!process.is_finished());
    stop.send(()).unwrap();
    timeout(Duration::from_secs(3), process)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(pool.is_closed());
}
