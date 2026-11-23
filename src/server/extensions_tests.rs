use super::process_request;
use crate::config::{settings::Settings, state::AppState};
use shiguredo_http11::{Request, RequestDecoder, Response};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
    time::{Duration, timeout},
};

async fn upstream(replies: Vec<(u16, &'static str)>) -> (AppState, JoinHandle<Vec<Request>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        timeout(Duration::from_secs(5), async move {
            let mut requests = Vec::new();
            for (status, body) in replies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut decoder = RequestDecoder::new();
                let request = loop {
                    if let Some(request) = decoder.decode().unwrap() { break request; }
                    let mut buffer = [0; 4096];
                    let n = socket.read(&mut buffer).await.unwrap();
                    assert_ne!(n, 0);
                    decoder.feed(&buffer[..n]).unwrap();
                };
                requests.push(request);
                let response = format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
            }
            requests
        }).await.unwrap()
    });
    let state = AppState {
        database: None,
        client: crate::http_client::new(),
        settings: Settings {
            slack_bot_token: "test-token".into(),
            slack_api_base_url: endpoint.clone(),
            s3_access_key_id: "test".into(),
            s3_secret_access_key: "test".into(),
            s3_region: "us-east-1".into(),
            s3_endpoint: Some(endpoint),
            s3_browser_endpoint: None,
            s3_use_path_style: true,
            s3_session_token: None,
        },
    };
    (state, task)
}

async fn post(state: &AppState, path: &str, body: &str) -> Response {
    timeout(
        Duration::from_secs(5),
        process_request(
            Request::new("POST", path).body(body.as_bytes().to_vec()),
            state,
        ),
    )
    .await
    .unwrap()
}

fn string_member(body: &[u8], name: &str) -> String {
    let json = nojson::RawJson::parse(std::str::from_utf8(body).unwrap()).unwrap();
    String::try_from(json.value().to_member(name).unwrap().required().unwrap()).unwrap()
}

#[tokio::test]
async fn browser_signing_is_scoped_and_uses_external_host() {
    let (mut state, task) = upstream(vec![]).await;
    state.settings.s3_browser_endpoint = Some("https://s3.internal.example".into());
    for (bucket, key) in [
        ("pdfs", "hitomi/20261024/001.pdf"),
        ("pdfs", "hitomi/20261024/001_prev.jpg"),
        ("pdfs", "videos/20261024/source/001.mp4"),
        (
            "home-camera-recordings",
            "camera/2026/10/30/1793314800-session.mp4",
        ),
    ] {
        let body = nojson::object(|f| {
            f.member("bucket", bucket)?;
            f.member("key", key)?;
            f.member("expires_in_secs", 604800)
        })
        .to_string();
        let response = post(&state, "/s3/browser_get_object", &body).await;
        assert_eq!(response.status_code, 200);
        assert_eq!(response.get_header("cache-control"), Some("no-store"));
        let url = url::Url::parse(&string_member(&response.body, "url")).unwrap();
        assert_eq!(url.host_str(), Some("s3.internal.example"));
        assert_eq!(
            url.query_pairs()
                .find(|(k, _)| k == "X-Amz-Expires")
                .unwrap()
                .1,
            "900"
        );
        assert_eq!(
            url.query_pairs()
                .find(|(k, _)| k == "X-Amz-SignedHeaders")
                .unwrap()
                .1,
            "host"
        );
    }
    for (bucket, key) in [
        ("home-camera-recordings", "camera/2026/10/30/manifest.json"),
        ("home-camera-recordings", "camera/2026/10/30/../secret.mp4"),
        ("private", "camera/2026/10/30/1793314800-session.mp4"),
        ("private", "hitomi/test.pdf"),
        ("pdfs", "backup/test.pdf"),
        ("pdfs", "hitomi/../private.pdf"),
        ("pdfs", "videos/a.txt"),
    ] {
        let body = nojson::object(|f| {
            f.member("bucket", bucket)?;
            f.member("key", key)
        })
        .to_string();
        assert_eq!(
            post(&state, "/s3/browser_get_object", &body)
                .await
                .status_code,
            400
        );
    }
    state.settings.s3_browser_endpoint = None;
    assert_eq!(
        post(
            &state,
            "/s3/browser_get_object",
            r#"{"bucket":"pdfs","key":"hitomi/test.pdf"}"#
        )
        .await
        .status_code,
        503
    );
    task.await.unwrap();
}

#[tokio::test]
async fn list_objects_forwards_opaque_token_to_s3() {
    let (state, task) = upstream(vec![
        (200, "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>a+/=</NextContinuationToken><Contents><Key>first.pdf</Key></Contents></ListBucketResult>"),
        (200, "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>second.pdf</Key></Contents></ListBucketResult>"),
    ]).await;
    let first = post(
        &state,
        "/s3/list_objects_v2",
        r#"{"bucket":"pdfs","max_keys":100}"#,
    )
    .await;
    assert_eq!(first.status_code, 200);
    assert_eq!(
        string_member(&first.body, "next_continuation_token"),
        "a+/="
    );
    let second = post(
        &state,
        "/s3/list_objects_v2",
        r#"{"bucket":"pdfs","max_keys":100,"continuation_token":"a+/=","start_after":"ignored"}"#,
    )
    .await;
    assert_eq!(second.status_code, 200);
    assert!(
        std::str::from_utf8(&second.body)
            .unwrap()
            .contains("second.pdf")
    );
    let requests = task.await.unwrap();
    assert!(!requests[0].uri.contains("continuation-token="));
    assert!(requests[1].uri.contains("continuation-token=a%2B%2F%3D"));
    assert!(!requests[1].uri.contains("start-after="));
}

#[tokio::test]
async fn slack_forwards_threads_blocks_and_updates() {
    let (state, task) = upstream(vec![(200, r#"{"ok":true,"ts":"123.456"}"#); 3]).await;
    let plain = post(
        &state,
        "/slack/message",
        r#"{"channel":"C1","text":"hello"}"#,
    )
    .await;
    assert_eq!(plain.status_code, 200);
    let envelope = nojson::RawJson::parse(std::str::from_utf8(&plain.body).unwrap()).unwrap();
    let slack_json = String::try_from(envelope.value()).unwrap();
    assert_eq!(string_member(slack_json.as_bytes(), "ts"), "123.456");
    let body = r#"{"channel":"C1","text":"fallback","thread_ts":"123.456","blocks":[{"type":"section","text":{"type":"mrkdwn","text":"*Done*"}}]}"#;
    assert_eq!(post(&state, "/slack/message", body).await.status_code, 200);
    assert_eq!(
        post(
            &state,
            "/slack/message/update",
            r#"{"channel":"C1","text":"done","ts":"123.456","blocks":[]}"#
        )
        .await
        .status_code,
        200
    );
    let requests = task.await.unwrap();
    assert_eq!(requests[1].uri, "/chat.postMessage");
    assert_eq!(string_member(&requests[1].body, "thread_ts"), "123.456");
    assert!(
        std::str::from_utf8(&requests[1].body)
            .unwrap()
            .contains("*Done*")
    );
    assert_eq!(requests[2].uri, "/chat.update");
    assert_eq!(string_member(&requests[2].body, "ts"), "123.456");
    assert_eq!(
        requests[0].get_header("authorization"),
        Some("Bearer test-token")
    );
}

#[tokio::test]
async fn slack_api_and_http_errors_are_not_success() {
    let (state, task) = upstream(vec![
        (200, r#"{"ok":false,"error":"missing_scope"}"#),
        (429, "rate limited"),
    ])
    .await;
    for _ in 0..2 {
        assert_eq!(
            post(
                &state,
                "/slack/message",
                r#"{"channel":"C1","text":"hello"}"#
            )
            .await
            .status_code,
            500
        );
    }
    assert_eq!(task.await.unwrap().len(), 2);
}

#[tokio::test]
async fn copy_encodes_source_and_rejects_embedded_s3_error() {
    let (state, task) = upstream(vec![
        (
            200,
            "<CopyObjectResult><ETag>&quot;copy&quot;</ETag></CopyObjectResult>",
        ),
        (
            200,
            "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>",
        ),
    ])
    .await;
    let body = r#"{"bucket":"dest","key":"copy.txt","source_bucket":"source","source_key":"dir/日本 +%?#.txt","source_version_id":"a+/="}"#;
    let result = post(&state, "/s3/copy_object", body).await;
    assert_eq!(result.status_code, 200);
    assert_eq!(string_member(&result.body, "e_tag"), "\"copy\"");
    assert_eq!(post(&state, "/s3/copy_object", body).await.status_code, 500);
    let requests = task.await.unwrap();
    assert_eq!(requests[0].method, "PUT");
    assert_eq!(requests[0].uri, "/dest/copy.txt");
    assert_eq!(
        requests[0].get_header("x-amz-copy-source"),
        Some("/source/dir/%E6%97%A5%E6%9C%AC%20%2B%25%3F%23.txt?versionId=a%2B%2F%3D")
    );
    assert!(
        requests[0]
            .get_header("authorization")
            .unwrap()
            .starts_with("AWS4-HMAC-SHA256")
    );
}

#[tokio::test]
async fn tags_round_trip_and_delete_with_version() {
    let (state, task) = upstream(vec![
        (200, ""),
        (200, "<Tagging><TagSet><Tag><Key>project</Key><Value>A&amp;B</Value></Tag></TagSet></Tagging>"),
        (204, ""),
    ]).await;
    assert_eq!(post(&state, "/s3/put_object_tagging", r#"{"bucket":"b","key":"k","version_id":"v+1","tags":[{"key":"project","value":"A&B"}]}"#).await.status_code, 200);
    let body = r#"{"bucket":"b","key":"k","version_id":"v+1"}"#;
    let get = post(&state, "/s3/get_object_tagging", body).await;
    assert_eq!(get.status_code, 200);
    assert!(std::str::from_utf8(&get.body).unwrap().contains("A&B"));
    assert_eq!(
        post(&state, "/s3/delete_object_tagging", body)
            .await
            .status_code,
        200
    );
    let requests = task.await.unwrap();
    assert_eq!(
        requests
            .iter()
            .map(|r| r.method.as_str())
            .collect::<Vec<_>>(),
        ["PUT", "GET", "DELETE"]
    );
    for request in &requests {
        assert!(request.uri.contains("versionId=v%2B1"));
        assert!(request.uri.contains("tagging"));
    }
    assert!(
        std::str::from_utf8(&requests[0].body)
            .unwrap()
            .contains("A&amp;B")
    );
}

#[tokio::test]
async fn upload_notification_reports_partial_success_and_skips_slack_on_upload_failure() {
    for (s3_status, slack_reply, notified) in [
        (200, r#"{"ok":true,"ts":"123.456"}"#, true),
        (200, r#"{"ok":false,"error":"channel_not_found"}"#, false),
        (403, "", false),
    ] {
        let mut replies = vec![(s3_status, "")];
        if s3_status == 200 {
            replies.push((200, slack_reply));
        }
        let (state, task) = upstream(replies).await;
        let response = post(&state, "/s3/put_object_and_notify", r#"{"bucket":"b","key":"report.txt","file_data_base64":"aGk=","content_type":"text/plain","channel":"C1","text":"Report","expires_in_secs":900}"#).await;
        let requests = task.await.unwrap();
        assert_eq!(requests[0].body, b"hi");
        if s3_status == 200 {
            assert_eq!(response.status_code, 200);
            let json =
                nojson::RawJson::parse(std::str::from_utf8(&response.body).unwrap()).unwrap();
            assert!(
                bool::try_from(
                    json.value()
                        .to_member("uploaded")
                        .unwrap()
                        .required()
                        .unwrap()
                )
                .unwrap()
            );
            assert_eq!(
                bool::try_from(
                    json.value()
                        .to_member("notified")
                        .unwrap()
                        .required()
                        .unwrap()
                )
                .unwrap(),
                notified
            );
            let url = string_member(&response.body, "url");
            assert!(url.contains("X-Amz-Expires=900"));
            assert_eq!(
                string_member(&requests[1].body, "text"),
                format!("Report\n{url}")
            );
            if !notified {
                assert_eq!(
                    string_member(&response.body, "notification_error"),
                    "channel_not_found"
                );
            }
        } else {
            assert_eq!(response.status_code, 500);
            assert_eq!(requests.len(), 1);
        }
    }
}

#[tokio::test]
async fn invalid_inputs_fail_before_network_and_new_routes_reject_get() {
    let (state, task) = upstream(vec![]).await;
    for (path, body) in [
        (
            "/slack/message",
            r#"{"channel":"C1","text":"x","blocks":{}}"#,
        ),
        (
            "/slack/message",
            r#"{"channel":"C1","text":"x","thread_ts":1}"#,
        ),
        ("/slack/message/update", r#"{"channel":"C1","text":"x"}"#),
        (
            "/s3/copy_object",
            r#"{"bucket":"b","key":"k","source_bucket":"b","source_key":""}"#,
        ),
        (
            "/s3/put_object_tagging",
            r#"{"bucket":"b","key":"k","tags":[{"key":"a","value":"1"},{"key":"a","value":"2"}]}"#,
        ),
        (
            "/s3/put_object_tagging",
            r#"{"bucket":"b","key":"k","tags":null}"#,
        ),
        (
            "/s3/put_object_and_notify",
            r#"{"bucket":"b","key":"k","file_data_base64":"aGk=","channel":"C1","expires_in_secs":0}"#,
        ),
        (
            "/s3/put_object_and_notify",
            r#"{"bucket":"b","key":"k","file_data_base64":"!invalid","channel":"C1"}"#,
        ),
    ] {
        assert_eq!(
            post(&state, path, body).await.status_code,
            400,
            "{path}: {body}"
        );
    }
    for path in [
        "/slack/message/update",
        "/s3/copy_object",
        "/s3/put_object_tagging",
        "/s3/get_object_tagging",
        "/s3/delete_object_tagging",
        "/s3/put_object_and_notify",
    ] {
        assert_eq!(
            process_request(Request::new("GET", path), &state)
                .await
                .status_code,
            405
        );
    }
    task.await.unwrap();
}

/// 本番と同じ Router へ生の HTTP を送り、応答全体を返す。
async fn raw(state: &AppState, request: String) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let state = state.clone();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        super::handle_connection(socket, state).await;
    });
    client.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    timeout(Duration::from_secs(5), client.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    server.await.unwrap();
    String::from_utf8_lossy(&response).into_owned()
}

fn post_request(path: &str, headers: &str, body: &str) -> String {
    format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n{headers}\r\n{body}",
        body.len()
    )
}

#[tokio::test]
async fn web_pages_cannot_call_update_apis() {
    const VIEWER: &str = "Origin: https://hitomi-upload-viewer.internal.qroksera.com\r\n";
    let body = r#"{"bucket":"b"}"#;
    // 上流は応答を用意しない。到達すれば 403/415 以外になる。
    let (state, _upstream) = upstream(vec![]).await;
    for (path, headers) in [
        // 任意のサイトからの preflight 不要な POST。
        (
            "/s3/delete_bucket",
            "Origin: https://attacker.example\r\nContent-Type: text/plain\r\n",
        ),
        // preview で開いた文書（sandbox）や同一オリジンのページ。
        (
            "/s3/delete_bucket",
            "Origin: null\r\nContent-Type: application/json\r\n",
        ),
        (
            "/s3/list_buckets",
            "Sec-Fetch-Site: same-origin\r\nContent-Type: application/json\r\n",
        ),
        // viewer でも、画面が使わない更新 API は呼べない。
        ("/s3/delete_bucket", VIEWER),
        ("/slack/message", VIEWER),
    ] {
        let headers = if headers == VIEWER {
            format!("{VIEWER}Content-Type: application/json\r\n")
        } else {
            headers.to_string()
        };
        let response = raw(&state, post_request(path, &headers, body)).await;
        assert!(response.starts_with("HTTP/1.1 403"), "{path}: {response}");
        assert!(
            !response
                .to_ascii_lowercase()
                .contains("access-control-allow-origin: https://attacker")
        );
    }
    // ブラウザー以外でも、JSON API は application/json だけを受け付ける。
    for headers in ["", "Content-Type: text/plain\r\n"] {
        let response = raw(&state, post_request("/s3/delete_bucket", headers, body)).await;
        assert!(response.starts_with("HTTP/1.1 415"), "{response}");
    }
}

#[tokio::test]
async fn viewer_and_cluster_clients_keep_working() {
    let list = "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";
    let (state, upstream) = upstream(vec![(200, list), (200, list)]).await;
    for headers in [
        "Origin: https://hitomi-upload-viewer.internal.qroksera.com\r\nSec-Fetch-Site: same-site\r\nContent-Type: application/json\r\n",
        "Content-Type: application/json; charset=utf-8\r\n",
    ] {
        let response = raw(
            &state,
            post_request("/s3/list_objects_v2", headers, r#"{"bucket":"b"}"#),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    }
    assert_eq!(upstream.await.unwrap().len(), 2);
}

/// 後処理は HTTP 層で1回だけ行う。ID がない要求にも1つだけ採番して返す。
#[tokio::test]
async fn every_response_gets_one_request_id_and_one_cors_header() {
    let (state, _upstream) = upstream(vec![]).await;
    for request in [
        "GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".to_string(),
        "GET /missing HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nx-request-id: given\r\n\r\n".to_string(),
        post_request("/s3/head_object", "Content-Type: text/plain\r\n", "{}"),
        "OPTIONS /s3/list_objects_v2 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".to_string(),
        "GET /s3/preview/bucket-only HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".to_string(),
    ] {
        let response = raw(&state, request.clone()).await.to_ascii_lowercase();
        let head = response.split("\r\n\r\n").next().unwrap();
        assert_eq!(head.matches("\r\nx-request-id: ").count(), 1, "{request}: {head}");
        if request.contains("x-request-id: given") {
            assert!(head.contains("\r\nx-request-id: given"), "{head}");
        }
        let cors = usize::from(request.contains(" /s3/"));
        assert_eq!(
            head.matches("\r\naccess-control-allow-origin: ").count(),
            cors,
            "{request}: {head}"
        );
    }
}

/// TCP を使わず、メモリ上の接続で本番と同じ Router へ生の HTTP を送る。
/// ポートを消費しないので、大量の要求を続けて送れる。
async fn raw_in_memory(state: &AppState, request: String) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (mut client, server) = tokio::io::duplex(1 << 20);
    let state = state.clone();
    let server = tokio::spawn(super::handle_connection(server, state));
    client.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    timeout(Duration::from_secs(5), client.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    server.await.unwrap();
    String::from_utf8(response).unwrap()
}

/// 期待値ファイルと比べる。`UPDATE_GOLDEN=1` のときは期待値を書き換える。
pub(crate) fn assert_golden(name: &str, actual: &str) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, actual).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path)
        .unwrap_or_else(|_| panic!("{} is missing; run with UPDATE_GOLDEN=1", path.display()));
    if expected != actual {
        let first = expected
            .lines()
            .zip(actual.lines())
            .position(|(e, a)| e != a)
            .unwrap_or_else(|| expected.lines().count().min(actual.lines().count()));
        panic!(
            "{name} differs at line {}:\n  expected: {}\n  actual:   {}\nrun with UPDATE_GOLDEN=1 if the change is intended",
            first + 1,
            expected.lines().nth(first).unwrap_or("<end>"),
            actual.lines().nth(first).unwrap_or("<end>"),
        );
    }
}

/// HTTP の I/F を固定する。経路・メソッド・ヘッダーの組み合わせごとに、
/// 状態コード・ヘッダー・本文を期待値ファイルと比べる。上流には接続できない状態で実行する。
#[tokio::test]
async fn http_interface_matches_golden() {
    // 上流はポートを持ち続け、接続をすぐ切る。閉じたポートが再利用されて
    // 別の fixture へ届くことを防ぎ、上流に触れる要求の結果を毎回同じにする。
    let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", closed.local_addr().unwrap());
    let refuser = tokio::spawn(async move {
        loop {
            if let Ok((socket, _)) = closed.accept().await {
                drop(socket);
            }
        }
    });
    let (mut state, _upstream) = upstream(vec![]).await;
    state.settings.s3_endpoint = Some(endpoint.clone());
    state.settings.slack_api_base_url = endpoint;
    let paths = [
        "/health",
        "/health/",
        "/health?x=1",
        "/metrics",
        "/openapi.json",
        "/notifications",
        "/notifications/",
        "/notifications/1",
        "/notifications/x",
        "/notifications/retry",
        "/notifications/1/retry",
        "/slack/message",
        "/slack/message/update",
        "/slack/upload/image",
        "/slack/upload/pdf",
        "/slack/upload/other",
        "/s3/put_object_base64",
        "/s3/put_object_stream?bucket=b&key=k",
        "/s3/put_object_stream",
        "/s3/put_object_and_notify",
        "/s3/get_object_base64",
        "/s3/head_object",
        "/s3/delete_object",
        "/s3/delete_objects",
        "/s3/copy_object",
        "/s3/get_object_tagging",
        "/s3/put_object_tagging",
        "/s3/delete_object_tagging",
        "/s3/list_objects_v2",
        "/s3/create_multipart_upload",
        "/s3/upload_part_base64",
        "/s3/complete_multipart_upload",
        "/s3/abort_multipart_upload",
        "/s3/list_parts",
        "/s3/list_multipart_uploads",
        "/s3/presigned_get_object",
        "/s3/presigned_put_object",
        "/s3/browser_get_object",
        "/s3/browser_open?bucket=pdfs&key=x",
        "/s3/browser_open?bucket=pdfs&key=hitomi/a.pdf&key=x",
        "/s3/preview/b/k.pdf",
        "/s3/preview/b",
        "/s3/list_buckets",
        "/s3/create_bucket",
        "/s3/head_bucket",
        "/s3/delete_bucket",
        "/s3/missing",
        "/s3/x_base64",
        "/missing",
    ];
    let variants = [
        "Content-Type: application/json\r\n",
        "",
        "Content-Type: text/plain\r\n",
        "Origin: https://hitomi-upload-viewer.internal.qroksera.com\r\nContent-Type: application/json\r\n",
        "Origin: https://attacker.example\r\nContent-Type: application/json\r\n",
        "Sec-Fetch-Site: same-origin\r\nContent-Type: application/json\r\n",
    ];
    let mut requests = Vec::new();
    for path in paths {
        for method in ["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD"] {
            for headers in variants {
                requests.push((method, path, headers));
            }
        }
    }
    use futures_util::StreamExt;
    // 順序を保ったまま並行に送る。
    let lines: Vec<String> = futures_util::stream::iter(requests)
        .map(|(method, path, headers)| {
            let state = state.clone();
            async move {
                let request = format!(
                    "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nx-request-id: fixed\r\nContent-Length: 2\r\n{headers}\r\n{{}}"
                );
                let response = raw_in_memory(&state, request).await;
                let (head, body) = response.split_once("\r\n\r\n").unwrap_or((&response, ""));
                let mut lines: Vec<String> = head
                    .lines()
                    .skip(1)
                    .map(str::to_ascii_lowercase)
                    .filter(|line| !line.starts_with("date:"))
                    .collect();
                lines.sort();
                let body = if body.len() > 1024 {
                    format!("<{} bytes> {}…", body.len(), &body[..64])
                } else {
                    body.to_string()
                };
                format!(
                    "{method} {path} [{}] => {} | {} | {body}\n",
                    headers.trim_end().replace("\r\n", "; "),
                    head.lines().next().unwrap_or(""),
                    lines.join(", "),
                )
            }
        })
        .buffered(32)
        .collect()
        .await;
    refuser.abort();
    let out = lines.concat();
    assert_golden("http_interface.golden", &out);
}

/// `put_object_and_notify` は Slack の失敗理由を `notification_error` としてそのまま返す。
/// Slack の呼び出しを変えても、この文字列が変わらないことを固定する。
#[tokio::test]
async fn notification_error_strings_match_golden() {
    use std::fmt::Write;
    let request = r#"{"bucket":"b","key":"report.txt","file_data_base64":"aGk=","channel":"C1","text":"Report"}"#;
    let mut out = String::new();
    for (label, status, body) in [
        ("ok", 200, r#"{"ok":true,"ts":"1.2"}"#),
        (
            "api error",
            200,
            r#"{"ok":false,"error":"channel_not_found"}"#,
        ),
        ("api error without code", 200, r#"{"ok":false}"#),
        (
            "api error with non-string code",
            200,
            r#"{"ok":false,"error":3}"#,
        ),
        ("missing ok", 200, r#"{"ts":"1.2"}"#),
        ("ok is not a bool", 200, r#"{"ok":"true"}"#),
        ("not json", 200, "<html>"),
        ("empty body", 200, ""),
        ("array", 200, "[]"),
        ("rate limited", 429, r#"{"ok":false,"error":"ratelimited"}"#),
        ("server error", 500, "oops"),
        ("client error", 404, r#"{"ok":false,"error":"x"}"#),
    ] {
        let (state, task) = upstream(vec![(200, ""), (status, body)]).await;
        let response = post(&state, "/s3/put_object_and_notify", request).await;
        task.await.unwrap();
        let json = nojson::RawJson::parse(std::str::from_utf8(&response.body).unwrap()).unwrap();
        let notified = json
            .value()
            .to_member("notified")
            .unwrap()
            .required()
            .unwrap();
        let error = json
            .value()
            .to_member("notification_error")
            .unwrap()
            .required()
            .unwrap();
        writeln!(
            out,
            "{label} ({status} {body}) => {} notified={} notification_error={}",
            response.status_code,
            notified.as_raw_str(),
            error.as_raw_str()
        )
        .unwrap();
    }
    // Slack へ接続できない場合。エラー文には URL が入るため、ポートを伏せる。
    let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", closed.local_addr().unwrap());
    let refuser = tokio::spawn(async move {
        loop {
            if let Ok((socket, _)) = closed.accept().await {
                drop(socket);
            }
        }
    });
    let (mut state, task) = upstream(vec![(200, "")]).await;
    state.settings.slack_api_base_url = endpoint.clone();
    let response = post(&state, "/s3/put_object_and_notify", request).await;
    task.await.unwrap();
    refuser.abort();
    let json = nojson::RawJson::parse(std::str::from_utf8(&response.body).unwrap()).unwrap();
    let error = json
        .value()
        .to_member("notification_error")
        .unwrap()
        .required()
        .unwrap();
    writeln!(
        out,
        "connection closed => {} notification_error={}",
        response.status_code,
        error.as_raw_str().replace(&endpoint, "<slack>")
    )
    .unwrap();
    assert_golden("notification_errors.golden", &out);
}
