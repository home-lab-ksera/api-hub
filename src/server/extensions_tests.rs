use super::process_request;
use crate::{
    config::{settings::Settings, state::AppState},
    http_client::HttpClient,
};
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
        client: HttpClient::new(),
        settings: Settings {
            slack_bot_token: "test-token".into(),
            slack_api_base_url: endpoint.clone(),
            s3_access_key_id: "test".into(),
            s3_secret_access_key: "test".into(),
            s3_region: "us-east-1".into(),
            s3_endpoint: Some(endpoint),
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
