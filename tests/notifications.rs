use api_hub::{
    config::{settings::Settings, state::AppState},
    database,
    notifications::{self, Notification},
};
use sqlx::{Row, postgres::PgPoolOptions};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

/// 専用の空DBでのみ実行する。通常のunit testからは本番DBへ接続しない。
#[tokio::test]
#[ignore = "requires isolated TEST_DATABASE_URL"]
async fn durable_notifications() {
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var("TEST_DATABASE_URL").expect("isolated database required"))
        .await
        .unwrap();
    let unique: String = sqlx::query_scalar("SELECT replace(gen_random_uuid()::text,'-','')")
        .fetch_one(&admin)
        .await
        .unwrap();
    let schema = format!("notification_test_{unique}");
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();
    let path = format!("SET search_path TO {schema}");
    let pool = PgPoolOptions::new()
        .max_connections(3)
        .after_connect(move |connection, _| {
            let path = path.clone();
            Box::pin(async move {
                sqlx::query(&path).execute(connection).await?;
                Ok(())
            })
        })
        .connect(&std::env::var("TEST_DATABASE_URL").expect("isolated database required"))
        .await
        .unwrap();
    database::migrate(&pool).await.unwrap();
    let empty = api_hub::queue_metrics::render(&pool).await.unwrap();
    assert!(empty.contains("application_queue_jobs{queue=\"notifications\",state=\"pending\"} 0"));
    assert!(
        empty.contains("application_queue_oldest_pending_age_seconds{queue=\"notifications\"} 0")
    );
    assert!(
        empty.contains(
            "application_queue_last_success_timestamp_seconds{queue=\"notifications\"} 0"
        )
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let state = AppState {
        database: Some(pool.clone()),
        client: api_hub::http_client::new(),
        settings: Settings {
            slack_bot_token: "test".into(),
            slack_api_base_url: format!("http://{}", listener.local_addr().unwrap()),
            s3_access_key_id: "test".into(),
            s3_secret_access_key: "test".into(),
            s3_region: "us-east-1".into(),
            s3_endpoint: None,
            s3_browser_endpoint: None,
            s3_use_path_style: true,
            s3_session_token: None,
        },
    };
    let item = Notification {
        producer: "test".into(),
        file: None,
        event_id: "event-1".into(),
        channel: "C1".into(),
        text: "hello".into(),
    };
    let (first, second) = tokio::join!(
        notifications::enqueue(&pool, &item),
        notifications::enqueue(&pool, &item)
    );
    let id = first.unwrap().0;
    assert_eq!(id, second.unwrap().0);
    let changed = Notification {
        text: "different".into(),
        ..item
    };
    assert!(notifications::enqueue(&pool, &changed).await.is_err());
    let server = tokio::spawn(async move {
        for (code, headers, body) in [
            (429, "Retry-After: 120\r\n", ""),
            (200, "", r#"{"ok":true,"ts":"123.4"}"#),
        ] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut decoder = shiguredo_http11::RequestDecoder::new();
            loop {
                if decoder.decode().unwrap().is_some() {
                    break;
                }
                let mut bytes = [0; 4096];
                let n = socket.read(&mut bytes).await.unwrap();
                assert!(n > 0);
                decoder.feed(&bytes[..n]).unwrap();
            }
            socket.write_all(format!("HTTP/1.1 {code} Test\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        }
    });
    assert!(notifications::deliver_one(&state).await.unwrap());
    let row=sqlx::query("SELECT state,available_at>now()+interval '110 seconds' AS delayed FROM notifications WHERE id=$1").bind(id).fetch_one(&pool).await.unwrap();
    assert_eq!(row.get::<String, _>("state"), "retry");
    assert!(row.get::<bool, _>("delayed"));
    assert!(!notifications::deliver_one(&state).await.unwrap());
    sqlx::query("UPDATE notifications SET available_at=now() WHERE id=$1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        notifications::deliver_one(&state),
        notifications::deliver_one(&state)
    );
    assert_ne!(a.unwrap(), b.unwrap());
    server.await.unwrap();
    let row = sqlx::query("SELECT state,slack_ts,attempts FROM notifications WHERE id=$1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>("state"), "sent");
    assert_eq!(row.get::<String, _>("slack_ts"), "123.4");
    assert_eq!(row.get::<i32, _>("attempts"), 2);
    let item = Notification {
        event_id: "expired".into(),
        text: "hello".into(),
        ..changed
    };
    let expired = notifications::enqueue(&pool, &item).await.unwrap().0;
    sqlx::query("UPDATE notifications SET state='sending',lease_until=now()-interval '1 second' WHERE id=$1").bind(expired).execute(&pool).await.unwrap();
    assert!(!notifications::deliver_one(&state).await.unwrap());
    let status: String = sqlx::query_scalar("SELECT state FROM notifications WHERE id=$1")
        .bind(expired)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "uncertain");
    assert_eq!(
        notifications::handle_retry(&state, &expired.to_string())
            .await
            .status_code,
        202
    );
    assert_eq!(
        notifications::handle_retry(&state, &id.to_string())
            .await
            .status_code,
        409
    );
    sqlx::query("UPDATE notifications SET state='uncertain' WHERE id=$1")
        .bind(expired)
        .execute(&pool)
        .await
        .unwrap();
    for completion_status in [200, 503] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let mut image_state = state.clone();
        image_state.settings.s3_endpoint = Some(endpoint.clone());
        image_state.settings.slack_api_base_url = endpoint.clone();
        let server = tokio::spawn(async move {
            let ticket=serde_json::json!({"ok":true,"upload_url":format!("{endpoint}/upload"),"file_id":"F1"}).to_string();
            for (status, body) in [
                (200, b"\xff\xd8\xffimage".to_vec()),
                (200, ticket.into_bytes()),
                (200, b"ok".to_vec()),
                (
                    completion_status,
                    br#"{"ok":true,"files":[{"id":"F1"}]}"#.to_vec(),
                ),
            ] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut decoder = shiguredo_http11::RequestDecoder::new();
                let request = loop {
                    if let Some(request) = decoder.decode().unwrap() {
                        break request;
                    }
                    let mut bytes = [0; 4096];
                    let n = socket.read(&mut bytes).await.unwrap();
                    assert!(n > 0);
                    decoder.feed(&bytes[..n]).unwrap();
                };
                if request.uri == "/files.completeUploadExternal" {
                    let value: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
                    assert_eq!(value["initial_comment"], "image caption");
                    assert_eq!(value["files"][0]["id"], "F1");
                }
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
            }
        });
        let item = Notification {
            producer: "test".into(),
            event_id: format!("image-{completion_status}"),
            channel: "C1".into(),
            text: "image caption".into(),
            file: Some(notifications::NotificationFile {
                bucket: "pdfs".into(),
                key: format!("hitomi-notifications/{}.jpg", "a".repeat(64)),
                name: "image.jpg".into(),
                content_type: "image/jpeg".into(),
            }),
        };
        let id = notifications::enqueue(&pool, &item).await.unwrap().0;
        assert!(notifications::deliver_one(&image_state).await.unwrap());
        server.await.unwrap();
        let row = sqlx::query("SELECT state,slack_file_id FROM notifications WHERE id=$1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            row.get::<String, _>("state"),
            if completion_status == 200 {
                "sent"
            } else {
                "uncertain"
            }
        );
        assert_eq!(row.get::<String, _>("slack_file_id"), "F1");
    }
    shutdown_drains_one_delivery(&pool, &state).await;
    pool.close().await;
    assert_eq!(
        api_hub::queue_metrics::handle(&state).await.status_code,
        503
    );
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
}

/// Slackが応答を返す前に終了要求を送っても、DB確定を待ち、次の通知は取得しない。
async fn shutdown_drains_one_delivery(pool: &sqlx::PgPool, state: &AppState) {
    use std::time::Duration;
    use tokio::sync::oneshot;
    sqlx::query("UPDATE notifications SET state='sent'")
        .execute(pool)
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut state = state.clone();
    state.settings.slack_api_base_url = format!("http://{}", listener.local_addr().unwrap());
    let (received_tx, received_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut decoder = shiguredo_http11::RequestDecoder::new();
        loop {
            if decoder.decode().unwrap().is_some() {
                break;
            }
            let mut bytes = [0; 4096];
            let n = socket.read(&mut bytes).await.unwrap();
            assert!(n > 0);
            decoder.feed(&bytes[..n]).unwrap();
        }
        received_tx.send(()).unwrap();
        release_rx.await.unwrap();
        let body = r#"{"ok":true,"ts":"drained"}"#;
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    });
    let mut ids = Vec::new();
    for event in ["shutdown-first", "shutdown-second"] {
        let item = Notification {
            producer: "test".into(),
            event_id: event.into(),
            channel: "C1".into(),
            text: "drain".into(),
            file: None,
        };
        ids.push(notifications::enqueue(pool, &item).await.unwrap().0);
    }
    sqlx::query("UPDATE notifications SET created_at=now()-interval '1 hour' WHERE id=ANY($1)")
        .bind(&ids)
        .execute(pool)
        .await
        .unwrap();
    let metrics = api_hub::queue_metrics::render(pool).await.unwrap();
    let age: f64 = metrics
        .lines()
        .find(|line| line.starts_with("application_queue_oldest_pending_age_seconds{"))
        .unwrap()
        .split_whitespace()
        .last()
        .unwrap()
        .parse()
        .unwrap();
    assert!(age >= 3600.0);
    let (stop_tx, stop_rx) = oneshot::channel();
    let mut worker = tokio::spawn(notifications::worker(state.clone(), async {
        let _ = stop_rx.await;
    }));
    tokio::time::timeout(Duration::from_secs(5), received_rx)
        .await
        .unwrap()
        .unwrap();
    stop_tx.send(()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut worker)
            .await
            .is_err()
    );
    release_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .unwrap()
        .unwrap();
    server.await.unwrap();
    let rows: Vec<String> =
        sqlx::query_scalar("SELECT state FROM notifications WHERE id=ANY($1) ORDER BY id")
            .bind(&ids)
            .fetch_all(pool)
            .await
            .unwrap();
    assert_eq!(rows, ["sent", "pending"]);
    notifications::worker(state.clone(), std::future::ready(())).await;
    let pending: String = sqlx::query_scalar("SELECT state FROM notifications WHERE id=$1")
        .bind(ids[1])
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(pending, "pending");
    sqlx::query("UPDATE notifications SET state='uncertain' WHERE id=$1")
        .bind(ids[1])
        .execute(pool)
        .await
        .unwrap();
    let metrics = api_hub::queue_metrics::render(pool).await.unwrap();
    assert!(
        metrics.contains("application_queue_jobs{queue=\"notifications\",state=\"uncertain\"} 1")
    );
    assert!(
        metrics.contains("application_queue_oldest_pending_age_seconds{queue=\"notifications\"} 0")
    );
    let last: f64 = metrics
        .lines()
        .find(|line| line.starts_with("application_queue_last_success_timestamp_seconds{"))
        .unwrap()
        .split_whitespace()
        .last()
        .unwrap()
        .parse()
        .unwrap();
    assert!(last > 0.0);
    assert_eq!(
        api_hub::queue_metrics::handle(&state).await.status_code,
        200
    );
}
