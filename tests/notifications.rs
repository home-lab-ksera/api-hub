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

async fn isolated_pool() -> (sqlx::PgPool, sqlx::PgPool, String) {
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
    (pool, admin, schema)
}

/// 専用の空DBでのみ実行する。通常のunit testからは本番DBへ接続しない。
#[tokio::test]
#[ignore = "requires isolated TEST_DATABASE_URL"]
async fn durable_notifications() {
    let (pool, admin, schema) = isolated_pool().await;
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
            s3_endpoint: None,
            ..Settings::for_test(&format!("http://{}", listener.local_addr().unwrap()))
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
            let ticket =
                format!(r#"{{"ok":true,"upload_url":"{endpoint}/upload","file_id":"F1"}}"#);
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
                    // 以前の serde_json の出力とバイト単位で同じ。
                    assert_eq!(
                        String::from_utf8(request.body.clone()).unwrap(),
                        r#"{"channel_id":"C1","files":[{"id":"F1","title":"image.jpg"}],"initial_comment":"image caption"}"#
                    );
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

#[tokio::test]
#[ignore = "requires isolated TEST_DATABASE_URL"]
async fn integrated_api_delivers_and_drains_with_shared_three_connection_pool() {
    use tokio::{
        sync::oneshot,
        time::{Duration, timeout},
    };
    let (pool, admin, schema) = isolated_pool().await;
    database::migrate(&pool).await.unwrap();
    let slack = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slack_url = format!("http://{}", slack.local_addr().unwrap());
    let (received, arrival) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let fixture = tokio::spawn(async move {
        let (mut socket, _) = slack.accept().await.unwrap();
        let mut decoder = shiguredo_http11::RequestDecoder::new();
        let request = loop {
            if let Some(request) = decoder.decode().unwrap() {
                break request;
            }
            let mut chunk = [0; 4096];
            let n = socket.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            decoder.feed(&chunk[..n]).unwrap();
        };
        assert!(request.uri.ends_with("/chat.postMessage"));
        // 以前の serde_json の出力とバイト単位で同じ。
        assert_eq!(request.body, br#"{"channel":"C1","text":"fixture"}"#);
        received.send(()).unwrap();
        wait.await.unwrap();
        let body = r#"{"ok":true,"ts":"fixture"}"#;
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
    // 実CLIとSIGTERMで終了境界を確認する（HTTP fixture以外へは接続しない）。
    let mut url = url::Url::parse(&std::env::var("TEST_DATABASE_URL").unwrap()).unwrap();
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let mut process = tokio::process::Command::new(env!("CARGO_BIN_EXE_api-hub"))
        .env("DATABASE_URL", url.as_str())
        .env("PGPOOL_MAX", "3")
        .env("SLACK_BOT_TOKEN", "fixture")
        .env("SLACK_API_BASE_URL", slack_url)
        .env("RUSTFS_S3_ACCESS_KEY_ID", "fixture")
        .env("RUSTFS_S3_SECRET_ACCESS_KEY", "fixture")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let address = "127.0.0.1:3000";
    let base = format!("http://{address}");
    let client = api_hub::http_client::new();
    timeout(Duration::from_secs(5), async {
        loop {
            assert!(process.try_wait().unwrap().is_none());
            if client.get(format!("{base}/health")).send().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let post = |event| {
        client
            .post(format!("{base}/notifications"))
            .header("content-type", "application/json")
            .body(format!(
                r#"{{"producer":"fixture","event_id":"{event}","channel":"C1","text":"fixture"}}"#
            ))
    };
    assert_eq!(post("first").send().await.unwrap().status(), 202);
    timeout(Duration::from_secs(5), arrival)
        .await
        .unwrap()
        .unwrap();
    // Slack待ち中もpoolを占有せず、API受付・metricsを処理できる。
    assert_eq!(post("second").send().await.unwrap().status(), 202);
    let metrics = client.get(format!("{base}/metrics")).send().await.unwrap();
    assert_eq!(metrics.status(), 200);
    assert!(
        metrics
            .text()
            .await
            .unwrap()
            .contains("state=\"sending\"} 1")
    );
    assert!(
        std::process::Command::new("kill")
            .args(["-TERM", &process.id().unwrap().to_string()])
            .status()
            .unwrap()
            .success()
    );
    timeout(Duration::from_secs(3), async {
        loop {
            if tokio::net::TcpStream::connect(address).await.is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(process.try_wait().unwrap().is_none());
    release.send(()).unwrap();
    assert!(
        timeout(Duration::from_secs(10), process.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    fixture.await.unwrap();
    pool.close().await;
    let states: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT state FROM {schema}.notifications ORDER BY id"
    ))
    .fetch_all(&admin)
    .await
    .unwrap();
    assert_eq!(states, ["sent", "pending"]);
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires isolated TEST_DATABASE_URL"]
async fn migration_cli_does_not_load_http_settings_or_deliver() {
    let (pool, admin, schema) = isolated_pool().await;
    database::migrate(&pool).await.unwrap();
    notifications::enqueue(
        &pool,
        &Notification {
            producer: "fixture".into(),
            event_id: "migration".into(),
            channel: "C1".into(),
            text: "fixture".into(),
            file: None,
        },
    )
    .await
    .unwrap();
    let mut url = url::Url::parse(&std::env::var("TEST_DATABASE_URL").unwrap()).unwrap();
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_api-hub"))
        .arg("--migrate")
        .env("DATABASE_URL", url.as_str())
        .env_remove("SLACK_BOT_TOKEN")
        .env_remove("RUSTFS_S3_ACCESS_KEY_ID")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let state: String = sqlx::query_scalar("SELECT state FROM notifications")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "pending");
    pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
}

/// 期待値ファイルと比べる。`UPDATE_GOLDEN=1` のときは期待値を書き換える。
fn assert_golden(name: &str, actual: &str) {
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

/// 通知 API の HTTP 応答と、DB に保存される payload を固定する。
#[tokio::test]
#[ignore = "requires isolated TEST_DATABASE_URL"]
async fn notification_api_matches_golden() {
    use std::fmt::Write;
    let (pool, admin, schema) = isolated_pool().await;
    database::migrate(&pool).await.unwrap();
    let state = AppState {
        database: Some(pool.clone()),
        client: api_hub::http_client::new(),
        settings: Settings {
            s3_endpoint: None,
            ..Settings::for_test("http://127.0.0.1:9")
        },
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(api_hub::server::serve(listener, state, async {
        let _ = stopped.await;
    }));
    let client = api_hub::http_client::new();
    let mut out = String::new();
    let hash = "0123456789abcdef".repeat(4);
    let long_text = "a".repeat(16001);
    let posts: Vec<(&str, String)> = vec![
        ("new", r#"{"producer":"p","event_id":"e1","channel":"C1","text":"hello"}"#.into()),
        ("same event", r#"{"producer":"p","event_id":"e1","channel":"C1","text":"hello"}"#.into()),
        ("same event, other order", r#"{"text":"hello","channel":"C1","event_id":"e1","producer":"p"}"#.into()),
        ("changed payload", r#"{"producer":"p","event_id":"e1","channel":"C1","text":"changed"}"#.into()),
        ("escapes and unicode", r#"{"producer":"p","event_id":"e2","channel":"C1","text":"改行\n\"quote\" \\ \u0001 😀 </script>"}"#.into()),
        ("unknown field", r#"{"producer":"p","event_id":"e3","channel":"C1","text":"t","extra":1}"#.into()),
        ("null file", r#"{"producer":"p","event_id":"e4","channel":"C1","text":"t","file":null}"#.into()),
        ("file", format!(r#"{{"producer":"p","event_id":"e5","channel":"C1","text":"t","file":{{"name":"a.png","content_type":"image/png","key":"hitomi-notifications/{hash}.png","bucket":"pdfs"}}}}"#)),
        ("invalid file", format!(r#"{{"producer":"p","event_id":"e6","channel":"C1","text":"t","file":{{"name":"a.png","content_type":"image/jpeg","key":"hitomi-notifications/{hash}.png","bucket":"pdfs"}}}}"#)),
        ("file missing field", r#"{"producer":"p","event_id":"e7","channel":"C1","text":"t","file":{"bucket":"pdfs"}}"#.into()),
        ("missing text", r#"{"producer":"p","event_id":"e8","channel":"C1"}"#.into()),
        ("blank producer", r#"{"producer":" ","event_id":"e9","channel":"C1","text":"t"}"#.into()),
        ("text too long", format!(r#"{{"producer":"p","event_id":"e10","channel":"C1","text":"{long_text}"}}"#)),
        ("wrong type", r#"{"producer":1,"event_id":"e11","channel":"C1","text":"t"}"#.into()),
        ("null text", r#"{"producer":"p","event_id":"e12","channel":"C1","text":null}"#.into()),
        ("duplicate key", r#"{"producer":"p","producer":"q","event_id":"e13","channel":"C1","text":"t"}"#.into()),
        ("array", "[]".into()),
        // serde の構造体は、フィールド順に並べた配列としても読めた。
        ("array form", r#"["p","e15","C1","t",null]"#.into()),
        ("unknown key twice", r#"{"producer":"p","event_id":"e16","channel":"C1","text":"t","x":1,"x":2}"#.into()),
        ("deep unknown field", format!(r#"{{"producer":"p","event_id":"e17","channel":"C1","text":"t","x":{}{}}}"#, "[".repeat(200), "]".repeat(200))),
        ("lone surrogate", r#"{"producer":"p","event_id":"e18","channel":"C1","text":"\ud800"}"#.into()),
        ("bom", "\u{feff}{\"producer\":\"p\",\"event_id\":\"e19\",\"channel\":\"C1\",\"text\":\"t\"}".into()),
        ("not json", "nope".into()),
        ("trailing data", r#"{"producer":"p","event_id":"e14","channel":"C1","text":"t"} x"#.into()),
    ];
    for (label, body) in posts {
        let response = client
            .post(format!("{base}/notifications"))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let content_type = response.headers()["content-type"]
            .to_str()
            .unwrap()
            .to_string();
        let body = response.text().await.unwrap();
        writeln!(
            out,
            "POST /notifications ({label}) => {status} {content_type} {body}"
        )
        .unwrap();
    }
    sqlx::query(
        "UPDATE notifications SET state='failed',attempts=8,last_error='x' WHERE event_id='e2'",
    )
    .execute(&pool)
    .await
    .unwrap();
    for (method, path) in [
        // 重複した POST でも連番を消費するため、e2 の ID は 5。
        ("GET", "/notifications/1"),
        ("GET", "/notifications/5"),
        ("GET", "/notifications/2"),
        ("GET", "/notifications/999"),
        ("GET", "/notifications/abc"),
        ("POST", "/notifications/1/retry"),
        ("POST", "/notifications/5/retry"),
        ("GET", "/notifications/5"),
        ("POST", "/notifications/999/retry"),
        ("POST", "/notifications/abc/retry"),
        ("GET", "/metrics"),
    ] {
        let response = client
            .request(method.parse().unwrap(), format!("{base}{path}"))
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .map(|v| v.to_str().unwrap().to_string())
            .unwrap_or_default();
        let body = response.text().await.unwrap();
        // 経過時間は実行ごとに変わる。
        let body = body
            .lines()
            .map(|line| match line.rsplit_once(' ') {
                Some((name, _))
                    if name.starts_with("application_queue_oldest_pending_age_seconds") =>
                {
                    format!("{name} <age>")
                }
                _ => line.to_string(),
            })
            .collect::<Vec<_>>()
            .join("\n");
        writeln!(out, "{method} {path} => {status} {content_type} {body}").unwrap();
    }
    for row in sqlx::query("SELECT id,producer,event_id,channel,state,payload::text AS payload FROM notifications ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap()
    {
        writeln!(
            out,
            "row {} {} {} {} {} {}",
            row.get::<i64, _>("id"),
            row.get::<String, _>("producer"),
            row.get::<String, _>("event_id"),
            row.get::<String, _>("channel"),
            row.get::<String, _>("state"),
            row.get::<String, _>("payload"),
        )
        .unwrap();
    }
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
    pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    assert_golden("notification_api.golden", &out);
}

/// 経路ごとの固定応答を返す上流。S3 と Slack の両方を兼ねる。
async fn routed_upstream(
    routes: Vec<(&'static str, u16, &'static str, Vec<u8>)>,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let base = endpoint.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let routes = routes.clone();
            let base = base.clone();
            tokio::spawn(async move {
                let mut decoder = shiguredo_http11::RequestDecoder::with_limits(
                    shiguredo_http11::DecoderLimits::unlimited(),
                );
                let request = loop {
                    if let Some(request) = decoder.decode().unwrap() {
                        break request;
                    }
                    let mut bytes = [0; 8192];
                    let n = socket.read(&mut bytes).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    decoder.feed(&bytes[..n]).unwrap();
                };
                let path = request.uri.split('?').next().unwrap().to_string();
                let (status, headers, body) = routes
                    .iter()
                    .find(|(prefix, ..)| path.starts_with(prefix))
                    .map(|(_, status, headers, body)| {
                        // 文字列の本文だけ、上流自身の URL を埋め込む。
                        let body = match std::str::from_utf8(body) {
                            Ok(text) => text.replace("<endpoint>", &base).into_bytes(),
                            Err(_) => body.clone(),
                        };
                        (*status, *headers, body)
                    })
                    .unwrap_or((599, "", Vec::new()));
                if status == 0 {
                    // 応答せずに切断する。
                    return;
                }
                let head = format!(
                    "HTTP/1.1 {status} Test\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(&[head.into_bytes(), body].concat()).await;
            });
        }
    });
    (endpoint, task)
}

/// 配送の結果として DB に残る内容を固定する。
#[tokio::test]
#[ignore = "requires isolated TEST_DATABASE_URL"]
async fn delivery_outcomes_match_golden() {
    use std::fmt::Write;
    let (pool, admin, schema) = isolated_pool().await;
    database::migrate(&pool).await.unwrap();
    let hash = "0123456789abcdef".repeat(4);
    let key = format!("hitomi-notifications/{hash}.png");
    let png = b"\x89PNG\r\n\x1a\nimage".to_vec();
    let ticket = br#"{"ok":true,"upload_url":"<endpoint>/upload/1","file_id":"F1"}"#.to_vec();
    let ok = br#"{"ok":true,"ts":"1.2"}"#.to_vec();
    let text = |body: &str| body.as_bytes().to_vec();
    let image = format!("/pdfs/{key}");
    let image: &'static str = Box::leak(image.into_boxed_str());
    // (ラベル, ファイル付きか, 送信前の試行回数, 経路ごとの応答)
    type Route = (&'static str, u16, &'static str, Vec<u8>);
    let scenarios: Vec<(&str, bool, i32, Vec<Route>)> = vec![
        (
            "text sent",
            false,
            0,
            vec![("/chat.postMessage", 200, "", ok.clone())],
        ),
        (
            "text api error",
            false,
            0,
            vec![(
                "/chat.postMessage",
                200,
                "",
                text(r#"{"ok":false,"error":"channel_not_found"}"#),
            )],
        ),
        (
            "text ratelimited code",
            false,
            0,
            vec![(
                "/chat.postMessage",
                200,
                "",
                text(r#"{"ok":false,"error":"ratelimited"}"#),
            )],
        ),
        (
            "text 429",
            false,
            0,
            vec![("/chat.postMessage", 429, "Retry-After: 7\r\n", Vec::new())],
        ),
        (
            "text 429 at limit",
            false,
            7,
            vec![("/chat.postMessage", 429, "Retry-After: 7\r\n", Vec::new())],
        ),
        (
            "text 500",
            false,
            0,
            vec![("/chat.postMessage", 500, "", Vec::new())],
        ),
        (
            "text 404",
            false,
            0,
            vec![("/chat.postMessage", 404, "", Vec::new())],
        ),
        (
            "text invalid body",
            false,
            0,
            vec![("/chat.postMessage", 200, "", text("nope"))],
        ),
        (
            "text disconnect",
            false,
            0,
            vec![("/chat.postMessage", 0, "", Vec::new())],
        ),
        (
            "file sent",
            true,
            0,
            vec![
                (image, 200, "", png.clone()),
                ("/files.getUploadURLExternal", 200, "", ticket.clone()),
                ("/upload/", 200, "", Vec::new()),
                ("/files.completeUploadExternal", 200, "", ok.clone()),
            ],
        ),
        (
            "file image missing",
            true,
            0,
            vec![(image, 404, "", Vec::new())],
        ),
        (
            "file storage error",
            true,
            0,
            vec![(image, 503, "", Vec::new())],
        ),
        (
            "file storage error at limit",
            true,
            7,
            vec![(image, 503, "", Vec::new())],
        ),
        (
            "file bad signature",
            true,
            0,
            vec![(image, 200, "", text("GIF89a"))],
        ),
        (
            "file ticket 429",
            true,
            0,
            vec![
                (image, 200, "", png.clone()),
                (
                    "/files.getUploadURLExternal",
                    429,
                    "Retry-After: 9\r\n",
                    Vec::new(),
                ),
            ],
        ),
        (
            "file ticket rejected",
            true,
            0,
            vec![
                (image, 200, "", png.clone()),
                (
                    "/files.getUploadURLExternal",
                    200,
                    "",
                    text(r#"{"ok":false,"error":"invalid_auth"}"#),
                ),
            ],
        ),
        (
            "file transfer rejected",
            true,
            0,
            vec![
                (image, 200, "", png.clone()),
                ("/files.getUploadURLExternal", 200, "", ticket.clone()),
                ("/upload/", 500, "", Vec::new()),
            ],
        ),
        (
            "file share 500",
            true,
            0,
            vec![
                (image, 200, "", png.clone()),
                ("/files.getUploadURLExternal", 200, "", ticket.clone()),
                ("/upload/", 200, "", Vec::new()),
                ("/files.completeUploadExternal", 500, "", Vec::new()),
            ],
        ),
        (
            "file share disconnect",
            true,
            0,
            vec![
                (image, 200, "", png.clone()),
                ("/files.getUploadURLExternal", 200, "", ticket.clone()),
                ("/upload/", 200, "", Vec::new()),
                ("/files.completeUploadExternal", 0, "", Vec::new()),
            ],
        ),
    ];
    let mut out = String::new();
    for (index, (label, with_file, attempts, routes)) in scenarios.into_iter().enumerate() {
        let (endpoint, server) = routed_upstream(routes).await;
        let state = AppState {
            database: Some(pool.clone()),
            client: api_hub::http_client::new(),
            settings: Settings::for_test(&endpoint),
        };
        let item = Notification {
            producer: "golden".into(),
            event_id: format!("event-{index}"),
            channel: "C1".into(),
            text: "caption".into(),
            file: with_file.then(|| api_hub::notifications::NotificationFile {
                bucket: "pdfs".into(),
                key: key.clone(),
                name: "image.png".into(),
                content_type: "image/png".into(),
            }),
        };
        let id = notifications::enqueue(&pool, &item).await.unwrap().0;
        sqlx::query("UPDATE notifications SET attempts=$2 WHERE id=$1")
            .bind(id)
            .bind(attempts)
            .execute(&pool)
            .await
            .unwrap();
        assert!(notifications::deliver_one(&state).await.unwrap());
        server.abort();
        let row = sqlx::query("SELECT state,attempts,slack_ts,slack_file_id,last_error,round(EXTRACT(EPOCH FROM available_at-updated_at))::bigint AS delay,lease_until IS NULL AS released FROM notifications WHERE id=$1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let delay: i64 = row.get("delay");
        writeln!(
            out,
            "{label}: state={} attempts={} delay={} slack_ts={:?} slack_file_id={:?} last_error={:?} released={}",
            row.get::<String, _>("state"),
            row.get::<i32, _>("attempts"),
            if row.get::<String, _>("state") == "retry" { delay } else { 0 },
            row.get::<Option<String>, _>("slack_ts"),
            row.get::<Option<String>, _>("slack_file_id"),
            row.get::<Option<String>, _>("last_error"),
            row.get::<bool, _>("released"),
        )
        .unwrap();
    }
    pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    assert_golden("delivery_outcomes.golden", &out);
}
