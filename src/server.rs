//! HTTP サーバー。
//!
//! - `routes` — 経路の表
//! - `pipeline` — 要求ごとの処理の流れ（検証・呼び出し・後処理・アクセスログ）
//! - `guard` — ブラウザーからの要求の制限と CORS
//! - `endpoints` — エンドポイントごとの handler 呼び出し
//! - `lifecycle` — 起動・停止と task の監督

use crate::config::state::AppState;

mod endpoints;
mod guard;
mod lifecycle;
mod pipeline;
mod routes;

pub use lifecycle::{serve, serve_integrated};
#[cfg(test)]
pub(crate) use pipeline::handle_connection;
#[cfg(test)]
use pipeline::process_request;

/// 経路の判定は [`routes`] の表で行うため、axum にはすべての要求を1つの handler へ渡させる。
pub fn router(state: AppState) -> axum::Router {
    axum::Router::new()
        .fallback(pipeline::handle_http)
        .with_state(state)
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn preview_streams_range_before_upstream_finishes_and_reuses_connection() {
        use crate::config::{settings::Settings, state::AppState};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::{TcpListener, TcpStream},
            sync::oneshot,
            time::{Duration, timeout},
        };

        timeout(Duration::from_secs(5), async {
            let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", upstream.local_addr().unwrap());
            let (release, wait) = oneshot::channel();
            let payload: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
            let expected = payload.clone();
            let upstream_task = tokio::spawn(async move {
                let (mut socket, _) = upstream.accept().await.unwrap();
                let mut reader = crate::testing::RequestReader::new();
                let mut wait = Some(wait);
                for index in 0..2 {
                    let request = reader.next(&mut socket).await.unwrap();
                    if index == 0 {
                        assert_eq!(request.get_header("range"), Some("bytes=0-262143"));
                        let head = format!("HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes 0-262143/524288\r\nETag: \"test\"\r\n\r\n", payload.len());
                        socket.write_all(head.as_bytes()).await.unwrap();
                        socket.write_all(&payload[..3]).await.unwrap();
                        wait.take().unwrap().await.unwrap();
                        socket.write_all(&payload[3..]).await.unwrap();
                    } else {
                        assert_eq!(request.get_header("if-none-match"), Some("\"test\""));
                        socket.write_all(b"HTTP/1.1 304 Not Modified\r\nETag: \"test\"\r\n\r\n").await.unwrap();
                    }
                }
            });
            let state = AppState {
                database: None,
                client: crate::http_client::new(),
                settings: Settings::for_test(&endpoint),
            };
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut client = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
            let api_task = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                super::handle_connection(socket, state).await;
            });
            // Use a raw downstream socket so the new upstream fast path is
            // checked independently from the client's response decoder.
            client.write_all(b"GET /s3/preview/b/file.bin HTTP/1.1\r\nHost: localhost\r\nRange: bytes=0-262143\r\n\r\n").await.unwrap();
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") { head.push(client.read_u8().await.unwrap()); }
            let head = String::from_utf8(head).unwrap();
            assert!(head.starts_with("HTTP/1.1 206"));
            assert!(head.to_ascii_lowercase().contains("content-range: bytes 0-262143/524288"));
            let mut actual = vec![0; expected.len()];
            client.read_exact(&mut actual[..3]).await.unwrap();
            assert_eq!(&actual[..3], &expected[..3]);
            release.send(()).unwrap();
            client.read_exact(&mut actual[3..]).await.unwrap();
            assert_eq!(actual, expected);
            client.write_all(b"GET /s3/preview/b/file.bin HTTP/1.1\r\nHost: localhost\r\nIf-None-Match: \"test\"\r\nConnection: close\r\n\r\n").await.unwrap();
            let mut response = Vec::new();
            client.read_to_end(&mut response).await.unwrap();
            assert!(response.starts_with(b"HTTP/1.1 304"));
            assert!(response.ends_with(b"\r\n\r\n"));
            upstream_task.await.unwrap();
            api_task.await.unwrap();
        }).await.unwrap();
    }
}

#[cfg(test)]
mod extensions_tests;
