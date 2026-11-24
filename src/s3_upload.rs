use crate::reply::{Reply, ReplyExt, reply};
use futures_util::StreamExt;
use shiguredo_http11::uri::percent_decode;

use crate::{
    config::state::AppState,
    errors::api_error::ApiError,
    service::s3_service::{self, PutObjectInput, StreamingUpload},
};

const PART_SIZE: usize = 8 * 1024 * 1024;

fn parse_target(uri: &str) -> Result<(String, String), ApiError> {
    let mut bucket = None;
    let mut key = None;
    for pair in uri
        .split_once('?')
        .map(|(_, q)| q)
        .unwrap_or_default()
        .split('&')
    {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        let decode = |value: &str| {
            percent_decode(&value.replace('+', " "))
                .map_err(|_| ApiError::BadRequest("Invalid upload query string".into()))
        };
        match decode(name)?.as_str() {
            "bucket" => bucket = Some(decode(value)?),
            "key" => key = Some(decode(value)?),
            _ => {}
        }
    }
    let required = |value: Option<String>, name: &str| {
        value.filter(|s| !s.trim().is_empty()).ok_or_else(|| {
            ApiError::BadRequest(format!("Missing required query parameter '{name}'"))
        })
    };
    Ok((required(bucket, "bucket")?, required(key, "key")?))
}

pub(crate) async fn upload(
    body: axum::body::Body,
    uri: &axum::http::Uri,
    headers: &axum::http::HeaderMap,
    state: &AppState,
) -> Result<Reply, ApiError> {
    let (bucket, key) = parse_target(&uri.to_string())?;
    let content_type =
        crate::reply::request_header(headers, "content-type").unwrap_or("application/octet-stream");
    let mut session: Option<StreamingUpload> = None;
    let mut chunks = body.into_data_stream();
    let result = async {
        let mut part = Vec::with_capacity(PART_SIZE);
        while let Some(chunk) =
            tokio::time::timeout(std::time::Duration::from_secs(60), chunks.next())
                .await
                .map_err(|_| ApiError::BadRequest("Upload stalled".into()))?
        {
            let chunk = chunk.map_err(|_| ApiError::BadRequest("Incomplete HTTP body".into()))?;
            let mut remaining = chunk.as_ref();
            while !remaining.is_empty() {
                let count = remaining.len().min(PART_SIZE - part.len());
                part.extend_from_slice(&remaining[..count]);
                remaining = &remaining[count..];
                if part.len() == PART_SIZE {
                    if session.is_none() {
                        session = Some(
                            StreamingUpload::start(
                                &state.client,
                                &state.settings,
                                bucket.clone(),
                                key.clone(),
                                content_type,
                            )
                            .await?,
                        );
                    }
                    session
                        .as_mut()
                        .unwrap()
                        .write_part(
                            &state.client,
                            std::mem::replace(&mut part, Vec::with_capacity(PART_SIZE)),
                        )
                        .await?;
                }
            }
        }
        if let Some(upload) = session.as_mut() {
            if !part.is_empty() {
                upload.write_part(&state.client, part).await?;
            }
            let output = upload.finish(&state.client).await?;
            Ok((output.e_tag, output.version_id))
        } else {
            // Small and empty objects need no multipart session.
            let output = s3_service::put_object(
                &state.client,
                &state.settings,
                PutObjectInput {
                    bucket,
                    key,
                    body: part,
                    content_type: Some(content_type.into()),
                },
            )
            .await?;
            Ok((output.e_tag, output.version_id))
        }
    }
    .await;
    if result.is_err()
        && let Some(upload) = session
        && let Err(error) = upload.abort(&state.client).await
    {
        tracing::warn!(%error, "Failed to abort streaming multipart upload");
    }
    // 小さいファイルでも multipart でも、同じ形で返す。
    result.map(|(e_tag, version_id)| {
        let body = nojson::json(|f| {
            f.object(|f| {
                f.member("e_tag", &e_tag)?;
                f.member("version_id", &version_id)
            })
        });
        reply(200)
            .header("Content-Type", "application/json")
            .with_body(body.to_string().into_bytes())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::settings::Settings;
    use shiguredo_http11::{Request, Response};
    use tokio::net::TcpStream;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::oneshot,
        time::{Duration, timeout},
    };

    const CREATED: &str = "<InitiateMultipartUploadResult><Bucket>b</Bucket><Key>folder/a +.mp4</Key><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>";
    const COMPLETED: &str = "<CompleteMultipartUploadResult><Bucket>b</Bucket><Key>folder/a +.mp4</Key><ETag>final-tag</ETag></CompleteMultipartUploadResult>";

    async fn fixture(
        replies: Vec<(u16, &'static str)>,
    ) -> (
        TcpStream,
        oneshot::Receiver<()>,
        tokio::task::JoinHandle<Vec<Request>>,
    ) {
        let s3 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", s3.local_addr().unwrap());
        let (part_tx, part_rx) = oneshot::channel();
        let s3_task = tokio::spawn(async move {
            let mut requests = Vec::new();
            let mut part_tx = Some(part_tx);
            for (status, body) in replies {
                let (mut socket, _) = s3.accept().await.unwrap();
                let request = crate::testing::read_request(&mut socket).await.unwrap();
                let is_part = request.method == "PUT";
                requests.push(request);
                let response = Response::new(status, "Mock")
                    .header("ETag", "part-tag")
                    .header("Connection", "close")
                    .body(body.as_bytes().to_vec());
                socket.write_all(&response.encode()).await.unwrap();
                if is_part && let Some(tx) = part_tx.take() {
                    let _ = tx.send(());
                }
            }
            requests
        });
        let state = AppState {
            database: None,
            client: crate::http_client::new(),
            settings: Settings::for_test(&endpoint),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            crate::server::handle_connection(socket, state).await;
        });
        (client, part_rx, s3_task)
    }

    async fn response(client: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        timeout(Duration::from_secs(10), client.read_to_end(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        String::from_utf8(bytes).unwrap()
    }

    #[tokio::test]
    async fn forwards_parts_before_full_body_arrives_with_both_framings() {
        for chunked in [false, true] {
            let (mut client, part_rx, s3) =
                fixture(vec![(200, CREATED), (200, ""), (200, ""), (200, COMPLETED)]).await;
            let framing = if chunked {
                "Transfer-Encoding: chunked".into()
            } else {
                format!("Content-Length: {}", PART_SIZE + 3)
            };
            let headers = format!(
                "POST /s3/put_object_stream?bucket=b&key=folder%2Fa+%2B.mp4 HTTP/1.1\r\nHost: localhost\r\nContent-Type: video/mp4\r\nConnection: close\r\n{framing}\r\n\r\n"
            );
            client.write_all(headers.as_bytes()).await.unwrap();
            if chunked {
                client
                    .write_all(format!("{PART_SIZE:x}\r\n").as_bytes())
                    .await
                    .unwrap();
            }
            client.write_all(&vec![0xab; PART_SIZE]).await.unwrap();
            // This must complete before the client sends the rest of the video.
            timeout(Duration::from_secs(10), part_rx)
                .await
                .unwrap()
                .unwrap();
            client
                .write_all(if chunked {
                    b"\r\n3\r\nend\r\n0\r\n\r\n"
                } else {
                    b"end"
                })
                .await
                .unwrap();
            let response = response(&mut client).await;
            assert!(response.starts_with("HTTP/1.1 200"), "{response}");
            assert!(response.contains("final-tag"));
            let requests = s3.await.unwrap();
            assert_eq!(requests[0].get_header("content-type"), Some("video/mp4"));
            assert!(
                requests[0].uri.starts_with("/b/folder/a%20%2B.mp4?"),
                "{}",
                requests[0].uri
            );
            assert_eq!(requests[1].body, vec![0xab; PART_SIZE]);
            assert_eq!(requests[2].body, b"end");
            assert!(String::from_utf8_lossy(&requests[3].body).contains("part-tag"));
        }
    }

    #[tokio::test]
    async fn aborts_on_disconnect_and_s3_part_failure() {
        for s3_failure in [false, true] {
            let (mut client, part_rx, s3) = fixture(vec![
                (200, CREATED),
                (
                    if s3_failure { 500 } else { 200 },
                    if s3_failure {
                        "<Error><Code>InternalError</Code><Message>failed</Message></Error>"
                    } else {
                        ""
                    },
                ),
                (204, ""),
            ])
            .await;
            client.write_all(format!("POST /s3/put_object_stream?bucket=b&key=k HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n", PART_SIZE + 10).as_bytes()).await.unwrap();
            client.write_all(&vec![0; PART_SIZE]).await.unwrap();
            timeout(Duration::from_secs(10), part_rx)
                .await
                .unwrap()
                .unwrap();
            client.shutdown().await.unwrap();
            let response = response(&mut client).await;
            assert!(
                response.starts_with(if s3_failure {
                    "HTTP/1.1 500"
                } else {
                    "HTTP/1.1 400"
                }),
                "{response}"
            );
            let requests = s3.await.unwrap();
            assert_eq!(requests[2].method, "DELETE");
            assert!(requests[2].uri.contains("uploadId=upload-1"));
        }
    }

    #[tokio::test]
    async fn small_and_empty_objects_preserve_keep_alive_and_pipelined_requests() {
        for body in [b"small".as_slice(), b""] {
            let (mut client, _, s3) = fixture(vec![(200, "")]).await;
            client.write_all(format!("POST /s3/put_object_stream?bucket=b&key=k HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n", body.len()).as_bytes()).await.unwrap();
            client.write_all(body).await.unwrap();
            client
                .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            let response = response(&mut client).await;
            assert_eq!(response.matches("HTTP/1.1 200").count(), 2, "{response}");
            assert_eq!(s3.await.unwrap()[0].body, body);
        }
    }

    #[tokio::test]
    async fn handles_exact_part_boundary_and_completion_errors() {
        for completion_error in [false, true] {
            let mut replies = vec![
                (200, CREATED),
                (200, ""),
                (
                    200,
                    if completion_error {
                        "<Error><Code>InternalError</Code><Message>completion failed</Message></Error>"
                    } else {
                        COMPLETED
                    },
                ),
            ];
            if completion_error {
                replies.push((204, ""));
            }
            let (mut client, _, s3) = fixture(replies).await;
            client.write_all(format!("POST /s3/put_object_stream?bucket=b&key=k HTTP/1.1\r\nHost: localhost\r\nContent-Length: {PART_SIZE}\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
            client.write_all(&vec![0; PART_SIZE]).await.unwrap();
            let response = response(&mut client).await;
            assert!(
                response.starts_with(if completion_error {
                    "HTTP/1.1 500"
                } else {
                    "HTTP/1.1 200"
                }),
                "{response}"
            );
            let requests = s3.await.unwrap();
            assert_eq!(requests.iter().filter(|r| r.method == "PUT").count(), 1);
            if completion_error {
                assert_eq!(requests.last().unwrap().method, "DELETE");
            }
        }
    }
}
