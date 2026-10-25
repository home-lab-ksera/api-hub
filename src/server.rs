use crate::config::state::AppState;
use crate::errors::api_error::{ApiError, reason_phrase};
use crate::handlers::{health_handler, openapi_handler, s3_handler, slack_handler};
use crate::request_id;
use shiguredo_http11::{Request, Response, uri::percent_decode};
use tracing::{Instrument, debug, error, info, info_span, warn};
const S3_CORS_ALLOWED_ORIGIN: &str = "https://hitomi-upload-viewer.internal.qroksera.com";

pub fn router(state: AppState) -> axum::Router {
    axum::Router::new()
        .route("/s3/put_object_stream", axum::routing::any(handle_http))
        .route("/s3/preview/{*key}", axum::routing::any(handle_http))
        .fallback(handle_http)
        .with_state(state)
}

pub async fn serve(
    listener: tokio::net::TcpListener,
    state: AppState,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    use std::future::IntoFuture;
    let (stopped, waiting) = tokio::sync::oneshot::channel();
    let serve = axum::serve(listener, router(state))
        .with_graceful_shutdown(async move {
            shutdown.await;
            let _ = stopped.send(());
        })
        .into_future();
    tokio::pin!(serve);
    tokio::select! {
        result=&mut serve=>result,
        _=waiting=>tokio::time::timeout(std::time::Duration::from_secs(80),&mut serve).await
            .unwrap_or_else(|_|Err(std::io::Error::new(std::io::ErrorKind::TimedOut,"HTTP drain deadline exceeded"))),
    }
}

async fn handle_http(
    axum::extract::State(state): axum::extract::State<AppState>,
    request: axum::extract::Request,
) -> axum::response::Response {
    use axum::{
        body::{Body, to_bytes},
        http::{HeaderName, HeaderValue, StatusCode},
    };
    let (parts, body) = request.into_parts();
    let mut metadata = Request::new(parts.method.as_str(), &parts.uri.to_string());
    for (name, value) in &parts.headers {
        if let Ok(value) = value.to_str() {
            metadata.add_header(name.as_str(), value);
        }
    }
    let path = parts.uri.path();
    let id = metadata
        .get_header("x-request-id")
        .map(str::to_owned)
        .unwrap_or_else(request_id::generate_request_id);
    let result = if parts.method == axum::http::Method::POST && path == "/s3/put_object_stream" {
        crate::s3_upload::upload(body, &metadata, &state)
            .await
            .map(|r| (r, None))
    } else if parts.method == axum::http::Method::GET && path.starts_with("/s3/preview/") {
        match parse_preview_bucket_and_key(path) {
            Ok((bucket, key)) => {
                s3_handler::preview_object_stream(&state, bucket, key, &metadata.headers)
                    .await
                    .map(|reply| {
                        let mut head = Response::new(reply.status_code, reply.reason_phrase);
                        for (name, value) in reply.headers {
                            head.add_header(&name, &value);
                        }
                        // reqwest/axumのbackpressureで全量を保持せず転送する。
                        let stream = reply.body_stream.bytes_stream();
                        (head, Some(Body::from_stream(stream)))
                    })
            }
            Err(error) => Err(error),
        }
    } else {
        let limit = if path.contains("base64") || path.starts_with("/slack/upload/") {
            64 * 1024 * 1024
        } else {
            1024 * 1024
        };
        match tokio::time::timeout(std::time::Duration::from_secs(60), to_bytes(body, limit)).await
        {
            Ok(Ok(bytes)) => {
                metadata.body = bytes.to_vec();
                Ok((process_request(metadata.clone(), &state).await, None))
            }
            Ok(Err(error)) => {
                use std::error::Error as _;
                let too_large = error
                    .source()
                    .is_some_and(|source| source.is::<http_body_util::LengthLimitError>());
                Ok((
                    crate::errors::api_error::problem_details_response(
                        if too_large { 413 } else { 400 },
                        "Request body is incomplete or too large",
                    ),
                    None,
                ))
            }
            Err(_) => Ok((
                crate::errors::api_error::problem_details_response(408, "Request body timed out"),
                None,
            )),
        }
    };
    let (mut head, stream) = result.unwrap_or_else(|error| (error.into_response(), None));
    if stream.is_none() {
        apply_problem_details(&mut head);
    }
    if !head.has_header("Access-Control-Allow-Origin") {
        apply_s3_cors(path, &metadata, &mut head);
    }
    if !head.has_header("x-request-id") {
        head.add_header("x-request-id", &id);
    }
    let mut response =
        axum::response::Response::new(stream.unwrap_or_else(|| Body::from(head.body)));
    *response.status_mut() =
        StatusCode::from_u16(head.status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    for (name, value) in head.headers {
        if let (Ok(name), Ok(value)) = (name.parse::<HeaderName>(), value.parse::<HeaderValue>()) {
            response.headers_mut().append(name, value);
        }
    }
    response
}

// raw HTTP fixtureも本番と同じRouterを使う。wire処理はHyperへ任せる。
#[cfg(test)]
pub(crate) async fn handle_connection(stream: tokio::net::TcpStream, state: AppState) {
    let io = hyper_util::rt::TokioIo::new(stream);
    let service = hyper_util::service::TowerToHyperService::new(router(state));
    let _ = hyper::server::conn::http1::Builder::new()
        .half_close(true)
        .serve_connection(io, service)
        .await;
}
fn parse_preview_bucket_and_key(path: &str) -> Result<(String, String), ApiError> {
    let remaining = &path["/s3/preview/".len()..];
    let (bucket, key) = remaining
        .split_once('/')
        .ok_or_else(|| ApiError::BadRequest("Invalid preview path".to_string()))?;
    if bucket.is_empty() || key.is_empty() {
        return Err(ApiError::BadRequest("Invalid preview path".to_string()));
    }

    let decoded_bucket = percent_decode(bucket)
        .map_err(|_| ApiError::BadRequest("Invalid preview path".to_string()))?;
    let decoded_key = percent_decode(key)
        .map_err(|_| ApiError::BadRequest("Invalid preview path".to_string()))?;
    Ok((decoded_bucket, decoded_key))
}

async fn process_request(request: Request, app_state: &AppState) -> Response {
    let (path, query) = split_uri(&request.uri);
    let path = path.to_string();
    let query = query.map(ToString::to_string);
    let request_id = request
        .get_header("x-request-id")
        .map(ToString::to_string)
        .unwrap_or_else(request_id::generate_request_id);

    let method = request.method.clone();
    let user_agent = request.get_header("user-agent").map(ToString::to_string);
    let client_ip = request
        .get_header("x-forwarded-for")
        .map(ToString::to_string)
        .or_else(|| request.get_header("x-real-ip").map(ToString::to_string));
    let content_length = request.content_length();

    let span = info_span!(
        "http_request",
        request_id = %request_id,
        method = %method,
        path = %path,
        query = ?query,
        version = %request.version,
        user_agent = ?user_agent,
        client_ip = ?client_ip,
        content_length = ?content_length,
        status = tracing::field::Empty,
        latency_ms = tracing::field::Empty,
        error = tracing::field::Empty,
    );

    async move {
        info!(
            target: "http::request",
            request_id = %request_id,
            method = %method,
            path = %path,
            query = ?query,
            user_agent = ?user_agent,
            client_ip = ?client_ip,
            "Incoming request"
        );

        let start = std::time::Instant::now();
        let mut response =
            match Box::pin(route_request(&request, app_state, &path, query.as_deref())).await {
                Ok(response) => response,
                Err(error) => error.into_response(),
            };

        apply_problem_details(&mut response);
        apply_s3_cors(&path, &request, &mut response);
        if !response.has_header("x-request-id") {
            response.add_header("x-request-id", &request_id);
        }

        let latency_ms = start.elapsed().as_millis() as u64;
        let status = response.status_code;

        tracing::Span::current().record("status", status);
        tracing::Span::current().record("latency_ms", latency_ms);

        match status {
            200..=299 => info!(
                target: "http::response",
                request_id = %request_id,
                method = %method,
                path = %path,
                status,
                latency_ms,
                "Request completed"
            ),
            300..=399 => info!(
                target: "http::response",
                request_id = %request_id,
                method = %method,
                path = %path,
                status,
                latency_ms,
                "Request redirected"
            ),
            400..=499 => warn!(
                target: "http::response",
                request_id = %request_id,
                method = %method,
                path = %path,
                status,
                latency_ms,
                "Client error"
            ),
            500..=599 => {
                error!(
                    target: "http::response",
                    request_id = %request_id,
                    method = %method,
                    path = %path,
                    status,
                    latency_ms,
                    "Server error"
                );
                tracing::Span::current().record("error", "server_error");
            }
            _ => debug!(
                target: "http::response",
                request_id = %request_id,
                method = %method,
                path = %path,
                status,
                latency_ms,
                "Unexpected status"
            ),
        }

        response
    }
    .instrument(span)
    .await
}

async fn route_request(
    request: &Request,
    app_state: &AppState,
    path: &str,
    query: Option<&str>,
) -> Result<Response, ApiError> {
    if let Some(id) = path.strip_prefix("/notifications/") {
        if request.method == "GET" {
            return Ok(crate::notifications::handle_get(app_state, id).await);
        }
        if request.method == "POST" {
            if let Some(id) = id.strip_suffix("/retry") {
                return Ok(crate::notifications::handle_retry(app_state, id).await);
            }
        }
    }
    match (request.method.as_str(), path) {
        ("POST", "/notifications") => {
            return Ok(crate::notifications::handle_post(app_state, &request.body).await);
        }
        ("GET", "/health") => return Ok(health_handler::health()),
        ("GET", "/metrics") => return Ok(crate::queue_metrics::handle(app_state).await),
        ("GET", "/openapi.json") => return Ok(openapi_handler::openapi_json()),
        ("POST", "/slack/message/update") => {
            return slack_handler::update_message(app_state, &request.body).await;
        }
        ("POST", "/s3/copy_object") => {
            return s3_handler::copy_object(app_state, &request.body).await;
        }
        ("POST", "/s3/get_object_tagging") => {
            return s3_handler::get_object_tagging(app_state, &request.body).await;
        }
        ("POST", "/s3/put_object_tagging") => {
            return s3_handler::put_object_tagging(app_state, &request.body).await;
        }
        ("POST", "/s3/delete_object_tagging") => {
            return s3_handler::delete_object_tagging(app_state, &request.body).await;
        }
        ("POST", "/s3/put_object_and_notify") => {
            return s3_handler::put_object_and_notify(app_state, &request.body).await;
        }
        ("POST", "/slack/message") => {
            return slack_handler::post_message(app_state, &request.body).await;
        }
        ("POST", "/slack/upload/image") => {
            return slack_handler::upload_image_raw(
                app_state,
                query,
                request.headers.as_slice(),
                &request.body,
            )
            .await;
        }
        ("POST", "/slack/upload/pdf") => {
            return slack_handler::upload_pdf_raw(
                app_state,
                query,
                request.headers.as_slice(),
                &request.body,
            )
            .await;
        }
        ("POST", "/s3/put_object_base64") => {
            return s3_handler::put_object_base64(app_state, &request.body).await;
        }
        ("POST", "/s3/get_object_base64") => {
            return s3_handler::get_object_base64(app_state, &request.body).await;
        }
        ("POST", "/s3/head_object") => {
            return s3_handler::head_object(app_state, &request.body).await;
        }
        ("POST", "/s3/delete_object") => {
            return s3_handler::delete_object(app_state, &request.body).await;
        }
        ("POST", "/s3/delete_objects") => {
            return s3_handler::delete_objects(app_state, &request.body).await;
        }
        ("POST", "/s3/list_objects_v2") => {
            return s3_handler::list_objects_v2(app_state, &request.body).await;
        }
        ("POST", "/s3/create_multipart_upload") => {
            return s3_handler::create_multipart_upload(app_state, &request.body).await;
        }
        ("POST", "/s3/upload_part_base64") => {
            return s3_handler::upload_part_base64(app_state, &request.body).await;
        }
        ("POST", "/s3/complete_multipart_upload") => {
            return s3_handler::complete_multipart_upload(app_state, &request.body).await;
        }
        ("POST", "/s3/abort_multipart_upload") => {
            return s3_handler::abort_multipart_upload(app_state, &request.body).await;
        }
        ("POST", "/s3/list_parts") => {
            return s3_handler::list_parts(app_state, &request.body).await;
        }
        ("POST", "/s3/list_multipart_uploads") => {
            return s3_handler::list_multipart_uploads(app_state, &request.body).await;
        }
        ("POST", "/s3/presigned_get_object") => {
            return s3_handler::presigned_get_object(app_state, &request.body).await;
        }
        ("POST", "/s3/browser_get_object") => {
            return s3_handler::browser_get_object(app_state, &request.body).await;
        }
        ("GET", "/s3/browser_open") => {
            let fields: std::collections::HashMap<_, _> =
                url::form_urlencoded::parse(query.unwrap_or("").as_bytes())
                    .into_owned()
                    .collect();
            let body = serde_json::to_vec(&fields)
                .map_err(|_| ApiError::BadRequest("Invalid object".into()))?;
            let response = s3_handler::browser_get_object(app_state, &body).await?;
            if response.status_code != 200 {
                return Ok(response);
            }
            let value: serde_json::Value = serde_json::from_slice(&response.body)
                .map_err(|_| ApiError::InternalServerError("Invalid signed response".into()))?;
            let location = value["url"]
                .as_str()
                .ok_or_else(|| ApiError::InternalServerError("Missing signed URL".into()))?;
            return Ok(Response::new(307, "Temporary Redirect")
                .header("Location", location)
                .header("Cache-Control", "no-store")
                .header("Referrer-Policy", "no-referrer"));
        }
        ("POST", "/s3/presigned_put_object") => {
            return s3_handler::presigned_put_object(app_state, &request.body).await;
        }
        ("POST", "/s3/list_buckets") => {
            return s3_handler::list_buckets(app_state).await;
        }
        ("POST", "/s3/create_bucket") => {
            return s3_handler::create_bucket(app_state, &request.body).await;
        }
        ("POST", "/s3/head_bucket") => {
            return s3_handler::head_bucket(app_state, &request.body).await;
        }
        ("POST", "/s3/delete_bucket") => {
            return s3_handler::delete_bucket(app_state, &request.body).await;
        }
        ("OPTIONS", "/s3/list_objects_v2")
        | ("OPTIONS", "/s3/browser_get_object")
        | ("OPTIONS", "/s3/presigned_get_object")
        | ("OPTIONS", "/s3/put_object_stream") => {
            return Ok(s3_handler::s3_preflight());
        }
        _ => {}
    }

    if is_known_path(path) {
        return Err(ApiError::MethodNotAllowed(format!(
            "Method {} is not allowed for {}",
            request.method, path
        )));
    }

    Err(ApiError::NotFound(format!("Route not found: {}", path)))
}

fn split_uri(uri: &str) -> (&str, Option<&str>) {
    if let Some((path, query)) = uri.split_once('?') {
        (path, Some(query))
    } else {
        (uri, None)
    }
}

fn is_known_path(path: &str) -> bool {
    matches!(
        path,
        "/notifications"
            | "/health"
            | "/metrics"
            | "/openapi.json"
            | "/slack/message/update"
            | "/s3/copy_object"
            | "/s3/get_object_tagging"
            | "/s3/put_object_tagging"
            | "/s3/delete_object_tagging"
            | "/s3/put_object_and_notify"
            | "/slack/message"
            | "/slack/upload/image"
            | "/slack/upload/pdf"
            | "/s3/put_object_base64"
            | "/s3/put_object_stream"
            | "/s3/get_object_base64"
            | "/s3/head_object"
            | "/s3/delete_object"
            | "/s3/delete_objects"
            | "/s3/list_objects_v2"
            | "/s3/create_multipart_upload"
            | "/s3/upload_part_base64"
            | "/s3/complete_multipart_upload"
            | "/s3/abort_multipart_upload"
            | "/s3/list_parts"
            | "/s3/list_multipart_uploads"
            | "/s3/presigned_get_object"
            | "/s3/browser_get_object"
            | "/s3/browser_open"
            | "/s3/presigned_put_object"
            | "/s3/list_buckets"
            | "/s3/create_bucket"
            | "/s3/head_bucket"
            | "/s3/delete_bucket"
    ) || path.starts_with("/notifications/")
        || path.starts_with("/s3/preview/")
}

fn apply_problem_details(response: &mut Response) {
    if !(400..=599).contains(&response.status_code) {
        return;
    }

    let is_problem_details = response
        .get_header("content-type")
        .map(|value| value.starts_with("application/problem+json"))
        .unwrap_or(false);
    if is_problem_details {
        return;
    }

    *response = crate::errors::api_error::problem_details_response(
        response.status_code,
        reason_phrase(response.status_code),
    );
}

fn apply_s3_cors(path: &str, request: &Request, response: &mut Response) {
    if !path.starts_with("/s3/") {
        return;
    }

    let allow_origin = request
        .get_header("origin")
        .filter(|origin| *origin == S3_CORS_ALLOWED_ORIGIN)
        .unwrap_or(S3_CORS_ALLOWED_ORIGIN);

    response.add_header("Access-Control-Allow-Origin", allow_origin);
    response.add_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    response.add_header("Access-Control-Allow-Headers", "content-type, x-request-id");
    response.add_header("Access-Control-Max-Age", "600");
    response.add_header("Vary", "Origin");
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn preview_streams_range_before_upstream_finishes_and_reuses_connection() {
        use crate::config::{settings::Settings, state::AppState};
        use shiguredo_http11::RequestDecoder;
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
                let mut decoder = RequestDecoder::new();
                let mut wait = Some(wait);
                for index in 0..2 {
                    let request = loop {
                        if let Some(request) = decoder.decode().unwrap() { break request; }
                        let mut buffer = [0; 4096];
                        let n = socket.read(&mut buffer).await.unwrap();
                        assert_ne!(n, 0);
                        decoder.feed(&buffer[..n]).unwrap();
                    };
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
                settings: Settings {
                    slack_bot_token: String::new(), slack_api_base_url: String::new(),
                    s3_access_key_id: "test".into(), s3_secret_access_key: "test".into(),
                    s3_region: "us-east-1".into(), s3_endpoint: Some(endpoint), s3_browser_endpoint: None,
                    s3_use_path_style: true, s3_session_token: None,
                },
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
