use crate::config::state::AppState;
use crate::errors::api_error::{ApiError, reason_phrase};
use crate::handlers::{health_handler, openapi_handler, s3_handler, slack_handler};
use crate::request_id;
use shiguredo_http11::uri::percent_decode;
use shiguredo_http11::{BodyProgress, DecoderLimits, Request, RequestDecoder, Response};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{Instrument, debug, error, info, info_span, warn};

const S3_CORS_ALLOWED_ORIGIN: &str = "https://hitomi-upload-viewer.internal.qroksera.com";

pub async fn handle_connection(mut stream: TcpStream, app_state: AppState) {
    if let Err(error) = stream.set_nodelay(true) {
        warn!(%error, "Failed to enable TCP_NODELAY");
    }
    let mut decoder = request_decoder();
    let mut buffer = vec![0_u8; 8192];

    loop {
        let request = loop {
            match decoder.decode_headers() {
                Ok(Some((head, _))) => {
                    break Request {
                        method: head.method,
                        uri: head.uri,
                        version: head.version,
                        headers: head.headers,
                        body: Vec::new(),
                    };
                }
                Ok(None) => {
                    let n = match stream.read(&mut buffer).await {
                        Ok(n) => n,
                        Err(e) => {
                            error!(error = %e, "Failed to read from socket");
                            return;
                        }
                    };
                    if n == 0 {
                        return;
                    }
                    if let Err(e) = decoder.feed(&buffer[..n]) {
                        let response = ApiError::BadRequest(format!("Invalid HTTP request: {e}"))
                            .into_response();
                        let _ = write_response(&mut stream, response).await;
                        return;
                    }
                }
                Err(e) => {
                    let response =
                        ApiError::BadRequest(format!("Invalid HTTP request: {e}")).into_response();
                    let _ = write_response(&mut stream, response).await;
                    return;
                }
            }
        };

        let keep_alive = request.is_keep_alive();

        if request.method == "POST" && split_uri(&request.uri).0 == "/s3/put_object_stream" {
            let request_id = request
                .get_header("x-request-id")
                .map(str::to_owned)
                .unwrap_or_else(request_id::generate_request_id);
            let start = std::time::Instant::now();
            let result =
                crate::s3_upload::upload(&mut stream, &mut decoder, &request, &app_state).await;
            let failed = result.is_err();
            if let Err(error) = &result {
                warn!(%error, request_id = %request_id, "Streaming upload failed");
            }
            let mut response = result.unwrap_or_else(ApiError::into_response);
            apply_problem_details(&mut response);
            apply_s3_cors("/s3/put_object_stream", &request, &mut response);
            response.add_header("x-request-id", &request_id);
            info!(operation = "s3_stream_upload", status = response.status_code,
                latency_ms = start.elapsed().as_millis() as u64, request_id = %request_id);
            // On errors the request body may not have been consumed.
            if failed || !keep_alive {
                response.add_header("Connection", "close");
            }
            if write_response(&mut stream, response).await.is_err() || failed || !keep_alive {
                return;
            }
            continue;
        }

        let mut request = request;
        loop {
            match read_body_chunk(&mut stream, &mut decoder).await {
                Ok(Some(chunk)) => request.body.extend_from_slice(&chunk),
                Ok(None) => break,
                Err(error) => {
                    let _ = write_response(&mut stream, error.into_response()).await;
                    return;
                }
            }
        }

        if is_preview_get(&request) {
            if write_preview_stream_response(&mut stream, request, &app_state, keep_alive)
                .await
                .is_err()
            {
                return;
            }
            if !keep_alive {
                return;
            }
            continue;
        }

        let mut response = process_request(request, &app_state).await;

        if !keep_alive {
            response.add_header("Connection", "close");
        }

        if write_response(&mut stream, response).await.is_err() {
            return;
        }
        if !keep_alive {
            return;
        }
    }
}

pub(crate) async fn read_body_chunk(
    stream: &mut TcpStream,
    decoder: &mut RequestDecoder,
) -> Result<Option<Vec<u8>>, ApiError> {
    loop {
        if let Some(data) = decoder.peek_body().filter(|data| !data.is_empty()) {
            let chunk = data.to_vec();
            decoder
                .consume_body(chunk.len())
                .map_err(|e| ApiError::BadRequest(format!("Invalid HTTP body: {e}")))?;
            return Ok(Some(chunk));
        }
        let buffered_before = decoder.remaining().len();
        if matches!(
            decoder
                .progress()
                .map_err(|e| ApiError::BadRequest(format!("Invalid HTTP body: {e}")))?,
            BodyProgress::Complete { .. }
        ) {
            return Ok(None);
        }
        // progress() may have exposed chunk data already buffered by the decoder.
        if decoder.remaining().len() != buffered_before
            || decoder.peek_body().is_some_and(|data| !data.is_empty())
        {
            continue;
        }
        let mut buffer = [0; 8192];
        let n = stream
            .read(&mut buffer)
            .await
            .map_err(|e| ApiError::BadRequest(format!("Failed to read HTTP body: {e}")))?;
        if n == 0 {
            return Err(ApiError::BadRequest("Incomplete HTTP body".into()));
        }
        decoder
            .feed(&buffer[..n])
            .map_err(|e| ApiError::BadRequest(format!("Invalid HTTP body: {e}")))?;
    }
}

fn request_decoder() -> RequestDecoder {
    // Accept uploads of any size, retaining the HTTP header and framing limits.
    RequestDecoder::with_limits(DecoderLimits {
        max_body_size: usize::MAX,
        max_buffer_size: usize::MAX,
        ..DecoderLimits::default()
    })
}

async fn write_response(stream: &mut TcpStream, response: Response) -> std::io::Result<()> {
    let encoded = response.encode();

    stream.write_all(&encoded).await?;
    stream.flush().await
}

fn is_preview_get(request: &Request) -> bool {
    let (path, _) = split_uri(&request.uri);
    request.method == "GET" && path.starts_with("/s3/preview/")
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

async fn write_preview_stream_response(
    stream: &mut TcpStream,
    request: Request,
    app_state: &AppState,
    keep_alive: bool,
) -> std::io::Result<()> {
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

    let result = async {
        let (bucket, key) = parse_preview_bucket_and_key(&path)?;
        s3_handler::preview_object_stream(app_state, bucket, key, request.headers.as_slice()).await
    }
    .await;

    match result {
        Ok(mut stream_response) => {
            let upstream_headers_ms = start.elapsed().as_millis() as u64;
            let mut head =
                Response::new(stream_response.status_code, stream_response.reason_phrase)
                    .omit_body(true);
            for (name, value) in &stream_response.headers {
                head.add_header(name, value);
            }
            apply_s3_cors(&path, &request, &mut head);
            if !head.has_header("x-request-id") {
                head.add_header("x-request-id", &request_id);
            }
            if !keep_alive {
                head.add_header("Connection", "close");
            }

            let encoded_head = head.encode();
            stream.write_all(&encoded_head).await?;

            let transfer_start = std::time::Instant::now();
            let mut bytes_sent = 0_u64;
            let mut chunk = vec![0_u8; 64 * 1024];
            loop {
                let n = stream_response
                    .body_stream
                    .read_chunk(&mut chunk)
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                if n == 0 {
                    break;
                }
                stream.write_all(&chunk[..n]).await?;
                bytes_sent += n as u64;
            }
            stream.flush().await?;

            let latency_ms = start.elapsed().as_millis() as u64;
            info!(
                target: "s3::download",
                request_id = %request_id,
                status = stream_response.status_code,
                upstream_headers_ms,
                transfer_ms = transfer_start.elapsed().as_millis() as u64,
                bytes_sent,
                "Download transfer completed"
            );
            match stream_response.status_code {
                200..=299 => info!(
                    target: "http::response",
                    request_id = %request_id,
                    method = %method,
                    path = %path,
                    status = stream_response.status_code,
                    latency_ms,
                    "Request completed"
                ),
                300..=399 => info!(
                    target: "http::response",
                    request_id = %request_id,
                    method = %method,
                    path = %path,
                    status = stream_response.status_code,
                    latency_ms,
                    "Request redirected"
                ),
                400..=499 => warn!(
                    target: "http::response",
                    request_id = %request_id,
                    method = %method,
                    path = %path,
                    status = stream_response.status_code,
                    latency_ms,
                    "Client error"
                ),
                500..=599 => error!(
                    target: "http::response",
                    request_id = %request_id,
                    method = %method,
                    path = %path,
                    status = stream_response.status_code,
                    latency_ms,
                    "Server error"
                ),
                _ => debug!(
                    target: "http::response",
                    request_id = %request_id,
                    method = %method,
                    path = %path,
                    status = stream_response.status_code,
                    latency_ms,
                    "Unexpected status"
                ),
            }

            Ok(())
        }
        Err(error) => {
            let mut response = error.into_response();
            apply_problem_details(&mut response);
            apply_s3_cors(&path, &request, &mut response);
            if !response.has_header("x-request-id") {
                response.add_header("x-request-id", &request_id);
            }
            if !keep_alive {
                response.add_header("Connection", "close");
            }

            let status = response.status_code;
            write_response(stream, response).await?;

            let latency_ms = start.elapsed().as_millis() as u64;
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
                500..=599 => error!(
                    target: "http::response",
                    request_id = %request_id,
                    method = %method,
                    path = %path,
                    status,
                    latency_ms,
                    "Server error"
                ),
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

            Ok(())
        }
    }
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
        let mut response = match route_request(&request, app_state, &path, query.as_deref()).await {
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
    use super::request_decoder;

    #[tokio::test]
    async fn preview_streams_range_before_upstream_finishes_and_reuses_connection() {
        use crate::{
            config::{settings::Settings, state::AppState},
            http_client::HttpClient,
        };
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
                client: HttpClient::new(),
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

    #[test]
    fn accepts_uploads_larger_than_default_limit() {
        let body = vec![b'a'; 11 * 1024 * 1024];
        for chunked in [false, true] {
            let mut decoder = request_decoder();
            let mut encoded = if chunked {
                format!(
                    "POST /s3/upload_part_base64 HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n",
                    body.len()
                ).into_bytes()
            } else {
                format!(
                    "POST /s3/put_object_base64 HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
                    body.len()
                ).into_bytes()
            };
            encoded.extend_from_slice(&body);
            if chunked {
                encoded.extend_from_slice(b"\r\n0\r\n\r\n");
            }
            // A large feed also verifies that the receive buffer has no size cap.
            decoder.feed(&encoded).unwrap();
            let request = decoder.decode().unwrap().unwrap();
            assert_eq!(request.body, body);
        }
    }
}

#[cfg(test)]
mod extensions_tests;
