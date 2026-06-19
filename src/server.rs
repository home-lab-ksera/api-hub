use crate::config::state::AppState;
use crate::errors::api_error::{ApiError, reason_phrase};
use crate::handlers::{health_handler, openapi_handler, s3_handler, slack_handler};
use crate::request_id;
use shiguredo_http11::uri::percent_decode;
use shiguredo_http11::{Request, RequestDecoder, Response};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{Instrument, debug, error, info, info_span, warn};

const S3_CORS_ALLOWED_ORIGIN: &str = "https://hitomi-upload-viewer.internal.qroksera.com";

pub async fn handle_connection(mut stream: TcpStream, app_state: AppState) {
    let mut decoder = RequestDecoder::new();
    let mut buffer = vec![0_u8; 8192];

    loop {
        let request = loop {
            match decoder.decode() {
                Ok(Some(request)) => break request,
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

            let mut chunk = vec![0_u8; 16 * 1024];
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
            }
            stream.flush().await?;

            let latency_ms = start.elapsed().as_millis() as u64;
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
    match (request.method.as_str(), path) {
        ("GET", "/health") => return Ok(health_handler::health()),
        ("GET", "/openapi.json") => return Ok(openapi_handler::openapi_json()),
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
        ("OPTIONS", "/s3/list_objects_v2") | ("OPTIONS", "/s3/presigned_get_object") => {
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
        "/health"
            | "/openapi.json"
            | "/slack/message"
            | "/slack/upload/image"
            | "/slack/upload/pdf"
            | "/s3/put_object_base64"
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
            | "/s3/presigned_put_object"
            | "/s3/list_buckets"
            | "/s3/create_bucket"
            | "/s3/head_bucket"
            | "/s3/delete_bucket"
    ) || path.starts_with("/s3/preview/")
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
    use super::{S3_CORS_ALLOWED_ORIGIN, parse_preview_bucket_and_key, process_request};
    use crate::{
        config::{settings::Settings, state::AppState},
        http_client::HttpClient,
    };
    use shiguredo_http11::Request;

    fn app_state() -> AppState {
        AppState {
            settings: Settings {
                slack_bot_token: "xoxb-test".to_string(),
                slack_api_base_url: "https://slack.com/api".to_string(),
                s3_access_key_id: "access-key".to_string(),
                s3_secret_access_key: "secret-key".to_string(),
                s3_region: "us-east-1".to_string(),
                s3_endpoint: Some("http://127.0.0.1:9000".to_string()),
                s3_use_path_style: true,
                s3_session_token: None,
            },
            client: HttpClient::new(),
        }
    }

    #[tokio::test]
    async fn unknown_path_returns_404_problem_details() {
        let response = process_request(Request::new("GET", "/missing"), &app_state()).await;

        assert_eq!(response.status_code, 404);
        assert_eq!(
            response.get_header("content-type"),
            Some("application/problem+json")
        );
    }

    #[tokio::test]
    async fn known_path_with_invalid_method_returns_405() {
        let response = process_request(Request::new("POST", "/health"), &app_state()).await;

        assert_eq!(response.status_code, 405);
        assert_eq!(
            response.get_header("content-type"),
            Some("application/problem+json")
        );
    }

    #[tokio::test]
    async fn request_id_is_generated_when_missing() {
        let response = process_request(Request::new("GET", "/health"), &app_state()).await;

        assert!(response.get_header("x-request-id").is_some());
    }

    #[tokio::test]
    async fn request_id_is_preserved_when_present() {
        let request = Request::new("GET", "/health").header("x-request-id", "req-test");
        let response = process_request(request, &app_state()).await;

        assert_eq!(response.get_header("x-request-id"), Some("req-test"));
    }

    #[tokio::test]
    async fn s3_preflight_is_limited_to_declared_endpoints() {
        let response =
            process_request(Request::new("OPTIONS", "/s3/list_objects_v2"), &app_state()).await;
        assert_eq!(response.status_code, 204);
        assert_eq!(
            response.get_header("Access-Control-Allow-Origin"),
            Some(S3_CORS_ALLOWED_ORIGIN)
        );
        assert_eq!(
            response.get_header("Access-Control-Allow-Methods"),
            Some("GET, POST, OPTIONS")
        );

        let response =
            process_request(Request::new("OPTIONS", "/s3/head_object"), &app_state()).await;
        assert_eq!(response.status_code, 405);
        assert_eq!(
            response.get_header("Access-Control-Allow-Origin"),
            Some(S3_CORS_ALLOWED_ORIGIN)
        );
    }

    #[tokio::test]
    async fn s3_error_responses_include_cors() {
        let response = process_request(Request::new("GET", "/s3/not-a-route"), &app_state()).await;

        assert_eq!(response.status_code, 404);
        assert_eq!(
            response.get_header("Access-Control-Allow-Origin"),
            Some(S3_CORS_ALLOWED_ORIGIN)
        );
        assert_eq!(
            response.get_header("content-type"),
            Some("application/problem+json")
        );
    }

    #[test]
    fn preview_path_decodes_bucket_and_key() {
        let (bucket, key) =
            parse_preview_bucket_and_key("/s3/preview/my%20bucket/path%2Fto%2Fa.pdf")
                .expect("preview path should parse");

        assert_eq!(bucket, "my bucket");
        assert_eq!(key, "path/to/a.pdf");
    }

    #[test]
    fn preview_path_rejects_missing_or_invalid_segments() {
        assert!(parse_preview_bucket_and_key("/s3/preview/bucket").is_err());
        assert!(parse_preview_bucket_and_key("/s3/preview//key").is_err());
        assert!(parse_preview_bucket_and_key("/s3/preview/bucket/").is_err());
        assert!(parse_preview_bucket_and_key("/s3/preview/bucket/%ZZ").is_err());
    }
}
