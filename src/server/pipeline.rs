//! HTTP の処理の流れ。要求ごとに1回だけ、検証・呼び出し・後処理・アクセスログを行う。

use super::endpoints::{parse_preview_bucket_and_key, route_request};
use super::guard::{apply_s3_cors, check_request_source, is_json_request};
use super::routes::{self, Body, Endpoint, Lookup};
use crate::config::state::AppState;
use crate::errors::api_error::{ApiError, problem_details_response, reason_phrase};
use crate::handlers::s3_handler;
use crate::request_id;
use axum::body::Body as AxumBody;
use shiguredo_http11::{Request, Response};
use tracing::{Instrument, debug, error, info, info_span, warn};

pub(super) async fn handle_http(
    axum::extract::State(state): axum::extract::State<AppState>,
    request: axum::extract::Request,
) -> axum::response::Response {
    use axum::http::{HeaderName, HeaderValue, StatusCode};
    let (parts, body) = request.into_parts();
    let mut metadata = Request::new(parts.method.as_str(), &parts.uri.to_string());
    for (name, value) in &parts.headers {
        if let Ok(value) = value.to_str() {
            metadata.add_header(name.as_str(), value);
        }
    }
    let context = RequestContext::new(&metadata, parts.uri.path(), parts.uri.query());
    let span = context.span(&metadata);
    async move {
        context.log_incoming();
        let start = std::time::Instant::now();
        let (mut head, stream) = dispatch(&state, &parts.method, &parts.uri, body, &mut metadata)
            .await
            .unwrap_or_else(|error| (error.into_response(), None));
        finish_response(&mut head, stream.is_some(), &context);
        context.log_completed(head.status_code, start.elapsed());

        let mut response =
            axum::response::Response::new(stream.unwrap_or_else(|| AxumBody::from(head.body)));
        *response.status_mut() =
            StatusCode::from_u16(head.status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        for (name, value) in head.headers {
            if let (Ok(name), Ok(value)) =
                (name.parse::<HeaderName>(), value.parse::<HeaderValue>())
            {
                response.headers_mut().append(name, value);
            }
        }
        response
    }
    .instrument(span)
    .await
}

/// 要求の検証・本文の読み込み・handler の呼び出しまで。応答の後処理は呼び出し側で1回だけ行う。
/// 本文を流す応答は2つ目の値に本文 stream を返す。
async fn dispatch(
    state: &AppState,
    method: &axum::http::Method,
    uri: &axum::http::Uri,
    body: AxumBody,
    metadata: &mut Request,
) -> Result<(Response, Option<AxumBody>), ApiError> {
    use axum::body::to_bytes;
    let (path, query) = (uri.path(), uri.query());
    check_request_source(method, path, metadata)?;
    let lookup = routes::lookup(method.as_str(), path);
    if let Lookup::Found(route, param) = lookup
        && route.body == Body::Stream
    {
        return match route.endpoint {
            Endpoint::S3PutObjectStream => crate::s3_upload::upload(body, metadata, state)
                .await
                .map(|r| (r, None)),
            Endpoint::S3Preview => {
                let (bucket, key) = parse_preview_bucket_and_key(param)?;
                let reply =
                    s3_handler::preview_object_stream(state, bucket, key, &metadata.headers)
                        .await?;
                let mut head = Response::new(reply.status_code, reason_phrase(reply.status_code));
                for (name, value) in reply.headers {
                    head.add_header(&name, &value);
                }
                // reqwest/axumのbackpressureで全量を保持せず転送する。
                let stream = reply.body_stream.bytes_stream();
                Ok((head, Some(AxumBody::from_stream(stream))))
            }
            other => Err(ApiError::InternalServerError(format!(
                "no streaming handler for {other:?}"
            ))),
        };
    }
    let (limit, json_only) = match lookup.body() {
        Body::Json(limit) => (limit, true),
        Body::Raw(limit) => (limit, false),
        Body::Stream => unreachable!("streaming routes are handled above"),
    };
    let bytes =
        match tokio::time::timeout(std::time::Duration::from_secs(60), to_bytes(body, limit)).await
        {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(error)) => {
                use std::error::Error as _;
                let too_large = error
                    .source()
                    .is_some_and(|source| source.is::<http_body_util::LengthLimitError>());
                return Ok((
                    problem_details_response(
                        if too_large { 413 } else { 400 },
                        "Request body is incomplete or too large",
                    ),
                    None,
                ));
            }
            Err(_) => {
                return Ok((
                    problem_details_response(408, "Request body timed out"),
                    None,
                ));
            }
        };
    if json_only && !is_json_request(method, &bytes, metadata) {
        return Ok((
            problem_details_response(415, "Content-Type must be application/json"),
            None,
        ));
    }
    metadata.body = bytes.to_vec();
    let response = route_request(metadata, state, path, query).await?;
    Ok((response, None))
}

/// すべての応答に1回だけ適用する後処理。
fn finish_response(response: &mut Response, streamed: bool, context: &RequestContext) {
    // 本文を流す応答は上流の本文をそのまま返すため、problem details へ置き換えない。
    if !streamed {
        apply_problem_details(response);
    }
    apply_s3_cors(&context.path, response);
    response.add_header("x-request-id", &context.request_id);
}

/// アクセスログに使う要求の属性。
struct RequestContext {
    request_id: String,
    method: String,
    path: String,
    query: Option<String>,
    user_agent: Option<String>,
    client_ip: Option<String>,
}

impl RequestContext {
    fn new(request: &Request, path: &str, query: Option<&str>) -> Self {
        let header = |name: &str| request.get_header(name).map(ToString::to_string);
        Self {
            request_id: header("x-request-id").unwrap_or_else(request_id::generate_request_id),
            method: request.method.clone(),
            path: path.to_string(),
            query: query.map(ToString::to_string),
            user_agent: header("user-agent"),
            client_ip: header("x-forwarded-for").or_else(|| header("x-real-ip")),
        }
    }

    fn span(&self, request: &Request) -> tracing::Span {
        info_span!(
            "http_request",
            request_id = %self.request_id,
            method = %self.method,
            path = %self.path,
            query = ?self.query,
            version = %request.version,
            user_agent = ?self.user_agent,
            client_ip = ?self.client_ip,
            content_length = ?request.content_length(),
            status = tracing::field::Empty,
            latency_ms = tracing::field::Empty,
            error = tracing::field::Empty,
        )
    }

    fn log_incoming(&self) {
        info!(
            target: "http::request",
            request_id = %self.request_id,
            method = %self.method,
            path = %self.path,
            query = ?self.query,
            user_agent = ?self.user_agent,
            client_ip = ?self.client_ip,
            "Incoming request"
        );
    }

    /// 本文を流す応答では、応答ヘッダーを返すまでの時間になる。
    fn log_completed(&self, status: u16, latency: std::time::Duration) {
        let latency_ms = latency.as_millis() as u64;
        let span = tracing::Span::current();
        span.record("status", status);
        span.record("latency_ms", latency_ms);
        let (request_id, method, path) = (&self.request_id, &self.method, &self.path);
        macro_rules! log {
            ($level:ident, $message:literal) => {
                $level!(
                    target: "http::response",
                    request_id = %request_id,
                    method = %method,
                    path = %path,
                    status,
                    latency_ms,
                    $message
                )
            };
        }
        match status {
            200..=299 => log!(info, "Request completed"),
            300..=399 => log!(info, "Request redirected"),
            400..=499 => log!(warn, "Client error"),
            500..=599 => {
                log!(error, "Server error");
                span.record("error", "server_error");
            }
            _ => log!(debug, "Unexpected status"),
        }
    }
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

    *response = problem_details_response(response.status_code, reason_phrase(response.status_code));
}

// raw HTTP fixtureも本番と同じRouterを使う。wire処理はHyperへ任せる。
#[cfg(test)]
pub(crate) async fn handle_connection<S>(stream: S, state: AppState)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let io = hyper_util::rt::TokioIo::new(stream);
    let service = hyper_util::service::TowerToHyperService::new(super::router(state));
    let _ = hyper::server::conn::http1::Builder::new()
        .half_close(true)
        .serve_connection(io, service)
        .await;
}

/// 本文を読み込み済みの要求を handler へ渡し、HTTP 層と同じ後処理をした応答を返す。
#[cfg(test)]
pub(super) async fn process_request(request: Request, app_state: &AppState) -> Response {
    let (path, query) = split_uri(&request.uri);
    let context = RequestContext::new(&request, path, query);
    let mut response = route_request(&request, app_state, path, query)
        .await
        .unwrap_or_else(ApiError::into_response);
    finish_response(&mut response, false, &context);
    response
}

#[cfg(test)]
fn split_uri(uri: &str) -> (&str, Option<&str>) {
    if let Some((path, query)) = uri.split_once('?') {
        (path, Some(query))
    } else {
        (uri, None)
    }
}
