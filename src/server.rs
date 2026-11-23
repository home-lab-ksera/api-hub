use crate::config::state::AppState;
use crate::errors::api_error::{ApiError, problem_details_response, reason_phrase};
use crate::handlers::{health_handler, openapi_handler, s3_handler, slack_handler};
use crate::request_id;
use axum::body::Body as AxumBody;
use shiguredo_http11::{Request, Response, uri::percent_decode};
use tracing::{Instrument, debug, error, info, info_span, warn};

mod routes;
use routes::{Body, Endpoint, Lookup};

const S3_CORS_ALLOWED_ORIGIN: &str = "https://hitomi-upload-viewer.internal.qroksera.com";

/// 経路の判定は [`routes`] の表で行うため、axum にはすべての要求を1つの handler へ渡させる。
pub fn router(state: AppState) -> axum::Router {
    axum::Router::new().fallback(handle_http).with_state(state)
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

/// 同じPgPoolを共有し、APIと1本の通知配送taskを監督する。
pub async fn serve_integrated(
    listener: tokio::net::TcpListener,
    state: AppState,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let pool = state
        .database
        .clone()
        .ok_or_else(|| std::io::Error::other("notification database is required"))?;
    let (stop, rx) = tokio::sync::watch::channel(false);
    let signal_stop = stop.clone();
    let signal = tokio::spawn(async move {
        shutdown.await;
        let _ = signal_stop.send(true);
    });
    let api = tokio::spawn(serve(listener, state.clone(), stop_requested(rx.clone())));
    let worker = tokio::spawn(crate::notifications::worker(state, stop_requested(rx)));
    let result = supervise(api, worker, stop).await;
    signal.abort();
    // HTTPと進行中通知の確定後にだけ、共有poolを閉じる。
    pool.close().await;
    result
}
async fn stop_requested(mut rx: tokio::sync::watch::Receiver<bool>) {
    let _ = rx.wait_for(|stop| *stop).await;
}
async fn supervise(
    mut api: tokio::task::JoinHandle<std::io::Result<()>>,
    mut worker: tokio::task::JoinHandle<()>,
    stop: tokio::sync::watch::Sender<bool>,
) -> std::io::Result<()> {
    let (api_done, result) = tokio::select! {
        result=&mut api=>(true,match result {
            Ok(Ok(())) if *stop.borrow()=>Ok(()),
            Ok(Err(error))=>Err(error),
            other=>Err(std::io::Error::other(format!("HTTP task exited unexpectedly: {other:?}"))),
        }),
        result=&mut worker=>(false,match result {
            Ok(()) if *stop.borrow()=>Ok(()),
            other=>Err(std::io::Error::other(format!("notification task exited unexpectedly: {other:?}"))),
        }),
    };
    let _ = stop.send(true);
    // 片方の異常終了でも残ったtaskを放置せず、新規受付/claimを止める。
    let remaining = async {
        if api_done {
            (&mut worker).await.map_err(std::io::Error::other)?;
        } else {
            (&mut api).await.map_err(std::io::Error::other)??;
        }
        Ok(())
    };
    let drained = tokio::time::timeout(std::time::Duration::from_secs(85), remaining)
        .await
        .unwrap_or_else(|_| {
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "process drain deadline exceeded",
            ))
        });
    if drained.is_err() {
        api.abort();
        worker.abort();
    }
    result.and(drained)
}

#[cfg(test)]
mod supervision_tests {
    use super::*;
    #[tokio::test]
    async fn unexpected_worker_exit_stops_http_and_fails_process() {
        for panic in [false, true] {
            let (stop, rx) = tokio::sync::watch::channel(false);
            let api = tokio::spawn(async move {
                stop_requested(rx).await;
                Ok(())
            });
            let worker = tokio::spawn(async move {
                assert!(!panic, "fixture worker panic");
            });
            let result = supervise(api, worker, stop.clone()).await;
            assert!(result.is_err());
            assert!(*stop.borrow());
        }
    }
    #[tokio::test]
    async fn api_failure_stops_claims_and_waits_for_worker() {
        let (stop, rx) = tokio::sync::watch::channel(false);
        let api = tokio::spawn(async { Err(std::io::Error::other("fixture HTTP failure")) });
        let worker = tokio::spawn(async move {
            stop_requested(rx).await;
        });
        assert!(supervise(api, worker, stop.clone()).await.is_err());
        assert!(*stop.borrow());
    }
}
async fn handle_http(
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
        finish_response(&mut head, stream.is_some(), &context, &metadata);
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
                let mut head = Response::new(reply.status_code, reply.reason_phrase);
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
fn finish_response(
    response: &mut Response,
    streamed: bool,
    context: &RequestContext,
    request: &Request,
) {
    // 本文を流す応答は上流の本文をそのまま返すため、problem details へ置き換えない。
    if !streamed {
        apply_problem_details(response);
    }
    apply_s3_cors(&context.path, request, response);
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

/// ブラウザーからの更新操作を、viewer が使う経路だけに限定する。
///
/// api-hub に利用者認証はないため、tailnet 上の端末が開いた任意の Web ページから
/// 更新 API を呼べないようにする。ブラウザーは別オリジンへの POST に必ず `Origin` を付ける。
/// クラスタ内のクライアントはどちらのヘッダーも送らない。
fn check_request_source(
    method: &axum::http::Method,
    path: &str,
    request: &Request,
) -> Result<(), ApiError> {
    use axum::http::Method;
    if matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS) {
        return Ok(());
    }
    let origin = request.get_header("origin");
    let from_browser = origin.is_some()
        || request
            .get_header("sec-fetch-site")
            .is_some_and(|site| !site.eq_ignore_ascii_case("none"));
    if !from_browser {
        return Ok(());
    }
    if origin == Some(S3_CORS_ALLOWED_ORIGIN) && routes::allows_browser(path) {
        return Ok(());
    }
    Err(ApiError::Forbidden(
        "This operation is not available from a web page".into(),
    ))
}

/// 本文がない、または `application/json` の要求か。
/// `text/plain` などの preflight 不要な形式で JSON を送り込む経路を閉じる。
fn is_json_request(method: &axum::http::Method, body: &[u8], request: &Request) -> bool {
    if *method != axum::http::Method::POST || body.is_empty() {
        return true;
    }
    request.get_header("content-type").is_some_and(|value| {
        value
            .split(';')
            .next()
            .is_some_and(|media| media.trim().eq_ignore_ascii_case("application/json"))
    })
}

// raw HTTP fixtureも本番と同じRouterを使う。wire処理はHyperへ任せる。
#[cfg(test)]
pub(crate) async fn handle_connection<S>(stream: S, state: AppState)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let io = hyper_util::rt::TokioIo::new(stream);
    let service = hyper_util::service::TowerToHyperService::new(router(state));
    let _ = hyper::server::conn::http1::Builder::new()
        .half_close(true)
        .serve_connection(io, service)
        .await;
}
/// `/s3/preview/` に続く `{bucket}/{key}` を復号する。
fn parse_preview_bucket_and_key(remaining: &str) -> Result<(String, String), ApiError> {
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

/// 本文を読み込み済みの要求を handler へ渡し、HTTP 層と同じ後処理をした応答を返す。
#[cfg(test)]
async fn process_request(request: Request, app_state: &AppState) -> Response {
    let (path, query) = split_uri(&request.uri);
    let context = RequestContext::new(&request, path, query);
    let mut response = route_request(&request, app_state, path, query)
        .await
        .unwrap_or_else(ApiError::into_response);
    finish_response(&mut response, false, &context, &request);
    response
}

async fn route_request(
    request: &Request,
    app_state: &AppState,
    path: &str,
    query: Option<&str>,
) -> Result<Response, ApiError> {
    let (route, param) = match routes::lookup(&request.method, path) {
        Lookup::Found(route, param) => (route, param),
        Lookup::Preflight => return Ok(s3_handler::s3_preflight()),
        Lookup::MethodNotAllowed => {
            return Err(ApiError::MethodNotAllowed(format!(
                "Method {} is not allowed for {}",
                request.method, path
            )));
        }
        Lookup::NotFound => return Err(ApiError::NotFound(format!("Route not found: {path}"))),
    };
    let body = request.body.as_slice();
    let headers = request.headers.as_slice();
    use Endpoint as E;
    match route.endpoint {
        E::Health => Ok(health_handler::health()),
        E::Metrics => Ok(crate::queue_metrics::handle(app_state).await),
        E::OpenApi => Ok(openapi_handler::openapi_json()),
        E::NotificationCreate => Ok(crate::notifications::handle_post(app_state, body).await),
        E::NotificationGet => Ok(crate::notifications::handle_get(app_state, param).await),
        E::NotificationRetry => Ok(crate::notifications::handle_retry(app_state, param).await),
        E::SlackMessage => slack_handler::post_message(app_state, body).await,
        E::SlackMessageUpdate => slack_handler::update_message(app_state, body).await,
        E::SlackUploadImage => {
            slack_handler::upload_image_raw(app_state, query, headers, body).await
        }
        E::SlackUploadPdf => slack_handler::upload_pdf_raw(app_state, query, headers, body).await,
        E::S3PutObjectBase64 => s3_handler::put_object_base64(app_state, body).await,
        E::S3PutObjectAndNotify => s3_handler::put_object_and_notify(app_state, body).await,
        E::S3GetObjectBase64 => s3_handler::get_object_base64(app_state, body).await,
        E::S3HeadObject => s3_handler::head_object(app_state, body).await,
        E::S3DeleteObject => s3_handler::delete_object(app_state, body).await,
        E::S3DeleteObjects => s3_handler::delete_objects(app_state, body).await,
        E::S3CopyObject => s3_handler::copy_object(app_state, body).await,
        E::S3GetObjectTagging => s3_handler::get_object_tagging(app_state, body).await,
        E::S3PutObjectTagging => s3_handler::put_object_tagging(app_state, body).await,
        E::S3DeleteObjectTagging => s3_handler::delete_object_tagging(app_state, body).await,
        E::S3ListObjectsV2 => s3_handler::list_objects_v2(app_state, body).await,
        E::S3CreateMultipartUpload => s3_handler::create_multipart_upload(app_state, body).await,
        E::S3UploadPartBase64 => s3_handler::upload_part_base64(app_state, body).await,
        E::S3CompleteMultipartUpload => {
            s3_handler::complete_multipart_upload(app_state, body).await
        }
        E::S3AbortMultipartUpload => s3_handler::abort_multipart_upload(app_state, body).await,
        E::S3ListParts => s3_handler::list_parts(app_state, body).await,
        E::S3ListMultipartUploads => s3_handler::list_multipart_uploads(app_state, body).await,
        E::S3PresignedGetObject => s3_handler::presigned_get_object(app_state, body).await,
        E::S3PresignedPutObject => s3_handler::presigned_put_object(app_state, body).await,
        E::S3BrowserGetObject => s3_handler::browser_get_object(app_state, body).await,
        E::S3BrowserOpen => browser_open(app_state, query).await,
        E::S3ListBuckets => s3_handler::list_buckets(app_state).await,
        E::S3CreateBucket => s3_handler::create_bucket(app_state, body).await,
        E::S3HeadBucket => s3_handler::head_bucket(app_state, body).await,
        E::S3DeleteBucket => s3_handler::delete_bucket(app_state, body).await,
        // 本文を流す経路は handle_http が先に処理する。ここへ来るのはその経路を通らない呼び出しだけ。
        E::S3PutObjectStream | E::S3Preview => Err(ApiError::MethodNotAllowed(format!(
            "Method {} is not allowed for {}",
            request.method, path
        ))),
    }
}

/// viewer がリンクから開けるよう、署名済み URL へ転送する。
async fn browser_open(app_state: &AppState, query: Option<&str>) -> Result<Response, ApiError> {
    // 同じ名前が複数あるときは後の値を使う。
    let fields: std::collections::BTreeMap<String, String> =
        url::form_urlencoded::parse(query.unwrap_or("").as_bytes())
            .into_owned()
            .collect();
    let body = nojson::json(|f| f.value(&fields)).to_string();
    let response = s3_handler::browser_get_object(app_state, body.as_bytes()).await?;
    if response.status_code != 200 {
        return Ok(response);
    }
    let signed = std::str::from_utf8(&response.body)
        .ok()
        .and_then(|text| nojson::RawJson::parse(text).ok())
        .ok_or_else(|| ApiError::InternalServerError("Invalid signed response".into()))?;
    let location = signed
        .value()
        .to_member("url")
        .ok()
        .and_then(|member| member.optional())
        .and_then(|url| String::try_from(url).ok())
        .ok_or_else(|| ApiError::InternalServerError("Missing signed URL".into()))?;
    Ok(Response::new(307, "Temporary Redirect")
        .header("Location", &location)
        .header("Cache-Control", "no-store")
        .header("Referrer-Policy", "no-referrer"))
}

#[cfg(test)]
fn split_uri(uri: &str) -> (&str, Option<&str>) {
    if let Some((path, query)) = uri.split_once('?') {
        (path, Some(query))
    } else {
        (uri, None)
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
