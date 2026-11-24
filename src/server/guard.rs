//! ブラウザーからの要求の制限と CORS。

use super::routes;
use crate::errors::api_error::ApiError;
use shiguredo_http11::{Request, Response};

const S3_CORS_ALLOWED_ORIGIN: &str = "https://hitomi-upload-viewer.internal.qroksera.com";

/// ブラウザーからの更新操作を、viewer が使う経路だけに限定する。
///
/// api-hub に利用者認証はないため、tailnet 上の端末が開いた任意の Web ページから
/// 更新 API を呼べないようにする。ブラウザーは別オリジンへの POST に必ず `Origin` を付ける。
/// クラスタ内のクライアントはどちらのヘッダーも送らない。
pub(super) fn check_request_source(
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
pub(super) fn is_json_request(method: &axum::http::Method, body: &[u8], request: &Request) -> bool {
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

pub(super) fn apply_s3_cors(path: &str, response: &mut Response) {
    if !path.starts_with("/s3/") {
        return;
    }
    // 許可するオリジンは viewer の1つだけなので、要求の Origin によらず同じ値を返す。
    response.add_header("Access-Control-Allow-Origin", S3_CORS_ALLOWED_ORIGIN);
    response.add_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    response.add_header("Access-Control-Allow-Headers", "content-type, x-request-id");
    response.add_header("Access-Control-Max-Age", "600");
    response.add_header("Vary", "Origin");
}
