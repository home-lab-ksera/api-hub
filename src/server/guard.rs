//! ブラウザーからの要求の制限と CORS。

use super::routes;
use crate::errors::api_error::ApiError;
use crate::reply::{Reply, append_header, request_header};
use axum::http::HeaderMap;

/// ブラウザーから S3 の経路を呼べる画面のオリジン。先頭は Origin のない要求に返す既定値。
/// viewer を dashboard へ移す間だけ両方を許可する（home-lab-k8s tasks/dashboard-consolidation-plan.md）。
const S3_CORS_ALLOWED_ORIGINS: &[&str] = &[
    "https://hitomi-upload-viewer.internal.qroksera.com",
    "https://dashboard.internal.qroksera.com",
];

fn allowed_origin(origin: Option<&str>) -> Option<&'static str> {
    let origin = origin?;
    S3_CORS_ALLOWED_ORIGINS
        .iter()
        .copied()
        .find(|allowed| *allowed == origin)
}

/// ブラウザーからの更新操作を、許可した画面が使う経路だけに限定する。
///
/// api-hub に利用者認証はないため、tailnet 上の端末が開いた任意の Web ページから
/// 更新 API を呼べないようにする。ブラウザーは別オリジンへの POST に必ず `Origin` を付ける。
/// クラスタ内のクライアントはどちらのヘッダーも送らない。
pub(super) fn check_request_source(
    method: &axum::http::Method,
    path: &str,
    headers: &HeaderMap,
) -> Result<(), ApiError> {
    use axum::http::Method;
    if matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS) {
        return Ok(());
    }
    let origin = request_header(headers, "origin");
    let from_browser = origin.is_some()
        || request_header(headers, "sec-fetch-site")
            .is_some_and(|site| !site.eq_ignore_ascii_case("none"));
    if !from_browser {
        return Ok(());
    }
    if allowed_origin(origin).is_some() && routes::allows_browser(path) {
        return Ok(());
    }
    Err(ApiError::Forbidden(
        "This operation is not available from a web page".into(),
    ))
}

/// 本文がない、または `application/json` の要求か。
/// `text/plain` などの preflight 不要な形式で JSON を送り込む経路を閉じる。
pub(super) fn is_json_request(
    method: &axum::http::Method,
    body: &[u8],
    headers: &HeaderMap,
) -> bool {
    if *method != axum::http::Method::POST || body.is_empty() {
        return true;
    }
    request_header(headers, "content-type").is_some_and(|value| {
        value
            .split(';')
            .next()
            .is_some_and(|media| media.trim().eq_ignore_ascii_case("application/json"))
    })
}

pub(super) fn apply_s3_cors(path: &str, origin: Option<&str>, response: &mut Reply) {
    if !path.starts_with("/s3/") {
        return;
    }
    // 許可したオリジンからの要求にはそのオリジンを、それ以外には既定値を返す。
    // 既定値は要求元と一致しないため、ブラウザーは許可しない。
    let allow = allowed_origin(origin).unwrap_or(S3_CORS_ALLOWED_ORIGINS[0]);
    append_header(response, "Access-Control-Allow-Origin", allow);
    append_header(
        response,
        "Access-Control-Allow-Methods",
        "GET, POST, OPTIONS",
    );
    append_header(
        response,
        "Access-Control-Allow-Headers",
        "content-type, x-request-id",
    );
    append_header(response, "Access-Control-Max-Age", "600");
    append_header(response, "Vary", "Origin");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cors_echoes_only_allowed_origins() {
        let reply_for = |origin: Option<&str>| {
            let mut response = crate::reply::reply(204);
            apply_s3_cors("/s3/browser_get_object", origin, &mut response);
            response
                .headers()
                .get("Access-Control-Allow-Origin")
                .map(|value| value.to_str().unwrap().to_string())
        };
        for allowed in S3_CORS_ALLOWED_ORIGINS {
            assert_eq!(reply_for(Some(allowed)).as_deref(), Some(*allowed));
        }
        assert_eq!(
            reply_for(Some("https://attacker.example")).as_deref(),
            Some(S3_CORS_ALLOWED_ORIGINS[0])
        );
        assert_eq!(reply_for(None).as_deref(), Some(S3_CORS_ALLOWED_ORIGINS[0]));
    }
}
