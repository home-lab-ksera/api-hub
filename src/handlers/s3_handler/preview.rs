//! ブラウザ向けの preview 応答（ヘッダーの組み立てと検証）。

use super::*;
use crate::http_client::find_header;

pub struct PreviewObjectStreamResponse {
    pub status_code: u16,
    pub headers: Vec<(String, String)>,
    pub body_stream: reqwest::Response,
}

pub async fn preview_object_stream(
    app_state: &AppState,
    bucket: String,
    key: String,
    request_headers: &[(String, String)],
) -> Result<PreviewObjectStreamResponse, ApiError> {
    // 条件付き GET と Range は、そのまま S3 へ渡す。
    let forward = |name: &str| find_header(request_headers, name).map(str::to_owned);
    let s3_response = s3_service::get_object_proxy_stream(
        &app_state.client,
        &app_state.settings,
        ProxyObjectStreamInput {
            bucket,
            key: key.clone(),
            range: forward("range"),
            if_match: forward("if-match"),
            if_none_match: forward("if-none-match"),
            if_modified_since: forward("if-modified-since"),
            if_unmodified_since: forward("if-unmodified-since"),
        },
    )
    .await?;

    let headers = build_preview_response_headers(
        &crate::http_client::headers(&s3_response),
        &key,
        s3_response.status().as_u16(),
    );

    Ok(PreviewObjectStreamResponse {
        status_code: s3_response.status().as_u16(),
        headers,
        body_stream: s3_response,
    })
}

pub(super) fn build_preview_response_headers(
    source_headers: &[(String, String)],
    key: &str,
    status_code: u16,
) -> Vec<(String, String)> {
    let pass_through_headers = [
        "content-type",
        "content-length",
        "etag",
        "last-modified",
        "cache-control",
        "content-range",
        "accept-ranges",
    ];
    let mut headers = Vec::new();

    for (name, value) in source_headers {
        if !pass_through_headers
            .iter()
            .any(|allowed| name.eq_ignore_ascii_case(allowed))
        {
            continue;
        }
        if !is_valid_header_name(name) || !is_valid_header_value(value) {
            continue;
        }
        headers.push((name.clone(), value.clone()));
    }

    if should_force_pdf_preview(find_header(&headers, "content-type"), key) {
        upsert_header(&mut headers, "Content-Type", "application/pdf");
        let file_name = sanitize_pdf_filename(key);
        upsert_header(
            &mut headers,
            "Content-Disposition",
            &format!("inline; filename=\"{}\"", file_name),
        );
    }

    // 保存された Content-Type を信用して HTML などを api-hub のオリジンで表示しない。
    upsert_header(&mut headers, "X-Content-Type-Options", "nosniff");
    // 304 は本文も Content-Type も持たないため、保存済みの応答ヘッダーを書き換えない。
    let content_type = find_header(&headers, "content-type");
    if status_code != 304 && !content_type.is_some_and(is_inline_media) {
        if content_type.is_none() {
            upsert_header(&mut headers, "Content-Type", "application/octet-stream");
        }
        upsert_header(&mut headers, "Content-Disposition", "attachment");
        upsert_header(
            &mut headers,
            "Content-Security-Policy",
            "sandbox; default-src 'none'",
        );
    }

    headers
}

pub(super) fn upsert_header(headers: &mut Vec<(String, String)>, name: &str, value: &str) {
    headers.retain(|(header_name, _)| !header_name.eq_ignore_ascii_case(name));
    headers.push((name.to_string(), value.to_string()));
}

pub(super) fn should_force_pdf_preview(content_type: Option<&str>, key: &str) -> bool {
    let is_pdf_key = key.to_ascii_lowercase().ends_with(".pdf");
    let is_pdf_content_type = content_type
        .map(|value| {
            value
                .split(';')
                .next()
                .map(str::trim)
                .is_some_and(|media_type| media_type.eq_ignore_ascii_case("application/pdf"))
        })
        .unwrap_or(false);

    is_pdf_key || is_pdf_content_type
}

/// ブラウザーがスクリプトを実行せずに表示できる形式。SVG は文書として扱われるため含めない。
pub(super) fn is_inline_media(content_type: &str) -> bool {
    let media = content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    media == "application/pdf"
        || matches!(
            media.as_str(),
            "image/jpeg" | "image/png" | "image/gif" | "image/webp" | "image/avif"
        )
        || media.starts_with("video/")
        || media.starts_with("audio/")
}

pub(super) fn is_valid_header_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|b| {
            b.is_ascii_alphanumeric()
                || matches!(
                    b,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

pub(super) fn is_valid_header_value(value: &str) -> bool {
    !value.bytes().any(|b| b == b'\r' || b == b'\n' || b == 0)
}

pub(super) fn sanitize_pdf_filename(key: &str) -> String {
    let candidate = key.rsplit('/').next().unwrap_or("file.pdf");
    let mut filename = candidate
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
        .collect::<String>();

    if filename.is_empty() {
        filename = "file.pdf".to_string();
    }

    if !filename.to_ascii_lowercase().ends_with(".pdf") {
        filename.push_str(".pdf");
    }

    filename
}
