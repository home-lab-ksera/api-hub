//! ブラウザ向けの preview 応答（ヘッダーの組み立てと検証）。

use super::*;

pub struct PreviewObjectStreamResponse {
    pub status_code: u16,
    pub reason_phrase: &'static str,
    pub headers: Vec<(String, String)>,
    pub body_stream: reqwest::Response,
}

pub async fn preview_object(
    app_state: &AppState,
    bucket: String,
    key: String,
) -> Result<Response, ApiError> {
    let s3_response = s3_service::get_object_proxy(
        &app_state.client,
        &app_state.settings,
        ProxyObjectInput {
            bucket,
            key: key.clone(),
        },
    )
    .await?;

    let mut response = Response::new(
        s3_response.status_code,
        response_reason(s3_response.status_code),
    )
    .body(s3_response.body);

    for (name, value) in build_preview_response_headers(&s3_response.headers, &key) {
        response.add_header(&name, &value);
    }

    Ok(response)
}

pub async fn preview_object_stream(
    app_state: &AppState,
    bucket: String,
    key: String,
    request_headers: &[(String, String)],
) -> Result<PreviewObjectStreamResponse, ApiError> {
    let s3_response = s3_service::get_object_proxy_stream(
        &app_state.client,
        &app_state.settings,
        ProxyObjectStreamInput {
            bucket,
            key: key.clone(),
            range: extract_forward_header(request_headers, "range"),
            if_match: extract_forward_header(request_headers, "if-match"),
            if_none_match: extract_forward_header(request_headers, "if-none-match"),
            if_modified_since: extract_forward_header(request_headers, "if-modified-since"),
            if_unmodified_since: extract_forward_header(request_headers, "if-unmodified-since"),
        },
    )
    .await?;

    let headers = build_preview_response_headers(&crate::http_client::headers(&s3_response), &key);

    Ok(PreviewObjectStreamResponse {
        status_code: s3_response.status().as_u16(),
        reason_phrase: response_reason(s3_response.status().as_u16()),
        headers,
        body_stream: s3_response,
    })
}

pub(super) fn extract_forward_header(headers: &[(String, String)], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.clone())
}

pub(super) fn build_preview_response_headers(
    source_headers: &[(String, String)],
    key: &str,
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

    if should_force_pdf_preview(get_header_value(&headers, "content-type"), key) {
        upsert_header(&mut headers, "Content-Type", "application/pdf");
        let file_name = sanitize_pdf_filename(key);
        upsert_header(
            &mut headers,
            "Content-Disposition",
            &format!("inline; filename=\"{}\"", file_name),
        );
    }

    headers
}

pub(super) fn get_header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
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

pub(super) fn response_reason(status_code: u16) -> &'static str {
    match status_code {
        200 => "OK",
        206 => "Partial Content",
        304 => "Not Modified",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        416 => "Range Not Satisfiable",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        _ => "OK",
    }
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
