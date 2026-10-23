use shiguredo_http11::Response;
use shiguredo_http11::uri::percent_decode;
use std::time::Instant;
use tracing::{debug, error, info, instrument, warn};

use crate::{config::state::AppState, errors::api_error::ApiError, service::slack_service};

struct UploadQuery {
    pub channel: String,
    pub file_name: Option<String>,
}

fn message_payload(body: &str, update: bool) -> Result<String, ApiError> {
    use super::s3_handler::{get_optional_string, get_required_string};
    let json = nojson::RawJson::parse(body)
        .map_err(|e| ApiError::BadRequest(format!("Invalid JSON: {e}")))?;
    let root = json.value();
    let channel = get_required_string(root, "channel")?;
    let text = get_required_string(root, "text")?;
    if channel.trim().is_empty() || text.trim().is_empty() {
        return Err(ApiError::BadRequest(
            "channel and text must not be empty".into(),
        ));
    }
    let ts = if update {
        Some(get_required_string(root, "ts")?)
    } else {
        get_optional_string(root, "thread_ts")?
    };
    if ts.as_ref().is_some_and(|v| v.trim().is_empty()) {
        return Err(ApiError::BadRequest(
            "Message timestamp must not be empty".into(),
        ));
    }
    let blocks = root
        .to_member("blocks")
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
        .optional();
    if let Some(blocks) = blocks {
        let blocks = blocks
            .to_array()
            .map_err(|_| ApiError::BadRequest("blocks must be an array".into()))?;
        let mut count = 0;
        for block in blocks {
            get_required_string(block, "type")?;
            count += 1;
        }
        if count > 50 {
            return Err(ApiError::BadRequest("At most 50 blocks are allowed".into()));
        }
    }
    Ok(nojson::json(|f| {
        f.object(|f| {
            f.member("channel", &channel)?;
            f.member("text", &text)?;
            if let Some(ts) = &ts {
                f.member(if update { "ts" } else { "thread_ts" }, ts)?;
            }
            if let Some(blocks) = blocks {
                f.member("blocks", blocks)?;
            }
            Ok(())
        })
    })
    .to_string())
}

fn parse_upload_query(raw_query: Option<&str>) -> Result<UploadQuery, ApiError> {
    let query = raw_query.unwrap_or_default();
    let mut channel = None;
    let mut file_name = None;

    for pair in query.split('&').filter(|s| !s.is_empty()) {
        let (raw_key, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_query_component(raw_key)?;
        let value = decode_query_component(raw_value)?;

        match key.as_ref() {
            "channel" => channel = Some(value),
            "file_name" => file_name = Some(value),
            _ => {}
        }
    }

    let channel = channel.filter(|c| !c.trim().is_empty()).ok_or_else(|| {
        ApiError::BadRequest("Missing required query parameter 'channel'".to_string())
    })?;

    let file_name = file_name.and_then(|name| {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });

    Ok(UploadQuery { channel, file_name })
}

fn decode_query_component(value: &str) -> Result<String, ApiError> {
    let replaced = value.replace('+', " ");
    percent_decode(&replaced).map_err(|_| ApiError::BadRequest("Invalid query string".to_string()))
}

fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn content_type(headers: &[(String, String)]) -> Result<String, ApiError> {
    let content_type = header_value(headers, "content-type")
        .ok_or_else(|| ApiError::BadRequest("Missing Content-Type header".to_string()))?
        .trim();

    let normalized = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .to_ascii_lowercase();

    Ok(normalized)
}

fn looks_like_pdf(data: &[u8]) -> bool {
    data.starts_with(b"%PDF-")
}

fn looks_like_png(data: &[u8]) -> bool {
    data.starts_with(&[0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1A, b'\n'])
}

fn looks_like_jpeg(data: &[u8]) -> bool {
    data.starts_with(&[0xFF, 0xD8, 0xFF])
}

fn looks_like_gif(data: &[u8]) -> bool {
    data.starts_with(b"GIF87a") || data.starts_with(b"GIF89a")
}

fn looks_like_webp(data: &[u8]) -> bool {
    data.len() >= 12 && &data[0..4] == b"RIFF" && &data[8..12] == b"WEBP"
}

fn ensure_image_content(content_type: &str, data: &[u8]) -> Result<&'static str, ApiError> {
    let extension = match content_type {
        "image/png" => {
            if !looks_like_png(data) {
                return Err(ApiError::BadRequest(
                    "Body is not a valid PNG image".to_string(),
                ));
            }
            "png"
        }
        "image/jpeg" => {
            if !looks_like_jpeg(data) {
                return Err(ApiError::BadRequest(
                    "Body is not a valid JPEG image".to_string(),
                ));
            }
            "jpg"
        }
        "image/gif" => {
            if !looks_like_gif(data) {
                return Err(ApiError::BadRequest(
                    "Body is not a valid GIF image".to_string(),
                ));
            }
            "gif"
        }
        "image/webp" => {
            if !looks_like_webp(data) {
                return Err(ApiError::BadRequest(
                    "Body is not a valid WEBP image".to_string(),
                ));
            }
            "webp"
        }
        _ => {
            return Err(ApiError::BadRequest(
                "Unsupported image Content-Type. Use image/png, image/jpeg, image/gif, or image/webp"
                    .to_string(),
            ));
        }
    };

    Ok(extension)
}

fn build_default_name(prefix: &str, extension: &str) -> String {
    format!("{prefix}.{extension}")
}

fn validate_file_not_empty(file_data: &[u8]) -> Result<(), ApiError> {
    if file_data.is_empty() {
        return Err(ApiError::BadRequest(
            "Request body must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn json_string_response(value: String) -> Response {
    let body = nojson::json(|f| f.value(&value)).to_string();
    Response::new(200, "OK")
        .header("Content-Type", "application/json")
        .body(body.into_bytes())
}

pub async fn post_message(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    message(app_state, body, false).await
}

pub async fn update_message(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    message(app_state, body, true).await
}

#[instrument(skip(app_state, body))]
async fn message(app_state: &AppState, body: &[u8], update: bool) -> Result<Response, ApiError> {
    let body = std::str::from_utf8(body)
        .map_err(|_| ApiError::BadRequest("Request body must be valid UTF-8".into()))?;
    let payload = message_payload(body, update)?;
    let response = slack_service::send_message(
        &app_state.client,
        &app_state.settings.slack_bot_token,
        &app_state.settings.slack_api_base_url,
        update,
        &payload,
    )
    .await
    .map_err(|e| ApiError::InternalServerError(e.to_string()))?;
    Ok(json_string_response(response))
}

#[instrument(skip(app_state, headers, body))]
pub async fn upload_image_raw(
    app_state: &AppState,
    raw_query: Option<&str>,
    headers: &[(String, String)],
    body: &[u8],
) -> Result<Response, ApiError> {
    let payload = parse_upload_query(raw_query)?;
    let content_type = content_type(headers)?;
    validate_file_not_empty(body)?;
    let extension = ensure_image_content(&content_type, body)?;
    let file_name = payload
        .file_name
        .unwrap_or_else(|| build_default_name("image-upload", extension));

    debug!(
        file_name = %file_name,
        channel = %payload.channel,
        content_type = %content_type,
        file_size = body.len(),
        "Processing raw image upload request"
    );

    let start = Instant::now();

    let response_text = slack_service::send_single_file_to_slack(
        &app_state.client,
        &app_state.settings.slack_bot_token,
        &app_state.settings.slack_api_base_url,
        body,
        &file_name,
        &payload.channel,
    )
    .await
    .map_err(|e| {
        error!(
            error = %e,
            file_name = %file_name,
            channel = %payload.channel,
            "Failed to upload image to Slack"
        );
        ApiError::InternalServerError(e.to_string())
    })?;

    let duration = start.elapsed();
    info!(
        file_name = %file_name,
        channel = %payload.channel,
        file_size = body.len(),
        duration_ms = duration.as_millis() as u64,
        "Successfully uploaded image to Slack"
    );

    Ok(json_string_response(response_text))
}

#[instrument(skip(app_state, headers, body))]
pub async fn upload_pdf_raw(
    app_state: &AppState,
    raw_query: Option<&str>,
    headers: &[(String, String)],
    body: &[u8],
) -> Result<Response, ApiError> {
    let payload = parse_upload_query(raw_query)?;
    let content_type = content_type(headers)?;
    validate_file_not_empty(body)?;

    if content_type != "application/pdf" {
        return Err(ApiError::BadRequest(
            "Unsupported Content-Type. Use application/pdf".to_string(),
        ));
    }

    if !looks_like_pdf(body) {
        warn!(channel = %payload.channel, "PDF signature check failed");
        return Err(ApiError::BadRequest(
            "Body is not a valid PDF document".to_string(),
        ));
    }

    let file_name = payload
        .file_name
        .unwrap_or_else(|| build_default_name("document", "pdf"));

    debug!(
        file_name = %file_name,
        channel = %payload.channel,
        file_size = body.len(),
        "Processing raw PDF upload request"
    );

    let start = Instant::now();

    let response_text = slack_service::send_single_file_to_slack(
        &app_state.client,
        &app_state.settings.slack_bot_token,
        &app_state.settings.slack_api_base_url,
        body,
        &file_name,
        &payload.channel,
    )
    .await
    .map_err(|e| {
        error!(
            error = %e,
            file_name = %file_name,
            channel = %payload.channel,
            "Failed to upload PDF to Slack"
        );
        ApiError::InternalServerError(e.to_string())
    })?;

    let duration = start.elapsed();
    info!(
        file_name = %file_name,
        channel = %payload.channel,
        file_size = body.len(),
        duration_ms = duration.as_millis() as u64,
        "Successfully uploaded PDF to Slack"
    );

    Ok(json_string_response(response_text))
}
