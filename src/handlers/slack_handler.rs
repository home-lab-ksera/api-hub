use shiguredo_http11::Response;
use shiguredo_http11::uri::percent_decode;
use std::time::Instant;
use tracing::{debug, error, info, instrument, warn};

use crate::{config::state::AppState, errors::api_error::ApiError, media, service::slack_service};

struct UploadQuery {
    pub channel: String,
    pub file_name: Option<String>,
}

fn message_payload(body: &str, update: bool) -> Result<String, ApiError> {
    use super::s3_handler::{optional, required};
    let json = nojson::RawJson::parse(body)
        .map_err(|e| ApiError::BadRequest(format!("Invalid JSON: {e}")))?;
    let root = json.value();
    let channel: String = required(root, "channel")?;
    let text: String = required(root, "text")?;
    if channel.trim().is_empty() || text.trim().is_empty() {
        return Err(ApiError::BadRequest(
            "channel and text must not be empty".into(),
        ));
    }
    let ts = if update {
        Some(required::<String>(root, "ts")?)
    } else {
        optional::<String>(root, "thread_ts")?
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
            required::<String>(block, "type")?;
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

fn content_type(headers: &[(String, String)]) -> Result<String, ApiError> {
    let content_type = crate::http_client::find_header(headers, "content-type")
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

/// 受け付ける画像の形式。Content-Type・拡張子・エラーに出す形式名・先頭バイトの確認。
struct ImageType {
    media_type: &'static str,
    extension: &'static str,
    label: &'static str,
    matches: fn(&[u8]) -> bool,
}

const IMAGE_TYPES: [ImageType; 4] = [
    ImageType {
        media_type: "image/png",
        extension: "png",
        label: "PNG",
        matches: media::is_png,
    },
    ImageType {
        media_type: "image/jpeg",
        extension: "jpg",
        label: "JPEG",
        matches: media::is_jpeg,
    },
    ImageType {
        media_type: "image/gif",
        extension: "gif",
        label: "GIF",
        matches: media::is_gif,
    },
    ImageType {
        media_type: "image/webp",
        extension: "webp",
        label: "WEBP",
        matches: media::is_webp,
    },
];

/// Content-Type と先頭バイトが一致する画像なら拡張子を返す。
fn ensure_image_content(content_type: &str, data: &[u8]) -> Result<&'static str, ApiError> {
    let Some(image) = IMAGE_TYPES
        .iter()
        .find(|image| image.media_type == content_type)
    else {
        return Err(ApiError::BadRequest(
            "Unsupported image Content-Type. Use image/png, image/jpeg, image/gif, or image/webp"
                .to_string(),
        ));
    };
    if !(image.matches)(data) {
        return Err(ApiError::BadRequest(format!(
            "Body is not a valid {} image",
            image.label
        )));
    }
    Ok(image.extension)
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
    let slack = slack_service::SlackClient::new(&app_state.client, &app_state.settings);
    let response = slack_service::send_message(&slack, update, &payload)
        .await
        .map_err(|e| ApiError::InternalServerError(e.to_string()))?;
    Ok(json_string_response(response))
}

/// 検証済みのファイルを Slack へ送り、Slack の応答本文を返す。`kind` はログに出す種類名。
async fn share_file(
    app_state: &AppState,
    channel: &str,
    file_name: &str,
    body: &[u8],
    kind: &str,
) -> Result<Response, ApiError> {
    let start = Instant::now();
    let response_text = slack_service::send_single_file_to_slack(
        &slack_service::SlackClient::new(&app_state.client, &app_state.settings),
        body,
        file_name,
        channel,
    )
    .await
    .map_err(|e| {
        error!(
            error = %e,
            file_name = %file_name,
            channel = %channel,
            "Failed to upload {kind} to Slack"
        );
        ApiError::InternalServerError(e.to_string())
    })?;
    info!(
        file_name = %file_name,
        channel = %channel,
        file_size = body.len(),
        duration_ms = start.elapsed().as_millis() as u64,
        "Successfully uploaded {kind} to Slack"
    );
    Ok(json_string_response(response_text))
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
    share_file(app_state, &payload.channel, &file_name, body, "image").await
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
    if !media::is_pdf(body) {
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
    share_file(app_state, &payload.channel, &file_name, body, "PDF").await
}

#[cfg(test)]
mod tests {
    use super::ensure_image_content;

    #[test]
    fn image_content_errors_name_the_format() {
        let png = b"\x89PNG\r\n\x1a\nimage";
        assert_eq!(ensure_image_content("image/png", png).unwrap(), "png");
        for (media_type, label) in [
            ("image/png", "PNG"),
            ("image/jpeg", "JPEG"),
            ("image/gif", "GIF"),
            ("image/webp", "WEBP"),
        ] {
            assert_eq!(
                ensure_image_content(media_type, b"x")
                    .unwrap_err()
                    .to_string(),
                format!("Bad Request: Body is not a valid {label} image")
            );
        }
        assert_eq!(
            ensure_image_content("image/svg+xml", png)
                .unwrap_err()
                .to_string(),
            "Bad Request: Unsupported image Content-Type. Use image/png, image/jpeg, image/gif, or image/webp"
        );
    }
}
