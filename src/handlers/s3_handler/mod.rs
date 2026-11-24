use crate::reply::{Reply, ReplyExt, reply};

use crate::{
    config::state::AppState,
    errors::api_error::ApiError,
    service::s3_service::{
        self, AbortMultipartUploadInput, CompleteMultipartUploadInput, CompletePartInput,
        CopyObjectInput, CreateBucketInput, CreateMultipartUploadInput, DeleteBucketInput,
        DeleteObjectIdentifierInput, DeleteObjectInput, DeleteObjectsInput, GetObjectInput,
        HeadBucketInput, HeadObjectInput, ListMultipartUploadsInput, ListObjectsV2Input,
        ListPartsInput, ObjectTaggingInput, PresignedObjectInput, ProxyObjectStreamInput,
        PutObjectInput, UploadPartInput,
    },
};

mod buckets;
mod multipart;
mod output;
mod preview;
mod requests;
pub use buckets::*;
pub use multipart::*;
pub use preview::*;
pub(in crate::handlers) use requests::*;

pub(super) fn json_response(body: String) -> Reply {
    reply(200)
        .header("Content-Type", "application/json")
        .with_body(body.into_bytes())
}

pub async fn put_object_base64(app_state: &AppState, body: &[u8]) -> Result<Reply, ApiError> {
    let input = parse_body(body, put_object_base64_input)?;
    let output = s3_service::put_object(&app_state.client, &app_state.settings, input).await?;
    Ok(json_response(output::put_object(&output).to_string()))
}

pub async fn get_object_base64(app_state: &AppState, body: &[u8]) -> Result<Reply, ApiError> {
    let input = parse_body(body, get_object_input)?;
    let output = s3_service::get_object(&app_state.client, &app_state.settings, input).await?;
    Ok(json_response(output::get_object(&output).to_string()))
}

pub async fn head_object(app_state: &AppState, body: &[u8]) -> Result<Reply, ApiError> {
    let input = parse_body(body, head_object_input)?;
    let output = s3_service::head_object(&app_state.client, &app_state.settings, input).await?;
    Ok(json_response(output::head_object(&output).to_string()))
}

pub async fn delete_object(app_state: &AppState, body: &[u8]) -> Result<Reply, ApiError> {
    let input = parse_body(body, delete_object_input)?;
    let output = s3_service::delete_object(&app_state.client, &app_state.settings, input).await?;
    Ok(json_response(output::delete_object(&output).to_string()))
}

pub async fn delete_objects(app_state: &AppState, body: &[u8]) -> Result<Reply, ApiError> {
    let input = parse_body(body, delete_objects_input)?;
    let output = s3_service::delete_objects(&app_state.client, &app_state.settings, input).await?;
    Ok(json_response(output::delete_objects(&output).to_string()))
}

pub async fn list_objects_v2(app_state: &AppState, body: &[u8]) -> Result<Reply, ApiError> {
    let input = parse_body(body, list_objects_v2_input)?;
    let output = s3_service::list_objects_v2(&app_state.client, &app_state.settings, input).await?;
    Ok(json_response(output::list_objects_v2(&output).to_string()))
}

pub async fn presigned_get_object(app_state: &AppState, body: &[u8]) -> Result<Reply, ApiError> {
    let input = parse_body(body, presigned_object_input)?;
    let output = s3_service::presigned_get(&app_state.settings, input)?;
    Ok(json_response(output::presigned(&output).to_string()))
}

// home-cameraのUTC日付フォルダに保存されたMP4だけを配信対象にする。
pub(super) fn is_camera_recording_key(key: &str) -> bool {
    let parts: Vec<_> = key.split('/').collect();
    parts.len() == 5
        && !parts[0].is_empty()
        && parts[0]
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        && [4, 2, 2]
            .iter()
            .zip(&parts[1..4])
            .all(|(len, part)| part.len() == *len && part.bytes().all(|c| c.is_ascii_digit()))
        && parts[4].ends_with(".mp4")
        && parts[4].len() > 4
}

#[cfg(test)]
mod camera_viewer_tests {
    use super::is_camera_recording_key;

    #[test]
    fn camera_recording_scope() {
        assert!(is_camera_recording_key(
            "camera/2026/10/30/1793314800-session.mp4"
        ));
        for key in [
            "camera/../10/30/a.mp4",
            "camera/2026/10/30/a.json",
            "camera/a.mp4",
            "/2026/10/30/a.mp4",
            "camera/2026/10/30/nested/a.mp4",
        ] {
            assert!(!is_camera_recording_key(key), "{key}");
        }
    }
}

/// 内部viewer専用。通常のS3 APIとは別に、公開する保存領域と署名期限を固定する。
pub async fn browser_get_object(app_state: &AppState, body: &[u8]) -> Result<Reply, ApiError> {
    match browser_presign(app_state, body)? {
        Some(output) => Ok(json_response(output::presigned(&output).to_string())
            .header("Cache-Control", "no-store")),
        None => Ok(browser_unavailable()),
    }
}

/// viewer 向けの外部 endpoint が設定されていないときの応答。
pub fn browser_unavailable() -> Reply {
    reply(503).header("Cache-Control", "no-store")
}

/// viewer が開いてよいオブジェクトなら、外部 endpoint で署名した GET を返す。
/// 外部 endpoint が設定されていなければ `None`。
pub fn browser_presign(
    app_state: &AppState,
    body: &[u8],
) -> Result<Option<shiguredo_s3::api::PresignedRequest>, ApiError> {
    let payload = parse_body(body, get_object_input)?;
    let allowed = ((payload.bucket == "pdfs"
        && ((payload.key.starts_with("hitomi/")
            && (payload.key.ends_with(".pdf") || payload.key.ends_with("_prev.jpg")))
            || (payload.key.starts_with("videos/") && payload.key.ends_with(".mp4"))))
        || (payload.bucket == "home-camera-recordings" && is_camera_recording_key(&payload.key)))
        && !payload
            .key
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        && !payload.key.contains(['\\', '\r', '\n']);
    if !allowed {
        return Err(ApiError::BadRequest(
            "Object is outside the viewer media area".into(),
        ));
    }
    let Some(endpoint) = &app_state.settings.s3_browser_endpoint else {
        return Ok(None);
    };
    let parsed = url::Url::parse(endpoint)
        .map_err(|_| ApiError::InternalServerError("Invalid browser S3 endpoint".into()))?;
    if parsed.scheme() != "https"
        || parsed.host_str().is_none()
        || parsed.path() != "/"
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(ApiError::InternalServerError(
            "Browser S3 endpoint must be an HTTPS origin".into(),
        ));
    }
    let mut settings = app_state.settings.clone();
    settings.s3_endpoint = Some(endpoint.trim_end_matches('/').to_string());
    settings.s3_use_path_style = true;
    s3_service::presigned_get(
        &settings,
        PresignedObjectInput {
            bucket: payload.bucket,
            key: payload.key,
            expires_in_secs: 900,
        },
    )
    .map(Some)
}

pub async fn presigned_put_object(app_state: &AppState, body: &[u8]) -> Result<Reply, ApiError> {
    let input = parse_body(body, presigned_object_input)?;
    let output = s3_service::presigned_put(&app_state.settings, input)?;
    Ok(json_response(output::presigned(&output).to_string()))
}

pub fn s3_preflight() -> Reply {
    reply(204)
}

pub async fn copy_object(app_state: &AppState, body: &[u8]) -> Result<Reply, ApiError> {
    let input = parse_body(body, copy_object_input)?;
    let output = s3_service::copy_object(&app_state.client, &app_state.settings, input).await?;
    Ok(json_response(output::copy_object(&output).to_string()))
}

pub async fn get_object_tagging(app_state: &AppState, body: &[u8]) -> Result<Reply, ApiError> {
    let input = parse_body(body, object_tagging_input)?;
    let output =
        s3_service::get_object_tagging(&app_state.client, &app_state.settings, input).await?;
    Ok(json_response(
        output::get_object_tagging(&output).to_string(),
    ))
}

pub async fn put_object_tagging(app_state: &AppState, body: &[u8]) -> Result<Reply, ApiError> {
    let (input, tags) = parse_body(body, |root| {
        Ok((object_tagging_input(root)?, parse_tags(root)?))
    })?;
    let output =
        s3_service::put_object_tagging(&app_state.client, &app_state.settings, input, tags).await?;
    Ok(json_response(
        output::put_object_tagging(&output).to_string(),
    ))
}

pub async fn delete_object_tagging(app_state: &AppState, body: &[u8]) -> Result<Reply, ApiError> {
    let input = parse_body(body, object_tagging_input)?;
    let output =
        s3_service::delete_object_tagging(&app_state.client, &app_state.settings, input).await?;
    Ok(json_response(
        output::delete_object_tagging(&output).to_string(),
    ))
}

/// A successful upload is not rolled back when notification fails. The caller
/// can retry just /slack/message using the returned URL.
pub async fn put_object_and_notify(app_state: &AppState, body: &[u8]) -> Result<Reply, ApiError> {
    struct Notify {
        bucket: String,
        key: String,
        file_data_base64: String,
        content_type: Option<String>,
        channel: String,
        expires_in_secs: u64,
        text: String,
    }
    let payload = parse_body(body, |root| {
        let bucket = required(root, "bucket")?;
        let key = required(root, "key")?;
        let file_data_base64 = required(root, "file_data_base64")?;
        let content_type = optional(root, "content_type")?;
        let channel = nonempty_string(root, "channel")?;
        nonempty_string(root, "bucket")?;
        nonempty_string(root, "key")?;
        let expires_in_secs = optional(root, "expires_in_secs")?.unwrap_or(900);
        if !(1..=604800).contains(&expires_in_secs) {
            return Err(ApiError::BadRequest(
                "expires_in_secs must be between 1 and 604800".into(),
            ));
        }
        let text = optional(root, "text")?.unwrap_or_else(|| "File uploaded".into());
        Ok(Notify {
            bucket,
            key,
            file_data_base64,
            content_type,
            channel,
            expires_in_secs,
            text,
        })
    })?;
    let data = s3_service::decode_base64_payload(&payload.file_data_base64)?;
    // Validate signing configuration before the upload has any side effects.
    let signed = s3_service::presigned_get(
        &app_state.settings,
        PresignedObjectInput {
            bucket: payload.bucket.clone(),
            key: payload.key.clone(),
            expires_in_secs: payload.expires_in_secs,
        },
    )?;
    let url = signed.url;
    let stored = s3_service::put_object(
        &app_state.client,
        &app_state.settings,
        PutObjectInput {
            bucket: payload.bucket,
            key: payload.key,
            body: data,
            content_type: payload.content_type,
        },
    )
    .await?;
    use crate::service::slack_service::{SlackClient, post_message};
    let notification = post_message(
        &SlackClient::new(&app_state.client, &app_state.settings),
        &payload.channel,
        &format!("{}\n{url}", payload.text),
    )
    .await;
    let (slack_response, notification_error) = match notification {
        Ok(response) => (Some(response), None),
        Err(error) => (None, Some(error.to_string())),
    };
    Ok(json_response(
        nojson::json(|f| {
            f.object(|f| {
                f.member("uploaded", true)?;
                f.member("object", output::put_object(&stored))?;
                f.member("url", &url)?;
                f.member("notified", slack_response.is_some())?;
                f.member("notification_error", &notification_error)
            })
        })
        .to_string(),
    ))
}

#[cfg(test)]
mod tests;
