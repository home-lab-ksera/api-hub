use shiguredo_http11::Response;

use crate::{
    config::state::AppState,
    errors::api_error::ApiError,
    service::s3_service::{
        self, AbortMultipartUploadInput, CompleteMultipartUploadInput, CompletePartInput,
        CreateBucketInput, CreateMultipartUploadInput, DeleteBucketInput,
        DeleteObjectIdentifierInput, DeleteObjectInput, DeleteObjectsInput, GetObjectInput,
        HeadBucketInput, HeadObjectInput, ListMultipartUploadsInput, ListObjectsV2Input,
        ListPartsInput, PresignedObjectInput, ProxyObjectInput, ProxyObjectStreamInput,
        PutObjectInput, UploadPartInput,
    },
};

mod buckets;
mod multipart;
mod preview;
mod requests;
pub use buckets::*;
pub use multipart::*;
pub use preview::*;
pub use requests::*;

pub(super) fn json_response(body: String) -> Response {
    Response::new(200, "OK")
        .header("Content-Type", "application/json")
        .body(body.into_bytes())
}

pub async fn put_object_base64(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_put_object_base64_request(&body)?;
    let body = s3_service::decode_base64_payload(&payload.file_data_base64)?;
    let result = s3_service::put_object(
        &app_state.client,
        &app_state.settings,
        PutObjectInput {
            bucket: payload.bucket,
            key: payload.key,
            body,
            content_type: payload.content_type,
        },
    )
    .await?;
    Ok(json_response(result))
}

pub async fn get_object_base64(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_get_object_request(&body)?;
    let result = s3_service::get_object(
        &app_state.client,
        &app_state.settings,
        GetObjectInput {
            bucket: payload.bucket,
            key: payload.key,
        },
    )
    .await?;
    Ok(json_response(result))
}

pub async fn head_object(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_head_object_request(&body)?;
    let result = s3_service::head_object(
        &app_state.client,
        &app_state.settings,
        HeadObjectInput {
            bucket: payload.bucket,
            key: payload.key,
        },
    )
    .await?;
    Ok(json_response(result))
}

pub async fn delete_object(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_delete_object_request(&body)?;
    let result = s3_service::delete_object(
        &app_state.client,
        &app_state.settings,
        DeleteObjectInput {
            bucket: payload.bucket,
            key: payload.key,
        },
    )
    .await?;
    Ok(json_response(result))
}

pub async fn delete_objects(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_delete_objects_request(&body)?;
    let objects = payload
        .objects
        .into_iter()
        .map(|object| DeleteObjectIdentifierInput {
            key: object.key,
            version_id: object.version_id,
        })
        .collect::<Vec<_>>();
    let result = s3_service::delete_objects(
        &app_state.client,
        &app_state.settings,
        DeleteObjectsInput {
            bucket: payload.bucket,
            objects,
            quiet: payload.quiet.unwrap_or(false),
        },
    )
    .await?;
    Ok(json_response(result))
}

pub async fn list_objects_v2(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_list_objects_v2_request(&body)?;
    let result = s3_service::list_objects_v2(
        &app_state.client,
        &app_state.settings,
        ListObjectsV2Input {
            bucket: payload.bucket,
            prefix: payload.prefix,
            delimiter: payload.delimiter,
            max_keys: payload.max_keys,
            start_after: payload.start_after,
            continuation_token: payload.continuation_token,
        },
    )
    .await?;
    Ok(json_response(result))
}

pub async fn presigned_get_object(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_presigned_object_request(&body)?;
    let result = s3_service::presigned_get(
        &app_state.settings,
        PresignedObjectInput {
            bucket: payload.bucket,
            key: payload.key,
            expires_in_secs: payload.expires_in_secs.unwrap_or(900),
        },
    )?;
    Ok(json_response(result))
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
pub async fn browser_get_object(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let payload = parse_get_object_request(&body_to_utf8(body)?)?;
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
        return Ok(Response::new(503, "Service Unavailable").header("Cache-Control", "no-store"));
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
    let result = s3_service::presigned_get(
        &settings,
        PresignedObjectInput {
            bucket: payload.bucket,
            key: payload.key,
            expires_in_secs: 900,
        },
    )?;
    Ok(json_response(result).header("Cache-Control", "no-store"))
}

pub async fn presigned_put_object(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_presigned_object_request(&body)?;
    let result = s3_service::presigned_put(
        &app_state.settings,
        PresignedObjectInput {
            bucket: payload.bucket,
            key: payload.key,
            expires_in_secs: payload.expires_in_secs.unwrap_or(900),
        },
    )?;
    Ok(json_response(result))
}

pub fn s3_preflight() -> Response {
    Response::new(204, "No Content")
}

pub async fn copy_object(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let json = parse_json_body(&body)?;
    let root = json.value();
    let input = s3_service::CopyObjectInput {
        bucket: nonempty_string(root, "bucket")?,
        key: nonempty_string(root, "key")?,
        source_bucket: nonempty_string(root, "source_bucket")?,
        source_key: nonempty_string(root, "source_key")?,
        source_version_id: get_optional_string(root, "source_version_id")?,
    };
    Ok(json_response(
        s3_service::copy_object(&app_state.client, &app_state.settings, input).await?,
    ))
}

pub async fn get_object_tagging(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let json = parse_json_body(&body)?;
    let root = json.value();
    let input = s3_service::ObjectTaggingInput {
        bucket: nonempty_string(root, "bucket")?,
        key: nonempty_string(root, "key")?,
        version_id: get_optional_string(root, "version_id")?,
    };
    Ok(json_response(
        s3_service::get_object_tagging(&app_state.client, &app_state.settings, input).await?,
    ))
}

pub async fn put_object_tagging(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let json = parse_json_body(&body)?;
    let root = json.value();
    let input = s3_service::ObjectTaggingInput {
        bucket: nonempty_string(root, "bucket")?,
        key: nonempty_string(root, "key")?,
        version_id: get_optional_string(root, "version_id")?,
    };
    let tags = parse_tags(root)?;
    Ok(json_response(
        s3_service::put_object_tagging(&app_state.client, &app_state.settings, input, tags).await?,
    ))
}

pub async fn delete_object_tagging(
    app_state: &AppState,
    body: &[u8],
) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let json = parse_json_body(&body)?;
    let root = json.value();
    let input = s3_service::ObjectTaggingInput {
        bucket: nonempty_string(root, "bucket")?,
        key: nonempty_string(root, "key")?,
        version_id: get_optional_string(root, "version_id")?,
    };
    Ok(json_response(
        s3_service::delete_object_tagging(&app_state.client, &app_state.settings, input).await?,
    ))
}

/// A successful upload is not rolled back when notification fails. The caller
/// can retry just /slack/message using the returned URL.
pub async fn put_object_and_notify(
    app_state: &AppState,
    body: &[u8],
) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_put_object_base64_request(&body)?;
    let json = parse_json_body(&body)?;
    let root = json.value();
    let channel = nonempty_string(root, "channel")?;
    nonempty_string(root, "bucket")?;
    nonempty_string(root, "key")?;
    let expires = get_optional_u64(root, "expires_in_secs")?.unwrap_or(900);
    if !(1..=604800).contains(&expires) {
        return Err(ApiError::BadRequest(
            "expires_in_secs must be between 1 and 604800".into(),
        ));
    }
    let text = get_optional_string(root, "text")?.unwrap_or_else(|| "File uploaded".into());
    let data = s3_service::decode_base64_payload(&payload.file_data_base64)?;
    // Validate signing configuration before the upload has any side effects.
    let signed = s3_service::presigned_get(
        &app_state.settings,
        PresignedObjectInput {
            bucket: payload.bucket.clone(),
            key: payload.key.clone(),
            expires_in_secs: expires,
        },
    )?;
    let signed_json = parse_json_body(&signed)?;
    let url = get_required_string(signed_json.value(), "url")?;
    let result = s3_service::put_object(
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
    let result_json = parse_json_body(&result)?;
    let notification = crate::service::slack_service::post_message(
        &app_state.client,
        &app_state.settings.slack_bot_token,
        &app_state.settings.slack_api_base_url,
        &channel,
        &format!("{text}\n{url}"),
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
                f.member("object", result_json.value())?;
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
