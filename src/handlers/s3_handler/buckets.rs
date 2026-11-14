//! bucket 操作の handler。

use super::*;

pub async fn list_buckets(app_state: &AppState) -> Result<Response, ApiError> {
    let result = s3_service::list_buckets(&app_state.client, &app_state.settings).await?;
    Ok(json_response(result))
}

pub async fn create_bucket(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_bucket_request(&body)?;
    let result = s3_service::create_bucket(
        &app_state.client,
        &app_state.settings,
        CreateBucketInput {
            bucket: payload.bucket,
        },
    )
    .await?;
    Ok(json_response(result))
}

pub async fn head_bucket(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_bucket_request(&body)?;
    let result = s3_service::head_bucket(
        &app_state.client,
        &app_state.settings,
        HeadBucketInput {
            bucket: payload.bucket,
        },
    )
    .await?;
    Ok(json_response(result))
}

pub async fn delete_bucket(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_bucket_request(&body)?;
    let result = s3_service::delete_bucket(
        &app_state.client,
        &app_state.settings,
        DeleteBucketInput {
            bucket: payload.bucket,
        },
    )
    .await?;
    Ok(json_response(result))
}
