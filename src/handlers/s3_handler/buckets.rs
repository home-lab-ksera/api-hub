//! bucket 操作の handler。

use super::*;

pub async fn list_buckets(app_state: &AppState) -> Result<Response, ApiError> {
    let result = s3_service::list_buckets(&app_state.client, &app_state.settings).await?;
    Ok(json_response(result))
}

pub async fn create_bucket(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let bucket = parse_body(body, bucket_name)?;
    let input = CreateBucketInput { bucket };
    Ok(json_response(
        s3_service::create_bucket(&app_state.client, &app_state.settings, input).await?,
    ))
}

pub async fn head_bucket(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let bucket = parse_body(body, bucket_name)?;
    let input = HeadBucketInput { bucket };
    Ok(json_response(
        s3_service::head_bucket(&app_state.client, &app_state.settings, input).await?,
    ))
}

pub async fn delete_bucket(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let bucket = parse_body(body, bucket_name)?;
    let input = DeleteBucketInput { bucket };
    Ok(json_response(
        s3_service::delete_bucket(&app_state.client, &app_state.settings, input).await?,
    ))
}
