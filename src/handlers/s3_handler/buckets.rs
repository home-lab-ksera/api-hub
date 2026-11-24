//! bucket 操作の handler。

use super::*;

pub async fn list_buckets(app_state: &AppState) -> Result<Response, ApiError> {
    let output = s3_service::list_buckets(&app_state.client, &app_state.settings).await?;
    Ok(json_response(output::list_buckets(&output).to_string()))
}

pub async fn create_bucket(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let bucket = parse_body(body, bucket_name)?;
    let input = CreateBucketInput { bucket };
    let output = s3_service::create_bucket(&app_state.client, &app_state.settings, input).await?;
    Ok(json_response(output::create_bucket(&output).to_string()))
}

pub async fn head_bucket(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let bucket = parse_body(body, bucket_name)?;
    let input = HeadBucketInput { bucket };
    let output = s3_service::head_bucket(&app_state.client, &app_state.settings, input).await?;
    Ok(json_response(output::head_bucket(&output).to_string()))
}

pub async fn delete_bucket(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let bucket = parse_body(body, bucket_name)?;
    let input = DeleteBucketInput { bucket };
    s3_service::delete_bucket(&app_state.client, &app_state.settings, input).await?;
    Ok(json_response(output::delete_bucket().to_string()))
}
