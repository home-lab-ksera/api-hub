//! multipart upload の handler。

use super::*;

pub async fn create_multipart_upload(app_state: &AppState, body: &[u8]) -> Result<Reply, ApiError> {
    let input = parse_body(body, create_multipart_upload_input)?;
    let output =
        s3_service::create_multipart_upload(&app_state.client, &app_state.settings, input).await?;
    Ok(json_response(
        output::create_multipart_upload(&output).to_string(),
    ))
}

pub async fn upload_part_base64(app_state: &AppState, body: &[u8]) -> Result<Reply, ApiError> {
    let input = parse_body(body, upload_part_base64_input)?;
    let output = s3_service::upload_part(&app_state.client, &app_state.settings, input).await?;
    Ok(json_response(output::upload_part(&output).to_string()))
}

pub async fn complete_multipart_upload(
    app_state: &AppState,
    body: &[u8],
) -> Result<Reply, ApiError> {
    let input = parse_body(body, complete_multipart_upload_input)?;
    let output =
        s3_service::complete_multipart_upload(&app_state.client, &app_state.settings, input)
            .await?;
    Ok(json_response(
        output::complete_multipart_upload(&output).to_string(),
    ))
}

pub async fn abort_multipart_upload(app_state: &AppState, body: &[u8]) -> Result<Reply, ApiError> {
    let input = parse_body(body, abort_multipart_upload_input)?;
    s3_service::abort_multipart_upload(&app_state.client, &app_state.settings, input).await?;
    Ok(json_response(output::abort_multipart_upload().to_string()))
}

pub async fn list_parts(app_state: &AppState, body: &[u8]) -> Result<Reply, ApiError> {
    let input = parse_body(body, list_parts_input)?;
    let output = s3_service::list_parts(&app_state.client, &app_state.settings, input).await?;
    Ok(json_response(output::list_parts(&output).to_string()))
}

pub async fn list_multipart_uploads(app_state: &AppState, body: &[u8]) -> Result<Reply, ApiError> {
    let input = parse_body(body, list_multipart_uploads_input)?;
    let output =
        s3_service::list_multipart_uploads(&app_state.client, &app_state.settings, input).await?;
    Ok(json_response(
        output::list_multipart_uploads(&output).to_string(),
    ))
}
