//! multipart upload の handler。

use super::*;

pub async fn create_multipart_upload(
    app_state: &AppState,
    body: &[u8],
) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_create_multipart_upload_request(&body)?;
    let result = s3_service::create_multipart_upload(
        &app_state.client,
        &app_state.settings,
        CreateMultipartUploadInput {
            bucket: payload.bucket,
            key: payload.key,
            content_type: payload.content_type,
        },
    )
    .await?;
    Ok(json_response(result))
}

pub async fn upload_part_base64(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_upload_part_base64_request(&body)?;
    let body = s3_service::decode_base64_payload(&payload.part_data_base64)?;
    let result = s3_service::upload_part(
        &app_state.client,
        &app_state.settings,
        UploadPartInput {
            bucket: payload.bucket,
            key: payload.key,
            upload_id: payload.upload_id,
            part_number: payload.part_number,
            body,
        },
    )
    .await?;
    Ok(json_response(result))
}

pub async fn complete_multipart_upload(
    app_state: &AppState,
    body: &[u8],
) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_complete_multipart_upload_request(&body)?;
    let parts = payload
        .parts
        .into_iter()
        .map(|part| CompletePartInput {
            part_number: part.part_number,
            e_tag: part.e_tag,
        })
        .collect::<Vec<_>>();

    let result = s3_service::complete_multipart_upload(
        &app_state.client,
        &app_state.settings,
        CompleteMultipartUploadInput {
            bucket: payload.bucket,
            key: payload.key,
            upload_id: payload.upload_id,
            parts,
        },
    )
    .await?;
    Ok(json_response(result))
}

pub async fn abort_multipart_upload(
    app_state: &AppState,
    body: &[u8],
) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_abort_multipart_upload_request(&body)?;
    let result = s3_service::abort_multipart_upload(
        &app_state.client,
        &app_state.settings,
        AbortMultipartUploadInput {
            bucket: payload.bucket,
            key: payload.key,
            upload_id: payload.upload_id,
        },
    )
    .await?;
    Ok(json_response(result))
}

pub async fn list_parts(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_list_parts_request(&body)?;
    let result = s3_service::list_parts(
        &app_state.client,
        &app_state.settings,
        ListPartsInput {
            bucket: payload.bucket,
            key: payload.key,
            upload_id: payload.upload_id,
            max_parts: payload.max_parts,
            part_number_marker: payload.part_number_marker,
        },
    )
    .await?;
    Ok(json_response(result))
}

pub async fn list_multipart_uploads(
    app_state: &AppState,
    body: &[u8],
) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_list_multipart_uploads_request(&body)?;
    let result = s3_service::list_multipart_uploads(
        &app_state.client,
        &app_state.settings,
        ListMultipartUploadsInput {
            bucket: payload.bucket,
            prefix: payload.prefix,
            delimiter: payload.delimiter,
            max_uploads: payload.max_uploads,
            key_marker: payload.key_marker,
            upload_id_marker: payload.upload_id_marker,
        },
    )
    .await?;
    Ok(json_response(result))
}
