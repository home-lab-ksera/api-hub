//! multipart upload の操作。

use super::*;

pub async fn create_multipart_upload(
    http_client: &HttpClient,
    settings: &Settings,
    input: CreateMultipartUploadInput,
) -> Result<shiguredo_s3::types::CreateMultipartUploadOutput, ApiError> {
    let s3 = create_s3_client(settings)?;
    let mut req = s3
        .create_multipart_upload()
        .bucket(input.bucket)
        .key(input.key);
    if let Some(content_type) = input.content_type {
        req = req.content_type(content_type);
    }

    let output = send(
        http_client,
        req.build_request(),
        shiguredo_s3::api::CreateMultipartUploadFluentBuilder::parse_response,
    )
    .await?;

    Ok(output)
}

pub async fn upload_part(
    http_client: &HttpClient,
    settings: &Settings,
    input: UploadPartInput,
) -> Result<shiguredo_s3::types::UploadPartOutput, ApiError> {
    let s3 = create_s3_client(settings)?;
    let output = send(
        http_client,
        s3.upload_part()
            .bucket(input.bucket)
            .key(input.key)
            .upload_id(input.upload_id)
            .part_number(input.part_number)
            .body(input.body)
            .build_request(),
        shiguredo_s3::api::UploadPartFluentBuilder::parse_response,
    )
    .await?;

    Ok(output)
}

pub async fn complete_multipart_upload(
    http_client: &HttpClient,
    settings: &Settings,
    input: CompleteMultipartUploadInput,
) -> Result<shiguredo_s3::types::CompleteMultipartUploadOutput, ApiError> {
    let s3 = create_s3_client(settings)?;
    let multipart_upload = CompletedMultipartUpload {
        parts: Some(
            input
                .parts
                .into_iter()
                .map(|part| CompletedPart {
                    part_number: Some(part.part_number),
                    e_tag: Some(part.e_tag),
                })
                .collect(),
        ),
    };

    let output = send(
        http_client,
        s3.complete_multipart_upload()
            .bucket(input.bucket)
            .key(input.key)
            .upload_id(input.upload_id)
            .multipart_upload(multipart_upload)
            .build_request(),
        shiguredo_s3::api::CompleteMultipartUploadFluentBuilder::parse_response,
    )
    .await?;

    Ok(output)
}

pub async fn abort_multipart_upload(
    http_client: &HttpClient,
    settings: &Settings,
    input: AbortMultipartUploadInput,
) -> Result<(), ApiError> {
    let s3 = create_s3_client(settings)?;
    send(
        http_client,
        s3.abort_multipart_upload()
            .bucket(input.bucket)
            .key(input.key)
            .upload_id(input.upload_id)
            .build_request(),
        shiguredo_s3::api::AbortMultipartUploadFluentBuilder::parse_response,
    )
    .await?;

    Ok(())
}

pub async fn list_parts(
    http_client: &HttpClient,
    settings: &Settings,
    input: ListPartsInput,
) -> Result<shiguredo_s3::types::ListPartsOutput, ApiError> {
    let s3 = create_s3_client(settings)?;
    let mut req = s3
        .list_parts()
        .bucket(input.bucket)
        .key(input.key)
        .upload_id(input.upload_id);
    if let Some(max_parts) = input.max_parts {
        req = req.max_parts(max_parts);
    }
    if let Some(part_number_marker) = input.part_number_marker {
        req = req.part_number_marker(part_number_marker);
    }

    let output = send(
        http_client,
        req.build_request(),
        shiguredo_s3::api::ListPartsFluentBuilder::parse_response,
    )
    .await?;
    Ok(output)
}

pub async fn list_multipart_uploads(
    http_client: &HttpClient,
    settings: &Settings,
    input: ListMultipartUploadsInput,
) -> Result<shiguredo_s3::types::ListMultipartUploadsOutput, ApiError> {
    let s3 = create_s3_client(settings)?;
    let mut req = s3.list_multipart_uploads().bucket(input.bucket);
    if let Some(prefix) = input.prefix {
        req = req.prefix(prefix);
    }
    if let Some(delimiter) = input.delimiter {
        req = req.delimiter(delimiter);
    }
    if let Some(max_uploads) = input.max_uploads {
        req = req.max_uploads(max_uploads);
    }
    if let Some(key_marker) = input.key_marker {
        req = req.key_marker(key_marker);
    }
    if let Some(upload_id_marker) = input.upload_id_marker {
        req = req.upload_id_marker(upload_id_marker);
    }

    let output = send(
        http_client,
        req.build_request(),
        shiguredo_s3::api::ListMultipartUploadsFluentBuilder::parse_response,
    )
    .await?;
    Ok(output)
}
