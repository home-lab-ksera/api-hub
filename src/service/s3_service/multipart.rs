//! multipart upload の操作。

use super::*;

pub async fn create_multipart_upload(
    http_client: &HttpClient,
    settings: &Settings,
    input: CreateMultipartUploadInput,
) -> Result<String, ApiError> {
    let s3 = create_s3_client(settings)?;
    let mut req = s3
        .create_multipart_upload()
        .bucket(input.bucket)
        .key(input.key);
    if let Some(content_type) = input.content_type {
        req = req.content_type(content_type);
    }

    let request = req
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http_client, request).await?;
    let output = shiguredo_s3::api::CreateMultipartUploadFluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;

    Ok(nojson::json(|f| {
        f.object(|f| {
            f.member("bucket", &output.bucket)?;
            f.member("key", &output.key)?;
            f.member("upload_id", &output.upload_id)
        })
    })
    .to_string())
}

pub async fn upload_part(
    http_client: &HttpClient,
    settings: &Settings,
    input: UploadPartInput,
) -> Result<String, ApiError> {
    let s3 = create_s3_client(settings)?;
    let request = s3
        .upload_part()
        .bucket(input.bucket)
        .key(input.key)
        .upload_id(input.upload_id)
        .part_number(input.part_number)
        .body(input.body)
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http_client, request).await?;
    let output = shiguredo_s3::api::UploadPartFluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;

    Ok(nojson::json(|f| f.object(|f| f.member("e_tag", &output.e_tag))).to_string())
}

pub async fn complete_multipart_upload(
    http_client: &HttpClient,
    settings: &Settings,
    input: CompleteMultipartUploadInput,
) -> Result<String, ApiError> {
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

    let request = s3
        .complete_multipart_upload()
        .bucket(input.bucket)
        .key(input.key)
        .upload_id(input.upload_id)
        .multipart_upload(multipart_upload)
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http_client, request).await?;
    let output = shiguredo_s3::api::CompleteMultipartUploadFluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;

    Ok(nojson::json(|f| {
        f.object(|f| {
            f.member("location", &output.location)?;
            f.member("bucket", &output.bucket)?;
            f.member("key", &output.key)?;
            f.member("e_tag", &output.e_tag)?;
            f.member("version_id", &output.version_id)
        })
    })
    .to_string())
}

pub async fn abort_multipart_upload(
    http_client: &HttpClient,
    settings: &Settings,
    input: AbortMultipartUploadInput,
) -> Result<String, ApiError> {
    let s3 = create_s3_client(settings)?;
    let request = s3
        .abort_multipart_upload()
        .bucket(input.bucket)
        .key(input.key)
        .upload_id(input.upload_id)
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http_client, request).await?;
    shiguredo_s3::api::AbortMultipartUploadFluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;

    Ok(nojson::json(|f| f.object(|f| f.member("aborted", true))).to_string())
}

pub async fn list_parts(
    http_client: &HttpClient,
    settings: &Settings,
    input: ListPartsInput,
) -> Result<String, ApiError> {
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

    let request = req
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http_client, request).await?;
    let output = shiguredo_s3::api::ListPartsFluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;
    let parts = output.parts.unwrap_or_default();

    Ok(nojson::json(|f| {
        f.object(|f| {
            f.member("bucket", &output.bucket)?;
            f.member("key", &output.key)?;
            f.member("upload_id", &output.upload_id)?;
            f.member("part_number_marker", output.part_number_marker)?;
            f.member("next_part_number_marker", output.next_part_number_marker)?;
            f.member("max_parts", output.max_parts)?;
            f.member("is_truncated", output.is_truncated)?;
            f.member("storage_class", &output.storage_class)?;
            f.member(
                "parts",
                nojson::array(|f| {
                    for part in &parts {
                        f.element(nojson::object(|f| {
                            f.member("part_number", part.part_number)?;
                            f.member("last_modified", &part.last_modified)?;
                            f.member("e_tag", &part.e_tag)?;
                            f.member("size", part.size)
                        }))?;
                    }
                    Ok(())
                }),
            )
        })
    })
    .to_string())
}

pub async fn list_multipart_uploads(
    http_client: &HttpClient,
    settings: &Settings,
    input: ListMultipartUploadsInput,
) -> Result<String, ApiError> {
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

    let request = req
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http_client, request).await?;
    let output = shiguredo_s3::api::ListMultipartUploadsFluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;
    let uploads = output.uploads.unwrap_or_default();
    let common_prefixes = output.common_prefixes.unwrap_or_default();

    Ok(nojson::json(|f| {
        f.object(|f| {
            f.member("bucket", &output.bucket)?;
            f.member("key_marker", &output.key_marker)?;
            f.member("upload_id_marker", &output.upload_id_marker)?;
            f.member("next_key_marker", &output.next_key_marker)?;
            f.member("next_upload_id_marker", &output.next_upload_id_marker)?;
            f.member("prefix", &output.prefix)?;
            f.member("delimiter", &output.delimiter)?;
            f.member("max_uploads", output.max_uploads)?;
            f.member("is_truncated", output.is_truncated)?;
            f.member(
                "uploads",
                nojson::array(|f| {
                    for upload in &uploads {
                        f.element(nojson::object(|f| {
                            f.member("upload_id", &upload.upload_id)?;
                            f.member("key", &upload.key)?;
                            f.member("initiated", &upload.initiated)?;
                            f.member("storage_class", &upload.storage_class)
                        }))?;
                    }
                    Ok(())
                }),
            )?;
            f.member(
                "common_prefixes",
                nojson::array(|f| {
                    for prefix in &common_prefixes {
                        f.element(nojson::object(|f| f.member("prefix", &prefix.prefix)))?;
                    }
                    Ok(())
                }),
            )
        })
    })
    .to_string())
}
