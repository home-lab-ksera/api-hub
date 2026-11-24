use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use shiguredo_s3::{
    Credential, S3Client, S3Config, S3Request, S3Response,
    types::{CompletedMultipartUpload, CompletedPart, HttpDate, ObjectIdentifier},
};

use crate::{config::settings::Settings, errors::api_error::ApiError, http_client::HttpClient};

mod buckets;
mod client;
mod inputs;
mod multipart;
mod streaming;
pub use buckets::*;
pub use client::*;
pub use inputs::*;
pub use multipart::*;
pub(crate) use streaming::*;

pub async fn put_object(
    http_client: &HttpClient,
    settings: &Settings,
    input: PutObjectInput,
) -> Result<shiguredo_s3::types::PutObjectOutput, ApiError> {
    let s3 = create_s3_client(settings)?;
    let mut req = s3
        .put_object()
        .bucket(input.bucket)
        .key(input.key)
        .body(input.body);
    if let Some(content_type) = input.content_type {
        req = req.content_type(content_type);
    }

    let output = send(
        http_client,
        req.build_request(),
        shiguredo_s3::api::PutObjectFluentBuilder::parse_response,
    )
    .await?;

    Ok(output)
}

pub async fn get_object(
    http_client: &HttpClient,
    settings: &Settings,
    input: GetObjectInput,
) -> Result<shiguredo_s3::types::GetObjectOutput, ApiError> {
    let s3 = create_s3_client(settings)?;
    let output = send(
        http_client,
        s3.get_object()
            .bucket(input.bucket)
            .key(input.key)
            .build_request(),
        shiguredo_s3::api::GetObjectFluentBuilder::parse_response,
    )
    .await?;

    Ok(output)
}

pub async fn get_object_proxy_stream(
    http_client: &HttpClient,
    settings: &Settings,
    input: ProxyObjectStreamInput,
) -> Result<reqwest::Response, ApiError> {
    let s3 = create_s3_client(settings)?;
    let mut request_builder = s3.get_object().bucket(input.bucket).key(input.key);
    if let Some(range) = input.range {
        request_builder = request_builder.range(range);
    }
    if let Some(if_match) = input.if_match {
        request_builder = request_builder.if_match(if_match);
    }
    if let Some(if_none_match) = input.if_none_match {
        request_builder = request_builder.if_none_match(if_none_match);
    }
    if let Some(if_modified_since) = input.if_modified_since {
        request_builder =
            request_builder.if_modified_since(HttpDate::from_imf_fixdate(if_modified_since));
    }
    if let Some(if_unmodified_since) = input.if_unmodified_since {
        request_builder =
            request_builder.if_unmodified_since(HttpDate::from_imf_fixdate(if_unmodified_since));
    }

    let request = request_builder
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;

    let url = build_s3_url(&request)?;
    crate::http_client::request(
        http_client,
        request.method,
        url,
        request.headers,
        request.body,
        true,
    )
    .send()
    .await
    .map_err(|e| ApiError::InternalServerError(format!("S3 HTTP request failed: {e}")))
}

pub async fn head_object(
    http_client: &HttpClient,
    settings: &Settings,
    input: HeadObjectInput,
) -> Result<shiguredo_s3::types::HeadObjectOutput, ApiError> {
    let s3 = create_s3_client(settings)?;
    let request = s3
        .head_object()
        .bucket(input.bucket)
        .key(input.key)
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http_client, request).await?;
    if response.status_code == 404 {
        return Err(ApiError::NotFound("S3 object not found".into()));
    }
    let output = shiguredo_s3::api::HeadObjectFluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;

    Ok(output)
}

pub async fn delete_object(
    http_client: &HttpClient,
    settings: &Settings,
    input: DeleteObjectInput,
) -> Result<shiguredo_s3::types::DeleteObjectOutput, ApiError> {
    let s3 = create_s3_client(settings)?;
    let output = send(
        http_client,
        s3.delete_object()
            .bucket(input.bucket)
            .key(input.key)
            .build_request(),
        shiguredo_s3::api::DeleteObjectFluentBuilder::parse_response,
    )
    .await?;

    Ok(output)
}

pub async fn delete_objects(
    http_client: &HttpClient,
    settings: &Settings,
    input: DeleteObjectsInput,
) -> Result<shiguredo_s3::types::DeleteObjectsOutput, ApiError> {
    let s3 = create_s3_client(settings)?;
    let mut req = s3.delete_objects().bucket(input.bucket).quiet(input.quiet);
    for object in input.objects {
        req = req.object(ObjectIdentifier {
            key: object.key,
            version_id: object.version_id,
        });
    }

    let output = send(
        http_client,
        req.build_request(),
        shiguredo_s3::api::DeleteObjectsFluentBuilder::parse_response,
    )
    .await?;

    Ok(output)
}

pub async fn list_objects_v2(
    http_client: &HttpClient,
    settings: &Settings,
    input: ListObjectsV2Input,
) -> Result<shiguredo_s3::types::ListObjectsV2Output, ApiError> {
    let s3 = create_s3_client(settings)?;
    let mut req = s3.list_objects_v2().bucket(input.bucket);
    if let Some(prefix) = input.prefix {
        req = req.prefix(prefix);
    }
    if let Some(delimiter) = input.delimiter {
        req = req.delimiter(delimiter);
    }
    if let Some(max_keys) = input.max_keys {
        req = req.max_keys(max_keys);
    }
    if let Some(token) = input.continuation_token {
        req = req.continuation_token(token);
    } else if let Some(start_after) = input.start_after {
        req = req.start_after(start_after);
    }

    let output = send(
        http_client,
        req.build_request(),
        shiguredo_s3::api::ListObjectsV2FluentBuilder::parse_response,
    )
    .await?;

    Ok(output)
}

pub fn presigned_get(
    settings: &Settings,
    input: PresignedObjectInput,
) -> Result<shiguredo_s3::api::PresignedRequest, ApiError> {
    let s3 = create_s3_client(settings)?;
    let output = s3
        .get_object()
        .bucket(input.bucket)
        .key(input.key)
        .presigned(input.expires_in_secs)
        .map_err(map_s3_input_error_to_api_error)?;

    Ok(output)
}

pub fn presigned_put(
    settings: &Settings,
    input: PresignedObjectInput,
) -> Result<shiguredo_s3::api::PresignedRequest, ApiError> {
    let s3 = create_s3_client(settings)?;
    let output = s3
        .put_object()
        .bucket(input.bucket)
        .key(input.key)
        .presigned(input.expires_in_secs)
        .map_err(map_s3_input_error_to_api_error)?;

    Ok(output)
}

pub async fn copy_object(
    http: &HttpClient,
    settings: &Settings,
    input: CopyObjectInput,
) -> Result<shiguredo_s3::types::CopyObjectOutput, ApiError> {
    let s3 = create_s3_client(settings)?;
    let mut source = format!(
        "{}/{}",
        encode_copy_component(&input.source_bucket),
        input
            .source_key
            .split('/')
            .map(encode_copy_component)
            .collect::<Vec<_>>()
            .join("/")
    );
    if let Some(version) = input.source_version_id {
        source.push_str(&format!("?versionId={}", encode_copy_component(&version)));
    }
    let output = send(
        http,
        s3.copy_object()
            .bucket(input.bucket)
            .key(input.key)
            .copy_source(source)
            .build_request(),
        shiguredo_s3::api::CopyObjectFluentBuilder::parse_response,
    )
    .await?;
    Ok(output)
}

pub async fn get_object_tagging(
    http: &HttpClient,
    settings: &Settings,
    input: ObjectTaggingInput,
) -> Result<shiguredo_s3::types::GetObjectTaggingOutput, ApiError> {
    let s3 = create_s3_client(settings)?;
    let mut builder = s3.get_object_tagging().bucket(input.bucket).key(input.key);
    if let Some(version) = input.version_id {
        builder = builder.version_id(version);
    }
    let output = send(
        http,
        builder.build_request(),
        shiguredo_s3::api::GetObjectTaggingFluentBuilder::parse_response,
    )
    .await?;
    Ok(output)
}

pub async fn put_object_tagging(
    http: &HttpClient,
    settings: &Settings,
    input: ObjectTaggingInput,
    tags: Vec<shiguredo_s3::types::Tag>,
) -> Result<shiguredo_s3::types::PutObjectTaggingOutput, ApiError> {
    let s3 = create_s3_client(settings)?;
    let mut builder = s3.put_object_tagging().bucket(input.bucket).key(input.key);
    if let Some(version) = input.version_id {
        builder = builder.version_id(version);
    }
    builder = builder.tagging(shiguredo_s3::types::Tagging { tag_set: tags });
    let output = send(
        http,
        builder.build_request(),
        shiguredo_s3::api::PutObjectTaggingFluentBuilder::parse_response,
    )
    .await?;
    Ok(output)
}

pub async fn delete_object_tagging(
    http: &HttpClient,
    settings: &Settings,
    input: ObjectTaggingInput,
) -> Result<shiguredo_s3::types::DeleteObjectTaggingOutput, ApiError> {
    let s3 = create_s3_client(settings)?;
    let mut builder = s3
        .delete_object_tagging()
        .bucket(input.bucket)
        .key(input.key);
    if let Some(version) = input.version_id {
        builder = builder.version_id(version);
    }
    let output = send(
        http,
        builder.build_request(),
        shiguredo_s3::api::DeleteObjectTaggingFluentBuilder::parse_response,
    )
    .await?;
    Ok(output)
}

#[cfg(test)]
mod tests;
