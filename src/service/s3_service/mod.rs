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
) -> Result<String, ApiError> {
    let s3 = create_s3_client(settings)?;
    let mut req = s3
        .put_object()
        .bucket(input.bucket)
        .key(input.key)
        .body(input.body);
    if let Some(content_type) = input.content_type {
        req = req.content_type(content_type);
    }

    let request = req
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http_client, request).await?;
    let output = shiguredo_s3::api::PutObjectFluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;

    Ok(nojson::json(|f| {
        f.object(|f| {
            f.member("e_tag", &output.e_tag)?;
            f.member("version_id", &output.version_id)
        })
    })
    .to_string())
}

pub async fn get_object(
    http_client: &HttpClient,
    settings: &Settings,
    input: GetObjectInput,
) -> Result<String, ApiError> {
    let s3 = create_s3_client(settings)?;
    let request = s3
        .get_object()
        .bucket(input.bucket)
        .key(input.key)
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http_client, request).await?;
    let output = shiguredo_s3::api::GetObjectFluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;

    Ok(nojson::json(|f| {
        f.object(|f| {
            f.member("file_data_base64", BASE64_STANDARD.encode(&output.body))?;
            f.member("content_type", &output.content_type)?;
            f.member("content_length", output.content_length)?;
            f.member("e_tag", &output.e_tag)?;
            f.member("last_modified", &output.last_modified)?;
            f.member("version_id", &output.version_id)?;
            f.member("metadata", &output.metadata)
        })
    })
    .to_string())
}

pub async fn get_object_proxy(
    http_client: &HttpClient,
    settings: &Settings,
    input: ProxyObjectInput,
) -> Result<S3Response, ApiError> {
    let s3 = create_s3_client(settings)?;
    let request = s3
        .get_object()
        .bucket(input.bucket)
        .key(input.key)
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    execute_s3(http_client, request).await
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
) -> Result<String, ApiError> {
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

    Ok(nojson::json(|f| {
        f.object(|f| {
            f.member("content_type", &output.content_type)?;
            f.member("content_length", output.content_length)?;
            f.member("e_tag", &output.e_tag)?;
            f.member("last_modified", &output.last_modified)?;
            f.member("version_id", &output.version_id)?;
            f.member("metadata", &output.metadata)
        })
    })
    .to_string())
}

pub async fn delete_object(
    http_client: &HttpClient,
    settings: &Settings,
    input: DeleteObjectInput,
) -> Result<String, ApiError> {
    let s3 = create_s3_client(settings)?;
    let request = s3
        .delete_object()
        .bucket(input.bucket)
        .key(input.key)
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http_client, request).await?;
    let output = shiguredo_s3::api::DeleteObjectFluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;

    Ok(nojson::json(|f| {
        f.object(|f| {
            f.member("delete_marker", output.delete_marker)?;
            f.member("version_id", &output.version_id)
        })
    })
    .to_string())
}

pub async fn delete_objects(
    http_client: &HttpClient,
    settings: &Settings,
    input: DeleteObjectsInput,
) -> Result<String, ApiError> {
    let s3 = create_s3_client(settings)?;
    let mut req = s3.delete_objects().bucket(input.bucket).quiet(input.quiet);
    for object in input.objects {
        req = req.object(ObjectIdentifier {
            key: object.key,
            version_id: object.version_id,
        });
    }

    let request = req
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http_client, request).await?;
    let output = shiguredo_s3::api::DeleteObjectsFluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;

    let deleted = output.deleted.unwrap_or_default();
    let errors = output.errors.unwrap_or_default();

    Ok(nojson::json(|f| {
        f.object(|f| {
            f.member(
                "deleted",
                nojson::array(|f| {
                    for deleted in &deleted {
                        f.element(nojson::object(|f| {
                            f.member("key", &deleted.key)?;
                            f.member("version_id", &deleted.version_id)?;
                            f.member("delete_marker", deleted.delete_marker)?;
                            f.member(
                                "delete_marker_version_id",
                                &deleted.delete_marker_version_id,
                            )
                        }))?;
                    }
                    Ok(())
                }),
            )?;
            f.member(
                "errors",
                nojson::array(|f| {
                    for error in &errors {
                        f.element(nojson::object(|f| {
                            f.member("key", &error.key)?;
                            f.member("code", &error.code)?;
                            f.member("message", &error.message)
                        }))?;
                    }
                    Ok(())
                }),
            )
        })
    })
    .to_string())
}

pub async fn list_objects_v2(
    http_client: &HttpClient,
    settings: &Settings,
    input: ListObjectsV2Input,
) -> Result<String, ApiError> {
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

    let request = req
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http_client, request).await?;
    let output = shiguredo_s3::api::ListObjectsV2FluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;

    let contents = output.contents.unwrap_or_default();
    let common_prefixes = output.common_prefixes.unwrap_or_default();

    Ok(nojson::json(|f| {
        f.object(|f| {
            f.member("is_truncated", output.is_truncated)?;
            f.member("name", &output.name)?;
            f.member("prefix", &output.prefix)?;
            f.member("delimiter", &output.delimiter)?;
            f.member("max_keys", output.max_keys)?;
            f.member("key_count", output.key_count)?;
            f.member("continuation_token", &output.continuation_token)?;
            f.member("next_continuation_token", &output.next_continuation_token)?;
            f.member("start_after", &output.start_after)?;
            f.member(
                "contents",
                nojson::array(|f| {
                    for obj in &contents {
                        f.element(nojson::object(|f| {
                            f.member("key", &obj.key)?;
                            f.member("last_modified", &obj.last_modified)?;
                            f.member("e_tag", &obj.e_tag)?;
                            f.member("size", obj.size)?;
                            f.member("storage_class", &obj.storage_class)
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

pub fn presigned_get(settings: &Settings, input: PresignedObjectInput) -> Result<String, ApiError> {
    let s3 = create_s3_client(settings)?;
    let output = s3
        .get_object()
        .bucket(input.bucket)
        .key(input.key)
        .presigned(input.expires_in_secs)
        .map_err(map_s3_input_error_to_api_error)?;

    Ok(nojson::json(|f| {
        f.object(|f| {
            f.member("url", &output.url)?;
            f.member("method", &output.method)?;
            f.member(
                "headers",
                nojson::array(|f| {
                    for (name, value) in &output.headers {
                        f.element(nojson::object(|f| {
                            f.member("name", name)?;
                            f.member("value", value)
                        }))?;
                    }
                    Ok(())
                }),
            )
        })
    })
    .to_string())
}

pub fn presigned_put(settings: &Settings, input: PresignedObjectInput) -> Result<String, ApiError> {
    let s3 = create_s3_client(settings)?;
    let output = s3
        .put_object()
        .bucket(input.bucket)
        .key(input.key)
        .presigned(input.expires_in_secs)
        .map_err(map_s3_input_error_to_api_error)?;

    Ok(nojson::json(|f| {
        f.object(|f| {
            f.member("url", &output.url)?;
            f.member("method", &output.method)?;
            f.member(
                "headers",
                nojson::array(|f| {
                    for (name, value) in &output.headers {
                        f.element(nojson::object(|f| {
                            f.member("name", name)?;
                            f.member("value", value)
                        }))?;
                    }
                    Ok(())
                }),
            )
        })
    })
    .to_string())
}

pub async fn copy_object(
    http: &HttpClient,
    settings: &Settings,
    input: CopyObjectInput,
) -> Result<String, ApiError> {
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
    let request = s3
        .copy_object()
        .bucket(input.bucket)
        .key(input.key)
        .copy_source(source)
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http, request).await?;
    let output = shiguredo_s3::api::CopyObjectFluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;
    Ok(nojson::json(|f| {
        f.object(|f| {
            f.member("e_tag", &output.e_tag)?;
            f.member("last_modified", &output.last_modified)?;
            f.member("version_id", &output.version_id)?;
            f.member("copy_source_version_id", &output.copy_source_version_id)
        })
    })
    .to_string())
}

pub async fn get_object_tagging(
    http: &HttpClient,
    settings: &Settings,
    input: ObjectTaggingInput,
) -> Result<String, ApiError> {
    let s3 = create_s3_client(settings)?;
    let mut builder = s3.get_object_tagging().bucket(input.bucket).key(input.key);
    if let Some(version) = input.version_id {
        builder = builder.version_id(version);
    }
    let request = builder
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http, request).await?;
    let output = shiguredo_s3::api::GetObjectTaggingFluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;
    Ok(nojson::json(|f| {
        f.object(|f| {
            f.member("version_id", &output.version_id)?;
            f.member(
                "tags",
                nojson::array(|f| {
                    for tag in &output.tag_set {
                        f.element(nojson::object(|f| {
                            f.member("key", &tag.key)?;
                            f.member("value", &tag.value)
                        }))?;
                    }
                    Ok(())
                }),
            )?;
            Ok(())
        })
    })
    .to_string())
}

pub async fn put_object_tagging(
    http: &HttpClient,
    settings: &Settings,
    input: ObjectTaggingInput,
    tags: Vec<shiguredo_s3::types::Tag>,
) -> Result<String, ApiError> {
    let s3 = create_s3_client(settings)?;
    let mut builder = s3.put_object_tagging().bucket(input.bucket).key(input.key);
    if let Some(version) = input.version_id {
        builder = builder.version_id(version);
    }
    builder = builder.tagging(shiguredo_s3::types::Tagging { tag_set: tags });
    let request = builder
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http, request).await?;
    let output = shiguredo_s3::api::PutObjectTaggingFluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;
    Ok(nojson::json(|f| {
        f.object(|f| {
            f.member("version_id", &output.version_id)?;
            Ok(())
        })
    })
    .to_string())
}

pub async fn delete_object_tagging(
    http: &HttpClient,
    settings: &Settings,
    input: ObjectTaggingInput,
) -> Result<String, ApiError> {
    let s3 = create_s3_client(settings)?;
    let mut builder = s3
        .delete_object_tagging()
        .bucket(input.bucket)
        .key(input.key);
    if let Some(version) = input.version_id {
        builder = builder.version_id(version);
    }
    let request = builder
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http, request).await?;
    let output = shiguredo_s3::api::DeleteObjectTaggingFluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;
    Ok(nojson::json(|f| {
        f.object(|f| {
            f.member("version_id", &output.version_id)?;
            Ok(())
        })
    })
    .to_string())
}

#[cfg(test)]
mod tests;
