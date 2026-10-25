use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use shiguredo_s3::{
    Credential, S3Client, S3Config, S3Request, S3Response,
    types::{CompletedMultipartUpload, CompletedPart, HttpDate, ObjectIdentifier},
};

use crate::{config::settings::Settings, errors::api_error::ApiError, http_client::HttpClient};

pub struct PutObjectInput {
    pub bucket: String,
    pub key: String,
    pub body: Vec<u8>,
    pub content_type: Option<String>,
}

/// A bounded-memory upload; callers submit parts sequentially and abort on errors.
pub(crate) struct StreamingUpload {
    s3: S3Client,
    bucket: String,
    key: String,
    upload_id: String,
    parts: Vec<CompletedPart>,
}

impl StreamingUpload {
    pub async fn start(
        http: &HttpClient,
        settings: &Settings,
        bucket: String,
        key: String,
        content_type: &str,
    ) -> Result<Self, ApiError> {
        let s3 = create_s3_client(settings)?;
        let request = s3
            .create_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .content_type(content_type)
            .build_request()
            .map_err(map_s3_input_error_to_api_error)?;
        let response = execute_s3(http, request).await?;
        let output =
            shiguredo_s3::api::CreateMultipartUploadFluentBuilder::parse_response(&response)
                .map_err(map_s3_runtime_error_to_api_error)?;
        let upload_id = output.upload_id.ok_or_else(|| {
            ApiError::InternalServerError("S3 did not return an upload ID".into())
        })?;
        Ok(Self {
            s3,
            bucket,
            key,
            upload_id,
            parts: Vec::new(),
        })
    }

    pub async fn write_part(&mut self, http: &HttpClient, body: Vec<u8>) -> Result<(), ApiError> {
        if self.parts.len() == 10_000 {
            return Err(ApiError::BadRequest("Upload exceeds 10000 parts".into()));
        }
        let part_number = self.parts.len() as i32 + 1;
        let request = self
            .s3
            .upload_part()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .part_number(part_number)
            .body(body)
            .build_request()
            .map_err(map_s3_input_error_to_api_error)?;
        let response = execute_s3(http, request).await?;
        let output = shiguredo_s3::api::UploadPartFluentBuilder::parse_response(&response)
            .map_err(map_s3_runtime_error_to_api_error)?;
        let e_tag = output
            .e_tag
            .ok_or_else(|| ApiError::InternalServerError("S3 did not return a part ETag".into()))?;
        self.parts.push(CompletedPart {
            part_number: Some(part_number),
            e_tag: Some(e_tag),
        });
        Ok(())
    }

    pub async fn finish(&self, http: &HttpClient) -> Result<String, ApiError> {
        let request = self
            .s3
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .multipart_upload(CompletedMultipartUpload {
                parts: Some(self.parts.clone()),
            })
            .build_request()
            .map_err(map_s3_input_error_to_api_error)?;
        let response = execute_s3(http, request).await?;
        let output =
            shiguredo_s3::api::CompleteMultipartUploadFluentBuilder::parse_response(&response)
                .map_err(map_s3_runtime_error_to_api_error)?;
        Ok(nojson::json(|f| {
            f.object(|f| {
                f.member("e_tag", &output.e_tag)?;
                f.member("version_id", &output.version_id)
            })
        })
        .to_string())
    }

    pub async fn abort(&self, http: &HttpClient) -> Result<(), ApiError> {
        let request = self
            .s3
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .build_request()
            .map_err(map_s3_input_error_to_api_error)?;
        let response = execute_s3(http, request).await?;
        shiguredo_s3::api::AbortMultipartUploadFluentBuilder::parse_response(&response)
            .map_err(map_s3_runtime_error_to_api_error)?;
        Ok(())
    }
}

pub struct GetObjectInput {
    pub bucket: String,
    pub key: String,
}

pub struct ProxyObjectInput {
    pub bucket: String,
    pub key: String,
}

pub struct ProxyObjectStreamInput {
    pub bucket: String,
    pub key: String,
    pub range: Option<String>,
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<String>,
    pub if_unmodified_since: Option<String>,
}

pub struct HeadObjectInput {
    pub bucket: String,
    pub key: String,
}

pub struct DeleteObjectInput {
    pub bucket: String,
    pub key: String,
}

pub struct DeleteObjectIdentifierInput {
    pub key: String,
    pub version_id: Option<String>,
}

pub struct DeleteObjectsInput {
    pub bucket: String,
    pub objects: Vec<DeleteObjectIdentifierInput>,
    pub quiet: bool,
}

pub struct ListObjectsV2Input {
    pub bucket: String,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub max_keys: Option<i32>,
    pub start_after: Option<String>,
    pub continuation_token: Option<String>,
}

pub struct CreateMultipartUploadInput {
    pub bucket: String,
    pub key: String,
    pub content_type: Option<String>,
}

pub struct UploadPartInput {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub part_number: i32,
    pub body: Vec<u8>,
}

pub struct CompleteMultipartUploadInput {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub parts: Vec<CompletePartInput>,
}

pub struct CompletePartInput {
    pub part_number: i32,
    pub e_tag: String,
}

pub struct AbortMultipartUploadInput {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
}

pub struct ListPartsInput {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub max_parts: Option<i32>,
    pub part_number_marker: Option<i32>,
}

pub struct ListMultipartUploadsInput {
    pub bucket: String,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub max_uploads: Option<i32>,
    pub key_marker: Option<String>,
    pub upload_id_marker: Option<String>,
}

pub struct PresignedObjectInput {
    pub bucket: String,
    pub key: String,
    pub expires_in_secs: u64,
}

pub struct CreateBucketInput {
    pub bucket: String,
}

pub struct HeadBucketInput {
    pub bucket: String,
}

pub struct DeleteBucketInput {
    pub bucket: String,
}

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
        &http_client,
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

pub async fn list_buckets(
    http_client: &HttpClient,
    settings: &Settings,
) -> Result<String, ApiError> {
    let s3 = create_s3_client(settings)?;
    let request = s3
        .list_buckets()
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http_client, request).await?;
    let output = shiguredo_s3::api::ListBucketsFluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;

    Ok(nojson::json(|f| {
        f.object(|f| {
            f.member("continuation_token", &output.continuation_token)?;
            f.member("prefix", &output.prefix)?;
            f.member(
                "buckets",
                nojson::array(|f| {
                    for bucket in &output.buckets {
                        f.element(nojson::object(|f| {
                            f.member("name", &bucket.name)?;
                            f.member("creation_date", &bucket.creation_date)?;
                            f.member("bucket_region", &bucket.bucket_region)?;
                            f.member("bucket_arn", &bucket.bucket_arn)
                        }))?;
                    }
                    Ok(())
                }),
            )
        })
    })
    .to_string())
}

pub async fn create_bucket(
    http_client: &HttpClient,
    settings: &Settings,
    input: CreateBucketInput,
) -> Result<String, ApiError> {
    let s3 = create_s3_client(settings)?;
    let request = s3
        .create_bucket()
        .bucket(input.bucket)
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http_client, request).await?;
    let output = shiguredo_s3::api::CreateBucketFluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;

    Ok(nojson::json(|f| f.object(|f| f.member("location", &output.location))).to_string())
}

pub async fn head_bucket(
    http_client: &HttpClient,
    settings: &Settings,
    input: HeadBucketInput,
) -> Result<String, ApiError> {
    let s3 = create_s3_client(settings)?;
    let request = s3
        .head_bucket()
        .bucket(input.bucket)
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http_client, request).await?;
    let output = shiguredo_s3::api::HeadBucketFluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;

    Ok(
        nojson::json(|f| f.object(|f| f.member("bucket_region", &output.bucket_region)))
            .to_string(),
    )
}

pub async fn delete_bucket(
    http_client: &HttpClient,
    settings: &Settings,
    input: DeleteBucketInput,
) -> Result<String, ApiError> {
    let s3 = create_s3_client(settings)?;
    let request = s3
        .delete_bucket()
        .bucket(input.bucket)
        .build_request()
        .map_err(map_s3_input_error_to_api_error)?;
    let response = execute_s3(http_client, request).await?;
    shiguredo_s3::api::DeleteBucketFluentBuilder::parse_response(&response)
        .map_err(map_s3_runtime_error_to_api_error)?;

    Ok(nojson::json(|f| f.object(|f| f.member("deleted", true))).to_string())
}

pub fn decode_base64_payload(payload: &str) -> Result<Vec<u8>, ApiError> {
    BASE64_STANDARD
        .decode(payload)
        .map_err(|_| ApiError::BadRequest("Failed to decode base64 data".to_string()))
}

fn create_s3_client(settings: &Settings) -> Result<S3Client, ApiError> {
    let credential = if let Some(session_token) = &settings.s3_session_token {
        Credential::with_session_token(
            settings.s3_access_key_id.clone(),
            settings.s3_secret_access_key.clone(),
            session_token.clone(),
        )
    } else {
        Credential::new(
            settings.s3_access_key_id.clone(),
            settings.s3_secret_access_key.clone(),
        )
    };

    let mut config_builder = S3Config::builder()
        .region(settings.s3_region.clone())
        .credential(credential)
        .use_path_style(settings.s3_use_path_style)
        .ignore_cert_check(false);
    if let Some(endpoint) = &settings.s3_endpoint {
        config_builder = config_builder.endpoint(endpoint.clone());
    }

    let config = config_builder
        .build()
        .map_err(map_s3_input_error_to_api_error)?;

    Ok(S3Client::new(config))
}

async fn execute_s3(http_client: &HttpClient, request: S3Request) -> Result<S3Response, ApiError> {
    let url = build_s3_url(&request)?;
    let response = crate::http_client::request(
        &http_client,
        request.method,
        url,
        request.headers,
        request.body,
        false,
    )
    .send()
    .await
    .map_err(|e| ApiError::InternalServerError(format!("S3 HTTP request failed: {e}")))?;

    Ok(S3Response {
        status_code: response.status().as_u16(),
        headers: crate::http_client::headers(&response),
        body: response
            .bytes()
            .await
            .map_err(|e| ApiError::InternalServerError(format!("S3 body failed: {e}")))?
            .to_vec(),
    })
}

fn build_s3_url(request: &S3Request) -> Result<String, ApiError> {
    let scheme = if request.https { "https" } else { "http" };
    let uri = if request.uri.starts_with('/') {
        request.uri.as_str().to_owned()
    } else {
        format!("/{}", request.uri)
    };
    let url = format!("{scheme}://{}:{}{}", request.host, request.port, uri);

    shiguredo_http11::uri::Uri::parse(&url)
        .map(|_| url)
        .map_err(|e| ApiError::InternalServerError(format!("Invalid S3 URL: {e}")))
}

fn map_s3_input_error_to_api_error(error: shiguredo_s3::Error) -> ApiError {
    match error {
        shiguredo_s3::Error::InvalidInput(message) => ApiError::BadRequest(message),
        other => ApiError::InternalServerError(other.to_string()),
    }
}

fn map_s3_runtime_error_to_api_error(error: shiguredo_s3::Error) -> ApiError {
    ApiError::InternalServerError(error.to_string())
}

/// Copy-source is supplied as decoded bucket/key, then encoded exactly once.
pub struct CopyObjectInput {
    pub bucket: String,
    pub key: String,
    pub source_bucket: String,
    pub source_key: String,
    pub source_version_id: Option<String>,
}

fn encode_copy_component(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) {
            encoded.push(byte as char);
        } else {
            use std::fmt::Write;
            write!(&mut encoded, "%{byte:02X}").unwrap();
        }
    }
    encoded
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

pub struct ObjectTaggingInput {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
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
mod tests {
    use super::{build_s3_url, decode_base64_payload};
    use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
    use proptest::prop_assert_eq;
    use proptest::{
        prelude::{ProptestConfig, any},
        proptest,
        strategy::Strategy,
    };
    use shiguredo_s3::S3Response;

    fn arbitrary_path_string() -> impl proptest::strategy::Strategy<Value = String> {
        "[A-Za-z0-9_/-]{1,64}".prop_map(|s| format!("/{s}"))
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn base64_roundtrip_is_lossless(payload in any::<Vec<u8>>()) {
            let encoded = BASE64_STANDARD.encode(&payload);
            let decoded = decode_base64_payload(&encoded).expect("base64 decode should succeed");
            prop_assert_eq!(decoded, payload);
        }

        #[test]
        fn build_s3_url_preserves_scheme_host_port_and_uri(
            host in "[a-z]{1,8}(?:\\.[a-z]{1,8}){0,2}",
            port in any::<u16>(),
            https in any::<bool>(),
            uri in arbitrary_path_string(),
        ) {
            let request = shiguredo_s3::S3Request {
                method: "GET".to_string(),
                uri: uri.clone(),
                headers: Vec::new(),
                body: Vec::new(),
                host: host.clone(),
                port,
                https,
                ignore_cert_check: false,
                expect_no_body: false,
            };

            let built = build_s3_url(&request).expect("url should be valid");
            let parsed = shiguredo_http11::uri::Uri::parse(&built).expect("built URL should parse");

            let expected_scheme = if https { "https" } else { "http" };
            prop_assert_eq!(parsed.scheme(), Some(expected_scheme));
            prop_assert_eq!(parsed.host(), Some(host.as_str()));
            prop_assert_eq!(parsed.port(), Some(port));
            prop_assert_eq!(parsed.path(), uri);
        }
    }

    #[test]
    fn invalid_base64_is_rejected() {
        let err = decode_base64_payload("not_base64").expect_err("invalid base64 should fail");
        assert_eq!(err.to_string(), "Bad Request: Failed to decode base64 data");
    }

    #[test]
    fn s3_response_header_lookup_is_case_insensitive() {
        let response = S3Response {
            status_code: 200,
            headers: vec![("Content-Type".to_string(), "text/plain".to_string())],
            body: Vec::new(),
        };
        assert_eq!(response.get_header("content-type"), Some("text/plain"));
        assert_eq!(response.get_header("CONTENT-TYPE"), Some("text/plain"));
    }
}
