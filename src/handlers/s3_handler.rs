use shiguredo_http11::Response;

use crate::{
    config::state::AppState,
    errors::api_error::ApiError,
    http_client::HttpResponseStream,
    service::s3_service::{
        self, AbortMultipartUploadInput, CompleteMultipartUploadInput, CompletePartInput,
        CreateBucketInput, CreateMultipartUploadInput, DeleteBucketInput,
        DeleteObjectIdentifierInput, DeleteObjectInput, DeleteObjectsInput, GetObjectInput,
        HeadBucketInput, HeadObjectInput, ListMultipartUploadsInput, ListObjectsV2Input,
        ListPartsInput, PresignedObjectInput, ProxyObjectInput, ProxyObjectStreamInput,
        PutObjectInput, UploadPartInput,
    },
};

pub struct PreviewObjectStreamResponse {
    pub status_code: u16,
    pub reason_phrase: &'static str,
    pub headers: Vec<(String, String)>,
    pub body_stream: HttpResponseStream,
}

pub struct PutObjectBase64Request {
    pub bucket: String,
    pub key: String,
    pub file_data_base64: String,
    pub content_type: Option<String>,
}

pub struct GetObjectRequest {
    pub bucket: String,
    pub key: String,
}

pub struct HeadObjectRequest {
    pub bucket: String,
    pub key: String,
}

pub struct DeleteObjectRequest {
    pub bucket: String,
    pub key: String,
}

pub struct DeleteObjectIdentifierRequest {
    pub key: String,
    pub version_id: Option<String>,
}

pub struct DeleteObjectsRequest {
    pub bucket: String,
    pub objects: Vec<DeleteObjectIdentifierRequest>,
    pub quiet: Option<bool>,
}

pub struct ListObjectsV2Request {
    pub bucket: String,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub max_keys: Option<i32>,
    pub start_after: Option<String>,
}

pub struct CreateMultipartUploadRequest {
    pub bucket: String,
    pub key: String,
    pub content_type: Option<String>,
}

pub struct UploadPartBase64Request {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub part_number: i32,
    pub part_data_base64: String,
}

pub struct CompletePartRequest {
    pub part_number: i32,
    pub e_tag: String,
}

pub struct CompleteMultipartUploadRequest {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub parts: Vec<CompletePartRequest>,
}

pub struct AbortMultipartUploadRequest {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
}

pub struct ListPartsRequest {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub max_parts: Option<i32>,
    pub part_number_marker: Option<i32>,
}

pub struct ListMultipartUploadsRequest {
    pub bucket: String,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub max_uploads: Option<i32>,
    pub key_marker: Option<String>,
    pub upload_id_marker: Option<String>,
}

pub struct PresignedObjectRequest {
    pub bucket: String,
    pub key: String,
    pub expires_in_secs: Option<u64>,
}

pub struct BucketRequest {
    pub bucket: String,
}

fn parse_json_body(body: &str) -> Result<nojson::RawJson<'_>, ApiError> {
    nojson::RawJson::parse(body).map_err(|e| ApiError::BadRequest(format!("Invalid JSON: {e}")))
}

fn body_to_utf8(body: &[u8]) -> Result<String, ApiError> {
    String::from_utf8(body.to_vec())
        .map_err(|_| ApiError::BadRequest("Request body must be valid UTF-8".to_string()))
}

pub(super) fn get_required_string(
    root: nojson::RawJsonValue<'_, '_>,
    name: &str,
) -> Result<String, ApiError> {
    root.to_member(name)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
        .required()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
        .try_into()
        .map_err(|e| ApiError::BadRequest(format!("Invalid '{name}': {e}")))
}

pub(super) fn get_optional_string(
    root: nojson::RawJsonValue<'_, '_>,
    name: &str,
) -> Result<Option<String>, ApiError> {
    let Some(value) = root
        .to_member(name)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
        .optional()
    else {
        return Ok(None);
    };
    if value.kind().is_null() {
        return Ok(None);
    }
    let parsed = String::try_from(value)
        .map_err(|e| ApiError::BadRequest(format!("Invalid '{name}': {e}")))?;
    Ok(Some(parsed))
}

fn get_optional_i32(
    root: nojson::RawJsonValue<'_, '_>,
    name: &str,
) -> Result<Option<i32>, ApiError> {
    let Some(value) = root
        .to_member(name)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
        .optional()
    else {
        return Ok(None);
    };
    if value.kind().is_null() {
        return Ok(None);
    }
    let parsed =
        i32::try_from(value).map_err(|e| ApiError::BadRequest(format!("Invalid '{name}': {e}")))?;
    Ok(Some(parsed))
}

fn get_optional_u64(
    root: nojson::RawJsonValue<'_, '_>,
    name: &str,
) -> Result<Option<u64>, ApiError> {
    let Some(value) = root
        .to_member(name)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
        .optional()
    else {
        return Ok(None);
    };
    if value.kind().is_null() {
        return Ok(None);
    }
    let parsed =
        u64::try_from(value).map_err(|e| ApiError::BadRequest(format!("Invalid '{name}': {e}")))?;
    Ok(Some(parsed))
}

fn get_optional_bool(
    root: nojson::RawJsonValue<'_, '_>,
    name: &str,
) -> Result<Option<bool>, ApiError> {
    let Some(value) = root
        .to_member(name)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
        .optional()
    else {
        return Ok(None);
    };
    if value.kind().is_null() {
        return Ok(None);
    }
    let parsed = bool::try_from(value)
        .map_err(|e| ApiError::BadRequest(format!("Invalid '{name}': {e}")))?;
    Ok(Some(parsed))
}

fn json_response(body: String) -> Response {
    Response::new(200, "OK")
        .header("Content-Type", "application/json")
        .body(body.into_bytes())
}

fn parse_put_object_base64_request(body: &str) -> Result<PutObjectBase64Request, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    Ok(PutObjectBase64Request {
        bucket: get_required_string(root, "bucket")?,
        key: get_required_string(root, "key")?,
        file_data_base64: get_required_string(root, "file_data_base64")?,
        content_type: get_optional_string(root, "content_type")?,
    })
}

fn parse_get_object_request(body: &str) -> Result<GetObjectRequest, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    Ok(GetObjectRequest {
        bucket: get_required_string(root, "bucket")?,
        key: get_required_string(root, "key")?,
    })
}

fn parse_head_object_request(body: &str) -> Result<HeadObjectRequest, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    Ok(HeadObjectRequest {
        bucket: get_required_string(root, "bucket")?,
        key: get_required_string(root, "key")?,
    })
}

fn parse_delete_object_request(body: &str) -> Result<DeleteObjectRequest, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    Ok(DeleteObjectRequest {
        bucket: get_required_string(root, "bucket")?,
        key: get_required_string(root, "key")?,
    })
}

fn parse_delete_objects_request(body: &str) -> Result<DeleteObjectsRequest, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    let objects_raw = root
        .to_member("objects")
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
        .required()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    let mut objects = Vec::new();
    for object in objects_raw
        .to_array()
        .map_err(|e| ApiError::BadRequest(format!("Invalid 'objects': {e}")))?
    {
        objects.push(DeleteObjectIdentifierRequest {
            key: get_required_string(object, "key")?,
            version_id: get_optional_string(object, "version_id")?,
        });
    }

    Ok(DeleteObjectsRequest {
        bucket: get_required_string(root, "bucket")?,
        objects,
        quiet: get_optional_bool(root, "quiet")?,
    })
}

fn parse_list_objects_v2_request(body: &str) -> Result<ListObjectsV2Request, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    Ok(ListObjectsV2Request {
        bucket: get_required_string(root, "bucket")?,
        prefix: get_optional_string(root, "prefix")?,
        delimiter: get_optional_string(root, "delimiter")?,
        max_keys: get_optional_i32(root, "max_keys")?,
        start_after: get_optional_string(root, "start_after")?,
    })
}

fn parse_create_multipart_upload_request(
    body: &str,
) -> Result<CreateMultipartUploadRequest, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    Ok(CreateMultipartUploadRequest {
        bucket: get_required_string(root, "bucket")?,
        key: get_required_string(root, "key")?,
        content_type: get_optional_string(root, "content_type")?,
    })
}

fn parse_upload_part_base64_request(body: &str) -> Result<UploadPartBase64Request, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    Ok(UploadPartBase64Request {
        bucket: get_required_string(root, "bucket")?,
        key: get_required_string(root, "key")?,
        upload_id: get_required_string(root, "upload_id")?,
        part_number: get_required_i32(root, "part_number")?,
        part_data_base64: get_required_string(root, "part_data_base64")?,
    })
}

fn get_required_i32(root: nojson::RawJsonValue<'_, '_>, name: &str) -> Result<i32, ApiError> {
    root.to_member(name)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
        .required()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
        .try_into()
        .map_err(|e| ApiError::BadRequest(format!("Invalid '{name}': {e}")))
}

fn parse_complete_multipart_upload_request(
    body: &str,
) -> Result<CompleteMultipartUploadRequest, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    let parts_raw = root
        .to_member("parts")
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
        .required()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    let mut parts = Vec::new();
    for part in parts_raw
        .to_array()
        .map_err(|e| ApiError::BadRequest(format!("Invalid 'parts': {e}")))?
    {
        parts.push(CompletePartRequest {
            part_number: get_required_i32(part, "part_number")?,
            e_tag: get_required_string(part, "e_tag")?,
        });
    }

    Ok(CompleteMultipartUploadRequest {
        bucket: get_required_string(root, "bucket")?,
        key: get_required_string(root, "key")?,
        upload_id: get_required_string(root, "upload_id")?,
        parts,
    })
}

fn parse_abort_multipart_upload_request(
    body: &str,
) -> Result<AbortMultipartUploadRequest, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    Ok(AbortMultipartUploadRequest {
        bucket: get_required_string(root, "bucket")?,
        key: get_required_string(root, "key")?,
        upload_id: get_required_string(root, "upload_id")?,
    })
}

fn parse_list_parts_request(body: &str) -> Result<ListPartsRequest, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    Ok(ListPartsRequest {
        bucket: get_required_string(root, "bucket")?,
        key: get_required_string(root, "key")?,
        upload_id: get_required_string(root, "upload_id")?,
        max_parts: get_optional_i32(root, "max_parts")?,
        part_number_marker: get_optional_i32(root, "part_number_marker")?,
    })
}

fn parse_list_multipart_uploads_request(
    body: &str,
) -> Result<ListMultipartUploadsRequest, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    Ok(ListMultipartUploadsRequest {
        bucket: get_required_string(root, "bucket")?,
        prefix: get_optional_string(root, "prefix")?,
        delimiter: get_optional_string(root, "delimiter")?,
        max_uploads: get_optional_i32(root, "max_uploads")?,
        key_marker: get_optional_string(root, "key_marker")?,
        upload_id_marker: get_optional_string(root, "upload_id_marker")?,
    })
}

fn parse_presigned_object_request(body: &str) -> Result<PresignedObjectRequest, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    Ok(PresignedObjectRequest {
        bucket: get_required_string(root, "bucket")?,
        key: get_required_string(root, "key")?,
        expires_in_secs: get_optional_u64(root, "expires_in_secs")?,
    })
}

fn parse_bucket_request(body: &str) -> Result<BucketRequest, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    Ok(BucketRequest {
        bucket: get_required_string(root, "bucket")?,
    })
}

pub async fn put_object_base64(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_put_object_base64_request(&body)?;
    let body = s3_service::decode_base64_payload(&payload.file_data_base64)?;
    let result = s3_service::put_object(
        &app_state.client,
        &app_state.settings,
        PutObjectInput {
            bucket: payload.bucket,
            key: payload.key,
            body,
            content_type: payload.content_type,
        },
    )
    .await?;
    Ok(json_response(result))
}

pub async fn get_object_base64(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_get_object_request(&body)?;
    let result = s3_service::get_object(
        &app_state.client,
        &app_state.settings,
        GetObjectInput {
            bucket: payload.bucket,
            key: payload.key,
        },
    )
    .await?;
    Ok(json_response(result))
}

pub async fn head_object(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_head_object_request(&body)?;
    let result = s3_service::head_object(
        &app_state.client,
        &app_state.settings,
        HeadObjectInput {
            bucket: payload.bucket,
            key: payload.key,
        },
    )
    .await?;
    Ok(json_response(result))
}

pub async fn delete_object(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_delete_object_request(&body)?;
    let result = s3_service::delete_object(
        &app_state.client,
        &app_state.settings,
        DeleteObjectInput {
            bucket: payload.bucket,
            key: payload.key,
        },
    )
    .await?;
    Ok(json_response(result))
}

pub async fn delete_objects(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_delete_objects_request(&body)?;
    let objects = payload
        .objects
        .into_iter()
        .map(|object| DeleteObjectIdentifierInput {
            key: object.key,
            version_id: object.version_id,
        })
        .collect::<Vec<_>>();
    let result = s3_service::delete_objects(
        &app_state.client,
        &app_state.settings,
        DeleteObjectsInput {
            bucket: payload.bucket,
            objects,
            quiet: payload.quiet.unwrap_or(false),
        },
    )
    .await?;
    Ok(json_response(result))
}

pub async fn list_objects_v2(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_list_objects_v2_request(&body)?;
    let result = s3_service::list_objects_v2(
        &app_state.client,
        &app_state.settings,
        ListObjectsV2Input {
            bucket: payload.bucket,
            prefix: payload.prefix,
            delimiter: payload.delimiter,
            max_keys: payload.max_keys,
            start_after: payload.start_after,
        },
    )
    .await?;
    Ok(json_response(result))
}

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

pub async fn presigned_get_object(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_presigned_object_request(&body)?;
    let result = s3_service::presigned_get(
        &app_state.settings,
        PresignedObjectInput {
            bucket: payload.bucket,
            key: payload.key,
            expires_in_secs: payload.expires_in_secs.unwrap_or(900),
        },
    )?;
    Ok(json_response(result))
}

pub async fn presigned_put_object(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_presigned_object_request(&body)?;
    let result = s3_service::presigned_put(
        &app_state.settings,
        PresignedObjectInput {
            bucket: payload.bucket,
            key: payload.key,
            expires_in_secs: payload.expires_in_secs.unwrap_or(900),
        },
    )?;
    Ok(json_response(result))
}

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

pub async fn preview_object(
    app_state: &AppState,
    bucket: String,
    key: String,
) -> Result<Response, ApiError> {
    let s3_response = s3_service::get_object_proxy(
        &app_state.client,
        &app_state.settings,
        ProxyObjectInput {
            bucket,
            key: key.clone(),
        },
    )
    .await?;

    let mut response = Response::new(
        s3_response.status_code,
        response_reason(s3_response.status_code),
    )
    .body(s3_response.body);

    for (name, value) in build_preview_response_headers(&s3_response.headers, &key) {
        response.add_header(&name, &value);
    }

    Ok(response)
}

pub async fn preview_object_stream(
    app_state: &AppState,
    bucket: String,
    key: String,
    request_headers: &[(String, String)],
) -> Result<PreviewObjectStreamResponse, ApiError> {
    let s3_response = s3_service::get_object_proxy_stream(
        &app_state.client,
        &app_state.settings,
        ProxyObjectStreamInput {
            bucket,
            key: key.clone(),
            range: extract_forward_header(request_headers, "range"),
            if_match: extract_forward_header(request_headers, "if-match"),
            if_none_match: extract_forward_header(request_headers, "if-none-match"),
            if_modified_since: extract_forward_header(request_headers, "if-modified-since"),
            if_unmodified_since: extract_forward_header(request_headers, "if-unmodified-since"),
        },
    )
    .await?;

    let headers = build_preview_response_headers(&s3_response.headers, &key);

    Ok(PreviewObjectStreamResponse {
        status_code: s3_response.status_code,
        reason_phrase: response_reason(s3_response.status_code),
        headers,
        body_stream: s3_response,
    })
}

fn extract_forward_header(headers: &[(String, String)], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.clone())
}

fn build_preview_response_headers(
    source_headers: &[(String, String)],
    key: &str,
) -> Vec<(String, String)> {
    let pass_through_headers = [
        "content-type",
        "content-length",
        "etag",
        "last-modified",
        "cache-control",
        "content-range",
        "accept-ranges",
    ];
    let mut headers = Vec::new();

    for (name, value) in source_headers {
        if !pass_through_headers
            .iter()
            .any(|allowed| name.eq_ignore_ascii_case(allowed))
        {
            continue;
        }
        if !is_valid_header_name(name) || !is_valid_header_value(value) {
            continue;
        }
        headers.push((name.clone(), value.clone()));
    }

    if should_force_pdf_preview(get_header_value(&headers, "content-type"), key) {
        upsert_header(&mut headers, "Content-Type", "application/pdf");
        let file_name = sanitize_pdf_filename(key);
        upsert_header(
            &mut headers,
            "Content-Disposition",
            &format!("inline; filename=\"{}\"", file_name),
        );
    }

    headers
}

fn get_header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn upsert_header(headers: &mut Vec<(String, String)>, name: &str, value: &str) {
    headers.retain(|(header_name, _)| !header_name.eq_ignore_ascii_case(name));
    headers.push((name.to_string(), value.to_string()));
}

fn should_force_pdf_preview(content_type: Option<&str>, key: &str) -> bool {
    let is_pdf_key = key.to_ascii_lowercase().ends_with(".pdf");
    let is_pdf_content_type = content_type
        .map(|value| {
            value
                .split(';')
                .next()
                .map(str::trim)
                .is_some_and(|media_type| media_type.eq_ignore_ascii_case("application/pdf"))
        })
        .unwrap_or(false);

    is_pdf_key || is_pdf_content_type
}

fn response_reason(status_code: u16) -> &'static str {
    match status_code {
        200 => "OK",
        206 => "Partial Content",
        304 => "Not Modified",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        416 => "Range Not Satisfiable",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        _ => "OK",
    }
}

fn is_valid_header_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|b| {
            b.is_ascii_alphanumeric()
                || matches!(
                    b,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

fn is_valid_header_value(value: &str) -> bool {
    !value.bytes().any(|b| b == b'\r' || b == b'\n' || b == 0)
}

fn sanitize_pdf_filename(key: &str) -> String {
    let candidate = key.rsplit('/').next().unwrap_or("file.pdf");
    let mut filename = candidate
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
        .collect::<String>();

    if filename.is_empty() {
        filename = "file.pdf".to_string();
    }

    if !filename.to_ascii_lowercase().ends_with(".pdf") {
        filename.push_str(".pdf");
    }

    filename
}

pub fn s3_preflight() -> Response {
    Response::new(204, "No Content")
}

fn nonempty_string(root: nojson::RawJsonValue<'_, '_>, name: &str) -> Result<String, ApiError> {
    let value = get_required_string(root, name)?;
    if value.trim().is_empty() {
        return Err(ApiError::BadRequest(format!("'{name}' must not be empty")));
    }
    Ok(value)
}

pub async fn copy_object(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let json = parse_json_body(&body)?;
    let root = json.value();
    let input = s3_service::CopyObjectInput {
        bucket: nonempty_string(root, "bucket")?,
        key: nonempty_string(root, "key")?,
        source_bucket: nonempty_string(root, "source_bucket")?,
        source_key: nonempty_string(root, "source_key")?,
        source_version_id: get_optional_string(root, "source_version_id")?,
    };
    Ok(json_response(
        s3_service::copy_object(&app_state.client, &app_state.settings, input).await?,
    ))
}

fn parse_tags(
    root: nojson::RawJsonValue<'_, '_>,
) -> Result<Vec<shiguredo_s3::types::Tag>, ApiError> {
    let value = root
        .to_member("tags")
        .and_then(|v| v.required())
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let array = value
        .to_array()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let mut tags = Vec::<shiguredo_s3::types::Tag>::new();
    for tag in array {
        let key = nonempty_string(tag, "key")?;
        let value = get_required_string(tag, "value")?;
        if key.chars().count() > 128 || value.chars().count() > 256 {
            return Err(ApiError::BadRequest(
                "Tag key/value exceeds 128/256 characters".into(),
            ));
        }
        if tags.iter().any(|t| t.key == key) {
            return Err(ApiError::BadRequest("Duplicate tag key".into()));
        }
        tags.push(shiguredo_s3::types::Tag { key, value });
        if tags.len() > 10 {
            return Err(ApiError::BadRequest("At most 10 tags are allowed".into()));
        }
    }
    Ok(tags)
}

pub async fn get_object_tagging(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let json = parse_json_body(&body)?;
    let root = json.value();
    let input = s3_service::ObjectTaggingInput {
        bucket: nonempty_string(root, "bucket")?,
        key: nonempty_string(root, "key")?,
        version_id: get_optional_string(root, "version_id")?,
    };
    Ok(json_response(
        s3_service::get_object_tagging(&app_state.client, &app_state.settings, input).await?,
    ))
}

pub async fn put_object_tagging(app_state: &AppState, body: &[u8]) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let json = parse_json_body(&body)?;
    let root = json.value();
    let input = s3_service::ObjectTaggingInput {
        bucket: nonempty_string(root, "bucket")?,
        key: nonempty_string(root, "key")?,
        version_id: get_optional_string(root, "version_id")?,
    };
    let tags = parse_tags(root)?;
    Ok(json_response(
        s3_service::put_object_tagging(&app_state.client, &app_state.settings, input, tags).await?,
    ))
}

pub async fn delete_object_tagging(
    app_state: &AppState,
    body: &[u8],
) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let json = parse_json_body(&body)?;
    let root = json.value();
    let input = s3_service::ObjectTaggingInput {
        bucket: nonempty_string(root, "bucket")?,
        key: nonempty_string(root, "key")?,
        version_id: get_optional_string(root, "version_id")?,
    };
    Ok(json_response(
        s3_service::delete_object_tagging(&app_state.client, &app_state.settings, input).await?,
    ))
}

/// A successful upload is not rolled back when notification fails. The caller
/// can retry just /slack/message using the returned URL.
pub async fn put_object_and_notify(
    app_state: &AppState,
    body: &[u8],
) -> Result<Response, ApiError> {
    let body = body_to_utf8(body)?;
    let payload = parse_put_object_base64_request(&body)?;
    let json = parse_json_body(&body)?;
    let root = json.value();
    let channel = nonempty_string(root, "channel")?;
    nonempty_string(root, "bucket")?;
    nonempty_string(root, "key")?;
    let expires = get_optional_u64(root, "expires_in_secs")?.unwrap_or(900);
    if !(1..=604800).contains(&expires) {
        return Err(ApiError::BadRequest(
            "expires_in_secs must be between 1 and 604800".into(),
        ));
    }
    let text = get_optional_string(root, "text")?.unwrap_or_else(|| "File uploaded".into());
    let data = s3_service::decode_base64_payload(&payload.file_data_base64)?;
    // Validate signing configuration before the upload has any side effects.
    let signed = s3_service::presigned_get(
        &app_state.settings,
        PresignedObjectInput {
            bucket: payload.bucket.clone(),
            key: payload.key.clone(),
            expires_in_secs: expires,
        },
    )?;
    let signed_json = parse_json_body(&signed)?;
    let url = get_required_string(signed_json.value(), "url")?;
    let result = s3_service::put_object(
        &app_state.client,
        &app_state.settings,
        PutObjectInput {
            bucket: payload.bucket,
            key: payload.key,
            body: data,
            content_type: payload.content_type,
        },
    )
    .await?;
    let result_json = parse_json_body(&result)?;
    let notification = crate::service::slack_service::post_message(
        &app_state.client,
        &app_state.settings.slack_bot_token,
        &app_state.settings.slack_api_base_url,
        &channel,
        &format!("{text}\n{url}"),
    )
    .await;
    let (slack_response, notification_error) = match notification {
        Ok(response) => (Some(response), None),
        Err(error) => (None, Some(error.to_string())),
    };
    Ok(json_response(
        nojson::json(|f| {
            f.object(|f| {
                f.member("uploaded", true)?;
                f.member("object", result_json.value())?;
                f.member("url", &url)?;
                f.member("notified", slack_response.is_some())?;
                f.member("notification_error", &notification_error)
            })
        })
        .to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        extract_forward_header, parse_delete_objects_request, parse_list_objects_v2_request,
        parse_presigned_object_request,
    };
    use proptest::{prelude::ProptestConfig, prop_assert_eq, proptest};

    #[test]
    fn parse_delete_objects_allows_null_optionals() {
        let body = r#"{
            "bucket": "pdfs",
            "objects": [
                {"key": "20260418/a.pdf", "version_id": null},
                {"key": "20260418/b.pdf"}
            ],
            "quiet": null
        }"#;

        let parsed = parse_delete_objects_request(body).expect("request should parse");

        assert_eq!(parsed.bucket, "pdfs");
        assert_eq!(parsed.objects.len(), 2);
        assert_eq!(parsed.objects[0].key, "20260418/a.pdf");
        assert_eq!(parsed.objects[0].version_id, None);
        assert_eq!(parsed.objects[1].key, "20260418/b.pdf");
        assert_eq!(parsed.objects[1].version_id, None);
        assert_eq!(parsed.quiet, None);
    }

    #[test]
    fn parse_list_objects_v2_allows_null_optionals() {
        let body = r#"{
            "bucket": "pdfs",
            "prefix": null,
            "delimiter": null,
            "max_keys": null,
            "start_after": null
        }"#;

        let parsed = parse_list_objects_v2_request(body).expect("request should parse");

        assert_eq!(parsed.bucket, "pdfs");
        assert_eq!(parsed.prefix, None);
        assert_eq!(parsed.delimiter, None);
        assert_eq!(parsed.max_keys, None);
        assert_eq!(parsed.start_after, None);
    }

    #[test]
    fn parse_presigned_object_allows_null_expires_in_secs() {
        let body = r#"{
            "bucket": "pdfs",
            "key": "20260418/a.pdf",
            "expires_in_secs": null
        }"#;

        let parsed = parse_presigned_object_request(body).expect("request should parse");

        assert_eq!(parsed.bucket, "pdfs");
        assert_eq!(parsed.key, "20260418/a.pdf");
        assert_eq!(parsed.expires_in_secs, None);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn extract_forward_header_is_case_insensitive(value in "[ -~]{0,64}") {
            let headers = vec![
                ("RaNgE".to_string(), value.clone()),
                ("x-request-id".to_string(), "req-1".to_string()),
            ];

            let extracted = extract_forward_header(&headers, "range");
            prop_assert_eq!(extracted, Some(value));
        }
    }
}
