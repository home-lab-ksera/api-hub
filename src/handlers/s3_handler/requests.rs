//! リクエスト本文の型と JSON からの読み取り。

use super::*;

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
    pub continuation_token: Option<String>,
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

pub(super) fn parse_json_body(body: &str) -> Result<nojson::RawJson<'_>, ApiError> {
    nojson::RawJson::parse(body).map_err(|e| ApiError::BadRequest(format!("Invalid JSON: {e}")))
}

pub(super) fn body_to_utf8(body: &[u8]) -> Result<String, ApiError> {
    String::from_utf8(body.to_vec())
        .map_err(|_| ApiError::BadRequest("Request body must be valid UTF-8".to_string()))
}

pub(in crate::handlers) fn get_required_string(
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

pub(in crate::handlers) fn get_optional_string(
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

pub(super) fn get_optional_i32(
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

pub(super) fn get_optional_u64(
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

pub(super) fn get_optional_bool(
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

pub(super) fn parse_put_object_base64_request(
    body: &str,
) -> Result<PutObjectBase64Request, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    Ok(PutObjectBase64Request {
        bucket: get_required_string(root, "bucket")?,
        key: get_required_string(root, "key")?,
        file_data_base64: get_required_string(root, "file_data_base64")?,
        content_type: get_optional_string(root, "content_type")?,
    })
}

pub(super) fn parse_get_object_request(body: &str) -> Result<GetObjectRequest, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    Ok(GetObjectRequest {
        bucket: get_required_string(root, "bucket")?,
        key: get_required_string(root, "key")?,
    })
}

pub(super) fn parse_head_object_request(body: &str) -> Result<HeadObjectRequest, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    Ok(HeadObjectRequest {
        bucket: get_required_string(root, "bucket")?,
        key: get_required_string(root, "key")?,
    })
}

pub(super) fn parse_delete_object_request(body: &str) -> Result<DeleteObjectRequest, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    Ok(DeleteObjectRequest {
        bucket: get_required_string(root, "bucket")?,
        key: get_required_string(root, "key")?,
    })
}

pub(super) fn parse_delete_objects_request(body: &str) -> Result<DeleteObjectsRequest, ApiError> {
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

pub(super) fn parse_list_objects_v2_request(body: &str) -> Result<ListObjectsV2Request, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    Ok(ListObjectsV2Request {
        bucket: get_required_string(root, "bucket")?,
        prefix: get_optional_string(root, "prefix")?,
        delimiter: get_optional_string(root, "delimiter")?,
        max_keys: get_optional_i32(root, "max_keys")?,
        start_after: get_optional_string(root, "start_after")?,
        continuation_token: get_optional_string(root, "continuation_token")?,
    })
}

pub(super) fn parse_create_multipart_upload_request(
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

pub(super) fn parse_upload_part_base64_request(
    body: &str,
) -> Result<UploadPartBase64Request, ApiError> {
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

pub(super) fn get_required_i32(
    root: nojson::RawJsonValue<'_, '_>,
    name: &str,
) -> Result<i32, ApiError> {
    root.to_member(name)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
        .required()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
        .try_into()
        .map_err(|e| ApiError::BadRequest(format!("Invalid '{name}': {e}")))
}

pub(super) fn parse_complete_multipart_upload_request(
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

pub(super) fn parse_abort_multipart_upload_request(
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

pub(super) fn parse_list_parts_request(body: &str) -> Result<ListPartsRequest, ApiError> {
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

pub(super) fn parse_list_multipart_uploads_request(
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

pub(super) fn parse_presigned_object_request(
    body: &str,
) -> Result<PresignedObjectRequest, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    Ok(PresignedObjectRequest {
        bucket: get_required_string(root, "bucket")?,
        key: get_required_string(root, "key")?,
        expires_in_secs: get_optional_u64(root, "expires_in_secs")?,
    })
}

pub(super) fn parse_bucket_request(body: &str) -> Result<BucketRequest, ApiError> {
    let json = parse_json_body(body)?;
    let root = json.value();
    Ok(BucketRequest {
        bucket: get_required_string(root, "bucket")?,
    })
}

pub(super) fn nonempty_string(
    root: nojson::RawJsonValue<'_, '_>,
    name: &str,
) -> Result<String, ApiError> {
    let value = get_required_string(root, name)?;
    if value.trim().is_empty() {
        return Err(ApiError::BadRequest(format!("'{name}' must not be empty")));
    }
    Ok(value)
}

pub(super) fn parse_tags(
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
