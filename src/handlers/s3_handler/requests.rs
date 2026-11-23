//! リクエスト本文の JSON から、`s3_service` の入力型を読み取る。

use super::*;
use nojson::RawJsonValue;

/// UTF-8 の JSON 本文を読み、ルート値を `read` に渡す。
pub(in crate::handlers) fn parse_body<T>(
    body: &[u8],
    read: impl FnOnce(RawJsonValue<'_, '_>) -> Result<T, ApiError>,
) -> Result<T, ApiError> {
    let text = std::str::from_utf8(body)
        .map_err(|_| ApiError::BadRequest("Request body must be valid UTF-8".to_string()))?;
    let json = parse_json_body(text)?;
    read(json.value())
}

pub(super) fn parse_json_body(body: &str) -> Result<nojson::RawJson<'_>, ApiError> {
    nojson::RawJson::parse(body).map_err(|e| ApiError::BadRequest(format!("Invalid JSON: {e}")))
}

/// 必須のメンバー。
pub(in crate::handlers) fn required<'text, 'raw, T>(
    root: RawJsonValue<'text, 'raw>,
    name: &str,
) -> Result<T, ApiError>
where
    T: TryFrom<RawJsonValue<'text, 'raw>>,
    T::Error: std::fmt::Display,
{
    let value = root
        .to_member(name)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
        .required()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    T::try_from(value).map_err(|e| ApiError::BadRequest(format!("Invalid '{name}': {e}")))
}

/// 省略可能なメンバー。`null` は省略と同じ扱いにする。
pub(in crate::handlers) fn optional<'text, 'raw, T>(
    root: RawJsonValue<'text, 'raw>,
    name: &str,
) -> Result<Option<T>, ApiError>
where
    T: TryFrom<RawJsonValue<'text, 'raw>>,
    T::Error: std::fmt::Display,
{
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
    T::try_from(value)
        .map(Some)
        .map_err(|e| ApiError::BadRequest(format!("Invalid '{name}': {e}")))
}

/// 必須で、空白だけでもない文字列。
pub(super) fn nonempty_string(root: RawJsonValue<'_, '_>, name: &str) -> Result<String, ApiError> {
    let value: String = required(root, name)?;
    if value.trim().is_empty() {
        return Err(ApiError::BadRequest(format!("'{name}' must not be empty")));
    }
    Ok(value)
}

/// 必須の配列メンバーの各要素を `read` で読む。
fn required_array<T>(
    root: RawJsonValue<'_, '_>,
    name: &str,
    mut read: impl FnMut(RawJsonValue<'_, '_>) -> Result<T, ApiError>,
) -> Result<Vec<T>, ApiError> {
    let value = root
        .to_member(name)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
        .required()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    value
        .to_array()
        .map_err(|e| ApiError::BadRequest(format!("Invalid '{name}': {e}")))?
        .map(&mut read)
        .collect()
}

pub(super) fn put_object_base64_input(
    root: RawJsonValue<'_, '_>,
) -> Result<PutObjectInput, ApiError> {
    let bucket = required(root, "bucket")?;
    let key = required(root, "key")?;
    let file_data_base64: String = required(root, "file_data_base64")?;
    let content_type = optional(root, "content_type")?;
    Ok(PutObjectInput {
        bucket,
        key,
        body: s3_service::decode_base64_payload(&file_data_base64)?,
        content_type,
    })
}

pub(super) fn get_object_input(root: RawJsonValue<'_, '_>) -> Result<GetObjectInput, ApiError> {
    Ok(GetObjectInput {
        bucket: required(root, "bucket")?,
        key: required(root, "key")?,
    })
}

pub(super) fn head_object_input(root: RawJsonValue<'_, '_>) -> Result<HeadObjectInput, ApiError> {
    Ok(HeadObjectInput {
        bucket: required(root, "bucket")?,
        key: required(root, "key")?,
    })
}

pub(super) fn delete_object_input(
    root: RawJsonValue<'_, '_>,
) -> Result<DeleteObjectInput, ApiError> {
    Ok(DeleteObjectInput {
        bucket: required(root, "bucket")?,
        key: required(root, "key")?,
    })
}

pub(super) fn delete_objects_input(
    root: RawJsonValue<'_, '_>,
) -> Result<DeleteObjectsInput, ApiError> {
    let objects = required_array(root, "objects", |object| {
        Ok(DeleteObjectIdentifierInput {
            key: required(object, "key")?,
            version_id: optional(object, "version_id")?,
        })
    })?;
    Ok(DeleteObjectsInput {
        bucket: required(root, "bucket")?,
        objects,
        quiet: optional(root, "quiet")?.unwrap_or(false),
    })
}

pub(super) fn list_objects_v2_input(
    root: RawJsonValue<'_, '_>,
) -> Result<ListObjectsV2Input, ApiError> {
    Ok(ListObjectsV2Input {
        bucket: required(root, "bucket")?,
        prefix: optional(root, "prefix")?,
        delimiter: optional(root, "delimiter")?,
        max_keys: optional(root, "max_keys")?,
        start_after: optional(root, "start_after")?,
        continuation_token: optional(root, "continuation_token")?,
    })
}

pub(super) fn create_multipart_upload_input(
    root: RawJsonValue<'_, '_>,
) -> Result<CreateMultipartUploadInput, ApiError> {
    Ok(CreateMultipartUploadInput {
        bucket: required(root, "bucket")?,
        key: required(root, "key")?,
        content_type: optional(root, "content_type")?,
    })
}

pub(super) fn upload_part_base64_input(
    root: RawJsonValue<'_, '_>,
) -> Result<UploadPartInput, ApiError> {
    let bucket = required(root, "bucket")?;
    let key = required(root, "key")?;
    let upload_id = required(root, "upload_id")?;
    let part_number = required(root, "part_number")?;
    let part_data_base64: String = required(root, "part_data_base64")?;
    Ok(UploadPartInput {
        bucket,
        key,
        upload_id,
        part_number,
        body: s3_service::decode_base64_payload(&part_data_base64)?,
    })
}

pub(super) fn complete_multipart_upload_input(
    root: RawJsonValue<'_, '_>,
) -> Result<CompleteMultipartUploadInput, ApiError> {
    let parts = required_array(root, "parts", |part| {
        Ok(CompletePartInput {
            part_number: required(part, "part_number")?,
            e_tag: required(part, "e_tag")?,
        })
    })?;
    Ok(CompleteMultipartUploadInput {
        bucket: required(root, "bucket")?,
        key: required(root, "key")?,
        upload_id: required(root, "upload_id")?,
        parts,
    })
}

pub(super) fn abort_multipart_upload_input(
    root: RawJsonValue<'_, '_>,
) -> Result<AbortMultipartUploadInput, ApiError> {
    Ok(AbortMultipartUploadInput {
        bucket: required(root, "bucket")?,
        key: required(root, "key")?,
        upload_id: required(root, "upload_id")?,
    })
}

pub(super) fn list_parts_input(root: RawJsonValue<'_, '_>) -> Result<ListPartsInput, ApiError> {
    Ok(ListPartsInput {
        bucket: required(root, "bucket")?,
        key: required(root, "key")?,
        upload_id: required(root, "upload_id")?,
        max_parts: optional(root, "max_parts")?,
        part_number_marker: optional(root, "part_number_marker")?,
    })
}

pub(super) fn list_multipart_uploads_input(
    root: RawJsonValue<'_, '_>,
) -> Result<ListMultipartUploadsInput, ApiError> {
    Ok(ListMultipartUploadsInput {
        bucket: required(root, "bucket")?,
        prefix: optional(root, "prefix")?,
        delimiter: optional(root, "delimiter")?,
        max_uploads: optional(root, "max_uploads")?,
        key_marker: optional(root, "key_marker")?,
        upload_id_marker: optional(root, "upload_id_marker")?,
    })
}

/// 署名の有効期限は省略時 900 秒。
pub(super) fn presigned_object_input(
    root: RawJsonValue<'_, '_>,
) -> Result<PresignedObjectInput, ApiError> {
    Ok(PresignedObjectInput {
        bucket: required(root, "bucket")?,
        key: required(root, "key")?,
        expires_in_secs: optional(root, "expires_in_secs")?.unwrap_or(900),
    })
}

pub(super) fn bucket_name(root: RawJsonValue<'_, '_>) -> Result<String, ApiError> {
    required(root, "bucket")
}

pub(super) fn copy_object_input(root: RawJsonValue<'_, '_>) -> Result<CopyObjectInput, ApiError> {
    Ok(CopyObjectInput {
        bucket: nonempty_string(root, "bucket")?,
        key: nonempty_string(root, "key")?,
        source_bucket: nonempty_string(root, "source_bucket")?,
        source_key: nonempty_string(root, "source_key")?,
        source_version_id: optional(root, "source_version_id")?,
    })
}

pub(super) fn object_tagging_input(
    root: RawJsonValue<'_, '_>,
) -> Result<ObjectTaggingInput, ApiError> {
    Ok(ObjectTaggingInput {
        bucket: nonempty_string(root, "bucket")?,
        key: nonempty_string(root, "key")?,
        version_id: optional(root, "version_id")?,
    })
}

pub(super) fn parse_tags(
    root: RawJsonValue<'_, '_>,
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
        let value: String = required(tag, "value")?;
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
