//! 各操作の入力型。

pub struct PutObjectInput {
    pub bucket: String,
    pub key: String,
    pub body: Vec<u8>,
    pub content_type: Option<String>,
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

/// Copy-source is supplied as decoded bucket/key, then encoded exactly once.
pub struct CopyObjectInput {
    pub bucket: String,
    pub key: String,
    pub source_bucket: String,
    pub source_key: String,
    pub source_version_id: Option<String>,
}

pub struct ObjectTaggingInput {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
}
