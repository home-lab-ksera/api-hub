//! S3 の操作結果を、API の応答 JSON にする。
//!
//! `s3_service` は SDK の出力型をそのまま返し、ここで利用者向けの形に変える。

use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use nojson::DisplayJson;
use std::fmt::Display;

pub(super) fn put_object(
    output: &shiguredo_s3::types::PutObjectOutput,
) -> impl DisplayJson + Display + '_ {
    nojson::json(move |f| {
        f.object(|f| {
            f.member("e_tag", &output.e_tag)?;
            f.member("version_id", &output.version_id)
        })
    })
}

pub(super) fn get_object(
    output: &shiguredo_s3::types::GetObjectOutput,
) -> impl DisplayJson + Display + '_ {
    nojson::json(move |f| {
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
}

pub(super) fn delete_object(
    output: &shiguredo_s3::types::DeleteObjectOutput,
) -> impl DisplayJson + Display + '_ {
    nojson::json(move |f| {
        f.object(|f| {
            f.member("delete_marker", output.delete_marker)?;
            f.member("version_id", &output.version_id)
        })
    })
}

pub(super) fn delete_objects(
    output: &shiguredo_s3::types::DeleteObjectsOutput,
) -> impl DisplayJson + Display + '_ {
    let deleted = output.deleted.clone().unwrap_or_default();
    let errors = output.errors.clone().unwrap_or_default();
    nojson::json(move |f| {
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
}

pub(super) fn list_objects_v2(
    output: &shiguredo_s3::types::ListObjectsV2Output,
) -> impl DisplayJson + Display + '_ {
    let contents = output.contents.clone().unwrap_or_default();
    let common_prefixes = output.common_prefixes.clone().unwrap_or_default();
    nojson::json(move |f| {
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
}

pub(super) fn copy_object(
    output: &shiguredo_s3::types::CopyObjectOutput,
) -> impl DisplayJson + Display + '_ {
    nojson::json(move |f| {
        f.object(|f| {
            f.member("e_tag", &output.e_tag)?;
            f.member("last_modified", &output.last_modified)?;
            f.member("version_id", &output.version_id)?;
            f.member("copy_source_version_id", &output.copy_source_version_id)
        })
    })
}

pub(super) fn get_object_tagging(
    output: &shiguredo_s3::types::GetObjectTaggingOutput,
) -> impl DisplayJson + Display + '_ {
    nojson::json(move |f| {
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
}

pub(super) fn put_object_tagging(
    output: &shiguredo_s3::types::PutObjectTaggingOutput,
) -> impl DisplayJson + Display + '_ {
    nojson::json(move |f| {
        f.object(|f| {
            f.member("version_id", &output.version_id)?;
            Ok(())
        })
    })
}

pub(super) fn delete_object_tagging(
    output: &shiguredo_s3::types::DeleteObjectTaggingOutput,
) -> impl DisplayJson + Display + '_ {
    nojson::json(move |f| {
        f.object(|f| {
            f.member("version_id", &output.version_id)?;
            Ok(())
        })
    })
}

pub(super) fn list_buckets(
    output: &shiguredo_s3::types::ListBucketsOutput,
) -> impl DisplayJson + Display + '_ {
    nojson::json(move |f| {
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
}

pub(super) fn create_bucket(
    output: &shiguredo_s3::types::CreateBucketOutput,
) -> impl DisplayJson + Display + '_ {
    nojson::json(move |f| f.object(|f| f.member("location", &output.location)))
}

pub(super) fn head_bucket(
    output: &shiguredo_s3::types::HeadBucketOutput,
) -> impl DisplayJson + Display + '_ {
    nojson::json(move |f| f.object(|f| f.member("bucket_region", &output.bucket_region)))
}

pub(super) fn create_multipart_upload(
    output: &shiguredo_s3::types::CreateMultipartUploadOutput,
) -> impl DisplayJson + Display + '_ {
    nojson::json(move |f| {
        f.object(|f| {
            f.member("bucket", &output.bucket)?;
            f.member("key", &output.key)?;
            f.member("upload_id", &output.upload_id)
        })
    })
}

pub(super) fn upload_part(
    output: &shiguredo_s3::types::UploadPartOutput,
) -> impl DisplayJson + Display + '_ {
    nojson::json(move |f| f.object(|f| f.member("e_tag", &output.e_tag)))
}

pub(super) fn complete_multipart_upload(
    output: &shiguredo_s3::types::CompleteMultipartUploadOutput,
) -> impl DisplayJson + Display + '_ {
    nojson::json(move |f| {
        f.object(|f| {
            f.member("location", &output.location)?;
            f.member("bucket", &output.bucket)?;
            f.member("key", &output.key)?;
            f.member("e_tag", &output.e_tag)?;
            f.member("version_id", &output.version_id)
        })
    })
}

pub(super) fn list_parts(
    output: &shiguredo_s3::types::ListPartsOutput,
) -> impl DisplayJson + Display + '_ {
    let parts = output.parts.clone().unwrap_or_default();
    nojson::json(move |f| {
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
}

pub(super) fn list_multipart_uploads(
    output: &shiguredo_s3::types::ListMultipartUploadsOutput,
) -> impl DisplayJson + Display + '_ {
    let uploads = output.uploads.clone().unwrap_or_default();
    let common_prefixes = output.common_prefixes.clone().unwrap_or_default();
    nojson::json(move |f| {
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
}

pub(super) fn head_object(
    output: &shiguredo_s3::types::HeadObjectOutput,
) -> impl DisplayJson + Display + '_ {
    nojson::json(move |f| {
        f.object(|f| {
            f.member("content_type", &output.content_type)?;
            f.member("content_length", output.content_length)?;
            f.member("e_tag", &output.e_tag)?;
            f.member("last_modified", &output.last_modified)?;
            f.member("version_id", &output.version_id)?;
            f.member("metadata", &output.metadata)
        })
    })
}

/// 署名済み URL と、送信時に付ける必要があるヘッダー。
pub(super) fn presigned(
    output: &shiguredo_s3::api::PresignedRequest,
) -> impl DisplayJson + Display + '_ {
    nojson::json(move |f| {
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
}

pub(super) fn delete_bucket() -> impl DisplayJson + Display {
    nojson::json(|f| f.object(|f| f.member("deleted", true)))
}

pub(super) fn abort_multipart_upload() -> impl DisplayJson + Display {
    nojson::json(|f| f.object(|f| f.member("aborted", true)))
}
