//! 本文を溜めずに multipart で送るための upload。

use super::*;

/// A bounded-memory upload; callers submit parts sequentially and abort on errors.
pub(crate) struct StreamingUpload {
    pub(super) s3: S3Client,
    pub(super) bucket: String,
    pub(super) key: String,
    pub(super) upload_id: String,
    pub(super) parts: Vec<CompletedPart>,
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
