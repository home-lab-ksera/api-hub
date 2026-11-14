//! bucket の操作。

use super::*;

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
