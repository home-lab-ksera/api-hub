//! bucket の操作。

use super::*;

pub async fn list_buckets(
    http_client: &HttpClient,
    settings: &Settings,
) -> Result<shiguredo_s3::types::ListBucketsOutput, ApiError> {
    let s3 = create_s3_client(settings)?;
    let output = send(
        http_client,
        s3.list_buckets().build_request(),
        shiguredo_s3::api::ListBucketsFluentBuilder::parse_response,
    )
    .await?;

    Ok(output)
}

pub async fn create_bucket(
    http_client: &HttpClient,
    settings: &Settings,
    input: CreateBucketInput,
) -> Result<shiguredo_s3::types::CreateBucketOutput, ApiError> {
    let s3 = create_s3_client(settings)?;
    let output = send(
        http_client,
        s3.create_bucket().bucket(input.bucket).build_request(),
        shiguredo_s3::api::CreateBucketFluentBuilder::parse_response,
    )
    .await?;

    Ok(output)
}

pub async fn head_bucket(
    http_client: &HttpClient,
    settings: &Settings,
    input: HeadBucketInput,
) -> Result<shiguredo_s3::types::HeadBucketOutput, ApiError> {
    let s3 = create_s3_client(settings)?;
    let output = send(
        http_client,
        s3.head_bucket().bucket(input.bucket).build_request(),
        shiguredo_s3::api::HeadBucketFluentBuilder::parse_response,
    )
    .await?;

    Ok(output)
}

pub async fn delete_bucket(
    http_client: &HttpClient,
    settings: &Settings,
    input: DeleteBucketInput,
) -> Result<(), ApiError> {
    let s3 = create_s3_client(settings)?;
    send(
        http_client,
        s3.delete_bucket().bucket(input.bucket).build_request(),
        shiguredo_s3::api::DeleteBucketFluentBuilder::parse_response,
    )
    .await?;

    Ok(())
}
