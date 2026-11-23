//! bucket の操作。

use super::*;

pub async fn list_buckets(
    http_client: &HttpClient,
    settings: &Settings,
) -> Result<String, ApiError> {
    let s3 = create_s3_client(settings)?;
    let output = send(
        http_client,
        s3.list_buckets().build_request(),
        shiguredo_s3::api::ListBucketsFluentBuilder::parse_response,
    )
    .await?;

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
    let output = send(
        http_client,
        s3.create_bucket().bucket(input.bucket).build_request(),
        shiguredo_s3::api::CreateBucketFluentBuilder::parse_response,
    )
    .await?;

    Ok(nojson::json(|f| f.object(|f| f.member("location", &output.location))).to_string())
}

pub async fn head_bucket(
    http_client: &HttpClient,
    settings: &Settings,
    input: HeadBucketInput,
) -> Result<String, ApiError> {
    let s3 = create_s3_client(settings)?;
    let output = send(
        http_client,
        s3.head_bucket().bucket(input.bucket).build_request(),
        shiguredo_s3::api::HeadBucketFluentBuilder::parse_response,
    )
    .await?;

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
    send(
        http_client,
        s3.delete_bucket().bucket(input.bucket).build_request(),
        shiguredo_s3::api::DeleteBucketFluentBuilder::parse_response,
    )
    .await?;

    Ok(nojson::json(|f| f.object(|f| f.member("deleted", true))).to_string())
}
