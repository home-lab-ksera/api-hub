//! 経路の表で決まったエンドポイントを、handler へ渡す。

use super::routes::{self, Endpoint, Lookup};
use crate::config::state::AppState;
use crate::errors::api_error::ApiError;
use crate::handlers::{health_handler, openapi_handler, s3_handler, slack_handler};
use crate::reply::{Reply, ReplyExt, reply};
use axum::http::HeaderMap;
use shiguredo_http11::uri::percent_decode;

/// `/s3/preview/` に続く `{bucket}/{key}` を復号する。
pub(super) fn parse_preview_bucket_and_key(remaining: &str) -> Result<(String, String), ApiError> {
    let (bucket, key) = remaining
        .split_once('/')
        .ok_or_else(|| ApiError::BadRequest("Invalid preview path".to_string()))?;
    if bucket.is_empty() || key.is_empty() {
        return Err(ApiError::BadRequest("Invalid preview path".to_string()));
    }

    let decoded_bucket = percent_decode(bucket)
        .map_err(|_| ApiError::BadRequest("Invalid preview path".to_string()))?;
    let decoded_key = percent_decode(key)
        .map_err(|_| ApiError::BadRequest("Invalid preview path".to_string()))?;
    Ok((decoded_bucket, decoded_key))
}

pub(super) async fn route_request(
    app_state: &AppState,
    method: &str,
    headers: &HeaderMap,
    body: &[u8],
    path: &str,
    query: Option<&str>,
) -> Result<Reply, ApiError> {
    let (route, param) = match routes::lookup(method, path) {
        Lookup::Found(route, param) => (route, param),
        Lookup::Preflight => return Ok(s3_handler::s3_preflight()),
        Lookup::MethodNotAllowed => {
            return Err(ApiError::MethodNotAllowed(format!(
                "Method {method} is not allowed for {path}"
            )));
        }
        Lookup::NotFound => return Err(ApiError::NotFound(format!("Route not found: {path}"))),
    };
    use Endpoint as E;
    match route.endpoint {
        E::Health => Ok(health_handler::health()),
        E::Metrics => Ok(crate::queue_metrics::handle(app_state).await),
        E::OpenApi => Ok(openapi_handler::openapi_json()),
        E::NotificationCreate => Ok(crate::notifications::handle_post(app_state, body).await),
        E::NotificationGet => Ok(crate::notifications::handle_get(app_state, param).await),
        E::NotificationRetry => Ok(crate::notifications::handle_retry(app_state, param).await),
        E::SlackMessage => slack_handler::post_message(app_state, body).await,
        E::SlackMessageUpdate => slack_handler::update_message(app_state, body).await,
        E::SlackUploadImage => {
            slack_handler::upload_image_raw(app_state, query, headers, body).await
        }
        E::SlackUploadPdf => slack_handler::upload_pdf_raw(app_state, query, headers, body).await,
        E::S3PutObjectBase64 => s3_handler::put_object_base64(app_state, body).await,
        E::S3PutObjectAndNotify => s3_handler::put_object_and_notify(app_state, body).await,
        E::S3GetObjectBase64 => s3_handler::get_object_base64(app_state, body).await,
        E::S3HeadObject => s3_handler::head_object(app_state, body).await,
        E::S3DeleteObject => s3_handler::delete_object(app_state, body).await,
        E::S3DeleteObjects => s3_handler::delete_objects(app_state, body).await,
        E::S3CopyObject => s3_handler::copy_object(app_state, body).await,
        E::S3GetObjectTagging => s3_handler::get_object_tagging(app_state, body).await,
        E::S3PutObjectTagging => s3_handler::put_object_tagging(app_state, body).await,
        E::S3DeleteObjectTagging => s3_handler::delete_object_tagging(app_state, body).await,
        E::S3ListObjectsV2 => s3_handler::list_objects_v2(app_state, body).await,
        E::S3CreateMultipartUpload => s3_handler::create_multipart_upload(app_state, body).await,
        E::S3UploadPartBase64 => s3_handler::upload_part_base64(app_state, body).await,
        E::S3CompleteMultipartUpload => {
            s3_handler::complete_multipart_upload(app_state, body).await
        }
        E::S3AbortMultipartUpload => s3_handler::abort_multipart_upload(app_state, body).await,
        E::S3ListParts => s3_handler::list_parts(app_state, body).await,
        E::S3ListMultipartUploads => s3_handler::list_multipart_uploads(app_state, body).await,
        E::S3PresignedGetObject => s3_handler::presigned_get_object(app_state, body).await,
        E::S3PresignedPutObject => s3_handler::presigned_put_object(app_state, body).await,
        E::S3BrowserGetObject => s3_handler::browser_get_object(app_state, body).await,
        E::S3BrowserOpen => browser_open(app_state, query).await,
        E::S3ListBuckets => s3_handler::list_buckets(app_state).await,
        E::S3CreateBucket => s3_handler::create_bucket(app_state, body).await,
        E::S3HeadBucket => s3_handler::head_bucket(app_state, body).await,
        E::S3DeleteBucket => s3_handler::delete_bucket(app_state, body).await,
        // 本文を流す経路は handle_http が先に処理する。ここへ来るのはその経路を通らない呼び出しだけ。
        E::S3PutObjectStream | E::S3Preview => Err(ApiError::MethodNotAllowed(format!(
            "Method {method} is not allowed for {path}"
        ))),
    }
}

/// viewer がリンクから開けるよう、署名済み URL へ転送する。
async fn browser_open(app_state: &AppState, query: Option<&str>) -> Result<Reply, ApiError> {
    // 同じ名前が複数あるときは後の値を使う。
    let fields: std::collections::BTreeMap<String, String> =
        url::form_urlencoded::parse(query.unwrap_or("").as_bytes())
            .into_owned()
            .collect();
    let body = nojson::json(|f| f.value(&fields)).to_string();
    let Some(signed) = s3_handler::browser_presign(app_state, body.as_bytes())? else {
        return Ok(s3_handler::browser_unavailable());
    };
    Ok(reply(307)
        .header("Location", &signed.url)
        .header("Cache-Control", "no-store")
        .header("Referrer-Policy", "no-referrer"))
}
