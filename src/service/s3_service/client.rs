//! S3 クライアントの生成、要求の実行、エラーの変換。

use super::*;

pub fn decode_base64_payload(payload: &str) -> Result<Vec<u8>, ApiError> {
    BASE64_STANDARD
        .decode(payload)
        .map_err(|_| ApiError::BadRequest("Failed to decode base64 data".to_string()))
}

pub(super) fn create_s3_client(settings: &Settings) -> Result<S3Client, ApiError> {
    let credential = if let Some(session_token) = &settings.s3_session_token {
        Credential::with_session_token(
            settings.s3_access_key_id.clone(),
            settings.s3_secret_access_key.clone(),
            session_token.clone(),
        )
    } else {
        Credential::new(
            settings.s3_access_key_id.clone(),
            settings.s3_secret_access_key.clone(),
        )
    };

    let mut config_builder = S3Config::builder()
        .region(settings.s3_region.clone())
        .credential(credential)
        .use_path_style(settings.s3_use_path_style)
        .ignore_cert_check(false);
    if let Some(endpoint) = &settings.s3_endpoint {
        config_builder = config_builder.endpoint(endpoint.clone());
    }

    let config = config_builder
        .build()
        .map_err(map_s3_input_error_to_api_error)?;

    Ok(S3Client::new(config))
}

pub(super) async fn execute_s3(
    http_client: &HttpClient,
    request: S3Request,
) -> Result<S3Response, ApiError> {
    let url = build_s3_url(&request)?;
    let response = crate::http_client::request(
        http_client,
        request.method,
        url,
        request.headers,
        request.body,
        false,
    )
    .send()
    .await
    .map_err(|e| ApiError::InternalServerError(format!("S3 HTTP request failed: {e}")))?;

    Ok(S3Response {
        status_code: response.status().as_u16(),
        headers: crate::http_client::headers(&response),
        body: response
            .bytes()
            .await
            .map_err(|e| ApiError::InternalServerError(format!("S3 body failed: {e}")))?
            .to_vec(),
    })
}

pub(super) fn build_s3_url(request: &S3Request) -> Result<String, ApiError> {
    let scheme = if request.https { "https" } else { "http" };
    let uri = if request.uri.starts_with('/') {
        request.uri.as_str().to_owned()
    } else {
        format!("/{}", request.uri)
    };
    let url = format!("{scheme}://{}:{}{}", request.host, request.port, uri);

    shiguredo_http11::uri::Uri::parse(&url)
        .map(|_| url)
        .map_err(|e| ApiError::InternalServerError(format!("Invalid S3 URL: {e}")))
}

pub(super) fn map_s3_input_error_to_api_error(error: shiguredo_s3::Error) -> ApiError {
    match error {
        shiguredo_s3::Error::InvalidInput(message) => ApiError::BadRequest(message),
        other => ApiError::InternalServerError(other.to_string()),
    }
}

pub(super) fn map_s3_runtime_error_to_api_error(error: shiguredo_s3::Error) -> ApiError {
    ApiError::InternalServerError(error.to_string())
}

pub(super) fn encode_copy_component(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) {
            encoded.push(byte as char);
        } else {
            use std::fmt::Write;
            write!(&mut encoded, "%{byte:02X}").unwrap();
        }
    }
    encoded
}
