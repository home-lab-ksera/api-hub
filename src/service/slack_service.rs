use shiguredo_http11::uri::percent_encode_query;
use std::error::Error as StdError;
use tracing::{debug, error, info, instrument, warn};

use crate::http_client::{HttpClient, HttpRequest};

fn get_required_string(
    root: nojson::RawJsonValue<'_, '_>,
    name: &str,
) -> Result<String, Box<dyn StdError>> {
    let value = root
        .to_member(name)
        .map_err(Box::<dyn StdError>::from)?
        .required()
        .map_err(Box::<dyn StdError>::from)?;
    let converted = String::try_from(value).map_err(Box::<dyn StdError>::from)?;
    Ok(converted)
}

fn get_required_bool(
    root: nojson::RawJsonValue<'_, '_>,
    name: &str,
) -> Result<bool, Box<dyn StdError>> {
    let value = root
        .to_member(name)
        .map_err(Box::<dyn StdError>::from)?
        .required()
        .map_err(Box::<dyn StdError>::from)?;
    let converted = bool::try_from(value).map_err(Box::<dyn StdError>::from)?;
    Ok(converted)
}

fn get_optional_string(root: nojson::RawJsonValue<'_, '_>, name: &str) -> Option<String> {
    let value = root.to_member(name).ok()?.optional()?;
    String::try_from(value).ok()
}

fn response_body_to_text(body: Vec<u8>) -> Result<String, Box<dyn StdError>> {
    String::from_utf8(body)
        .map_err(|e| Box::<dyn StdError>::from(std::io::Error::other(e.to_string())))
}

fn ensure_slack_ok(response: &str) -> Result<(), Box<dyn StdError>> {
    let parsed = nojson::RawJson::parse(response)?;
    let root = parsed.value();
    let ok = get_required_bool(root, "ok")?;

    if !ok {
        let error_message =
            get_optional_string(root, "error").unwrap_or_else(|| "unknown_error".to_string());
        return Err(Box::new(std::io::Error::other(error_message)));
    }

    Ok(())
}

#[instrument(skip(client, slack_bot_token, text), fields(channel = %channel))]
pub async fn post_message(
    client: &HttpClient,
    slack_bot_token: &str,
    slack_api_base_url: &str,
    channel: &str,
    text: &str,
) -> Result<String, Box<dyn StdError>> {
    let url = format!("{}/chat.postMessage", slack_api_base_url);

    let payload = nojson::json(|f| {
        f.object(|f| {
            f.member("channel", channel)?;
            f.member("text", text)
        })
    });

    debug!(
        api_endpoint = "chat.postMessage",
        channel = %channel,
        "Calling Slack API"
    );

    let response = client
        .send(HttpRequest {
            method: "POST".to_string(),
            url,
            headers: vec![
                (
                    "Authorization".to_string(),
                    format!("Bearer {slack_bot_token}"),
                ),
                ("Content-Type".to_string(), "application/json".to_string()),
            ],
            body: payload.to_string().into_bytes(),
        })
        .await?;

    let response = response_body_to_text(response.body)?;
    if let Err(e) = ensure_slack_ok(&response) {
        warn!(
            error = %e,
            channel = %channel,
            "Slack API returned error response"
        );
        return Err(e);
    }

    debug!(
        channel = %channel,
        "Slack API call successful"
    );

    Ok(response)
}

#[instrument(skip(client, slack_bot_token, file_data), fields(file_name = %file_name, file_size = file_data.len()))]
pub async fn upload_file(
    client: &HttpClient,
    slack_bot_token: &str,
    slack_api_base_url: &str,
    file_name: &str,
    file_data: &[u8],
) -> Result<(String, String), Box<dyn StdError>> {
    let url = format!("{}/files.getUploadURLExternal", slack_api_base_url);

    debug!(
        api_endpoint = "files.getUploadURLExternal",
        file_name = %file_name,
        file_size = file_data.len(),
        "Getting upload URL from Slack"
    );

    let response = client
        .send(HttpRequest {
            method: "GET".to_string(),
            url: format!(
                "{}?filename={}&length={}",
                url,
                percent_encode_query(file_name),
                file_data.len()
            ),
            headers: vec![(
                "Authorization".to_string(),
                format!("Bearer {slack_bot_token}"),
            )],
            body: Vec::new(),
        })
        .await?;

    let response = response_body_to_text(response.body)?;
    if let Err(e) = ensure_slack_ok(&response) {
        error!(
            error = %e,
            file_name = %file_name,
            "Failed to get upload URL from Slack"
        );
        return Err(e);
    }

    let parsed = nojson::RawJson::parse(&response)?;
    let root = parsed.value();
    let upload_url = get_required_string(root, "upload_url")?;
    let file_id = get_required_string(root, "file_id")?;

    debug!(
        file_id = %file_id,
        "Uploading file content to Slack"
    );

    client
        .send(HttpRequest {
            method: "POST".to_string(),
            url: upload_url.clone(),
            headers: vec![(
                "Content-Type".to_string(),
                "application/octet-stream".to_string(),
            )],
            body: file_data.to_vec(),
        })
        .await?;

    debug!(
        file_id = %file_id,
        "File upload completed"
    );

    Ok((file_id, upload_url))
}

#[instrument(skip(client, token, file_data), fields(file_name = %file_name, channel = %channel, file_size = file_data.len()))]
pub async fn send_single_file_to_slack(
    client: &HttpClient,
    token: &str,
    slack_api_base_url: &str,
    file_data: &[u8],
    file_name: &str,
    channel: &str,
) -> Result<String, Box<dyn StdError>> {
    let (file_id, _upload_url) =
        upload_file(client, token, slack_api_base_url, file_name, file_data).await?;

    let url = format!("{}/files.completeUploadExternal", slack_api_base_url);

    let data = nojson::json(|f| {
        f.object(|f| {
            f.member(
                "files",
                nojson::array(|f| {
                    f.element(nojson::object(|f| {
                        f.member("id", &file_id)?;
                        f.member("title", file_name)
                    }))
                }),
            )?;
            f.member("channel_id", channel)
        })
    });

    debug!(
        api_endpoint = "files.completeUploadExternal",
        file_id = %file_id,
        channel = %channel,
        "Completing file upload to Slack"
    );

    let response_text = client
        .send(HttpRequest {
            method: "POST".to_string(),
            url,
            headers: vec![
                ("Authorization".to_string(), format!("Bearer {token}")),
                ("Content-Type".to_string(), "application/json".to_string()),
            ],
            body: data.to_string().into_bytes(),
        })
        .await?;

    let response_text = response_body_to_text(response_text.body)?;
    if let Err(e) = ensure_slack_ok(&response_text) {
        error!(
            error = %e,
            file_id = %file_id,
            channel = %channel,
            "Failed to complete file upload"
        );
        return Err(e);
    }

    info!(
        file_id = %file_id,
        file_name = %file_name,
        channel = %channel,
        "File successfully shared to Slack channel"
    );
    Ok(response_text)
}

#[cfg(test)]
mod tests {
    use super::{ensure_slack_ok, response_body_to_text};

    #[test]
    fn response_body_to_text_rejects_non_utf8() {
        assert!(response_body_to_text(vec![0xFF]).is_err());
    }

    #[test]
    fn ensure_slack_ok_accepts_ok_true() {
        assert!(ensure_slack_ok(r#"{"ok":true}"#).is_ok());
    }

    #[test]
    fn ensure_slack_ok_rejects_ok_false_with_error() {
        let error = ensure_slack_ok(r#"{"ok":false,"error":"channel_not_found"}"#)
            .expect_err("Slack error should be returned");

        assert_eq!(error.to_string(), "channel_not_found");
    }

    #[test]
    fn ensure_slack_ok_rejects_missing_ok_and_invalid_json() {
        assert!(ensure_slack_ok(r#"{"error":"missing_ok"}"#).is_err());
        assert!(ensure_slack_ok("not json").is_err());
    }
}
