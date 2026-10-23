use shiguredo_http11::uri::percent_encode_query;
use std::error::Error as StdError;
use tracing::{debug, error, info, instrument};

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

pub async fn post_message(
    client: &HttpClient,
    token: &str,
    base_url: &str,
    channel: &str,
    text: &str,
) -> Result<String, Box<dyn StdError>> {
    let payload = nojson::json(|f| {
        f.object(|f| {
            f.member("channel", channel)?;
            f.member("text", text)
        })
    })
    .to_string();
    send_message(client, token, base_url, false, &payload).await
}

/// The method is selected by the application, never by an arbitrary request URL.
#[instrument(skip(client, token, base_url, payload))]
pub async fn send_message(
    client: &HttpClient,
    token: &str,
    base_url: &str,
    update: bool,
    payload: &str,
) -> Result<String, Box<dyn StdError>> {
    let method = if update {
        "chat.update"
    } else {
        "chat.postMessage"
    };
    debug!(api_endpoint = method, "Calling Slack API");
    let response = client
        .send(HttpRequest {
            method: "POST".into(),
            url: format!("{}/{method}", base_url.trim_end_matches('/')),
            headers: vec![
                ("Authorization".into(), format!("Bearer {token}")),
                (
                    "Content-Type".into(),
                    "application/json; charset=utf-8".into(),
                ),
            ],
            body: payload.as_bytes().to_vec(),
        })
        .await?;
    if !(200..300).contains(&response.status_code) {
        return Err(
            std::io::Error::other(format!("Slack HTTP status {}", response.status_code)).into(),
        );
    }
    let response = String::from_utf8(response.body)?;
    let parsed = nojson::RawJson::parse(&response)?;
    if !get_required_bool(parsed.value(), "ok")? {
        let error =
            get_optional_string(parsed.value(), "error").unwrap_or_else(|| "unknown_error".into());
        return Err(std::io::Error::other(error).into());
    }
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

    let response = String::from_utf8(response.body)
        .map_err(|e| Box::<dyn StdError>::from(std::io::Error::other(e.to_string())))?;

    let parsed = nojson::RawJson::parse(&response)?;
    let root = parsed.value();
    let ok = get_required_bool(root, "ok")?;

    if !ok {
        let error_message =
            get_optional_string(root, "error").unwrap_or_else(|| "unknown_error".to_string());
        error!(
            error = %error_message,
            file_name = %file_name,
            "Failed to get upload URL from Slack"
        );
        return Err(Box::new(std::io::Error::other(error_message)));
    }

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

    let response_text = String::from_utf8(response_text.body)
        .map_err(|e| Box::<dyn StdError>::from(std::io::Error::other(e.to_string())))?;

    let parsed = nojson::RawJson::parse(&response_text)?;
    let root = parsed.value();
    let ok = get_required_bool(root, "ok")?;

    if ok {
        info!(
            file_id = %file_id,
            file_name = %file_name,
            channel = %channel,
            "File successfully shared to Slack channel"
        );
        Ok(response_text)
    } else {
        error!(
            error = %get_optional_string(root, "error").unwrap_or_else(|| "unknown_error".to_string()),
            file_id = %file_id,
            channel = %channel,
            "Failed to complete file upload"
        );
        let error_message =
            get_optional_string(root, "error").unwrap_or_else(|| "unknown_error".to_string());
        Err(Box::new(std::io::Error::other(error_message)))
    }
}
