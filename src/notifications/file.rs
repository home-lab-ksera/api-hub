use crate::{
    config::state::AppState,
    database::DbResult,
    http_client::{HttpRequest, HttpResponse},
    service::s3_service::{self, ProxyObjectStreamInput},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgPool;

#[derive(Deserialize, Serialize)]
pub struct NotificationFile {
    pub bucket: String,
    pub key: String,
    pub name: String,
    pub content_type: String,
}
impl NotificationFile {
    pub fn validate(&self) -> DbResult<()> {
        let leaf = self.key.strip_prefix("hitomi-notifications/").unwrap_or("");
        let Some((hash, extension)) = leaf.rsplit_once('.') else {
            return Err("invalid notification file key".into());
        };
        if self.bucket != "pdfs"
            || hash.len() != 64
            || !hash.bytes().all(|b| b.is_ascii_hexdigit())
            || !matches!(
                (extension, self.content_type.as_str()),
                ("jpg", "image/jpeg") | ("png", "image/png")
            )
            || self.name.is_empty()
            || self.name.len() > 128
            || self.name.contains(['\r', '\n', '/', '\\'])
        {
            return Err("invalid notification file reference".into());
        }
        Ok(())
    }
}

pub enum FileError {
    Retry(i64, &'static str),
    Failed(&'static str),
}
impl FileError {
    pub fn status(&self) -> (&'static str, i64, &'static str) {
        match self {
            Self::Retry(delay, message) => ("retry", *delay, *message),
            Self::Failed(message) => ("failed", 0, *message),
        }
    }
}
fn rate_delay(reply: &HttpResponse) -> i64 {
    reply
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("retry-after"))
        .and_then(|(_, v)| v.parse::<i64>().ok())
        .unwrap_or(60)
        .clamp(1, 86400)
}

pub async fn deliver(
    state: &AppState,
    pool: &PgPool,
    id: i64,
    claim: &str,
    payload: &Value,
    share_started: &mut bool,
) -> Result<HttpResponse, FileError> {
    let file: NotificationFile = serde_json::from_value(payload["file"].clone())
        .map_err(|_| FileError::Failed("invalid stored file reference"))?;
    file.validate()
        .map_err(|_| FileError::Failed("invalid stored file reference"))?;
    let mut response = s3_service::get_object_proxy_stream(
        &state.client,
        &state.settings,
        ProxyObjectStreamInput {
            bucket: file.bucket,
            key: file.key,
            range: None,
            if_match: None,
            if_none_match: None,
            if_modified_since: None,
            if_unmodified_since: None,
        },
    )
    .await
    .map_err(|_| FileError::Retry(60, "image storage unavailable"))?;
    if response.status_code == 404 {
        return Err(FileError::Failed("notification image expired or missing"));
    }
    if response.status_code != 200 {
        return Err(FileError::Retry(60, "image storage unavailable"));
    }
    let mut data = Vec::new();
    let mut chunk = [0; 8192];
    loop {
        let n = response
            .read_chunk(&mut chunk)
            .await
            .map_err(|_| FileError::Retry(60, "image read failed"))?;
        if n == 0 {
            break;
        }
        if data.len() + n > 2 * 1024 * 1024 {
            return Err(FileError::Failed("notification image exceeds 2 MiB"));
        }
        data.extend_from_slice(&chunk[..n]);
    }
    let valid = if file.content_type == "image/jpeg" {
        data.starts_with(b"\xff\xd8\xff")
    } else {
        data.starts_with(b"\x89PNG\r\n\x1a\n")
    };
    if !valid {
        return Err(FileError::Failed("notification image signature invalid"));
    }
    let reply = state
        .client
        .send(HttpRequest {
            method: "GET".into(),
            url: format!(
                "{}/files.getUploadURLExternal?filename={}&length={}",
                state.settings.slack_api_base_url,
                shiguredo_http11::uri::percent_encode_query(&file.name),
                data.len()
            ),
            headers: vec![(
                "Authorization".into(),
                format!("Bearer {}", state.settings.slack_bot_token),
            )],
            body: vec![],
        })
        .await
        .map_err(|_| FileError::Retry(60, "Slack upload preparation failed"))?;
    if reply.status_code == 429 {
        return Err(FileError::Retry(rate_delay(&reply), "Slack rate limited"));
    }
    if reply.status_code >= 500 {
        return Err(FileError::Retry(60, "Slack upload preparation unavailable"));
    }
    let ticket: Value = serde_json::from_slice(&reply.body)
        .map_err(|_| FileError::Retry(60, "invalid upload ticket response"))?;
    if !(200..300).contains(&reply.status_code) || ticket["ok"] != true {
        return Err(FileError::Failed("Slack rejected upload ticket"));
    }
    let url = ticket["upload_url"]
        .as_str()
        .ok_or(FileError::Failed("upload URL missing"))?;
    let file_id = ticket["file_id"]
        .as_str()
        .ok_or(FileError::Failed("file ID missing"))?;
    let upload = state
        .client
        .send(HttpRequest {
            method: "POST".into(),
            url: url.into(),
            headers: vec![("Content-Type".into(), "application/octet-stream".into())],
            body: data,
        })
        .await
        .map_err(|_| FileError::Retry(60, "Slack file transfer failed"))?;
    if !(200..300).contains(&upload.status_code) {
        return Err(FileError::Retry(60, "Slack file transfer rejected"));
    }
    // channelへの共有前にfile IDを記録する。completeの応答不明は自動で再共有しない。
    let saved=sqlx::query("UPDATE notifications SET slack_file_id=$3 WHERE id=$1 AND claim_token=$2::uuid AND state='sending'").bind(id).bind(claim).bind(file_id).execute(pool).await.map_err(|_|FileError::Retry(60,"file ID persistence failed"))?;
    if saved.rows_affected() != 1 {
        return Err(FileError::Failed("notification lease lost"));
    }
    *share_started = true;
    // ここからの通信失敗は共通の結果不明判定へ渡す。
    match state.client.send(HttpRequest{method:"POST".into(),url:format!("{}/files.completeUploadExternal",state.settings.slack_api_base_url),headers:vec![("Authorization".into(),format!("Bearer {}",state.settings.slack_bot_token)),("Content-Type".into(),"application/json".into())],body:json!({"files":[{"id":file_id,"title":file.name}],"channel_id":payload["channel"],"initial_comment":payload["text"]}).to_string().into_bytes()}).await {
        Ok(reply)=>Ok(reply),
        Err(_)=>Ok(HttpResponse{status_code:500,headers:vec![],body:vec![]}),
    }
}
