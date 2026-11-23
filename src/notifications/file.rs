use super::{Canonical, KnownFields};
use crate::{
    config::state::AppState,
    database::DbResult,
    service::s3_service::{self, ProxyObjectStreamInput},
};
use nojson::{DisplayJson, JsonFormatter, RawJson, RawJsonValue};
use sqlx::PgPool;

pub struct NotificationFile {
    pub bucket: String,
    pub key: String,
    pub name: String,
    pub content_type: String,
}
impl NotificationFile {
    /// 以前の serde の構造体と同じく、既知のキーの重複・型の誤り・欠落は拒否し、未知のキーは無視する。
    pub fn from_json(value: RawJsonValue<'_, '_>) -> Option<Self> {
        let mut fields = KnownFields::new(&["bucket", "key", "name", "content_type"]);
        for (key, value) in value.to_object().ok()? {
            fields.insert(&key.to_unquoted_string_str().ok()?, value)?;
        }
        Some(Self {
            bucket: fields.string("bucket")?,
            key: fields.string("key")?,
            name: fields.string("name")?,
            content_type: fields.string("content_type")?,
        })
    }

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

impl DisplayJson for NotificationFile {
    fn fmt(&self, f: &mut JsonFormatter<'_, '_>) -> std::fmt::Result {
        f.object(|f| {
            f.member("bucket", &self.bucket)?;
            f.member("key", &self.key)?;
            f.member("name", &self.name)?;
            f.member("content_type", &self.content_type)
        })
    }
}

/// `files.getUploadURLExternal` の応答本文。重複したキーは後の値を使う。
struct UploadTicket {
    ok: Option<bool>,
    upload_url: Option<String>,
    file_id: Option<String>,
}

impl UploadTicket {
    /// JSON として読めないときは `None`。
    fn parse(body: &[u8]) -> Option<Self> {
        let json = RawJson::parse(std::str::from_utf8(body).ok()?).ok()?;
        let mut ticket = Self {
            ok: None,
            upload_url: None,
            file_id: None,
        };
        let Ok(members) = json.value().to_object() else {
            return Some(ticket);
        };
        for (key, value) in members {
            match key.to_unquoted_string_str().ok().as_deref() {
                Some("ok") => ticket.ok = bool::try_from(value).ok(),
                Some("upload_url") => ticket.upload_url = String::try_from(value).ok(),
                Some("file_id") => ticket.file_id = String::try_from(value).ok(),
                _ => {}
            }
        }
        Some(ticket)
    }
}

pub enum FileError {
    Retry(i64, &'static str),
    Failed(&'static str),
    Uncertain(&'static str),
}
impl FileError {
    pub fn status(&self) -> (&'static str, i64, &'static str) {
        match self {
            Self::Retry(delay, message) => ("retry", *delay, *message),
            Self::Failed(message) => ("failed", 0, *message),
            Self::Uncertain(message) => ("uncertain", 0, *message),
        }
    }
}
fn rate_delay(reply: &reqwest::Response) -> i64 {
    reply
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(60)
        .clamp(1, 86400)
}

pub async fn deliver(
    state: &AppState,
    pool: &PgPool,
    id: i64,
    claim: &str,
    payload: RawJsonValue<'_, '_>,
    share_started: &mut bool,
) -> Result<reqwest::Response, FileError> {
    let member = |name: &str| payload.to_member(name).ok().and_then(|m| m.optional());
    let file = member("file")
        .and_then(NotificationFile::from_json)
        .ok_or(FileError::Failed("invalid stored file reference"))?;
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
    if response.status().as_u16() == 404 {
        return Err(FileError::Failed("notification image expired or missing"));
    }
    if response.status().as_u16() != 200 {
        return Err(FileError::Retry(60, "image storage unavailable"));
    }
    let mut data = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| FileError::Retry(60, "image read failed"))?
    {
        if data.len() + chunk.len() > 2 * 1024 * 1024 {
            return Err(FileError::Failed("notification image exceeds 2 MiB"));
        }
        data.extend_from_slice(&chunk);
    }
    let valid = if file.content_type == "image/jpeg" {
        data.starts_with(b"\xff\xd8\xff")
    } else {
        data.starts_with(b"\x89PNG\r\n\x1a\n")
    };
    if !valid {
        return Err(FileError::Failed("notification image signature invalid"));
    }
    let reply = crate::http_client::request(
        &state.client,
        "GET".into(),
        format!(
            "{}/files.getUploadURLExternal?filename={}&length={}",
            state.settings.slack_api_base_url,
            shiguredo_http11::uri::percent_encode_query(&file.name),
            data.len()
        ),
        vec![(
            "Authorization".into(),
            format!("Bearer {}", state.settings.slack_bot_token),
        )],
        vec![],
        false,
    )
    .send()
    .await
    .map_err(|_| FileError::Retry(60, "Slack upload preparation failed"))?;
    if reply.status().as_u16() == 429 {
        return Err(FileError::Retry(rate_delay(&reply), "Slack rate limited"));
    }
    if reply.status().as_u16() >= 500 {
        return Err(FileError::Retry(60, "Slack upload preparation unavailable"));
    }
    let ticket_status = reply.status().as_u16();
    let ticket = reply
        .bytes()
        .await
        .ok()
        .and_then(|body| UploadTicket::parse(&body))
        .ok_or(FileError::Retry(60, "invalid upload ticket response"))?;
    if !(200..300).contains(&ticket_status) || ticket.ok != Some(true) {
        return Err(FileError::Failed("Slack rejected upload ticket"));
    }
    let url = ticket
        .upload_url
        .ok_or(FileError::Failed("upload URL missing"))?;
    let file_id = ticket.file_id.ok_or(FileError::Failed("file ID missing"))?;
    let upload = crate::http_client::request(
        &state.client,
        "POST".into(),
        url,
        vec![("Content-Type".into(), "application/octet-stream".into())],
        data,
        false,
    )
    .send()
    .await
    .map_err(|_| FileError::Retry(60, "Slack file transfer failed"))?;
    if !(200..300).contains(&upload.status().as_u16()) {
        return Err(FileError::Retry(60, "Slack file transfer rejected"));
    }
    upload
        .bytes()
        .await
        .map_err(|_| FileError::Retry(60, "Slack file transfer incomplete"))?;
    // channelへの共有前にfile IDを記録する。completeの応答不明は自動で再共有しない。
    let saved=sqlx::query("UPDATE notifications SET slack_file_id=$3 WHERE id=$1 AND claim_token=$2::uuid AND state='sending'").bind(id).bind(claim).bind(&file_id).execute(pool).await.map_err(|_|FileError::Retry(60,"file ID persistence failed"))?;
    if saved.rows_affected() != 1 {
        return Err(FileError::Failed("notification lease lost"));
    }
    *share_started = true;
    // ここからの通信失敗は共通の結果不明判定へ渡す。
    match crate::http_client::request(
        &state.client,
        "POST".into(),
        format!(
            "{}/files.completeUploadExternal",
            state.settings.slack_api_base_url
        ),
        vec![
            (
                "Authorization".into(),
                format!("Bearer {}", state.settings.slack_bot_token),
            ),
            ("Content-Type".into(), "application/json".into()),
        ],
        complete_body(&file_id, &file.name, member("channel"), member("text")),
        false,
    )
    .send()
    .await
    {
        Ok(reply) => Ok(reply),
        Err(_) => Err(FileError::Uncertain("Slack share response lost")),
    }
}

/// `files.completeUploadExternal` の本文。キーは以前の serde_json の出力と同じ昇順。
/// channel と text は保存済みの payload の値をそのまま渡し、欠けていれば `null` にする。
fn complete_body(
    file_id: &str,
    title: &str,
    channel: Option<RawJsonValue<'_, '_>>,
    text: Option<RawJsonValue<'_, '_>>,
) -> Vec<u8> {
    nojson::json(|f| {
        f.object(|f| {
            f.member("channel_id", channel.map(Canonical))?;
            f.member(
                "files",
                nojson::array(|f| {
                    f.element(nojson::object(|f| {
                        f.member("id", file_id)?;
                        f.member("title", title)
                    }))
                }),
            )?;
            f.member("initial_comment", text.map(Canonical))
        })
    })
    .to_string()
    .into_bytes()
}
