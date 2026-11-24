//! Slack Web API の呼び出し。
//!
//! URL・認証・Content-Type の組み立てと応答本文の読み取りは [`SlackClient`] にまとめる。
//! 応答をどう扱うか（同期 API のエラーにするか、通知の再試行・結果不明にするか）は呼び出し側が決める。

use nojson::RawJson;
use shiguredo_http11::uri::percent_encode_query;
use tracing::{debug, error, info, instrument};

use crate::{config::settings::Settings, http_client::HttpClient};

/// 同期 API が返す Slack 呼び出しの失敗。`Display` の文言は利用者へそのまま返ることがある。
#[derive(Debug)]
pub enum SlackError {
    /// 接続や本文の受信に失敗した。
    Transport(reqwest::Error),
    /// HTTP の状態コードが 2xx ではない。
    Status(u16),
    /// Slack が `ok: false` を返した。値は Slack のエラーコード。
    Api(String),
    /// 応答本文を読めない。
    InvalidResponse(String),
}

impl std::fmt::Display for SlackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(error) => error.fmt(f),
            Self::Status(status) => write!(f, "Slack HTTP status {status}"),
            Self::Api(code) | Self::InvalidResponse(code) => f.write_str(code),
        }
    }
}

impl std::error::Error for SlackError {}

impl From<reqwest::Error> for SlackError {
    fn from(error: reqwest::Error) -> Self {
        Self::Transport(error)
    }
}

/// 送信前の要求を組み立てる。送信と応答の解釈は呼び出し側で行う。
pub struct SlackClient<'a> {
    http: &'a HttpClient,
    token: &'a str,
    base_url: &'a str,
}

impl<'a> SlackClient<'a> {
    pub fn new(http: &'a HttpClient, settings: &'a Settings) -> Self {
        Self {
            http,
            token: &settings.slack_bot_token,
            base_url: &settings.slack_api_base_url,
        }
    }

    /// API のメソッド名はアプリケーションが選ぶ。任意の要求 URL からは決めない。
    fn api_url(&self, method: &str) -> String {
        format!("{}/{method}", self.base_url.trim_end_matches('/'))
    }

    fn authorization(&self) -> (String, String) {
        ("Authorization".into(), format!("Bearer {}", self.token))
    }

    /// JSON 本文を Bearer 付きで POST する。
    pub fn post_json(
        &self,
        method: &str,
        content_type: &str,
        body: Vec<u8>,
    ) -> reqwest::RequestBuilder {
        crate::http_client::request(
            self.http,
            "POST".into(),
            self.api_url(method),
            vec![
                self.authorization(),
                ("Content-Type".into(), content_type.into()),
            ],
            body,
            false,
        )
    }

    /// `files.getUploadURLExternal`。ファイルを送る一時 URL と file ID を受け取る。
    pub fn get_upload_url(&self, file_name: &str, length: usize) -> reqwest::RequestBuilder {
        crate::http_client::request(
            self.http,
            "GET".into(),
            format!(
                "{}?filename={}&length={length}",
                self.api_url("files.getUploadURLExternal"),
                percent_encode_query(file_name)
            ),
            vec![self.authorization()],
            Vec::new(),
            false,
        )
    }

    /// 一時 URL へファイルの中身を送る。この URL には Bearer を付けない。
    pub fn upload_content(&self, url: String, data: Vec<u8>) -> reqwest::RequestBuilder {
        crate::http_client::request(
            self.http,
            "POST".into(),
            url,
            vec![("Content-Type".into(), "application/octet-stream".into())],
            data,
            false,
        )
    }
}

/// `Retry-After` の秒数。読めなければ 60 秒、1 秒から 1 日の範囲に収める。
pub fn retry_after_secs(value: Option<&str>) -> i64 {
    value
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(60)
        .clamp(1, 86400)
}

/// Slack の応答本文のうち、よく使うメンバー。
///
/// serde_json の `Value` と同じく、重複したキーは後の値を使い、
/// 型の違う値やオブジェクト以外の JSON は「値なし」として扱う。
#[derive(Debug, Default)]
pub struct SlackReplyBody {
    pub ok: Option<bool>,
    pub error: Option<String>,
    pub ts: Option<String>,
    pub upload_url: Option<String>,
    pub file_id: Option<String>,
}

impl SlackReplyBody {
    /// JSON として読めないときは `None`。
    pub fn parse(body: &[u8]) -> Option<Self> {
        let json = RawJson::parse(std::str::from_utf8(body).ok()?).ok()?;
        let mut parsed = Self::default();
        let Ok(members) = json.value().to_object() else {
            return Some(parsed);
        };
        for (key, value) in members {
            let text = || String::try_from(value).ok();
            match key.to_unquoted_string_str().ok().as_deref() {
                Some("ok") => parsed.ok = bool::try_from(value).ok(),
                Some("error") => parsed.error = text(),
                Some("ts") => parsed.ts = text(),
                Some("upload_url") => parsed.upload_url = text(),
                Some("file_id") => parsed.file_id = text(),
                _ => {}
            }
        }
        Some(parsed)
    }
}

/// 同期 API の応答を確かめ、本文をそのまま返す。
///
/// `ok` は必須の真偽値として読み、欠けていればその理由を、`false` なら Slack のエラーコードを返す。
async fn read_ok_reply(response: reqwest::Response) -> Result<String, SlackError> {
    let body = String::from_utf8(response.bytes().await?.to_vec())
        .map_err(|e| SlackError::InvalidResponse(e.to_string()))?;
    let parsed = RawJson::parse(&body).map_err(|e| SlackError::InvalidResponse(e.to_string()))?;
    let ok = parsed
        .value()
        .to_member("ok")
        .and_then(|member| member.required())
        .and_then(bool::try_from)
        .map_err(|e| SlackError::InvalidResponse(e.to_string()))?;
    if !ok {
        let code = parsed
            .value()
            .to_member("error")
            .ok()
            .and_then(|member| member.optional())
            .and_then(|value| String::try_from(value).ok())
            .unwrap_or_else(|| "unknown_error".into());
        return Err(SlackError::Api(code));
    }
    Ok(body)
}

pub async fn post_message(
    client: &SlackClient<'_>,
    channel: &str,
    text: &str,
) -> Result<String, SlackError> {
    let payload = nojson::json(|f| {
        f.object(|f| {
            f.member("channel", channel)?;
            f.member("text", text)
        })
    })
    .to_string();
    send_message(client, false, &payload).await
}

/// `chat.postMessage` または `chat.update`。
#[instrument(skip(client, payload))]
pub async fn send_message(
    client: &SlackClient<'_>,
    update: bool,
    payload: &str,
) -> Result<String, SlackError> {
    let method = if update {
        "chat.update"
    } else {
        "chat.postMessage"
    };
    debug!(api_endpoint = method, "Calling Slack API");
    let response = client
        .post_json(
            method,
            "application/json; charset=utf-8",
            payload.as_bytes().to_vec(),
        )
        .send()
        .await?;
    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        return Err(SlackError::Status(status));
    }
    read_ok_reply(response).await
}

/// ファイルを Slack へ送り、file ID を返す。チャンネルへの共有はまだ行わない。
#[instrument(skip(client, file_data), fields(file_name = %file_name, file_size = file_data.len()))]
async fn upload_file(
    client: &SlackClient<'_>,
    file_name: &str,
    file_data: &[u8],
) -> Result<String, SlackError> {
    debug!(
        api_endpoint = "files.getUploadURLExternal",
        "Getting upload URL from Slack"
    );
    let response = client
        .get_upload_url(file_name, file_data.len())
        .send()
        .await?;
    let body = response.bytes().await?;
    let ticket = SlackReplyBody::parse(&body)
        .ok_or_else(|| SlackError::InvalidResponse("invalid upload ticket response".into()))?;
    if ticket.ok != Some(true) {
        let code = ticket.error.unwrap_or_else(|| "unknown_error".into());
        error!(error = %code, file_name = %file_name, "Failed to get upload URL from Slack");
        return Err(SlackError::Api(code));
    }
    let (Some(upload_url), Some(file_id)) = (ticket.upload_url, ticket.file_id) else {
        return Err(SlackError::InvalidResponse(
            "upload URL or file ID missing".into(),
        ));
    };

    debug!(file_id = %file_id, "Uploading file content to Slack");
    client
        .upload_content(upload_url, file_data.to_vec())
        .send()
        .await?
        .bytes()
        .await?;
    debug!(file_id = %file_id, "File upload completed");
    Ok(file_id)
}

/// ファイルを送り、チャンネルへ共有する。成功時は Slack の応答本文をそのまま返す。
#[instrument(skip(client, file_data), fields(file_name = %file_name, channel = %channel, file_size = file_data.len()))]
pub async fn send_single_file_to_slack(
    client: &SlackClient<'_>,
    file_data: &[u8],
    file_name: &str,
    channel: &str,
) -> Result<String, SlackError> {
    let file_id = upload_file(client, file_name, file_data).await?;
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
    let response = client
        .post_json(
            "files.completeUploadExternal",
            "application/json",
            data.to_string().into_bytes(),
        )
        .send()
        .await?;
    match read_ok_reply(response).await {
        Ok(body) => {
            info!(file_id = %file_id, file_name = %file_name, channel = %channel, "File successfully shared to Slack channel");
            Ok(body)
        }
        Err(cause) => {
            error!(error = %cause, file_id = %file_id, channel = %channel, "Failed to complete file upload");
            Err(cause)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_defaults_and_bounds() {
        assert_eq!(retry_after_secs(None), 60);
        assert_eq!(retry_after_secs(Some("x")), 60);
        assert_eq!(retry_after_secs(Some("0")), 1);
        assert_eq!(retry_after_secs(Some("30")), 30);
        assert_eq!(retry_after_secs(Some("999999")), 86400);
    }
}
