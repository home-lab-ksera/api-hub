use crate::{
    config::state::AppState,
    database::DbResult,
    service::slack_service::{SlackClient, SlackReplyBody, retry_after_secs},
};
use nojson::{DisplayJson, JsonFormatter, JsonValueKind, RawJson, RawJsonValue};
use shiguredo_http11::Response;
use sqlx::{PgPool, Row};
use std::collections::BTreeMap;
use std::time::Duration;
mod file;
pub use file::NotificationFile;

pub struct Notification {
    pub producer: String,
    pub event_id: String,
    pub channel: String,
    pub text: String,
    pub file: Option<NotificationFile>,
}

impl Notification {
    /// `POST /notifications` の本文を読む。
    ///
    /// 以前の serde の構造体と同じく、既知のキーの重複・型の誤り・必須項目の欠落は拒否し、
    /// 未知のキーは無視する。`file` の `null` は省略と同じ扱い。
    pub fn from_json(body: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(body).ok()?;
        let json = RawJson::parse(text).ok()?;
        let mut fields = KnownFields::new(&["producer", "event_id", "channel", "text", "file"]);
        for (key, value) in json.value().to_object().ok()? {
            fields.insert(&key.to_unquoted_string_str().ok()?, value)?;
        }
        let file = match fields.take("file") {
            Some(value) if !value.kind().is_null() => Some(NotificationFile::from_json(value)?),
            _ => None,
        };
        Some(Self {
            producer: fields.string("producer")?,
            event_id: fields.string("event_id")?,
            channel: fields.string("channel")?,
            text: fields.string("text")?,
            file,
        })
    }
}

/// 既知のキーだけを集め、同じキーが2回現れたら拒否する。
pub(crate) struct KnownFields<'text, 'raw> {
    names: &'static [&'static str],
    values: Vec<Option<RawJsonValue<'text, 'raw>>>,
}

impl<'text, 'raw> KnownFields<'text, 'raw> {
    pub(crate) fn new(names: &'static [&'static str]) -> Self {
        Self {
            names,
            values: vec![None; names.len()],
        }
    }

    /// 未知のキーは無視する。既知のキーの重複は `None`。
    pub(crate) fn insert(&mut self, key: &str, value: RawJsonValue<'text, 'raw>) -> Option<()> {
        if let Some(index) = self.names.iter().position(|name| *name == key)
            && self.values[index].replace(value).is_some()
        {
            return None;
        }
        Some(())
    }

    pub(crate) fn take(&mut self, name: &str) -> Option<RawJsonValue<'text, 'raw>> {
        let index = self.names.iter().position(|known| *known == name)?;
        self.values[index].take()
    }

    /// 必須の文字列。`null` や欠落は `None`。
    pub(crate) fn string(&mut self, name: &str) -> Option<String> {
        String::try_from(self.take(name)?).ok()
    }
}

/// serde_json の `Value::to_string()` と同じ形で書き直す（キーは昇順、重複は後勝ち）。
pub(crate) struct Canonical<'text, 'raw>(pub RawJsonValue<'text, 'raw>);

impl DisplayJson for Canonical<'_, '_> {
    fn fmt(&self, f: &mut JsonFormatter<'_, '_>) -> std::fmt::Result {
        let invalid = |_| std::fmt::Error;
        match self.0.kind() {
            JsonValueKind::Object => {
                let mut members = BTreeMap::new();
                for (key, value) in self.0.to_object().map_err(invalid)? {
                    let key = key.to_unquoted_string_str().map_err(invalid)?.into_owned();
                    members.insert(key, Canonical(value));
                }
                f.value(members)
            }
            JsonValueKind::Array => f.array(|f| {
                for value in self.0.to_array().map_err(invalid)? {
                    f.element(Canonical(value))?;
                }
                Ok(())
            }),
            JsonValueKind::String => f.value(self.0.to_unquoted_string_str().map_err(invalid)?),
            _ => f.value(self.0),
        }
    }
}

fn response(status: u16, body: impl DisplayJson) -> Response {
    Response::new(status, crate::errors::api_error::reason_phrase(status))
        .header("Content-Type", "application/json")
        .header("Cache-Control", "no-store")
        .body(nojson::json(|f| f.value(&body)).to_string().into_bytes())
}

fn error(status: u16, message: &str) -> Response {
    response(status, nojson::object(|f| f.member("error", message)))
}

fn accepted(id: i64, state: &str) -> Response {
    response(
        202,
        nojson::object(|f| {
            f.member("id", id.to_string())?;
            f.member("state", state)
        }),
    )
}

/// DB の jsonb 列へ保存する payload。キーの順序は jsonb が正規化する。
fn payload_json(item: &Notification) -> String {
    nojson::json(|f| {
        f.object(|f| {
            f.member("channel", &item.channel)?;
            f.member("text", &item.text)?;
            if let Some(file) = &item.file {
                f.member("file", file)?;
            }
            Ok(())
        })
    })
    .to_string()
}

pub async fn enqueue(pool: &PgPool, item: &Notification) -> DbResult<(i64, String)> {
    for field in [&item.producer, &item.event_id, &item.channel] {
        if field.trim().is_empty() || field.len() > 256 {
            return Err("invalid notification identity".into());
        }
    }
    if item.text.trim().is_empty() || item.text.len() > 16000 {
        return Err("invalid notification text".into());
    }
    if let Some(file) = &item.file {
        file.validate()?;
    }
    let payload = payload_json(item);
    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO notifications (producer,event_id,channel,payload) VALUES ($1,$2,$3,$4::jsonb) ON CONFLICT DO NOTHING")
        .bind(&item.producer).bind(&item.event_id).bind(&item.channel).bind(&payload)
        .execute(&mut *tx).await?;
    // jsonb の比較はキーの順序や空白によらない。
    let row = sqlx::query("SELECT id,state,payload=$4::jsonb AS same_payload FROM notifications WHERE producer=$1 AND event_id=$2 AND channel=$3")
        .bind(&item.producer).bind(&item.event_id).bind(&item.channel).bind(&payload).fetch_one(&mut *tx).await?;
    if !row.get::<bool, _>("same_payload") {
        return Err("idempotency key reused with different payload".into());
    }
    let result = (row.get("id"), row.get("state"));
    tx.commit().await?;
    Ok(result)
}

pub async fn handle_post(state: &AppState, body: &[u8]) -> Response {
    let Some(pool) = &state.database else {
        return error(503, "notification database unavailable");
    };
    let Some(item) = Notification::from_json(body) else {
        return error(400, "producer, event_id, channel and text are required");
    };
    match enqueue(pool, &item).await {
        Ok((id, status)) => accepted(id, &status),
        Err(cause) => {
            let message = cause.to_string();
            if message == "idempotency key reused with different payload" {
                error(409, &message)
            } else if message.starts_with("invalid notification") {
                error(400, &message)
            } else {
                tracing::error!(error = %cause, "Notification persistence failed");
                error(503, "notification persistence failed")
            }
        }
    }
}

pub async fn handle_get(state: &AppState, id: &str) -> Response {
    let Some(pool) = &state.database else {
        return error(503, "notification database unavailable");
    };
    let Ok(id) = id.parse::<i64>() else {
        return error(400, "invalid notification id");
    };
    match sqlx::query("SELECT id,producer,event_id,channel,state,attempts,slack_ts,slack_file_id,last_error FROM notifications WHERE id=$1")
        .bind(id)
        .fetch_optional(pool)
        .await
    {
        // キーは昇順で返す（serde_json の出力と同じ）。
        Ok(Some(row)) => response(
            200,
            nojson::object(|f| {
                f.member("attempts", row.get::<i32, _>("attempts"))?;
                f.member("channel", row.get::<String, _>("channel"))?;
                f.member("event_id", row.get::<String, _>("event_id"))?;
                f.member("id", id.to_string())?;
                f.member("last_error", row.get::<Option<String>, _>("last_error"))?;
                f.member("producer", row.get::<String, _>("producer"))?;
                f.member("slack_file_id", row.get::<Option<String>, _>("slack_file_id"))?;
                f.member("slack_ts", row.get::<Option<String>, _>("slack_ts"))?;
                f.member("state", row.get::<String, _>("state"))
            }),
        ),
        Ok(None) => error(404, "notification not found"),
        Err(_) => error(503, "notification database unavailable"),
    }
}

/// 外部送信の結果不明は自動再送しない。重複を確認してから明示的に再試行する。
pub async fn handle_retry(state: &AppState, id: &str) -> Response {
    let Some(pool) = &state.database else {
        return error(503, "notification database unavailable");
    };
    let Ok(id) = id.parse::<i64>() else {
        return error(400, "invalid notification id");
    };
    match sqlx::query("UPDATE notifications SET state='pending',attempts=0,available_at=now(),claim_token=NULL,lease_until=NULL,last_error=NULL,updated_at=now() WHERE id=$1 AND state IN ('uncertain','failed')").bind(id).execute(pool).await {
        Ok(result) if result.rows_affected() == 1 => accepted(id, "pending"),
        Ok(_) => error(409, "only uncertain or failed notifications can be retried"),
        Err(_) => error(503, "notification database unavailable"),
    }
}

struct SlackReply {
    status_code: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}
async fn slack_reply(response: reqwest::Response) -> Result<SlackReply, reqwest::Error> {
    let status_code = response.status().as_u16();
    let headers = crate::http_client::headers(&response);
    let body = response.bytes().await?.to_vec();
    Ok(SlackReply {
        status_code,
        headers,
        body,
    })
}

pub async fn deliver_one(state: &AppState) -> DbResult<bool> {
    let pool = state.database.as_ref().ok_or("database required")?;
    // Slackが受け取った直後にworkerが落ちた可能性があるため、lease切れは結果不明にする。
    sqlx::query("UPDATE notifications SET state='uncertain',last_error='worker lease expired; verify Slack before retry',updated_at=now() WHERE state='sending' AND lease_until < now()")
        .execute(pool).await?;
    let row = sqlx::query("WITH candidate AS (SELECT id FROM notifications WHERE state IN ('pending','retry') AND available_at<=now() ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE notifications n SET state='sending',attempts=attempts+1,claim_token=gen_random_uuid(),lease_until=now()+interval '90 seconds',updated_at=now() FROM candidate WHERE n.id=candidate.id RETURNING n.id,n.payload::text AS payload,n.claim_token::text,n.attempts")
        .fetch_optional(pool).await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let id: i64 = row.get("id");
    let claim: String = row.get("claim_token");
    let attempts: i32 = row.get("attempts");
    let payload: String = row.get("payload");
    let stored = RawJson::parse(&payload)?;
    let has_file = stored
        .value()
        .to_member("file")
        .ok()
        .and_then(|member| member.optional())
        .is_some();
    let result = if has_file {
        let mut share_started = false;
        match tokio::time::timeout(
            Duration::from_secs(60),
            file::deliver(state, pool, id, &claim, stored.value(), &mut share_started),
        )
        .await
        {
            Ok(Ok(reply)) => Ok(Ok(reply)),
            Ok(Err(error)) => {
                let (status, delay, message) = error.status();
                sqlx::query("UPDATE notifications SET state=$3,available_at=now()+make_interval(secs=>$4),last_error=$5,lease_until=NULL,updated_at=now() WHERE id=$1 AND claim_token=$2::uuid AND state='sending'")
                    .bind(id).bind(&claim).bind(if status=="retry" && attempts>=8 {"failed"} else {status}).bind(delay as f64).bind(message).execute(pool).await?;
                return Ok(true);
            }
            Err(_) if !share_started => {
                sqlx::query("UPDATE notifications SET state=$3,available_at=now()+interval '60 seconds',last_error='file preparation timed out',lease_until=NULL,updated_at=now() WHERE id=$1 AND claim_token=$2::uuid AND state='sending'")
                    .bind(id).bind(&claim).bind(if attempts>=8 {"failed"}else{"retry"}).execute(pool).await?;
                return Ok(true);
            }
            Err(error) => Err(error),
        }
    } else {
        tokio::time::timeout(
            Duration::from_secs(60),
            SlackClient::new(&state.client, &state.settings)
                .post_json(
                    "chat.postMessage",
                    "application/json",
                    nojson::json(|f| f.value(Canonical(stored.value())))
                        .to_string()
                        .into_bytes(),
                )
                .send(),
        )
        .await
    };
    let result = match result {
        Ok(Ok(reply)) => Ok(slack_reply(reply).await),
        Ok(Err(error)) => Ok(Err(error)),
        Err(error) => Err(error),
    };
    let (status, delay, ts, error) = match result {
        Ok(Ok(reply)) if reply.status_code == 429 => {
            let delay = retry_after_secs(
                reply
                    .headers
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case("retry-after"))
                    .map(|(_, value)| value.as_str()),
            );
            ("retry", delay, None, Some("slack rate limited".to_string()))
        }
        Ok(Ok(reply)) if (200..300).contains(&reply.status_code) => {
            match SlackReplyBody::parse(&reply.body) {
                Some(SlackReplyBody {
                    ok: Some(true), ts, ..
                }) => ("sent", 0, ts, None),
                Some(SlackReplyBody {
                    ok: Some(false),
                    error,
                    ..
                }) => {
                    let error = error.as_deref().unwrap_or("Slack rejected request");
                    let status = match error {
                        "ratelimited" => "retry",
                        "internal_error"
                        | "fatal_error"
                        | "request_timeout"
                        | "service_unavailable" => "uncertain",
                        _ => "failed",
                    };
                    (status, 60, None, Some(error.to_string()))
                }
                _ => (
                    "uncertain",
                    0,
                    None,
                    Some("invalid Slack response; verify before retry".into()),
                ),
            }
        }
        Ok(Ok(reply)) if reply.status_code >= 500 => (
            "uncertain",
            0,
            None,
            Some(format!(
                "Slack HTTP {}; verify before retry",
                reply.status_code
            )),
        ),
        Ok(Ok(reply)) => (
            "failed",
            0,
            None,
            Some(format!("Slack HTTP {}", reply.status_code)),
        ),
        _ => (
            "uncertain",
            0,
            None,
            Some("transport failure; verify Slack before retry".into()),
        ),
    };
    let status = if status == "retry" && attempts >= 8 {
        "failed"
    } else {
        status
    };
    sqlx::query("UPDATE notifications SET state=$3,available_at=now()+make_interval(secs=>$4::double precision),slack_ts=$5,last_error=$6,lease_until=NULL,updated_at=now() WHERE id=$1 AND claim_token=$2::uuid AND state='sending'")
        .bind(id).bind(claim).bind(status).bind(delay as f64).bind(ts).bind(error).execute(pool).await?;
    Ok(true)
}

fn next_poll_delay(previous: Duration, worked: bool) -> Duration {
    if worked {
        Duration::from_secs(1)
    } else {
        previous
            .saturating_mul(2)
            .clamp(Duration::from_secs(1), Duration::from_secs(30))
    }
}

pub async fn worker(state: AppState, shutdown: impl std::future::Future<Output = ()>) {
    tokio::pin!(shutdown);
    let mut pause = Duration::ZERO;
    loop {
        // 初回claim前にsignal handlerを登録する。終了要求は次のclaimより優先する。
        tokio::select! {
            biased;
            _ = &mut shutdown => break,
            _ = tokio::time::sleep(pause) => {},
        }
        // 配送中はSIGTERMでfutureを捨てず、Slack応答とDB確定を待つ。
        // DB停止も含め75秒で打ち切り、90秒のPod終了猶予内に終了する。
        let worked = match tokio::time::timeout(Duration::from_secs(75), deliver_one(&state)).await
        {
            Ok(Ok(worked)) => worked,
            Ok(Err(error)) => {
                tracing::error!(%error, "Notification worker failed");
                false
            }
            Err(_) => {
                tracing::error!("Notification delivery deadline exceeded; lease recovery required");
                false
            }
        };
        // 処理中は1秒を保ち、空キュー・DB障害時だけ最大30秒まで待機する。
        pause = next_poll_delay(pause, worked);
    }
}

#[cfg(test)]
mod polling_tests {
    use super::*;

    #[test]
    fn idle_polling_is_bounded_and_work_resets_the_delay() {
        let mut pause = Duration::ZERO;
        for seconds in [1, 2, 4, 8, 16, 30, 30] {
            pause = next_poll_delay(pause, false);
            assert_eq!(pause, Duration::from_secs(seconds));
        }
        pause = next_poll_delay(pause, true);
        assert_eq!(pause, Duration::from_secs(1));
        assert_eq!(next_poll_delay(pause, false), Duration::from_secs(2));
        assert_eq!(next_poll_delay(pause, true), Duration::from_secs(1));
        assert_eq!(
            next_poll_delay(Duration::MAX, false),
            Duration::from_secs(30)
        );
    }
}
