use crate::config::state::AppState;
use crate::reply::{Reply, ReplyExt, reply};
use nojson::{DisplayJson, JsonFormatter, JsonValueKind, RawJson, RawJsonValue};
use sqlx::{PgPool, Row};
use std::collections::BTreeMap;
mod file;
mod worker;
pub use file::NotificationFile;
pub use worker::{deliver_one, worker};

/// 通知を登録できなかった理由。
#[derive(Debug)]
pub enum EnqueueError {
    /// 入力が不正。値は理由。
    Invalid(&'static str),
    /// 同じ producer・event_id・channel に、別の内容が登録済み。
    Conflict,
    Database(sqlx::Error),
}

impl std::fmt::Display for EnqueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(reason) => f.write_str(reason),
            Self::Conflict => f.write_str("idempotency key reused with different payload"),
            Self::Database(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for EnqueueError {}

impl From<sqlx::Error> for EnqueueError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

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

fn response(status: u16, body: impl DisplayJson) -> Reply {
    reply(status)
        .header("Content-Type", "application/json")
        .header("Cache-Control", "no-store")
        .with_body(nojson::json(|f| f.value(&body)).to_string().into_bytes())
}

/// 理由を `detail` に入れた problem details。HTTP 層の汎用の problem details に置き換えられない。
fn error(status: u16, message: &str) -> Reply {
    crate::errors::api_error::problem_details_response(status, message)
}

fn accepted(id: i64, state: &str) -> Reply {
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

pub async fn enqueue(pool: &PgPool, item: &Notification) -> Result<(i64, String), EnqueueError> {
    for field in [&item.producer, &item.event_id, &item.channel] {
        if field.trim().is_empty() || field.len() > 256 {
            return Err(EnqueueError::Invalid("invalid notification identity"));
        }
    }
    if item.text.trim().is_empty() || item.text.len() > 16000 {
        return Err(EnqueueError::Invalid("invalid notification text"));
    }
    if let Some(file) = &item.file {
        file.validate().map_err(EnqueueError::Invalid)?;
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
        return Err(EnqueueError::Conflict);
    }
    let result = (row.get("id"), row.get("state"));
    tx.commit().await?;
    Ok(result)
}

pub async fn handle_post(state: &AppState, body: &[u8]) -> Reply {
    let Some(pool) = &state.database else {
        return error(503, "notification database unavailable");
    };
    let Some(item) = Notification::from_json(body) else {
        return error(400, "producer, event_id, channel and text are required");
    };
    match enqueue(pool, &item).await {
        Ok((id, status)) => accepted(id, &status),
        Err(EnqueueError::Conflict) => error(409, &EnqueueError::Conflict.to_string()),
        Err(EnqueueError::Invalid(reason)) => error(400, reason),
        Err(cause @ EnqueueError::Database(_)) => {
            tracing::error!(error = %cause, "Notification persistence failed");
            error(503, "notification persistence failed")
        }
    }
}

pub async fn handle_get(state: &AppState, id: &str) -> Reply {
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
pub async fn handle_retry(state: &AppState, id: &str) -> Reply {
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
