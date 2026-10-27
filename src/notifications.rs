use crate::{config::state::AppState, database::DbResult};
use serde::Deserialize;
use serde_json::{Value, json};
use shiguredo_http11::Response;
use sqlx::{PgPool, Row};
use std::time::Duration;
mod file;
pub use file::NotificationFile;

#[derive(Deserialize)]
pub struct Notification {
    pub producer: String,
    pub event_id: String,
    pub channel: String,
    pub text: String,
    pub file: Option<NotificationFile>,
}

fn response(status: u16, value: Value) -> Response {
    Response::new(status, crate::errors::api_error::reason_phrase(status))
        .header("Content-Type", "application/json")
        .header("Cache-Control", "no-store")
        .body(value.to_string().into_bytes())
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
    let mut payload = json!({"channel": item.channel, "text": item.text});
    if let Some(file) = &item.file {
        file.validate()?;
        payload["file"] = serde_json::to_value(file)?;
    }
    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO notifications (producer,event_id,channel,payload) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING")
        .bind(&item.producer).bind(&item.event_id).bind(&item.channel).bind(&payload)
        .execute(&mut *tx).await?;
    let row = sqlx::query("SELECT id,state,payload FROM notifications WHERE producer=$1 AND event_id=$2 AND channel=$3")
        .bind(&item.producer).bind(&item.event_id).bind(&item.channel).fetch_one(&mut *tx).await?;
    if row.get::<Value, _>("payload") != payload {
        return Err("idempotency key reused with different payload".into());
    }
    let result = (row.get("id"), row.get("state"));
    tx.commit().await?;
    Ok(result)
}

pub async fn handle_post(state: &AppState, body: &[u8]) -> Response {
    let Some(pool) = &state.database else {
        return response(503, json!({"error":"notification database unavailable"}));
    };
    let item: Notification = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(_) => {
            return response(
                400,
                json!({"error":"producer, event_id, channel and text are required"}),
            );
        }
    };
    match enqueue(pool, &item).await {
        Ok((id, status)) => response(202, json!({"id":id.to_string(),"state":status})),
        Err(error) => {
            let message = error.to_string();
            if message == "idempotency key reused with different payload" {
                response(409, json!({"error":message}))
            } else if message.starts_with("invalid notification") {
                response(400, json!({"error":message}))
            } else {
                tracing::error!(error = %error, "Notification persistence failed");
                response(503, json!({"error":"notification persistence failed"}))
            }
        }
    }
}

pub async fn handle_get(state: &AppState, id: &str) -> Response {
    let Some(pool) = &state.database else {
        return response(503, json!({"error":"notification database unavailable"}));
    };
    let Ok(id) = id.parse::<i64>() else {
        return response(400, json!({"error":"invalid notification id"}));
    };
    match sqlx::query("SELECT id,producer,event_id,channel,state,attempts,slack_ts,slack_file_id,last_error FROM notifications WHERE id=$1")
        .bind(id)
        .fetch_optional(pool)
        .await
    {
        Ok(Some(row)) => response(
            200,
            json!({"id":id.to_string(),"producer":row.get::<String,_>("producer"),"event_id":row.get::<String,_>("event_id"),"channel":row.get::<String,_>("channel"),"state":row.get::<String,_>("state"),"attempts":row.get::<i32,_>("attempts"),"slack_ts":row.get::<Option<String>,_>("slack_ts"),"slack_file_id":row.get::<Option<String>,_>("slack_file_id"),"last_error":row.get::<Option<String>,_>("last_error")}),
        ),
        Ok(None) => response(404, json!({"error":"notification not found"})),
        Err(_) => response(503, json!({"error":"notification database unavailable"})),
    }
}

/// 外部送信の結果不明は自動再送しない。重複を確認してから明示的に再試行する。
pub async fn handle_retry(state: &AppState, id: &str) -> Response {
    let Some(pool) = &state.database else {
        return response(503, json!({"error":"notification database unavailable"}));
    };
    let Ok(id) = id.parse::<i64>() else {
        return response(400, json!({"error":"invalid notification id"}));
    };
    match sqlx::query("UPDATE notifications SET state='pending',attempts=0,available_at=now(),claim_token=NULL,lease_until=NULL,last_error=NULL,updated_at=now() WHERE id=$1 AND state IN ('uncertain','failed')").bind(id).execute(pool).await {
        Ok(result) if result.rows_affected() == 1 => response(202, json!({"id":id.to_string(),"state":"pending"})),
        Ok(_) => response(409, json!({"error":"only uncertain or failed notifications can be retried"})),
        Err(_) => response(503, json!({"error":"notification database unavailable"})),
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
    let row = sqlx::query("WITH candidate AS (SELECT id FROM notifications WHERE state IN ('pending','retry') AND available_at<=now() ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE notifications n SET state='sending',attempts=attempts+1,claim_token=gen_random_uuid(),lease_until=now()+interval '90 seconds',updated_at=now() FROM candidate WHERE n.id=candidate.id RETURNING n.id,n.payload,n.claim_token::text,n.attempts")
        .fetch_optional(pool).await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let id: i64 = row.get("id");
    let claim: String = row.get("claim_token");
    let attempts: i32 = row.get("attempts");
    let payload: Value = row.get("payload");
    let result = if payload.get("file").is_some() {
        let mut share_started = false;
        match tokio::time::timeout(
            Duration::from_secs(60),
            file::deliver(state, pool, id, &claim, &payload, &mut share_started),
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
            crate::http_client::request(
                &state.client,
                "POST".into(),
                format!("{}/chat.postMessage", state.settings.slack_api_base_url),
                vec![
                    (
                        "Authorization".into(),
                        format!("Bearer {}", state.settings.slack_bot_token),
                    ),
                    ("Content-Type".into(), "application/json".into()),
                ],
                payload.to_string().into_bytes(),
                false,
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
            let delay = reply
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("retry-after"))
                .and_then(|(_, value)| value.parse::<i64>().ok())
                .unwrap_or(60)
                .clamp(1, 86400);
            ("retry", delay, None, Some("slack rate limited".to_string()))
        }
        Ok(Ok(reply)) if (200..300).contains(&reply.status_code) => {
            match serde_json::from_slice::<Value>(&reply.body) {
                Ok(value) if value.get("ok").and_then(Value::as_bool) == Some(true) => (
                    "sent",
                    0,
                    value.get("ts").and_then(Value::as_str).map(str::to_owned),
                    None,
                ),
                Ok(value) if value.get("ok").and_then(Value::as_bool) == Some(false) => {
                    let error = value
                        .get("error")
                        .and_then(Value::as_str)
                        .unwrap_or("Slack rejected request");
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
        let worked = match tokio::time::timeout(Duration::from_secs(75), deliver_one(&state)).await {
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
        assert_eq!(next_poll_delay(Duration::MAX, false), Duration::from_secs(30));
    }
}
