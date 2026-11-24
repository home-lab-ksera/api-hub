//! 通知の配送。
//!
//! 1回の配送は「lease 切れの回収 → claim → Slack への送信 → 結果の分類 → DB への確定」の順に進む。
//! 結果の分類（[`classify`]）は DB に触れない純粋な関数で、状態の遷移はここだけで決まる。
//! 状態の意味と遷移は `docs/notification-state.md` を参照。

use super::{Canonical, file};
use crate::{
    config::state::AppState,
    database::DbResult,
    service::slack_service::{SlackClient, SlackReplyBody, retry_after_secs},
};
use nojson::RawJson;
use sqlx::{PgPool, Row};
use std::time::Duration;

/// 送信を試みる回数の上限。上限に達した再試行は失敗として確定する。
pub(super) const MAX_ATTEMPTS: i32 = 8;

/// Slack への1回の送信を待つ時間。
const SEND_TIMEOUT: Duration = Duration::from_secs(60);

/// 通知の状態。DB の `state` 列の値。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum State {
    /// 次の claim を待つ。
    Retry,
    /// Slack が受け付けた。
    Sent,
    /// Slack が受け取ったか分からない。重複を避けるため自動では再送しない。
    Uncertain,
    /// 送れないことが確定した。
    Failed,
}

impl State {
    fn as_str(self) -> &'static str {
        match self {
            Self::Retry => "retry",
            Self::Sent => "sent",
            Self::Uncertain => "uncertain",
            Self::Failed => "failed",
        }
    }
}

/// 1回の配送の結果。このまま DB へ確定する。
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Outcome {
    pub state: State,
    /// `retry` のとき、次の claim までの秒数。
    pub delay_secs: i64,
    pub slack_ts: Option<String>,
    pub error: Option<String>,
}

impl Outcome {
    fn sent(slack_ts: Option<String>) -> Self {
        Self {
            state: State::Sent,
            delay_secs: 0,
            slack_ts,
            error: None,
        }
    }

    pub(super) fn with_error(state: State, delay_secs: i64, error: impl Into<String>) -> Self {
        Self {
            state,
            delay_secs,
            slack_ts: None,
            error: Some(error.into()),
        }
    }

    /// 送信回数が上限に達していれば、再試行を失敗に変える。
    fn limit_retries(mut self, attempts: i32) -> Self {
        if self.state == State::Retry && attempts >= MAX_ATTEMPTS {
            self.state = State::Failed;
        }
        self
    }
}

/// Slack の応答。本文は読み切ってから分類する。
pub(super) struct SlackReply {
    pub status_code: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl SlackReply {
    async fn read(response: reqwest::Response) -> Result<Self, reqwest::Error> {
        let status_code = response.status().as_u16();
        let headers = crate::http_client::headers(&response);
        let body = response.bytes().await?.to_vec();
        Ok(Self {
            status_code,
            headers,
            body,
        })
    }
}

/// Slack の応答を配送結果に分類する。`None` は送信または応答の受信に失敗したこと。
///
/// Slack が受け取った可能性がある失敗（5xx・通信の失敗・読めない応答）は `uncertain` にする。
pub(super) fn classify(reply: Option<&SlackReply>) -> Outcome {
    use State::*;
    let Some(reply) = reply else {
        return Outcome::with_error(Uncertain, 0, "transport failure; verify Slack before retry");
    };
    match reply.status_code {
        429 => Outcome::with_error(
            Retry,
            retry_after_secs(crate::http_client::find_header(
                &reply.headers,
                "retry-after",
            )),
            "slack rate limited",
        ),
        200..=299 => match SlackReplyBody::parse(&reply.body) {
            Some(SlackReplyBody {
                ok: Some(true), ts, ..
            }) => Outcome::sent(ts),
            Some(SlackReplyBody {
                ok: Some(false),
                error,
                ..
            }) => {
                let error = error.as_deref().unwrap_or("Slack rejected request");
                let state = match error {
                    "ratelimited" => Retry,
                    "internal_error"
                    | "fatal_error"
                    | "request_timeout"
                    | "service_unavailable" => Uncertain,
                    _ => Failed,
                };
                Outcome::with_error(state, 60, error)
            }
            _ => Outcome::with_error(Uncertain, 0, "invalid Slack response; verify before retry"),
        },
        status @ 500.. => Outcome::with_error(
            Uncertain,
            0,
            format!("Slack HTTP {status}; verify before retry"),
        ),
        status => Outcome::with_error(Failed, 0, format!("Slack HTTP {status}")),
    }
}

/// claim した通知。
struct Claim {
    id: i64,
    token: String,
    attempts: i32,
    payload: String,
}

/// Slack が受け取った直後に worker が落ちた可能性があるため、lease 切れは結果不明にする。
async fn expire_leases(pool: &PgPool) -> DbResult<()> {
    sqlx::query("UPDATE notifications SET state='uncertain',last_error='worker lease expired; verify Slack before retry',updated_at=now() WHERE state='sending' AND lease_until < now()")
        .execute(pool).await?;
    Ok(())
}

/// 送信できる通知を1件だけ `sending` にして取り出す。
async fn claim(pool: &PgPool) -> DbResult<Option<Claim>> {
    let row = sqlx::query("WITH candidate AS (SELECT id FROM notifications WHERE state IN ('pending','retry') AND available_at<=now() ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE notifications n SET state='sending',attempts=attempts+1,claim_token=gen_random_uuid(),lease_until=now()+interval '90 seconds',updated_at=now() FROM candidate WHERE n.id=candidate.id RETURNING n.id,n.payload::text AS payload,n.claim_token::text,n.attempts")
        .fetch_optional(pool).await?;
    Ok(row.map(|row| Claim {
        id: row.get("id"),
        token: row.get("claim_token"),
        attempts: row.get("attempts"),
        payload: row.get("payload"),
    }))
}

/// claim した通知を送り、結果を返す。
async fn send(state: &AppState, pool: &PgPool, claim: &Claim) -> DbResult<Outcome> {
    let stored = RawJson::parse(&claim.payload)?;
    let has_file = stored
        .value()
        .to_member("file")
        .ok()
        .and_then(|member| member.optional())
        .is_some();
    let response = if has_file {
        let mut share_started = false;
        let delivery = file::deliver(
            state,
            pool,
            claim.id,
            &claim.token,
            stored.value(),
            &mut share_started,
        );
        match tokio::time::timeout(SEND_TIMEOUT, delivery).await {
            Ok(Ok(response)) => Some(response),
            Ok(Err(error)) => return Ok(error.into_outcome()),
            // チャンネルへの共有を始める前なら、Slack には何も届いていない。
            Err(_) if !share_started => {
                return Ok(Outcome::with_error(
                    State::Retry,
                    60,
                    "file preparation timed out",
                ));
            }
            Err(_) => None,
        }
    } else {
        let body = nojson::json(|f| f.value(Canonical(stored.value())))
            .to_string()
            .into_bytes();
        let request = SlackClient::new(&state.client, &state.settings)
            .post_json("chat.postMessage", "application/json", body)
            .send();
        tokio::time::timeout(SEND_TIMEOUT, request)
            .await
            .ok()
            .and_then(Result::ok)
    };
    let reply = match response {
        Some(response) => SlackReply::read(response).await.ok(),
        None => None,
    };
    Ok(classify(reply.as_ref()))
}

/// 結果を DB へ確定する。lease を失っていれば何も変えない。
///
/// 送信中の通知の `slack_ts` は常に NULL（`sent` の通知は再び claim されない）なので、
/// 送信できなかった結果でも `slack_ts` をそのまま書いてよい。
async fn finish(pool: &PgPool, claim: &Claim, outcome: Outcome) -> DbResult<()> {
    sqlx::query("UPDATE notifications SET state=$3,available_at=now()+make_interval(secs=>$4::double precision),slack_ts=$5,last_error=$6,lease_until=NULL,updated_at=now() WHERE id=$1 AND claim_token=$2::uuid AND state='sending'")
        .bind(claim.id)
        .bind(&claim.token)
        .bind(outcome.state.as_str())
        .bind(outcome.delay_secs as f64)
        .bind(outcome.slack_ts)
        .bind(outcome.error)
        .execute(pool)
        .await?;
    Ok(())
}

/// 通知を1件配送する。配送する通知がなければ `false`。
pub async fn deliver_one(state: &AppState) -> DbResult<bool> {
    let pool = state.database.as_ref().ok_or("database required")?;
    expire_leases(pool).await?;
    let Some(claim) = claim(pool).await? else {
        return Ok(false);
    };
    let outcome = send(state, pool, &claim)
        .await?
        .limit_retries(claim.attempts);
    finish(pool, &claim, outcome).await?;
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
mod tests {
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

    fn reply(status_code: u16, headers: &[(&str, &str)], body: &str) -> SlackReply {
        SlackReply {
            status_code,
            headers: headers
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
            body: body.as_bytes().to_vec(),
        }
    }

    #[test]
    fn slack_replies_are_classified() {
        use State::*;
        // 応答・状態・待ち時間・slack_ts・last_error。
        type Case<'a> = (SlackReply, State, i64, Option<&'a str>, Option<&'a str>);
        let cases: &[Case] = &[
            (
                reply(200, &[], r#"{"ok":true,"ts":"1.2"}"#),
                Sent,
                0,
                Some("1.2"),
                None,
            ),
            (reply(201, &[], r#"{"ok":true}"#), Sent, 0, None, None),
            (
                reply(200, &[], r#"{"ok":false,"error":"ratelimited"}"#),
                Retry,
                60,
                None,
                Some("ratelimited"),
            ),
            (
                reply(200, &[], r#"{"ok":false,"error":"internal_error"}"#),
                Uncertain,
                60,
                None,
                Some("internal_error"),
            ),
            (
                reply(200, &[], r#"{"ok":false,"error":"fatal_error"}"#),
                Uncertain,
                60,
                None,
                Some("fatal_error"),
            ),
            (
                reply(200, &[], r#"{"ok":false,"error":"request_timeout"}"#),
                Uncertain,
                60,
                None,
                Some("request_timeout"),
            ),
            (
                reply(200, &[], r#"{"ok":false,"error":"service_unavailable"}"#),
                Uncertain,
                60,
                None,
                Some("service_unavailable"),
            ),
            (
                reply(200, &[], r#"{"ok":false,"error":"channel_not_found"}"#),
                Failed,
                60,
                None,
                Some("channel_not_found"),
            ),
            (
                reply(200, &[], r#"{"ok":false}"#),
                Failed,
                60,
                None,
                Some("Slack rejected request"),
            ),
            (
                reply(200, &[], r#"{"ts":"1.2"}"#),
                Uncertain,
                0,
                None,
                Some("invalid Slack response; verify before retry"),
            ),
            (
                reply(200, &[], "nope"),
                Uncertain,
                0,
                None,
                Some("invalid Slack response; verify before retry"),
            ),
            (
                reply(429, &[("Retry-After", "5")], ""),
                Retry,
                5,
                None,
                Some("slack rate limited"),
            ),
            (
                reply(429, &[], ""),
                Retry,
                60,
                None,
                Some("slack rate limited"),
            ),
            (
                reply(500, &[], r#"{"ok":true}"#),
                Uncertain,
                0,
                None,
                Some("Slack HTTP 500; verify before retry"),
            ),
            (
                reply(503, &[], ""),
                Uncertain,
                0,
                None,
                Some("Slack HTTP 503; verify before retry"),
            ),
            (reply(404, &[], ""), Failed, 0, None, Some("Slack HTTP 404")),
            (reply(302, &[], ""), Failed, 0, None, Some("Slack HTTP 302")),
        ];
        for (reply, state, delay, ts, error) in cases {
            let outcome = classify(Some(reply));
            assert_eq!(
                outcome,
                Outcome {
                    state: *state,
                    delay_secs: *delay,
                    slack_ts: ts.map(str::to_owned),
                    error: error.map(str::to_owned),
                },
                "{} {}",
                reply.status_code,
                String::from_utf8_lossy(&reply.body)
            );
        }
        assert_eq!(
            classify(None),
            Outcome::with_error(Uncertain, 0, "transport failure; verify Slack before retry")
        );
    }

    #[test]
    fn retries_become_failures_at_the_attempt_limit() {
        let retry = || Outcome::with_error(State::Retry, 60, "x");
        assert_eq!(retry().limit_retries(MAX_ATTEMPTS - 1).state, State::Retry);
        assert_eq!(retry().limit_retries(MAX_ATTEMPTS).state, State::Failed);
        let uncertain = Outcome::with_error(State::Uncertain, 0, "x");
        assert_eq!(
            uncertain.limit_retries(MAX_ATTEMPTS).state,
            State::Uncertain
        );
    }
}
