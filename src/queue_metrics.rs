//! DBの業務状態を集計する。内容・保存キー・通知先はラベルへ含めない。
use crate::reply::{Reply, ReplyExt, reply};
use sqlx::{PgPool, Row};
use std::fmt::Write;
use std::time::Duration;

pub async fn render(pool: &PgPool) -> Result<String, sqlx::Error> {
    // scrapeを理由に既存pool上限を増やさず、接続待ち・SQLの両方を制限する。
    let row = tokio::time::timeout(
        Duration::from_secs(3),
        sqlx::query(include_str!("queue_metrics.sql")).fetch_one(pool),
    )
    .await
    .map_err(|_| sqlx::Error::PoolTimedOut)??;
    let mut text = String::from("# TYPE application_queue_jobs gauge\n");
    // 状態名の昇順で出す。
    for state in ["failed", "pending", "retry", "sending", "sent", "uncertain"] {
        let count: i64 = row.get(state);
        writeln!(
            text,
            "application_queue_jobs{{queue=\"notifications\",state=\"{state}\"}} {count}"
        )
        .unwrap();
    }
    writeln!(text, "# TYPE application_queue_oldest_pending_age_seconds gauge\napplication_queue_oldest_pending_age_seconds{{queue=\"notifications\"}} {}", row.get::<f64,_>("oldest").max(0.0)).unwrap();
    writeln!(text, "# TYPE application_queue_last_success_timestamp_seconds gauge\napplication_queue_last_success_timestamp_seconds{{queue=\"notifications\"}} {}", row.get::<f64,_>("last_success")).unwrap();
    Ok(text)
}

pub async fn handle(state: &crate::config::state::AppState) -> Reply {
    let result = match &state.database {
        Some(pool) => render(pool).await,
        None => Err(sqlx::Error::PoolClosed),
    };
    match result {
        Ok(body) => reply(200)
            .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            .header("Cache-Control", "no-store")
            .with_body(body.into_bytes()),
        Err(error) => {
            tracing::error!(%error, "Queue metrics unavailable");
            reply(503)
        }
    }
}
