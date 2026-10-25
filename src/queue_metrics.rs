//! DBの業務状態を集計する。内容・保存キー・通知先はラベルへ含めない。
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
    let counts: serde_json::Value = row.get("counts");
    let mut text = String::from("# TYPE application_queue_jobs gauge\n");
    for (state, count) in counts
        .as_object()
        .expect("SQL returns a fixed state object")
    {
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

pub async fn handle(state: &crate::config::state::AppState) -> shiguredo_http11::Response {
    let result = match &state.database {
        Some(pool) => render(pool).await,
        None => Err(sqlx::Error::PoolClosed),
    };
    match result {
        Ok(body) => shiguredo_http11::Response::new(200, "OK")
            .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            .header("Cache-Control", "no-store")
            .body(body.into_bytes()),
        Err(error) => {
            tracing::error!(%error, "Queue metrics unavailable");
            shiguredo_http11::Response::new(503, "Service Unavailable")
        }
    }
}
