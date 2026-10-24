use sqlx::{
    ConnectOptions, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
};
use std::{env, time::Duration};

pub type DbResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// 本番はPG*と既存app資格情報を使用。DATABASE_URLは隔離テストにも利用する。
pub async fn connect() -> DbResult<PgPool> {
    let options = if let Ok(url) = env::var("DATABASE_URL") {
        url.parse::<PgConnectOptions>()?
    } else {
        let mut options = PgConnectOptions::new()
            .host(&env::var("PGHOST")?)
            .database(&env::var("PGDATABASE")?)
            .username(&env::var("PGUSER").unwrap_or_else(|_| "app".into()))
            .password(&env::var("PGPASSWORD")?)
            .ssl_mode(PgSslMode::VerifyFull)
            .ssl_root_cert(env::var("PGSSLROOTCERT")?);
        if let Ok(port) = env::var("PGPORT") {
            options = options.port(port.parse()?);
        }
        options
    };
    let max_connections: u32 = env::var("PGPOOL_MAX")
        .unwrap_or_else(|_| "3".into())
        .parse()?;
    if !(1..=10).contains(&max_connections) {
        return Err("PGPOOL_MAX must be 1..10".into());
    }
    Ok(PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(5))
        .idle_timeout(Duration::from_secs(60))
        .connect_lazy_with(
            options
                .application_name("api-hub")
                .disable_statement_logging(),
        ))
}

pub async fn migrate(pool: &PgPool) -> DbResult<()> {
    sqlx::migrate!("./migrations").run(pool).await?;
    Ok(())
}
