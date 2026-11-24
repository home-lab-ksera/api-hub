use api_hub::{config, logging, server};
use std::{net::SocketAddr, process::ExitCode};
use tracing::{error, info, info_span};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> ExitCode {
    // ログシステムの初期化
    let log_config = logging::LogConfig::default();
    logging::init_tracing(log_config);

    let main_span = info_span!(
        "application",
        service = "api-hub",
        version = env!("CARGO_PKG_VERSION")
    );
    let _enter = main_span.enter();

    info!(
        service = "api-hub",
        version = env!("CARGO_PKG_VERSION"),
        "Starting application"
    );

    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(code) => code,
    }
}

/// 失敗は呼び出し元でログに残してから、終了コードとして返す。
fn failed(_: ()) -> ExitCode {
    ExitCode::FAILURE
}

async fn run() -> Result<(), ExitCode> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() > 1
        || args
            .first()
            .is_some_and(|arg| !["--serve", "--migrate"].contains(&arg.as_str()))
    {
        eprintln!("Usage: api-hub [--serve|--migrate]");
        return Err(ExitCode::from(2));
    }
    let mode = args.first().map(String::as_str).unwrap_or("--serve");
    if std::env::var_os("PGDATABASE").is_none() && std::env::var_os("DATABASE_URL").is_none() {
        error!("PGDATABASE or DATABASE_URL is required");
        return Err(ExitCode::FAILURE);
    }
    let database = api_hub::database::connect()
        .await
        .map_err(|error| error!(%error, "Database connection failed"))
        .map_err(failed)?;
    if mode == "--migrate" {
        api_hub::database::migrate(&database)
            .await
            .map_err(|error| error!(%error, "Database migration failed"))
            .map_err(failed)?;
        database.close().await;
        return Ok(());
    }

    let settings = config::settings::Settings::new()
        .map_err(|e| error!(error = %e, config_loaded = false, "Failed to load settings"))
        .map_err(failed)?;
    info!(config_loaded = true, "Configuration loaded successfully");

    let app_state = config::state::AppState {
        settings,
        client: api_hub::http_client::new(),
        database: Some(database),
    };
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    info!(addr = %addr, port = addr.port(), "Starting HTTP server");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| {
            error!(error = %e, address = %addr, port = addr.port(), "Failed to bind to address")
        })
        .map_err(failed)?;
    info!(addr = %addr, "Server successfully bound to address");

    server::serve_integrated(listener, app_state, shutdown_signal())
        .await
        .map_err(|error| error!(%error, "Server process failed"))
        .map_err(failed)?;
    info!("Server shutdown complete");
    Ok(())
}

fn shutdown_signal() -> impl std::future::Future<Output = ()> + Send + 'static {
    // task起動前にUnix signal handlerを登録し、初回claimと登録を競合させない。
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("failed to install SIGINT handler");
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");
    async move {
        tokio::select! { _=interrupt.recv()=>{},_=terminate.recv()=>{} }
    }
}
