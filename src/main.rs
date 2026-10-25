use api_hub::{config, logging, server};
use std::net::SocketAddr;
use tracing::{error, info, info_span};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
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

    let mode = std::env::args().nth(1).unwrap_or_default();
    if !["", "--migrate", "--notification-worker", "--serve"].contains(&mode.as_str()) {
        eprintln!("Usage: api-hub [--serve|--migrate|--notification-worker]");
        std::process::exit(2);
    }
    let database = if mode != ""
        || std::env::var_os("PGDATABASE").is_some()
        || std::env::var_os("DATABASE_URL").is_some()
    {
        match api_hub::database::connect().await {
            Ok(pool) => Some(pool),
            Err(error) => {
                error!(%error, "Database connection failed");
                std::process::exit(1);
            }
        }
    } else {
        None
    };
    if mode == "--migrate" {
        if let Err(error) = api_hub::database::migrate(database.as_ref().unwrap()).await {
            error!(%error, "Database migration failed");
            std::process::exit(1);
        }
        return;
    }

    let settings = match config::settings::Settings::new() {
        Ok(s) => {
            info!(config_loaded = true, "Configuration loaded successfully");
            s
        }
        Err(e) => {
            error!(
                error = %e,
                config_loaded = false,
                "Failed to load settings"
            );
            std::process::exit(1);
        }
    };

    let client = api_hub::http_client::new();

    let app_state = config::state::AppState {
        settings,
        client,
        database,
    };
    if mode == "--notification-worker" {
        let pool = app_state.database.clone().unwrap();
        api_hub::notifications::worker(app_state, shutdown_signal()).await;
        pool.close().await;
        return;
    }

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));

    info!(
        addr = %addr,
        port = addr.port(),
        "Starting HTTP server"
    );

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(
                error = %e,
                address = %addr,
                port = addr.port(),
                "Failed to bind to address"
            );
            std::process::exit(1);
        }
    };

    info!(
        addr = %addr,
        "Server successfully bound to address"
    );

    let result = if mode == "--serve" {
        server::serve_integrated(listener, app_state, shutdown_signal()).await
    } else {
        // 旧worker停止までの切替互換。通常起動への統合はP6で行う。
        let result = server::serve(listener, app_state.clone(), shutdown_signal()).await;
        if let Some(pool) = app_state.database {
            pool.close().await;
        }
        result
    };
    if let Err(error) = result {
        error!(%error,"Server process failed");
        std::process::exit(1);
    }
    info!("Server shutdown complete");
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
