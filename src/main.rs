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
    if !["", "--migrate", "--notification-worker"].contains(&mode.as_str()) {
        eprintln!("Usage: api-hub [--migrate|--notification-worker]");
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
        api_hub::notifications::worker(app_state, shutdown_signal()).await;
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

    if let Err(error) = server::serve(listener, app_state.clone(), shutdown_signal()).await {
        error!(%error,"HTTP server failed");
    }
    if let Some(pool) = app_state.database {
        pool.close().await;
    }
    info!("Server shutdown complete");
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!(signal = "SIGINT", "Received shutdown signal");
        },
        _ = terminate => {
            info!(signal = "SIGTERM", "Received shutdown signal");
        },
    }
}
