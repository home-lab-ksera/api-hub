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

    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() > 1
        || args
            .first()
            .is_some_and(|arg| !["--serve", "--migrate"].contains(&arg.as_str()))
    {
        eprintln!("Usage: api-hub [--serve|--migrate]");
        std::process::exit(2);
    }
    let mode = args.first().map(String::as_str).unwrap_or("--serve");
    if std::env::var_os("PGDATABASE").is_none() && std::env::var_os("DATABASE_URL").is_none() {
        error!("PGDATABASE or DATABASE_URL is required");
        std::process::exit(1);
    }
    let database = match api_hub::database::connect().await {
        Ok(pool) => pool,
        Err(error) => {
            error!(%error,"Database connection failed");
            std::process::exit(1);
        }
    };
    if mode == "--migrate" {
        if let Err(error) = api_hub::database::migrate(&database).await {
            error!(%error, "Database migration failed");
            std::process::exit(1);
        }
        database.close().await;
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
        database: Some(database),
    };
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

    let result = server::serve_integrated(listener, app_state, shutdown_signal()).await;
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
