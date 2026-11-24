use tracing_subscriber::{
    EnvFilter,
    fmt::{self, format::FmtSpan, time::SystemTime},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

/// ログ設定
#[derive(Debug, Clone)]
pub struct LogConfig {
    pub level: String,
    pub enable_target: bool,
    pub enable_thread: bool,
    pub enable_line_number: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),
            enable_target: std::env::var("LOG_TARGET").unwrap_or_default() == "true",
            enable_thread: std::env::var("LOG_THREAD").unwrap_or_default() == "true",
            enable_line_number: std::env::var("LOG_LINE").unwrap_or_default() == "true",
        }
    }
}

/// トレーシングサブスクライバーの初期化（JSONL形式固定）
pub fn init_tracing(config: LogConfig) {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.level));

    let jsonl_layer = fmt::layer()
        .json()
        .with_current_span(false) // Flatten span info into main object
        .with_span_list(false) // Don't include span list to keep each line independent
        .with_timer(SystemTime)
        .with_target(config.enable_target)
        .with_thread_ids(config.enable_thread)
        .with_thread_names(config.enable_thread)
        .with_line_number(config.enable_line_number)
        .with_file(config.enable_line_number)
        .with_span_events(FmtSpan::CLOSE) // Only log on span close to reduce noise
        .flatten_event(true); // Flatten fields for easier parsing

    tracing_subscriber::registry()
        .with(env_filter)
        .with(jsonl_layer)
        .init();
}
