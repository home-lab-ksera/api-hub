use std::env;

#[derive(Debug, Clone)]
pub struct Settings {
    pub slack_bot_token: String,
    pub slack_api_base_url: String,
    pub s3_access_key_id: String,
    pub s3_secret_access_key: String,
    pub s3_region: String,
    pub s3_endpoint: Option<String>,
    pub s3_browser_endpoint: Option<String>,
    pub s3_use_path_style: bool,
    pub s3_session_token: Option<String>,
}
#[derive(Debug)]
pub enum SettingError {
    MissingEnvVar(String),
}

impl std::fmt::Display for SettingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingEnvVar(name) => write!(f, "Missing environment variable {name}"),
        }
    }
}

impl std::error::Error for SettingError {}

impl Settings {
    pub fn new() -> Result<Self, SettingError> {
        let slack_bot_token = env::var("SLACK_BOT_TOKEN")
            .map_err(|_| SettingError::MissingEnvVar("SLACK_BOT_TOKEN".into()))?;
        let slack_api_base_url = env::var("SLACK_API_BASE_URL")
            .unwrap_or_else(|_| "https://slack.com/api".to_string())
            .trim_end_matches('/')
            .to_string();

        let s3_access_key_id = env::var("RUSTFS_S3_ACCESS_KEY_ID")
            .map_err(|_| SettingError::MissingEnvVar("RUSTFS_S3_ACCESS_KEY_ID".into()))?;
        let s3_secret_access_key = env::var("RUSTFS_S3_SECRET_ACCESS_KEY")
            .map_err(|_| SettingError::MissingEnvVar("RUSTFS_S3_SECRET_ACCESS_KEY".into()))?;
        let s3_region = env::var("RUSTFS_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let s3_endpoint = env::var("RUSTFS_S3_ENDPOINT")
            .ok()
            .map(|v| v.trim_end_matches('/').to_string())
            .filter(|v| !v.is_empty());
        let s3_use_path_style = parse_bool_env("RUSTFS_S3_USE_PATH_STYLE", true);
        let s3_session_token = env::var("RUSTFS_S3_SESSION_TOKEN")
            .ok()
            .filter(|v| !v.is_empty());

        Ok(Self {
            slack_bot_token,
            slack_api_base_url,
            s3_access_key_id,
            s3_secret_access_key,
            s3_region,
            s3_endpoint,
            s3_browser_endpoint: env::var("RUSTFS_S3_BROWSER_ENDPOINT")
                .ok()
                .filter(|value| !value.is_empty()),
            s3_use_path_style,
            s3_session_token,
        })
    }
}

fn parse_bool_env(name: &str, default_value: bool) -> bool {
    match env::var(name) {
        Ok(v) => matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"),
        Err(_) => default_value,
    }
}
