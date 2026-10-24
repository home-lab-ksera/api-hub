use crate::config::settings::Settings;
use crate::http_client::HttpClient;

#[derive(Clone)]
pub struct AppState {
    pub settings: Settings,
    pub client: HttpClient,
    pub database: Option<sqlx::PgPool>,
}
