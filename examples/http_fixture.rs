//! メモリ試験専用のHTTP fixture。配布image・運用コマンドには含めない。
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> std::io::Result<()> {
    let state = api_hub::config::state::AppState {
        settings: api_hub::config::settings::Settings::new().expect("fixture settings"),
        client: api_hub::http_client::new(),
        database: None,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    api_hub::server::serve(listener, state, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await
}
