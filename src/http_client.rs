//! 共有reqwest Clientの設定。HTTP接続・TLS・pool・本文streamはライブラリへ委譲する。
pub use reqwest::Client as HttpClient;
use std::time::Duration;

pub fn new() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .no_proxy()
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(60))
        .pool_idle_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(8)
        .build()
        .expect("valid HTTP client configuration")
}

/// SDKで署名したmethod/target/headersを変更せず、reqwestの要求を構築する。
pub fn request(
    client: &reqwest::Client,
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    streaming: bool,
) -> reqwest::RequestBuilder {
    let method: reqwest::Method = method
        .parse()
        .expect("SDK/application selects a valid method");
    let unchanged = shiguredo_http11::uri::Uri::parse(&url)
        .ok()
        .zip(reqwest::Url::parse(&url).ok())
        .is_some_and(|(original, parsed)| {
            original.path() == parsed.path() && original.query() == parsed.query()
        });
    // URL正規化で別objectを指す要求は送信しない。
    let mut builder = client
        .request(
            method.clone(),
            if unchanged {
                url.as_str()
            } else {
                "invalid signed target"
            },
        )
        .timeout(Duration::from_secs(
            if streaming || method == reqwest::Method::PUT {
                3600
            } else {
                60
            },
        ));
    for (name, value) in headers {
        builder = builder.header(name, value);
    }
    builder.body(body)
}
pub fn headers(response: &reqwest::Response) -> Vec<(String, String)> {
    response
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.to_string(), value.to_owned()))
        })
        .collect()
}
