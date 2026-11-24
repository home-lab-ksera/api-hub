//! handler が返す応答。本文を読み込み済みの `http::Response`。

use axum::body::Bytes;
use axum::http::{HeaderName, HeaderValue, StatusCode};

pub type Reply = axum::http::Response<Bytes>;

/// 本文が空の応答。範囲外の状態コードは 500 にする。
pub fn reply(status: u16) -> Reply {
    let mut reply = Reply::new(Bytes::new());
    *reply.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    reply
}

/// 応答を組み立てるための補助。
pub trait ReplyExt: Sized {
    /// ヘッダーを足す。名前や値として不正なものは送らない。
    fn header(self, name: &str, value: &str) -> Self;
    /// 本文を置き換える。
    fn with_body(self, body: impl Into<Bytes>) -> Self;
    fn status_code(&self) -> u16;
    /// UTF-8 として読める最初の値。
    fn header_str(&self, name: &str) -> Option<&str>;
}

impl ReplyExt for Reply {
    fn header(mut self, name: &str, value: &str) -> Self {
        append_header(&mut self, name, value);
        self
    }

    fn with_body(self, body: impl Into<Bytes>) -> Self {
        self.map(|_| body.into())
    }

    fn status_code(&self) -> u16 {
        self.status().as_u16()
    }

    fn header_str(&self, name: &str) -> Option<&str> {
        self.headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
    }
}

/// ヘッダーを足す。名前や値として不正なものは送らない。
pub fn append_header<B>(response: &mut axum::http::Response<B>, name: &str, value: &str) {
    if let (Ok(name), Ok(value)) = (name.parse::<HeaderName>(), HeaderValue::from_str(value)) {
        response.headers_mut().append(name, value);
    }
}

/// 要求ヘッダーのうち、UTF-8 として読める最初の値。
pub fn request_header<'a>(headers: &'a axum::http::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}
