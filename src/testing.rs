//! テスト用の部品。本番のコードからは使わない。
//!
//! ライブラリ内のテストと `tests/` の結合テストの両方から使うため、`cfg(test)` にはしない。

use shiguredo_http11::{DecoderLimits, Request, RequestDecoder};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::net::TcpListener;

/// 1本の接続から HTTP 要求を順に読む。大きな本文も扱えるよう上限は設けない。
pub struct RequestReader {
    decoder: RequestDecoder,
}

impl Default for RequestReader {
    fn default() -> Self {
        Self {
            decoder: RequestDecoder::with_limits(DecoderLimits::unlimited()),
        }
    }
}

impl RequestReader {
    pub fn new() -> Self {
        Self::default()
    }

    /// 次の要求。要求を読み終える前に接続が閉じたら `None`。
    pub async fn next<S: AsyncRead + Unpin>(&mut self, socket: &mut S) -> Option<Request> {
        let mut buffer = [0; 8192];
        loop {
            if let Some(request) = self.decoder.decode().unwrap() {
                return Some(request);
            }
            let n = socket.read(&mut buffer).await.unwrap();
            if n == 0 {
                return None;
            }
            self.decoder.feed(&buffer[..n]).unwrap();
        }
    }
}

/// 接続から要求を1つ読む。
pub async fn read_request<S: AsyncRead + Unpin>(socket: &mut S) -> Option<Request> {
    RequestReader::new().next(socket).await
}

/// ポートを持ったまま、接続をすぐ切る上流。
///
/// 閉じたポートは別の fixture に再利用されることがあるため、接続できない宛先の代わりに使う。
/// drop すると止まる。
pub struct ClosingUpstream {
    pub endpoint: String,
    task: tokio::task::JoinHandle<()>,
}

impl ClosingUpstream {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            loop {
                if let Ok((socket, _)) = listener.accept().await {
                    drop(socket);
                }
            }
        });
        Self { endpoint, task }
    }
}

impl Drop for ClosingUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// 期待値ファイル（`tests/fixtures/<name>`）と比べる。`UPDATE_GOLDEN=1` のときは期待値を書き換える。
pub fn assert_golden(name: &str, actual: &str) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, actual).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path)
        .unwrap_or_else(|_| panic!("{} is missing; run with UPDATE_GOLDEN=1", path.display()));
    if expected != actual {
        let first = expected
            .lines()
            .zip(actual.lines())
            .position(|(e, a)| e != a)
            .unwrap_or_else(|| expected.lines().count().min(actual.lines().count()));
        panic!(
            "{name} differs at line {}:\n  expected: {}\n  actual:   {}\nrun with UPDATE_GOLDEN=1 if the change is intended",
            first + 1,
            expected.lines().nth(first).unwrap_or("<end>"),
            actual.lines().nth(first).unwrap_or("<end>"),
        );
    }
}
