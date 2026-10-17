use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use shiguredo_http11::{
    BodyKind, BodyProgress, DecoderLimits, HttpHead, Request, ResponseDecoder, uri::Uri,
};
use std::{
    cmp::min,
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::TlsConnector;
use webpki_roots::TLS_SERVER_ROOTS;

type Header = (String, String);

pub struct HttpRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<Header>,
    pub body: Vec<u8>,
}

pub struct HttpResponse {
    pub status_code: u16,
    pub headers: Vec<Header>,
    pub body: Vec<u8>,
}

pub struct HttpResponseStream {
    pub status_code: u16,
    pub headers: Vec<Header>,
    stream: Option<Box<dyn AsyncReadWrite>>,
    pool: Arc<Mutex<ConnectionPool>>,
    origin: Origin,
    reusable: bool,
    decoder: ResponseDecoder,
    finished: bool,
}

impl HttpResponseStream {
    fn finish(&mut self) {
        self.finished = true;
        if let Some(stream) = self.stream.take()
            && self.reusable
            && self.decoder.remaining().is_empty()
        {
            self.pool.lock().unwrap().put(self.origin.clone(), stream);
        }
    }

    pub async fn read_chunk(&mut self, output: &mut [u8]) -> Result<usize, HttpClientError> {
        let result = self.read_chunk_inner(output).await;
        if result.is_err() {
            self.reusable = false;
            self.finished = true;
            self.stream.take();
        }
        result
    }

    async fn read_chunk_inner(&mut self, output: &mut [u8]) -> Result<usize, HttpClientError> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.finished {
            return Ok(0);
        }

        let mut read_buffer = [0_u8; 8192];

        loop {
            if let Some(data) = self.decoder.peek_body()
                && !data.is_empty()
            {
                let count = min(data.len(), output.len());
                output[..count].copy_from_slice(&data[..count]);
                let progress = self
                    .decoder
                    .consume_body(count)
                    .map_err(|e| HttpClientError::Decode(e.to_string()))?;
                if matches!(progress, BodyProgress::Complete { .. }) {
                    self.finish();
                }
                return Ok(count);
            }

            let buffered = self.decoder.remaining().len();
            match self
                .decoder
                .progress()
                .map_err(|e| HttpClientError::Decode(e.to_string()))?
            {
                BodyProgress::Complete { .. } => {
                    self.finish();
                    return Ok(0);
                }
                BodyProgress::Continue => {}
            }

            // progress() can consume chunk framing and expose already buffered
            // data. Drain it before waiting for another network packet.
            if self.decoder.remaining().len() < buffered
                || self
                    .decoder
                    .peek_body()
                    .is_some_and(|data| !data.is_empty())
            {
                continue;
            }

            let n = self
                .stream
                .as_mut()
                .expect("unfinished response has a connection")
                .read(&mut read_buffer)
                .await
                .map_err(|e| HttpClientError::Io(e.to_string()))?;

            if n == 0 {
                self.reusable = false;
                self.decoder.mark_eof();
                if matches!(
                    self.decoder
                        .progress()
                        .map_err(|e| HttpClientError::Decode(e.to_string()))?,
                    BodyProgress::Complete { .. }
                ) {
                    self.finish();
                    return Ok(0);
                }
                return Err(HttpClientError::Decode(
                    "Connection closed before a complete response was received".into(),
                ));
            }

            self.decoder
                .feed(&read_buffer[..n])
                .map_err(|e| HttpClientError::Decode(e.to_string()))?;
        }
    }
}

#[derive(Debug)]
pub enum HttpClientError {
    InvalidUrl(String),
    UnsupportedScheme(String),
    MissingHost,
    Io(String),
    Tls(String),
    Decode(String),
}

impl fmt::Display for HttpClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUrl(e) => write!(f, "Invalid URL: {e}"),
            Self::UnsupportedScheme(s) => write!(f, "Unsupported URL scheme: {s}"),
            Self::MissingHost => write!(f, "Missing URL host"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Tls(e) => write!(f, "TLS error: {e}"),
            Self::Decode(e) => write!(f, "HTTP decode error: {e}"),
        }
    }
}

impl std::error::Error for HttpClientError {}

trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

// Idle sockets are bounded globally and per origin. In-flight requests never hold
// the lock, and a partially consumed response is simply closed when dropped.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_IDLE_CONNECTIONS: usize = 64;
const MAX_IDLE_PER_ORIGIN: usize = 8;

#[derive(Clone, PartialEq, Eq)]
struct Origin {
    scheme: String,
    host: String,
    port: u16,
}

struct IdleConnection {
    origin: Origin,
    stream: Box<dyn AsyncReadWrite>,
    since: Instant,
}

#[derive(Default)]
struct ConnectionPool {
    idle: Vec<IdleConnection>,
}

impl ConnectionPool {
    fn prune(&mut self) {
        self.idle
            .retain(|connection| connection.since.elapsed() < IDLE_TIMEOUT);
    }

    fn take(&mut self, origin: &Origin) -> Option<Box<dyn AsyncReadWrite>> {
        self.prune();
        let index = self
            .idle
            .iter()
            .rposition(|connection| &connection.origin == origin)?;
        Some(self.idle.swap_remove(index).stream)
    }

    fn put(&mut self, origin: Origin, stream: Box<dyn AsyncReadWrite>) {
        self.prune();
        if self.idle.len() < MAX_IDLE_CONNECTIONS
            && self
                .idle
                .iter()
                .filter(|connection| connection.origin == origin)
                .count()
                < MAX_IDLE_PER_ORIGIN
        {
            self.idle.push(IdleConnection {
                origin,
                stream,
                since: Instant::now(),
            });
        }
    }
}

#[derive(Clone)]
pub struct HttpClient {
    tls_connector: TlsConnector,
    pool: Arc<Mutex<ConnectionPool>>,
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpClient {
    pub fn new() -> Self {
        let root_store = RootCertStore::from_iter(TLS_SERVER_ROOTS.iter().cloned());
        let tls_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        Self {
            tls_connector: TlsConnector::from(Arc::new(tls_config)),
            pool: Arc::new(Mutex::new(ConnectionPool::default())),
        }
    }

    pub async fn send(&self, request: HttpRequest) -> Result<HttpResponse, HttpClientError> {
        let mut response = self
            .send_with_limits(request, DecoderLimits::default())
            .await?;
        let mut body = Vec::new();
        let mut buffer = [0; 8192];
        loop {
            let count = response.read_chunk(&mut buffer).await?;
            if count == 0 {
                break;
            }
            body.extend_from_slice(&buffer[..count]);
        }
        Ok(HttpResponse {
            status_code: response.status_code,
            headers: response.headers,
            body,
        })
    }

    pub async fn send_streaming(
        &self,
        request: HttpRequest,
    ) -> Result<HttpResponseStream, HttpClientError> {
        self.send_with_limits(request, DecoderLimits::unlimited())
            .await
    }

    async fn connect(&self, origin: &Origin) -> Result<Box<dyn AsyncReadWrite>, HttpClientError> {
        let tcp = TcpStream::connect((origin.host.as_str(), origin.port))
            .await
            .map_err(|e| HttpClientError::Io(e.to_string()))?;
        tcp.set_nodelay(true)
            .map_err(|e| HttpClientError::Io(e.to_string()))?;
        if origin.scheme == "https" {
            let server_name = ServerName::try_from(origin.host.clone())
                .map_err(|e| HttpClientError::Tls(e.to_string()))?;
            let tls = self
                .tls_connector
                .connect(server_name, tcp)
                .await
                .map_err(|e| HttpClientError::Tls(e.to_string()))?;
            Ok(Box::new(tls))
        } else {
            Ok(Box::new(tcp))
        }
    }

    async fn send_with_limits(
        &self,
        request: HttpRequest,
        limits: DecoderLimits,
    ) -> Result<HttpResponseStream, HttpClientError> {
        let uri =
            Uri::parse(&request.url).map_err(|e| HttpClientError::InvalidUrl(e.to_string()))?;
        let scheme = uri
            .scheme()
            .ok_or_else(|| HttpClientError::InvalidUrl(request.url.clone()))?
            .to_ascii_lowercase();
        if scheme != "http" && scheme != "https" {
            return Err(HttpClientError::UnsupportedScheme(scheme));
        }
        let host = uri.host().ok_or(HttpClientError::MissingHost)?.to_string();
        let port = uri
            .port()
            .unwrap_or(if scheme == "https" { 443 } else { 80 });
        let mut target = uri.path().to_string();
        if target.is_empty() {
            target = "/".to_string();
        }
        if let Some(query) = uri.query() {
            target.push('?');
            target.push_str(query);
        }

        let mut req = Request::new(&request.method, &target);
        if !request
            .headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("host"))
        {
            let authority = if uri.port().is_some() {
                format!("{host}:{port}")
            } else {
                host.clone()
            };
            req = req.header("Host", &authority);
        }
        let request_close = request.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("connection")
                && value
                    .split(',')
                    .any(|token| token.trim().eq_ignore_ascii_case("close"))
        });
        for (name, value) in &request.headers {
            req = req.header(name, value);
        }
        if !request.body.is_empty() {
            req = req.body(request.body);
        }
        let req_bytes = req.encode();
        let origin = Origin { scheme, host, port };
        let pooled = self.pool.lock().unwrap().take(&origin);
        // Only retry safe reads on a stale pooled connection, before any response
        // bytes arrive. Never replay uploads or other mutations.
        let mut retry = pooled.is_some() && matches!(request.method.as_str(), "GET" | "HEAD");
        let mut stream = match pooled {
            Some(stream) => stream,
            None => self.connect(&origin).await?,
        };
        loop {
            let result = self
                .exchange(&mut stream, &req_bytes, &request.method, limits.clone())
                .await;
            match result {
                Ok((head, body_kind, decoder)) => {
                    let reusable = !request_close
                        && head.is_keep_alive()
                        && head.status_code != 101
                        && !matches!(body_kind, BodyKind::CloseDelimited | BodyKind::Tunnel);
                    return Ok(HttpResponseStream {
                        status_code: head.status_code,
                        headers: head.headers,
                        stream: Some(stream),
                        pool: self.pool.clone(),
                        origin,
                        reusable,
                        decoder,
                        finished: false,
                    });
                }
                Err((_, false)) if retry => {
                    retry = false;
                    stream = self.connect(&origin).await?;
                }
                Err((error, _)) => return Err(error),
            }
        }
    }

    async fn exchange(
        &self,
        stream: &mut Box<dyn AsyncReadWrite>,
        bytes: &[u8],
        method: &str,
        limits: DecoderLimits,
    ) -> Result<(shiguredo_http11::ResponseHead, BodyKind, ResponseDecoder), (HttpClientError, bool)>
    {
        stream
            .write_all(bytes)
            .await
            .map_err(|e| (HttpClientError::Io(e.to_string()), false))?;
        stream
            .flush()
            .await
            .map_err(|e| (HttpClientError::Io(e.to_string()), false))?;
        let mut decoder = ResponseDecoder::with_limits(limits);
        decoder.set_expect_no_body(method == "HEAD");
        decoder.set_request_method(method);
        let mut buffer = [0; 8192];
        let mut received = false;
        loop {
            if let Some((head, kind)) = decoder
                .decode_headers()
                .map_err(|e| (HttpClientError::Decode(e.to_string()), received))?
            {
                if head.is_informational() && head.status_code != 101 {
                    let remaining = decoder.take_remaining();
                    decoder.reset();
                    decoder.set_expect_no_body(method == "HEAD");
                    decoder.set_request_method(method);
                    decoder
                        .feed(&remaining)
                        .map_err(|e| (HttpClientError::Decode(e.to_string()), true))?;
                    continue;
                }
                return Ok((head, kind, decoder));
            }
            let count = stream
                .read(&mut buffer)
                .await
                .map_err(|e| (HttpClientError::Io(e.to_string()), received))?;
            if count == 0 {
                return Err((
                    HttpClientError::Decode(
                        "Connection closed before response headers were fully received".into(),
                    ),
                    received,
                ));
            }
            received = true;
            decoder
                .feed(&buffer[..count])
                .map_err(|e| (HttpClientError::Decode(e.to_string()), true))?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{net::TcpListener, time::timeout};

    fn request(url: &str, method: &str) -> HttpRequest {
        HttpRequest {
            method: method.into(),
            url: url.into(),
            headers: vec![],
            body: vec![],
        }
    }

    async fn read_request(socket: &mut TcpStream) {
        let mut bytes = Vec::new();
        while !bytes.ends_with(b"\r\n\r\n") {
            bytes.push(socket.read_u8().await.unwrap());
        }
    }

    async fn run_reuse_case(method: &str, response: &'static [u8], expected: &[u8]) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/object", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            for _ in 0..3 {
                read_request(&mut socket).await;
                socket.write_all(response).await.unwrap();
            }
        });
        let client = HttpClient::new();
        timeout(Duration::from_secs(3), async {
            for _ in 0..3 {
                let response = client.clone().send(request(&url, method)).await.unwrap();
                assert_eq!(response.body, expected);
            }
            server.await.unwrap();
        })
        .await
        .expect("three requests should complete on one connection");
    }

    #[test]
    fn pool_is_bounded_and_isolates_origins_and_expires_idle_connections() {
        let origin = Origin {
            scheme: "http".into(),
            host: "localhost".into(),
            port: 80,
        };
        let mut pool = ConnectionPool::default();
        for _ in 0..MAX_IDLE_PER_ORIGIN + 2 {
            pool.put(origin.clone(), Box::new(tokio::io::duplex(64).0));
        }
        assert_eq!(pool.idle.len(), MAX_IDLE_PER_ORIGIN);
        for other in [
            Origin {
                scheme: "https".into(),
                ..origin.clone()
            },
            Origin {
                host: "other".into(),
                ..origin.clone()
            },
            Origin {
                port: 81,
                ..origin.clone()
            },
        ] {
            assert!(pool.take(&other).is_none());
        }
        for port in 100..200 {
            pool.put(
                Origin {
                    port,
                    ..origin.clone()
                },
                Box::new(tokio::io::duplex(64).0),
            );
        }
        assert_eq!(pool.idle.len(), MAX_IDLE_CONNECTIONS);
        for entry in &mut pool.idle {
            entry.since = Instant::now() - IDLE_TIMEOUT;
        }
        assert!(pool.take(&origin).is_none());
        assert!(pool.idle.is_empty());
    }

    #[tokio::test]
    async fn reuses_connection_across_client_clones() {
        run_reuse_case(
            "GET",
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
            b"hello",
        )
        .await;
    }

    #[tokio::test]
    async fn head_does_not_wait_for_advertised_body() {
        run_reuse_case(
            "HEAD",
            b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\n",
            b"",
        )
        .await;
    }

    #[tokio::test]
    async fn chunked_response_with_trailers_is_reusable() {
        run_reuse_case("GET", b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\nX-End: yes\r\n\r\n", b"hello").await;
    }

    #[tokio::test]
    async fn skips_informational_responses() {
        run_reuse_case(
            "GET",
            b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
            b"hello",
        )
        .await;
    }

    #[tokio::test]
    async fn closed_responses_are_not_pooled() {
        for response in [
            &b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 5\r\n\r\nhello"[..],
            &b"HTTP/1.1 200 OK\r\n\r\nhello"[..],
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                read_request(&mut socket).await;
                socket.write_all(response).await.unwrap();
            });
            let client = HttpClient::new();
            let result = timeout(Duration::from_secs(3), client.send(request(&url, "GET")))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(result.body, b"hello");
            assert!(client.pool.lock().unwrap().idle.is_empty());
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn streaming_reuses_only_after_body_is_consumed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            for _ in 0..2 {
                read_request(&mut socket).await;
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
                    .await
                    .unwrap();
            }
        });
        let client = HttpClient::new();
        timeout(Duration::from_secs(3), async {
            let mut response = client.send_streaming(request(&url, "GET")).await.unwrap();
            assert!(client.pool.lock().unwrap().idle.is_empty());
            let mut body = [0; 2];
            let mut collected = Vec::new();
            loop {
                let n = response.read_chunk(&mut body).await.unwrap();
                if n == 0 {
                    break;
                }
                collected.extend_from_slice(&body[..n]);
            }
            assert_eq!(collected, b"hello");
            assert_eq!(client.pool.lock().unwrap().idle.len(), 1);
            let unread = client.send_streaming(request(&url, "GET")).await.unwrap();
            drop(unread);
            assert!(client.pool.lock().unwrap().idle.is_empty());
            server.await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn stale_connection_retries_reads_but_not_writes() {
        for method in ["GET", "HEAD", "POST", "PUT", "DELETE"] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/", listener.local_addr().unwrap());
            let safe = matches!(method, "GET" | "HEAD");
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                read_request(&mut socket).await;
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                    .await
                    .unwrap();
                // Simulate an idle connection closing just as the next request arrives.
                read_request(&mut socket).await;
                drop(socket);
                if safe {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    read_request(&mut socket).await;
                    socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                        .await
                        .unwrap();
                } else {
                    assert!(
                        timeout(Duration::from_millis(100), listener.accept())
                            .await
                            .is_err()
                    );
                }
            });
            let client = HttpClient::new();
            timeout(Duration::from_secs(3), async {
                client.send(request(&url, "GET")).await.unwrap();
                assert_eq!(client.send(request(&url, method)).await.is_ok(), safe);
                server.await.unwrap();
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn truncated_body_is_not_pooled() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort")
                .await
                .unwrap();
        });
        let client = HttpClient::new();
        assert!(
            timeout(Duration::from_secs(3), client.send(request(&url, "GET")))
                .await
                .unwrap()
                .is_err()
        );
        assert!(client.pool.lock().unwrap().idle.is_empty());
        server.await.unwrap();
    }
}
