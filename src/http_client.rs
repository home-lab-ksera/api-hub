use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use shiguredo_http11::{BodyProgress, DecoderLimits, Request, ResponseDecoder, uri::Uri};
use std::{cmp::min, fmt, sync::Arc};
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
    stream: Box<dyn AsyncReadWrite>,
    decoder: ResponseDecoder,
    finished: bool,
}

impl HttpResponseStream {
    pub async fn read_chunk(&mut self, output: &mut [u8]) -> Result<usize, HttpClientError> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.finished {
            return Ok(0);
        }

        let mut read_buffer = vec![0_u8; 8192];

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
                    self.finished = true;
                }
                return Ok(count);
            }

            match self
                .decoder
                .progress()
                .map_err(|e| HttpClientError::Decode(e.to_string()))?
            {
                BodyProgress::Complete { .. } => {
                    self.finished = true;
                    return Ok(0);
                }
                BodyProgress::Continue => {}
            }

            let n = self
                .stream
                .read(&mut read_buffer)
                .await
                .map_err(|e| HttpClientError::Io(e.to_string()))?;

            if n == 0 {
                self.decoder.mark_eof();
                if matches!(
                    self.decoder
                        .progress()
                        .map_err(|e| HttpClientError::Decode(e.to_string()))?,
                    BodyProgress::Complete { .. }
                ) {
                    self.finished = true;
                }
                continue;
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

#[derive(Clone)]
pub struct HttpClient {
    tls_connector: TlsConnector,
}

impl HttpClient {
    pub fn new() -> Self {
        let root_store = RootCertStore::from_iter(TLS_SERVER_ROOTS.iter().cloned());
        let tls_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        Self {
            tls_connector: TlsConnector::from(Arc::new(tls_config)),
        }
    }

    pub async fn send(&self, request: HttpRequest) -> Result<HttpResponse, HttpClientError> {
        let uri =
            Uri::parse(&request.url).map_err(|e| HttpClientError::InvalidUrl(e.to_string()))?;

        let scheme = uri
            .scheme()
            .ok_or_else(|| HttpClientError::InvalidUrl(request.url.clone()))?
            .to_ascii_lowercase();
        let host = uri.host().ok_or(HttpClientError::MissingHost)?.to_string();
        let port = uri
            .port()
            .unwrap_or_else(|| if scheme == "https" { 443 } else { 80 });

        let mut target = uri.path().to_string();
        if target.is_empty() {
            target = "/".to_string();
        }
        if let Some(query) = uri.query() {
            target.push('?');
            target.push_str(query);
        }

        let has_host_header = request
            .headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("host"));

        let mut req = Request::new(&request.method, &target);
        if !has_host_header {
            req = req.header("Host", &host);
        }
        for (name, value) in &request.headers {
            req = req.header(name, value);
        }
        if !request.body.is_empty() {
            req = req.body(request.body);
        }
        let req_bytes = req.encode();

        let mut stream: Box<dyn AsyncReadWrite> = match scheme.as_str() {
            "http" => {
                let tcp = TcpStream::connect((host.as_str(), port))
                    .await
                    .map_err(|e| HttpClientError::Io(e.to_string()))?;
                Box::new(tcp)
            }
            "https" => {
                let tcp = TcpStream::connect((host.as_str(), port))
                    .await
                    .map_err(|e| HttpClientError::Io(e.to_string()))?;
                let server_name = ServerName::try_from(host.clone())
                    .map_err(|e| HttpClientError::Tls(e.to_string()))?;
                let tls = self
                    .tls_connector
                    .connect(server_name, tcp)
                    .await
                    .map_err(|e| HttpClientError::Tls(e.to_string()))?;
                Box::new(tls)
            }
            other => return Err(HttpClientError::UnsupportedScheme(other.to_string())),
        };

        stream
            .write_all(&req_bytes)
            .await
            .map_err(|e| HttpClientError::Io(e.to_string()))?;
        stream
            .flush()
            .await
            .map_err(|e| HttpClientError::Io(e.to_string()))?;

        let mut decoder = ResponseDecoder::new();
        let mut buf = vec![0_u8; 8192];

        loop {
            if let Some(response) = decoder
                .decode()
                .map_err(|e| HttpClientError::Decode(e.to_string()))?
            {
                return Ok(HttpResponse {
                    status_code: response.status_code,
                    headers: response.headers,
                    body: response.body,
                });
            }

            let n = stream
                .read(&mut buf)
                .await
                .map_err(|e| HttpClientError::Io(e.to_string()))?;
            if n == 0 {
                decoder.mark_eof();
                if let Some(response) = decoder
                    .decode()
                    .map_err(|e| HttpClientError::Decode(e.to_string()))?
                {
                    return Ok(HttpResponse {
                        status_code: response.status_code,
                        headers: response.headers,
                        body: response.body,
                    });
                }
                return Err(HttpClientError::Decode(
                    "Connection closed before a complete response was received".to_string(),
                ));
            }

            decoder
                .feed(&buf[..n])
                .map_err(|e| HttpClientError::Decode(e.to_string()))?;
        }
    }

    pub async fn send_streaming(
        &self,
        request: HttpRequest,
    ) -> Result<HttpResponseStream, HttpClientError> {
        let uri =
            Uri::parse(&request.url).map_err(|e| HttpClientError::InvalidUrl(e.to_string()))?;

        let scheme = uri
            .scheme()
            .ok_or_else(|| HttpClientError::InvalidUrl(request.url.clone()))?
            .to_ascii_lowercase();
        let host = uri.host().ok_or(HttpClientError::MissingHost)?.to_string();
        let port = uri
            .port()
            .unwrap_or_else(|| if scheme == "https" { 443 } else { 80 });

        let mut target = uri.path().to_string();
        if target.is_empty() {
            target = "/".to_string();
        }
        if let Some(query) = uri.query() {
            target.push('?');
            target.push_str(query);
        }

        let has_host_header = request
            .headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("host"));

        let mut req = Request::new(&request.method, &target);
        if !has_host_header {
            req = req.header("Host", &host);
        }
        for (name, value) in &request.headers {
            req = req.header(name, value);
        }
        if !request.body.is_empty() {
            req = req.body(request.body);
        }
        let req_bytes = req.encode();

        let mut stream: Box<dyn AsyncReadWrite> = match scheme.as_str() {
            "http" => {
                let tcp = TcpStream::connect((host.as_str(), port))
                    .await
                    .map_err(|e| HttpClientError::Io(e.to_string()))?;
                Box::new(tcp)
            }
            "https" => {
                let tcp = TcpStream::connect((host.as_str(), port))
                    .await
                    .map_err(|e| HttpClientError::Io(e.to_string()))?;
                let server_name = ServerName::try_from(host.clone())
                    .map_err(|e| HttpClientError::Tls(e.to_string()))?;
                let tls = self
                    .tls_connector
                    .connect(server_name, tcp)
                    .await
                    .map_err(|e| HttpClientError::Tls(e.to_string()))?;
                Box::new(tls)
            }
            other => return Err(HttpClientError::UnsupportedScheme(other.to_string())),
        };

        stream
            .write_all(&req_bytes)
            .await
            .map_err(|e| HttpClientError::Io(e.to_string()))?;
        stream
            .flush()
            .await
            .map_err(|e| HttpClientError::Io(e.to_string()))?;

        let mut decoder = ResponseDecoder::with_limits(DecoderLimits::unlimited());
        let mut buf = vec![0_u8; 8192];

        loop {
            if let Some((head, _body_kind)) = decoder
                .decode_headers()
                .map_err(|e| HttpClientError::Decode(e.to_string()))?
            {
                return Ok(HttpResponseStream {
                    status_code: head.status_code,
                    headers: head.headers,
                    stream,
                    decoder,
                    finished: false,
                });
            }

            let n = stream
                .read(&mut buf)
                .await
                .map_err(|e| HttpClientError::Io(e.to_string()))?;
            if n == 0 {
                return Err(HttpClientError::Decode(
                    "Connection closed before response headers were fully received".to_string(),
                ));
            }

            decoder
                .feed(&buf[..n])
                .map_err(|e| HttpClientError::Decode(e.to_string()))?;
        }
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}
