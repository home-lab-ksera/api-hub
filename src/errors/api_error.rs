use crate::reply::{Reply, ReplyExt, reply};
use tracing::{error, warn};

#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    Forbidden(String),
    NotFound(String),
    MethodNotAllowed(String),
    InternalServerError(String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadRequest(message) => write!(f, "Bad Request: {message}"),
            Self::Forbidden(message) => write!(f, "Forbidden: {message}"),
            Self::NotFound(message) => write!(f, "Not Found: {message}"),
            Self::MethodNotAllowed(message) => write!(f, "Method Not Allowed: {message}"),
            Self::InternalServerError(_) => write!(f, "Internal Server Error"),
        }
    }
}

impl std::error::Error for ApiError {}

fn problem_details_json(status_code: u16, detail: impl Into<String>) -> String {
    let title = reason_phrase(status_code).to_string();
    let detail = detail.into();
    nojson::json(|f| {
        f.object(|f| {
            f.member("type", "about:blank")?;
            f.member("title", &title)?;
            f.member("status", status_code)?;
            f.member("detail", &detail)
        })
    })
    .to_string()
}

pub fn problem_details_response(status_code: u16, detail: impl Into<String>) -> Reply {
    reply(status_code)
        .header("Content-Type", "application/problem+json")
        .with_body(problem_details_json(status_code, detail).into_bytes())
}

impl ApiError {
    pub fn status_code(&self) -> u16 {
        match self {
            Self::BadRequest(_) => 400,
            Self::Forbidden(_) => 403,
            Self::NotFound(_) => 404,
            Self::MethodNotAllowed(_) => 405,
            Self::InternalServerError(_) => 500,
        }
    }

    fn error_type(&self) -> &'static str {
        match self {
            Self::BadRequest(_) => "bad_request",
            Self::Forbidden(_) => "forbidden",
            Self::NotFound(_) => "not_found",
            Self::MethodNotAllowed(_) => "method_not_allowed",
            Self::InternalServerError(_) => "internal_server_error",
        }
    }

    /// 利用者の誤りは warn、サーバーの障害は error で記録する。
    /// 内部エラーの詳細はログにだけ残し、応答には含めない。
    pub fn into_response(self) -> Reply {
        let status = self.status_code();
        let error_type = self.error_type();
        match self {
            Self::InternalServerError(details) => {
                error!(error_type, details = %details, status, "API error occurred");
                problem_details_response(status, "Internal Server Error")
            }
            Self::BadRequest(message)
            | Self::Forbidden(message)
            | Self::NotFound(message)
            | Self::MethodNotAllowed(message) => {
                warn!(error_type, message = %message, status, "API error occurred");
                problem_details_response(status, message)
            }
        }
    }
}

pub fn reason_phrase(status_code: u16) -> &'static str {
    match status_code {
        200 => "OK",
        202 => "Accepted",
        204 => "No Content",
        206 => "Partial Content",
        304 => "Not Modified",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        409 => "Conflict",
        413 => "Content Too Large",
        415 => "Unsupported Media Type",
        416 => "Range Not Satisfiable",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Unknown Error",
    }
}
