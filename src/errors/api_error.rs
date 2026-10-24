use shiguredo_http11::Response;
use tracing::error;

#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    NotFound(String),
    MethodNotAllowed(String),
    InternalServerError(String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadRequest(message) => write!(f, "Bad Request: {message}"),
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

pub fn problem_details_response(status_code: u16, detail: impl Into<String>) -> Response {
    Response::new(status_code, reason_phrase(status_code))
        .header("Content-Type", "application/problem+json")
        .body(problem_details_json(status_code, detail).into_bytes())
}

impl ApiError {
    pub fn into_response(self) -> Response {
        match self {
            ApiError::BadRequest(ref message) => {
                error!(
                    error_type = "bad_request",
                    message = %message,
                    status = 400,
                    "API error occurred"
                );
                problem_details_response(400, message.clone())
            }
            ApiError::NotFound(ref message) => {
                error!(
                    error_type = "not_found",
                    message = %message,
                    status = 404,
                    "API error occurred"
                );
                problem_details_response(404, message.clone())
            }
            ApiError::MethodNotAllowed(ref message) => {
                error!(
                    error_type = "method_not_allowed",
                    message = %message,
                    status = 405,
                    "API error occurred"
                );
                problem_details_response(405, message.clone())
            }
            ApiError::InternalServerError(ref details) => {
                error!(
                    error_type = "internal_server_error",
                    details = %details,
                    status = 500,
                    "API error occurred"
                );
                problem_details_response(500, "Internal Server Error")
            }
        }
    }
}

pub fn reason_phrase(status_code: u16) -> &'static str {
    match status_code {
        200 => "OK",
        202 => "Accepted",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Unknown Error",
    }
}
