//! エンドポイントの表。
//!
//! 経路・メソッド・本文の読み方・ブラウザーからの利用可否はここだけで定義する。
//! 405 の判定、CORS preflight、ブラウザーからの更新制限、本文の上限はすべてこの表から決まる。
//! エンドポイントを足すときは [`Endpoint`] に variant を追加し、[`ROUTES`] に1行足す。
//! 呼び出し先は `route_request` の網羅的な match がコンパイル時に要求する。

/// JSON API の本文上限。
pub(super) const SMALL_BODY: usize = 1024 * 1024;
/// base64 や生のファイルを受け取る経路の本文上限。
pub(super) const LARGE_BODY: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Endpoint {
    Health,
    Metrics,
    OpenApi,
    NotificationCreate,
    NotificationGet,
    NotificationRetry,
    SlackMessage,
    SlackMessageUpdate,
    SlackUploadImage,
    SlackUploadPdf,
    S3PutObjectBase64,
    S3PutObjectStream,
    S3PutObjectAndNotify,
    S3GetObjectBase64,
    S3HeadObject,
    S3DeleteObject,
    S3DeleteObjects,
    S3CopyObject,
    S3GetObjectTagging,
    S3PutObjectTagging,
    S3DeleteObjectTagging,
    S3ListObjectsV2,
    S3CreateMultipartUpload,
    S3UploadPartBase64,
    S3CompleteMultipartUpload,
    S3AbortMultipartUpload,
    S3ListParts,
    S3ListMultipartUploads,
    S3PresignedGetObject,
    S3PresignedPutObject,
    S3BrowserGetObject,
    S3BrowserOpen,
    S3Preview,
    S3ListBuckets,
    S3CreateBucket,
    S3HeadBucket,
    S3DeleteBucket,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum PathPattern {
    Exact(&'static str),
    /// `prefix` と `suffix` に挟まれた部分を引数として取り出す。空でもよい。
    Param {
        prefix: &'static str,
        suffix: &'static str,
    },
}

impl PathPattern {
    /// 一致したときは引数部分を返す。`Exact` は空文字列を返す。
    fn capture(self, path: &str) -> Option<&str> {
        match self {
            Self::Exact(exact) => (path == exact).then_some(""),
            Self::Param { prefix, suffix } => path.strip_prefix(prefix)?.strip_suffix(suffix),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Body {
    /// 上限まで読み込み、POST では `application/json` を要求する。
    Json(usize),
    /// 上限まで読み込み、形式は handler が検証する。
    Raw(usize),
    /// 読み込まずに handler へ流す。
    Stream,
}

#[derive(Debug)]
pub(super) struct Route {
    pub method: &'static str,
    pub path: PathPattern,
    pub endpoint: Endpoint,
    pub body: Body,
    /// viewer のオリジンからの呼び出しと CORS preflight を許可する。
    pub browser: bool,
}

impl Route {
    const fn new(method: &'static str, path: PathPattern, endpoint: Endpoint) -> Self {
        Self {
            method,
            path,
            endpoint,
            body: Body::Json(SMALL_BODY),
            browser: false,
        }
    }
    const fn body(self, body: Body) -> Self {
        Self { body, ..self }
    }
    const fn browser(self) -> Self {
        Self {
            browser: true,
            ..self
        }
    }
}

const fn get(path: &'static str, endpoint: Endpoint) -> Route {
    Route::new("GET", PathPattern::Exact(path), endpoint)
}
const fn post(path: &'static str, endpoint: Endpoint) -> Route {
    Route::new("POST", PathPattern::Exact(path), endpoint)
}

pub(super) static ROUTES: &[Route] = {
    use Endpoint as E;
    &[
        get("/health", E::Health),
        get("/metrics", E::Metrics),
        get("/openapi.json", E::OpenApi),
        post("/notifications", E::NotificationCreate),
        Route::new(
            "GET",
            PathPattern::Param {
                prefix: "/notifications/",
                suffix: "",
            },
            E::NotificationGet,
        ),
        Route::new(
            "POST",
            PathPattern::Param {
                prefix: "/notifications/",
                suffix: "/retry",
            },
            E::NotificationRetry,
        ),
        post("/slack/message", E::SlackMessage),
        post("/slack/message/update", E::SlackMessageUpdate),
        post("/slack/upload/image", E::SlackUploadImage).body(Body::Raw(LARGE_BODY)),
        post("/slack/upload/pdf", E::SlackUploadPdf).body(Body::Raw(LARGE_BODY)),
        post("/s3/put_object_base64", E::S3PutObjectBase64).body(Body::Json(LARGE_BODY)),
        post("/s3/put_object_stream", E::S3PutObjectStream)
            .body(Body::Stream)
            .browser(),
        post("/s3/put_object_and_notify", E::S3PutObjectAndNotify).body(Body::Json(LARGE_BODY)),
        post("/s3/get_object_base64", E::S3GetObjectBase64).body(Body::Json(LARGE_BODY)),
        post("/s3/head_object", E::S3HeadObject),
        post("/s3/delete_object", E::S3DeleteObject),
        post("/s3/delete_objects", E::S3DeleteObjects),
        post("/s3/copy_object", E::S3CopyObject),
        post("/s3/get_object_tagging", E::S3GetObjectTagging),
        post("/s3/put_object_tagging", E::S3PutObjectTagging),
        post("/s3/delete_object_tagging", E::S3DeleteObjectTagging),
        post("/s3/list_objects_v2", E::S3ListObjectsV2).browser(),
        post("/s3/create_multipart_upload", E::S3CreateMultipartUpload),
        post("/s3/upload_part_base64", E::S3UploadPartBase64).body(Body::Json(LARGE_BODY)),
        post(
            "/s3/complete_multipart_upload",
            E::S3CompleteMultipartUpload,
        ),
        post("/s3/abort_multipart_upload", E::S3AbortMultipartUpload),
        post("/s3/list_parts", E::S3ListParts),
        post("/s3/list_multipart_uploads", E::S3ListMultipartUploads),
        post("/s3/presigned_get_object", E::S3PresignedGetObject).browser(),
        post("/s3/presigned_put_object", E::S3PresignedPutObject),
        post("/s3/browser_get_object", E::S3BrowserGetObject).browser(),
        get("/s3/browser_open", E::S3BrowserOpen),
        Route::new(
            "GET",
            PathPattern::Param {
                prefix: "/s3/preview/",
                suffix: "",
            },
            E::S3Preview,
        )
        .body(Body::Stream),
        post("/s3/list_buckets", E::S3ListBuckets),
        post("/s3/create_bucket", E::S3CreateBucket),
        post("/s3/head_bucket", E::S3HeadBucket),
        post("/s3/delete_bucket", E::S3DeleteBucket),
    ]
};

#[derive(Debug)]
pub(super) enum Lookup<'a> {
    /// 一致した経路と、経路から取り出した引数。
    Found(&'static Route, &'a str),
    /// ブラウザー向け経路への OPTIONS。
    Preflight,
    MethodNotAllowed,
    NotFound,
}

impl Lookup<'_> {
    /// 本文の読み方。表にない要求は小さな JSON API と同じ扱いにする。
    pub(super) fn body(&self) -> Body {
        match self {
            Self::Found(route, _) => route.body,
            _ => Body::Json(SMALL_BODY),
        }
    }
}

pub(super) fn lookup<'a>(method: &str, path: &'a str) -> Lookup<'a> {
    let mut known_path = false;
    let mut browser_path = false;
    for route in ROUTES {
        let Some(param) = route.path.capture(path) else {
            continue;
        };
        if route.method == method {
            return Lookup::Found(route, param);
        }
        known_path = true;
        browser_path |= route.browser;
    }
    if method == "OPTIONS" && browser_path {
        Lookup::Preflight
    } else if known_path {
        Lookup::MethodNotAllowed
    } else {
        Lookup::NotFound
    }
}

/// viewer のオリジンから呼び出してよい経路か。メソッドは問わない。
pub(super) fn allows_browser(path: &str) -> bool {
    ROUTES
        .iter()
        .any(|route| route.browser && route.path.capture(path).is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(method: &str, path: &str) -> Option<(Endpoint, String)> {
        match lookup(method, path) {
            Lookup::Found(route, param) => Some((route.endpoint, param.to_string())),
            _ => None,
        }
    }

    #[test]
    fn each_method_and_path_appears_once() {
        for (index, route) in ROUTES.iter().enumerate() {
            for other in &ROUTES[index + 1..] {
                let same_path = match (route.path, other.path) {
                    (PathPattern::Exact(a), PathPattern::Exact(b)) => a == b,
                    (
                        PathPattern::Param {
                            prefix: a,
                            suffix: x,
                        },
                        PathPattern::Param {
                            prefix: b,
                            suffix: y,
                        },
                    ) => a == b && x == y,
                    _ => false,
                };
                assert!(
                    !(same_path && route.method == other.method),
                    "{route:?} / {other:?}"
                );
            }
        }
    }

    #[test]
    fn parameterized_paths_capture_their_argument() {
        assert_eq!(
            endpoint("GET", "/notifications/42"),
            Some((Endpoint::NotificationGet, "42".into()))
        );
        assert_eq!(
            endpoint("POST", "/notifications/42/retry"),
            Some((Endpoint::NotificationRetry, "42".into()))
        );
        // 接尾辞は引数部分にだけ一致させる。
        assert!(matches!(
            lookup("POST", "/notifications/retry"),
            Lookup::MethodNotAllowed
        ));
        assert_eq!(
            endpoint("GET", "/s3/preview/b/a/c.pdf"),
            Some((Endpoint::S3Preview, "b/a/c.pdf".into()))
        );
    }

    #[test]
    fn unmatched_methods_and_paths_are_classified() {
        assert!(matches!(
            lookup("POST", "/health"),
            Lookup::MethodNotAllowed
        ));
        assert!(matches!(
            lookup("POST", "/notifications/1"),
            Lookup::MethodNotAllowed
        ));
        assert!(matches!(
            lookup("HEAD", "/s3/preview/b/k"),
            Lookup::MethodNotAllowed
        ));
        assert!(matches!(
            lookup("OPTIONS", "/health"),
            Lookup::MethodNotAllowed
        ));
        assert!(matches!(lookup("GET", "/missing"), Lookup::NotFound));
    }

    #[test]
    fn preflight_and_browser_access_follow_the_browser_flag() {
        for path in [
            "/s3/list_objects_v2",
            "/s3/browser_get_object",
            "/s3/presigned_get_object",
            "/s3/put_object_stream",
        ] {
            assert!(
                matches!(lookup("OPTIONS", path), Lookup::Preflight),
                "{path}"
            );
            assert!(allows_browser(path), "{path}");
        }
        assert!(!allows_browser("/s3/delete_object"));
        assert!(matches!(
            lookup("OPTIONS", "/s3/delete_object"),
            Lookup::MethodNotAllowed
        ));
    }

    #[test]
    fn body_limits_follow_the_route() {
        assert_eq!(
            lookup("POST", "/s3/put_object_base64").body(),
            Body::Json(LARGE_BODY)
        );
        // base64 のファイルを受け取る経路は、パス名によらず大きな上限にする。
        assert_eq!(
            lookup("POST", "/s3/put_object_and_notify").body(),
            Body::Json(LARGE_BODY)
        );
        assert_eq!(
            lookup("POST", "/slack/upload/pdf").body(),
            Body::Raw(LARGE_BODY)
        );
        assert_eq!(
            lookup("POST", "/s3/head_object").body(),
            Body::Json(SMALL_BODY)
        );
        assert_eq!(lookup("POST", "/missing").body(), Body::Json(SMALL_BODY));
        assert_eq!(lookup("GET", "/s3/preview/b/k").body(), Body::Stream);
    }
}
