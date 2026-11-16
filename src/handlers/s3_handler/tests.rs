use super::{
    extract_forward_header, parse_delete_objects_request, parse_list_objects_v2_request,
    parse_presigned_object_request,
};
use proptest::{prelude::ProptestConfig, prop_assert_eq, proptest};

#[test]
fn parse_delete_objects_allows_null_optionals() {
    let body = r#"{
        "bucket": "pdfs",
        "objects": [
            {"key": "20260418/a.pdf", "version_id": null},
            {"key": "20260418/b.pdf"}
        ],
        "quiet": null
    }"#;

    let parsed = parse_delete_objects_request(body).expect("request should parse");

    assert_eq!(parsed.bucket, "pdfs");
    assert_eq!(parsed.objects.len(), 2);
    assert_eq!(parsed.objects[0].key, "20260418/a.pdf");
    assert_eq!(parsed.objects[0].version_id, None);
    assert_eq!(parsed.objects[1].key, "20260418/b.pdf");
    assert_eq!(parsed.objects[1].version_id, None);
    assert_eq!(parsed.quiet, None);
}

#[test]
fn parse_list_objects_v2_allows_null_optionals() {
    let body = r#"{
        "bucket": "pdfs",
        "prefix": null,
        "delimiter": null,
        "max_keys": null,
        "start_after": null
    }"#;

    let parsed = parse_list_objects_v2_request(body).expect("request should parse");

    assert_eq!(parsed.bucket, "pdfs");
    assert_eq!(parsed.prefix, None);
    assert_eq!(parsed.delimiter, None);
    assert_eq!(parsed.max_keys, None);
    assert_eq!(parsed.start_after, None);
}

#[test]
fn parse_presigned_object_allows_null_expires_in_secs() {
    let body = r#"{
        "bucket": "pdfs",
        "key": "20260418/a.pdf",
        "expires_in_secs": null
    }"#;

    let parsed = parse_presigned_object_request(body).expect("request should parse");

    assert_eq!(parsed.bucket, "pdfs");
    assert_eq!(parsed.key, "20260418/a.pdf");
    assert_eq!(parsed.expires_in_secs, None);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn extract_forward_header_is_case_insensitive(value in "[ -~]{0,64}") {
        let headers = vec![
            ("RaNgE".to_string(), value.clone()),
            ("x-request-id".to_string(), "req-1".to_string()),
        ];

        let extracted = extract_forward_header(&headers, "range");
        prop_assert_eq!(extracted, Some(value));
    }
}

#[test]
fn preview_never_renders_stored_documents_in_the_api_origin() {
    use super::{build_preview_response_headers, get_header_value};
    let source = |content_type: &str| vec![("Content-Type".to_string(), content_type.to_string())];

    for content_type in [
        "text/html; charset=utf-8",
        "image/svg+xml",
        "application/xhtml+xml",
        "text/plain",
    ] {
        let headers = build_preview_response_headers(&source(content_type), "a/page", 200);
        assert_eq!(
            get_header_value(&headers, "content-disposition"),
            Some("attachment")
        );
        assert_eq!(
            get_header_value(&headers, "content-security-policy"),
            Some("sandbox; default-src 'none'")
        );
        assert_eq!(
            get_header_value(&headers, "x-content-type-options"),
            Some("nosniff")
        );
    }
    // Content-Type がない本文も、ブラウザーに種類を推測させない。
    let headers = build_preview_response_headers(&[], "a/page", 200);
    assert_eq!(
        get_header_value(&headers, "content-type"),
        Some("application/octet-stream")
    );
    assert_eq!(
        get_header_value(&headers, "content-disposition"),
        Some("attachment")
    );

    // viewer が表示する形式はそのまま表示できる。
    for (content_type, key) in [
        ("image/jpeg", "hitomi/a_prev.jpg"),
        ("video/mp4", "videos/a.mp4"),
        ("application/pdf", "hitomi/a.pdf"),
    ] {
        for status in [200, 206] {
            let headers = build_preview_response_headers(&source(content_type), key, status);
            assert_ne!(
                get_header_value(&headers, "content-disposition"),
                Some("attachment")
            );
            assert_eq!(get_header_value(&headers, "content-security-policy"), None);
            assert_eq!(
                get_header_value(&headers, "x-content-type-options"),
                Some("nosniff")
            );
        }
    }
    // 304 は保存済み応答の表示方法を変えない。
    let headers = build_preview_response_headers(&[], "hitomi/a_prev.jpg", 304);
    assert_eq!(get_header_value(&headers, "content-disposition"), None);
    assert_eq!(get_header_value(&headers, "content-type"), None);
}
