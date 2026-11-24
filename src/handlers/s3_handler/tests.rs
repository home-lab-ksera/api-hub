use super::{delete_objects_input, list_objects_v2_input, parse_body, presigned_object_input};
use crate::http_client::find_header;

/// `null` は省略と同じに扱い、省略時の値を入れる。
#[test]
fn null_optionals_are_treated_as_omitted() {
    let delete = parse_body(
        br#"{"bucket":"pdfs","objects":[{"key":"a","version_id":null},{"key":"b"}],"quiet":null}"#,
        delete_objects_input,
    )
    .unwrap();
    assert_eq!(delete.objects[0].version_id, None);
    assert_eq!(delete.objects[1].version_id, None);
    assert!(!delete.quiet);

    let list = parse_body(
        br#"{"bucket":"pdfs","prefix":null,"max_keys":null}"#,
        list_objects_v2_input,
    )
    .unwrap();
    assert_eq!((list.prefix, list.max_keys), (None, None));

    let presigned = parse_body(
        br#"{"bucket":"pdfs","key":"a.pdf","expires_in_secs":null}"#,
        presigned_object_input,
    )
    .unwrap();
    assert_eq!(presigned.expires_in_secs, 900);
}

#[test]
fn preview_never_renders_stored_documents_in_the_api_origin() {
    use super::build_preview_response_headers;
    let source = |content_type: &str| vec![("Content-Type".to_string(), content_type.to_string())];

    for content_type in [
        "text/html; charset=utf-8",
        "image/svg+xml",
        "application/xhtml+xml",
        "text/plain",
    ] {
        let headers = build_preview_response_headers(&source(content_type), "a/page", 200);
        assert_eq!(
            find_header(&headers, "content-disposition"),
            Some("attachment")
        );
        assert_eq!(
            find_header(&headers, "content-security-policy"),
            Some("sandbox; default-src 'none'")
        );
        assert_eq!(
            find_header(&headers, "x-content-type-options"),
            Some("nosniff")
        );
    }
    // Content-Type がない本文も、ブラウザーに種類を推測させない。
    let headers = build_preview_response_headers(&[], "a/page", 200);
    assert_eq!(
        find_header(&headers, "content-type"),
        Some("application/octet-stream")
    );
    assert_eq!(
        find_header(&headers, "content-disposition"),
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
                find_header(&headers, "content-disposition"),
                Some("attachment")
            );
            assert_eq!(find_header(&headers, "content-security-policy"), None);
            assert_eq!(
                find_header(&headers, "x-content-type-options"),
                Some("nosniff")
            );
        }
    }
    // 304 は保存済み応答の表示方法を変えない。
    let headers = build_preview_response_headers(&[], "hitomi/a_prev.jpg", 304);
    assert_eq!(find_header(&headers, "content-disposition"), None);
    assert_eq!(find_header(&headers, "content-type"), None);
}
