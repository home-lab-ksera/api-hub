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
