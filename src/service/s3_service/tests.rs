use super::{build_s3_url, decode_base64_payload};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use proptest::prop_assert_eq;
use proptest::{
    prelude::{ProptestConfig, any},
    proptest,
    strategy::Strategy,
};
use shiguredo_s3::S3Response;

fn arbitrary_path_string() -> impl proptest::strategy::Strategy<Value = String> {
    "[A-Za-z0-9_/-]{1,64}".prop_map(|s| format!("/{s}"))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn base64_roundtrip_is_lossless(payload in any::<Vec<u8>>()) {
        let encoded = BASE64_STANDARD.encode(&payload);
        let decoded = decode_base64_payload(&encoded).expect("base64 decode should succeed");
        prop_assert_eq!(decoded, payload);
    }

    #[test]
    fn build_s3_url_preserves_scheme_host_port_and_uri(
        host in "[a-z]{1,8}(?:\\.[a-z]{1,8}){0,2}",
        port in any::<u16>(),
        https in any::<bool>(),
        uri in arbitrary_path_string(),
    ) {
        let request = shiguredo_s3::S3Request {
            method: "GET".to_string(),
            uri: uri.clone(),
            headers: Vec::new(),
            body: Vec::new(),
            host: host.clone(),
            port,
            https,
            ignore_cert_check: false,
            expect_no_body: false,
        };

        let built = build_s3_url(&request).expect("url should be valid");
        let parsed = shiguredo_http11::uri::Uri::parse(&built).expect("built URL should parse");

        let expected_scheme = if https { "https" } else { "http" };
        prop_assert_eq!(parsed.scheme(), Some(expected_scheme));
        prop_assert_eq!(parsed.host(), Some(host.as_str()));
        prop_assert_eq!(parsed.port(), Some(port));
        prop_assert_eq!(parsed.path(), uri);
    }
}

#[test]
fn invalid_base64_is_rejected() {
    let err = decode_base64_payload("not_base64").expect_err("invalid base64 should fail");
    assert_eq!(err.to_string(), "Bad Request: Failed to decode base64 data");
}

#[test]
fn s3_response_header_lookup_is_case_insensitive() {
    let response = S3Response {
        status_code: 200,
        headers: vec![("Content-Type".to_string(), "text/plain".to_string())],
        body: Vec::new(),
    };
    assert_eq!(response.get_header("content-type"), Some("text/plain"));
    assert_eq!(response.get_header("CONTENT-TYPE"), Some("text/plain"));
}
