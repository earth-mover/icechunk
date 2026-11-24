use std::num::NonZeroU16;

use icechunk::{
    Repository,
    config::{S3Credentials, S3Options},
    new_s3_storage,
    storage::{RetriesSettings, Settings},
};
use icechunk_macros::tokio_test;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::any};

const BLOCK_PAGE: &str = "<!DOCTYPE html><html><head><style>body{}</style>\
    <title>Website blocked</title></head><body>Access denied by policy</body></html>";

#[tokio_test]
async fn proxy_block_page_shows_status_and_title() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(403).set_body_raw(BLOCK_PAGE, "text/html"))
        .mount(&server)
        .await;
    let storage = new_s3_storage(
        S3Options::default()
            .with_region("us-east-1")
            .with_endpoint_url(server.uri())
            .with_allow_http(true)
            .with_force_path_style(true),
        "bucket".to_string(),
        Some("prefix".to_string()),
        Some(S3Credentials::Anonymous),
        Vec::new(),
        Vec::new(),
        Some(false),
    )
    .unwrap();
    let settings = Settings {
        retries: Some(RetriesSettings {
            max_tries: NonZeroU16::new(1),
            initial_backoff_ms: None,
            max_backoff_ms: None,
        }),
        ..Default::default()
    };

    let msg = Repository::exists(storage, Some(settings)).await.unwrap_err().to_string();

    assert!(msg.contains("HTTP 403 without an S3 error code"), "{msg}");
    assert!(msg.contains("body \"Website blocked\""), "{msg}");
    assert!(msg.contains("HTTPS_PROXY"), "{msg}");
}
