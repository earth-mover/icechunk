//! Wire-level tests that S3 requests failing because the network blocks,
//! intercepts or never answers them produce an actionable "unreachable" error.

use std::{
    collections::HashMap, net::TcpListener as StdTcpListener, num::NonZeroU16, sync::Arc,
};

use icechunk::{
    Storage,
    config::{GcsCredentials, S3Credentials, S3Options, S3StaticCredentials},
    new_s3_storage,
    storage::{
        NetworkFailure, RetriesSettings, Settings, StorageContext, StorageError,
        StorageErrorKind, TimeoutSettings, new_gcs_storage,
    },
};
use icechunk_macros::tokio_test;
use tokio::{io::AsyncReadExt as _, net::TcpListener};
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::any};

const KEY: &str = "refs/branch.main/ref.json";

/// A server that answers every request with the given status, content type
/// and body, whatever the method. `method` documents which request the test
/// expects to be the first one and is not used for matching.
async fn html_server(
    status: u16,
    method: &str,
    content_type: Option<&str>,
    body: &str,
) -> MockServer {
    let _ = method;
    let server = MockServer::start().await;
    let mut resp = ResponseTemplate::new(status);
    if let Some(ct) = content_type {
        resp = resp.insert_header("Content-Type", ct);
    }
    if !body.is_empty() {
        resp = resp.set_body_raw(body.as_bytes().to_vec(), content_type.unwrap_or(""));
    }
    Mock::given(any()).respond_with(resp).mount(&server).await;
    server
}

fn fast_settings() -> Settings {
    Settings {
        retries: Some(RetriesSettings {
            max_tries: NonZeroU16::new(2),
            initial_backoff_ms: Some(10),
            max_backoff_ms: Some(20),
        }),
        timeouts: Some(TimeoutSettings {
            connect_timeout_ms: Some(2000),
            operation_timeout_ms: Some(2000),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn s3_at(endpoint: &str, prefix: Option<&str>) -> Arc<dyn Storage + Send + Sync> {
    let options = S3Options::default()
        .with_region("us-east-1")
        .with_endpoint_url(endpoint)
        .with_allow_http(true)
        .with_force_path_style(true);
    let creds = Some(S3Credentials::Static(S3StaticCredentials {
        access_key_id: "k".into(),
        secret_access_key: "s".into(),
        session_token: None,
        expires_after: None,
    }));
    new_s3_storage(
        options,
        "bucket".to_string(),
        prefix.map(str::to_string),
        creds,
        Vec::new(),
        Vec::new(),
        Some(false),
    )
    .unwrap()
}

fn expect_unreachable(err: &StorageError) -> (&str, &NetworkFailure) {
    match &err.kind {
        StorageErrorKind::Unreachable { host, failure, .. } => (host, failure),
        other => panic!("expected Unreachable, got {other:?}: {err}"),
    }
}

async fn read_error(storage: &Arc<dyn Storage + Send + Sync>) -> StorageError {
    let settings = fast_settings();
    let ctx = StorageContext::unattributed(&settings);
    match storage.get_object(&ctx, KEY, None).await {
        Ok(_) => panic!("expected the read to fail"),
        Err(err) => err,
    }
}

fn host_of(server: &MockServer) -> String {
    server.uri().trim_start_matches("http://").to_string()
}

#[tokio_test]
async fn get_html_403_is_unexpected_response() {
    let server = html_server(
        403,
        "GET",
        Some("text/html"),
        "<html>Access blocked by policy</html>",
    )
    .await;
    let storage = s3_at(&server.uri(), Some("p"));
    let err = read_error(&storage).await;
    let (host, failure) = expect_unreachable(&err);
    assert_eq!(host, host_of(&server));
    assert!(
        matches!(failure, NetworkFailure::UnexpectedResponse { status: 403, .. }),
        "got {failure:?}"
    );
    let text = err.to_string();
    assert!(text.contains("firewall or proxy"), "{text}");
    assert!(text.contains("Access blocked by policy"), "{text}");
}

#[tokio_test]
async fn get_html_502_is_unexpected_response() {
    let server =
        html_server(502, "GET", Some("text/html"), "<html>Bad gateway</html>").await;
    let storage = s3_at(&server.uri(), Some("p"));
    let err = read_error(&storage).await;
    let (_, failure) = expect_unreachable(&err);
    assert!(
        matches!(failure, NetworkFailure::UnexpectedResponse { status: 502, .. }),
        "got {failure:?}"
    );
}

#[tokio_test]
async fn head_407_is_unexpected_response() {
    let server = html_server(407, "HEAD", None, "").await;
    let storage = s3_at(&server.uri(), None);
    let err = read_error(&storage).await;
    let (_, failure) = expect_unreachable(&err);
    assert!(
        matches!(failure, NetworkFailure::UnexpectedResponse { status: 407, .. }),
        "got {failure:?}"
    );
}

#[tokio_test]
async fn head_403_without_body_is_not_unreachable() {
    let server = html_server(403, "HEAD", None, "").await;
    let storage = s3_at(&server.uri(), None);
    let err = read_error(&storage).await;
    assert!(!err.kind.is_unreachable(), "got {err}");
}

#[tokio_test]
async fn get_404_html_stays_not_found() {
    let server = html_server(404, "GET", Some("text/html"), "<html>nope</html>").await;
    let storage = s3_at(&server.uri(), Some("p"));
    let err = read_error(&storage).await;
    assert!(matches!(err.kind, StorageErrorKind::ObjectNotFound), "got {err}");
}

#[tokio_test]
async fn closed_port_is_connect() {
    let port = {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let storage = s3_at(&format!("http://127.0.0.1:{port}"), Some("p"));
    let err = read_error(&storage).await;
    let (host, failure) = expect_unreachable(&err);
    assert_eq!(host, format!("127.0.0.1:{port}"));
    assert!(matches!(failure, NetworkFailure::Connect), "got {failure:?}");
}

#[tokio_test]
async fn unresolvable_host_is_connect() {
    let storage = s3_at("http://storage.invalid", Some("p"));
    let err = read_error(&storage).await;
    let (host, failure) = expect_unreachable(&err);
    assert_eq!(host, "storage.invalid");
    assert!(matches!(failure, NetworkFailure::Connect), "got {failure:?}");
}

#[tokio_test]
async fn https_to_plain_http_is_tls() {
    let server = html_server(200, "GET", None, "").await;
    let storage = s3_at(&format!("https://{}", host_of(&server)), Some("p"));
    let err = read_error(&storage).await;
    let (_, failure) = expect_unreachable(&err);
    assert!(matches!(failure, NetworkFailure::Tls), "got {failure:?}");
    assert!(err.to_string().contains("CA certificate"), "{err}");
}

#[tokio_test]
async fn hung_server_is_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((mut sock, _)) = listener.accept().await {
            let mut buf = [0u8; 1024];
            let _ = sock.read(&mut buf).await;
            held.push(sock);
        }
    });
    let storage = s3_at(&format!("http://127.0.0.1:{port}"), Some("p"));
    let err = read_error(&storage).await;
    let (_, failure) = expect_unreachable(&err);
    assert!(matches!(failure, NetworkFailure::Timeout), "got {failure:?}");
}

fn gcs_at(base_url: &str) -> Arc<dyn Storage + Send + Sync> {
    new_gcs_storage(
        "bucket".to_string(),
        Some("p".to_string()),
        Some(GcsCredentials::Anonymous),
        Some(HashMap::from([("google_base_url".to_string(), base_url.to_string())])),
        Vec::new(),
        Vec::new(),
    )
    .unwrap()
}

#[tokio_test]
async fn gcs_closed_port_is_connect() {
    let port = {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let storage = gcs_at(&format!("http://127.0.0.1:{port}"));
    let err = read_error(&storage).await;
    let (host, failure) = expect_unreachable(&err);
    assert_eq!(host, format!("127.0.0.1:{port}"));
    assert!(matches!(failure, NetworkFailure::Connect), "got {failure:?}");
    let text = err.to_string();
    assert!(text.contains("firewall or proxy"), "{text}");
}

/// `object_store` keeps the status and body of a failed response private, so an
/// intercepted GCS response cannot be recognised and stays a plain error.
#[tokio_test]
async fn gcs_html_403_is_not_unreachable() {
    let server =
        html_server(403, "GET", Some("text/html"), "<html>Access blocked</html>").await;
    let storage = gcs_at(&server.uri());
    let err = read_error(&storage).await;
    assert!(!err.kind.is_unreachable(), "{err}");
}
