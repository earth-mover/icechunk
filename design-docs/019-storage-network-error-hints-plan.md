# Storage Network Error Hints Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> Working plan. It will be removed from the PR before merge, together with the design doc.

**Goal:** When an S3/R2/GCS request can't get through, or gets a response that isn't from the storage service, Icechunk raises an error that names the host, describes the failure, and hints that a firewall or proxy may be responsible. In Python it carries the code `storage-unreachable`.

**Architecture:** A shared `NetworkFailure` type and a new `StorageErrorKind::Unreachable` live in `icechunk-storage`. There are two public classifiers: one for AWS `SdkError` in `icechunk-s3`, and one for `object_store::Error` in `icechunk-arrow-object-store`. Each returns `Option<NetworkFailure>` and is called at the existing fallback sites in the repo storage backends *and* in the virtual-chunk fetchers. Python maps the new kind (and its virtual-chunk counterpart) to one error code.

**Tech Stack:** Rust (aws-sdk-s3 1.127, aws-smithy runtime API, object_store 0.14.1, rustls 0.23 via the existing aws-smithy-http-client), wiremock 0.6 for tests, PyO3 + pytest.

**Spec:** `design-docs/019-storage-network-error-hints.md`

## Global Constraints

- **Never mention any customer, customer org, repo, user, or path** in code, tests, comments, commits or docs. Use generic hosts such as `abc123.r2.cloudflarestorage.com`, `127.0.0.1`, or `*.invalid`.
- Commit messages carry **no `Co-Authored-By` or other attribution trailers**.
- No change to retry, timeout, or request behaviour, and no extra network calls. Only how the *final* error is classified and worded changes.
- Never classify based on `Display`/`to_string()` text. Use typed errors, `source()` downcasts, status codes and headers only.
- Errors never include request headers, signed URLs, query strings or credentials. The only response headers allowed are `Content-Type`.
- The original SDK error stays reachable through `source()`.
- These keep exactly their current behaviour: `ObjectNotFound`/404 (including the R2 raw-404 path), 304, precondition failures, `Throttled`, and virtual `ObjectModified`.
- Unexpected-response shape fires **only** when status == 407, **or** the response has a non-empty body whose `Content-Type` media type is not XML (S3) / not JSON (GCS). XML means `application/xml` or `text/xml`, ignoring parameters such as `; charset=…`.
- Body snippet: at most 200 characters; non-printable characters become spaces, runs of whitespace collapse, and the result is trimmed. If empty, `None`.
- Exact wording (Display), with `{host}` substituted:
  - `could not reach storage at {host}: {shape}. This can happen when a firewall or proxy blocks or intercepts traffic to {host}. If you are on a corporate or institutional network, ask your network administrator to allow access to this host.`
  - Extra sentence appended for `Tls` only: ` If your network uses a TLS-inspecting proxy, its CA certificate may need to be trusted on this machine.`
  - `{shape}`:
    - Connect: `could not connect`
    - Timeout: `the request timed out`
    - Tls: `the TLS handshake failed`
    - UnexpectedResponse: `got an unexpected response (HTTP {status}, Content-Type: {ct or "none"}) instead of a storage service response`, followed by `; response begins: "{snippet}"` when there is a snippet.
- Python code: `STORAGE_UNREACHABLE => "storage-unreachable"`, exception class `StorageError`.
- Backends in scope: S3 / S3-compatible / R2 (`icechunk-s3`) and GCS. Deliberate plan decision: the object_store classifier is wired generically into `ObjectStorage` and `ObjectStoreFetcher`, but it fires only when a backend reports an endpoint host (Task 4). Only the GCS backend and the object_store-S3 backend do, so Azure, HTTP and local are unaffected.

## Review Focus

- A real S3 AccessDenied on **HEAD** (403, empty body) must not get the hint. Test in Task 3: `head_403_without_body_is_not_unreachable`.
- A well-formed XML S3 error on GET (403 `AccessDenied`, with code) must not get the hint. Test in Task 2: `xml_service_error_with_code_is_not_classified`.
- A 404 with an HTML body on GET must stay `ObjectNotFound`, because the R2 raw-404 path runs before the classifier. Test in Task 3: `get_404_html_stays_not_found`.
- `Content-Type: application/xml; charset=utf-8` counts as XML. Test in Task 2: `xml_content_type_with_params_is_xml`.
- A very long or binary block-page body produces a bounded, printable snippet. Test in Task 1: `snippet_is_capped_and_printable`.

---

### Task 1: Core error type, wording, and Python code

**Files:**
- Modify: `icechunk-storage/src/storage.rs` (add `NetworkFailure`, `StorageErrorKind::Unreachable`, helpers)
- Modify: `icechunk-storage/Cargo.toml` (add `rustls = { workspace = true }`), and root `Cargo.toml` `[workspace.dependencies]` (`rustls = { version = "0.23.46", default-features = false }`, pinned to the version already in `Cargo.lock`)
- Modify: `icechunk-python/src/errors.rs` (code + `classify_storage` arm; the match is exhaustive, so this has to land in this task)
- Modify: `icechunk-python/python/icechunk/_exceptions.py` (`ErrorKind.STORAGE_UNREACHABLE = "storage-unreachable"`, next to `THROTTLED`)
- Test: unit tests in `icechunk-storage/src/storage.rs` (`#[cfg(test)] mod network_failure_tests`)

**Interfaces:**
- Produces (in `icechunk_storage::storage`, re-exported wherever `StorageErrorKind` is):
  - `pub enum NetworkFailure { Connect, Timeout, Tls, UnexpectedResponse { status: u16, content_type: Option<String>, snippet: Option<String> } }`, which derives `Debug, Clone, PartialEq, Eq`
  - `StorageErrorKind::Unreachable { host: String, failure: NetworkFailure, #[source] cause: Box<dyn std::error::Error + Send + Sync + 'static> }`, with Display exactly as in Global Constraints
  - `pub fn unreachable_error(host: impl Into<String>, failure: NetworkFailure, cause: impl std::error::Error + Send + Sync + 'static) -> StorageError`
  - `pub fn sanitize_snippet(body: &[u8]) -> Option<String>`
  - `pub fn error_chain_has_tls_failure(err: &(dyn std::error::Error + 'static)) -> bool`: walks `source()` and returns true if any link downcasts to `rustls::Error`, or is a `std::io::Error` whose `get_ref()` downcasts to `rustls::Error`
  - `pub fn is_non_storage_content_type(ct: Option<&str>, expected: ExpectedBody) -> bool` with `pub enum ExpectedBody { Xml, Json }`. Returns true when `ct` is present and its media type (before `;`, trimmed, lowercased) isn't the expected one. `None` returns false.
  - `StorageErrorKind::is_unreachable(&self) -> bool`
  - `pub fn unreachable_message(host: &str, failure: &NetworkFailure) -> String`: the full Display text from Global Constraints. `StorageErrorKind::Unreachable` displays via this helper, and the virtual-chunk variant reuses it (Task 5).

- [ ] **Step 1: Write failing unit tests**

```rust
#[test] fn unexpected_response_display() {
    let e = StorageErrorKind::Unreachable { host: "abc123.r2.cloudflarestorage.com".into(),
        failure: NetworkFailure::UnexpectedResponse { status: 403, content_type: Some("text/html".into()), snippet: Some("Access blocked".into()) },
        cause: "x".into() };
    assert_eq!(e.to_string(), "could not reach storage at abc123.r2.cloudflarestorage.com: got an unexpected response (HTTP 403, Content-Type: text/html) instead of a storage service response; response begins: \"Access blocked\". This can happen when a firewall or proxy blocks or intercepts traffic to abc123.r2.cloudflarestorage.com. If you are on a corporate or institutional network, ask your network administrator to allow access to this host.");
}
#[test] fn tls_display_adds_ca_sentence() { /* Tls failure: to_string() ends with "its CA certificate may need to be trusted on this machine." */ }
#[test] fn connect_display_has_no_ca_sentence() { /* Connect: contains "could not connect." and not "CA certificate" */ }
#[test] fn snippet_is_capped_and_printable() {
    let mut body = b"<html>\n\t<body>\x00\x07Blocked".to_vec(); body.extend(std::iter::repeat(b'a').take(1000));
    let s = sanitize_snippet(&body).unwrap();
    assert!(s.chars().count() <= 200); assert!(s.starts_with("<html> <body> Blocked"));
    assert!(s.chars().all(|c| !c.is_control()));
}
#[test] fn empty_snippet_is_none() { assert_eq!(sanitize_snippet(b" \n\t"), None); }
#[test] fn content_type_checks() {
    assert!(!is_non_storage_content_type(Some("application/xml; charset=utf-8"), ExpectedBody::Xml));
    assert!(!is_non_storage_content_type(Some("TEXT/XML"), ExpectedBody::Xml));
    assert!(is_non_storage_content_type(Some("text/html"), ExpectedBody::Xml));
    assert!(!is_non_storage_content_type(None, ExpectedBody::Xml));
    assert!(!is_non_storage_content_type(Some("application/json"), ExpectedBody::Json));
}
#[test] fn tls_detected_through_io_error() {
    let io = std::io::Error::new(std::io::ErrorKind::Other, rustls::Error::General("bad".into()));
    assert!(error_chain_has_tls_failure(&io));
    assert!(!error_chain_has_tls_failure(&std::io::Error::from(std::io::ErrorKind::ConnectionRefused)));
}
```

- [ ] **Step 2: Run, expect compile failure** — `cargo test -p icechunk-storage network_failure_tests` (fails because the items are undefined).
- [ ] **Step 3: Implement the interfaces above.** Implement Display with a manual `impl Display for NetworkFailure` for the shape text, and `#[error(...)]` on the variant built from it. A `hint` helper method avoids duplicating the sentence.
- [ ] **Step 4: Add the Python mapping.** Add `STORAGE_UNREACHABLE => "storage-unreachable"` under `// storage / environment` in `codes`, and `K::Unreachable { .. } => class("StorageError", codes::STORAGE_UNREACHABLE)` in `classify_storage`. Then add the `ErrorKind` member.
- [ ] **Step 5: Run.** `cargo test -p icechunk-storage network_failure_tests` should PASS. Then run `cargo check --workspace --all-features` (clean), and `cd icechunk-python && maturin develop && pytest tests/test_exception_tree.py::test_error_kind_enum_in_sync_with_rust` (PASS). If the repo has its own just/Make recipe for building the Python extension, use that.
- [ ] **Step 6: Commit.** `feat(storage): add Unreachable storage error kind with firewall/proxy hint`

---

### Task 2: S3 classifier

**Files:**
- Modify: `icechunk-s3/src/lib.rs` (new pub fns near `mk_client`; unit tests in the existing or a new `#[cfg(test)]` module)

**Interfaces:**
- Consumes: Task 1 items.
- Produces (pub in `icechunk_s3`):
  - `pub fn classify_s3_error<E: ProvideErrorMetadata>(err: &SdkError<E, HttpResponse>) -> Option<NetworkFailure>`. `HttpResponse` is `aws_smithy_runtime_api::http::Response`, the SDK's default `R`. The rules:
    - `DispatchFailure`: if `error_chain_has_tls_failure(err)` then `Tls`. Else if the connector error `is_timeout()` then `Timeout`. Else if `is_io()` then `Connect`. Otherwise `None`.
    - `TimeoutError` → `Timeout`.
    - `ServiceError` / `ResponseError` with a raw response: status 407 → `UnexpectedResponse`. Else if the body is non-empty and `is_non_storage_content_type(ct, Xml)` → `UnexpectedResponse`, with the snippet from `sanitize_snippet(raw.body().bytes())`. Otherwise `None`.
    - Anything else → `None`.
  - `pub fn s3_endpoint_host(opts: &S3Options, bucket: &str) -> String`. Uses the host (and the port, if one is set explicitly) of `opts.endpoint_url` when present. Otherwise `"{bucket}.s3.{region}.amazonaws.com"`, falling back to `"{bucket}.s3.amazonaws.com"` when there's no region. Path-style (`force_path_style`) without an endpoint gives `"s3.{region}.amazonaws.com"`.
  - `pub fn s3_error<E>(err: SdkError<E, HttpResponse>, host: &str) -> StorageError where E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static`. Returns `unreachable_error(host, f, err)` when it classifies, otherwise the existing `obj_store_error(err)`.

- [ ] **Step 1: Write failing unit tests.** Build the errors with `SdkError::service_error(GetObjectError::generic(meta), raw)`, `SdkError::dispatch_failure(ConnectorError::io(..))`, `ConnectorError::timeout(..)` and `SdkError::timeout_error(..)`. Build `raw` with `HttpResponse::new(StatusCode::try_from(403).unwrap(), SdkBody::from(...))` plus headers.
  - `html_403_get_is_unexpected_response`: `Some(UnexpectedResponse { status: 403, content_type: Some("text/html"), snippet: Some(s) })` where `s` starts with the body text
  - `empty_403_is_not_classified`: no body, no content type → `None`
  - `proxy_auth_407_is_unexpected_even_without_body` → `Some(UnexpectedResponse { status: 407, .. })`
  - `xml_service_error_with_code_is_not_classified`: 403, `application/xml`, `<Error><Code>AccessDenied</Code>…` → `None`
  - `xml_content_type_with_params_is_xml`: 503 `application/xml; charset=utf-8` → `None`
  - `html_502_is_unexpected_response` → `Some(UnexpectedResponse { status: 502, .. })`
  - `io_dispatch_failure_is_connect` → `Some(Connect)`; `connector_timeout_is_timeout` and `sdk_timeout_is_timeout` → `Some(Timeout)`
  - `tls_dispatch_failure_is_tls`: `ConnectorError::io(Box::new(io::Error::other(rustls::Error::General(..))))` → `Some(Tls)`
  - `s3_endpoint_host_cases`:
    - endpoint `https://abc123.r2.cloudflarestorage.com` → `abc123.r2.cloudflarestorage.com`
    - `http://127.0.0.1:9000` → `127.0.0.1:9000`
    - no endpoint, region `us-west-2`, bucket `b` → `b.s3.us-west-2.amazonaws.com`
- [ ] **Step 2: Run, expect failure.** `cargo test -p icechunk-s3 classify`
- [ ] **Step 3: Implement the three functions.**
- [ ] **Step 4: Run, expect PASS.** `cargo test -p icechunk-s3 classify`
- [ ] **Step 5: Commit.** `feat(s3): classify connection failures and non-S3 responses`

---

### Task 3: Wire the classifier into S3 repo storage

**Files:**
- Modify: `icechunk-s3/src/lib.rs`. Add a field or method `fn host(&self) -> String` on the S3 storage struct that returns `s3_endpoint_host(&self.config, &self.bucket)`. Replace every terminal `obj_store_error_res(sdk_err)` / `obj_store_error(err)` / `Err(err) => obj_store_error_res(err)` / `.map_err(obj_store_error)` that wraps an `SdkError` with `s3_error(err, &self.host())`. That covers, among others, `head_etag`, `get_object_range_conditional`, the put/copy/delete/list paths, the readback HEAD, and the listing paginator. Existing special-case arms (not-found, 304, precondition, throttling, R2 404) stay first and unchanged.
- Create: `icechunk/tests/test_network_errors.rs`; register it in `icechunk/tests/main.rs` the way the other test modules are.

**Interfaces:**
- Consumes: `s3_error`, `s3_endpoint_host` (Task 2); `StorageErrorKind::Unreachable`, `NetworkFailure` (Task 1).
- Produces, for Task 4/5 tests (in `test_network_errors.rs`):
  - `async fn html_server(status: u16, method: &str, content_type: Option<&str>, body: &str) -> wiremock::MockServer`, which responds to every request with that status, content type and body.
  - `fn s3_at(endpoint: &str, prefix: Option<&str>) -> Arc<dyn Storage + Send + Sync>`, copying the `s3_options` / `static_creds` / `new_s3_storage` pattern from `icechunk/tests/test_attribution.rs`. It sets `Settings` with `connect_timeout_ms`/`operation_timeout_ms` of 2000 and retries kept small, using the existing settings fields, so failing tests finish quickly.
  - `fn expect_unreachable(err: &StorageError) -> (&str, &NetworkFailure)`, which asserts the kind is `Unreachable` and returns its host and failure.

- [ ] **Step 1: Write failing integration tests.** Each test calls `storage.get_object(&ctx, "refs/branch.main/ref.json", None)` (or the equivalent `Storage` read). With `prefix: None`, the first request is the layout-probe HEAD; with `Some("p")`, it's the GET.
  - `get_html_403_is_unexpected_response`: prefix `Some("p")`, server 403 `text/html` `<html>Access blocked by policy</html>`. Expect `UnexpectedResponse { status: 403 }`, a host equal to the server's `host:port`, and an error string containing `"firewall or proxy"` and `"Access blocked by policy"`.
  - `get_html_502_is_unexpected_response`: same setup with 502 → `status: 502`.
  - `head_407_is_unexpected_response`: prefix `None`, server 407, no body → `UnexpectedResponse { status: 407 }`.
  - `head_403_without_body_is_not_unreachable`: prefix `None`, server 403, no body → `!err.kind.is_unreachable()`.
  - `get_404_html_stays_not_found`: prefix `Some("p")`, 404 `text/html` → `StorageErrorKind::ObjectNotFound`.
  - `closed_port_is_connect`: bind a `TcpListener` on `127.0.0.1:0`, take the port, drop the listener, then use endpoint `http://127.0.0.1:{port}` → `Connect`.
  - `unresolvable_host_is_connect`: endpoint `http://storage.invalid` → `Connect`, host `storage.invalid`.
  - `https_to_plain_http_is_tls`: endpoint `https://{mock server host:port}` (wiremock speaks plain HTTP) → `Tls`; the string contains `"CA certificate"`.
  - `hung_server_is_timeout`: a tokio `TcpListener` that accepts connections and holds them open without writing (keep the sockets in a `Vec` inside a spawned task); with the short timeouts → `Timeout`.
- [ ] **Step 2: Run, expect failure.** `cargo test -p icechunk --test main test_network_errors`
- [ ] **Step 3: Wire `s3_error` into the call sites listed under Files.**
- [ ] **Step 4: Run, expect PASS.** `cargo test -p icechunk --test main test_network_errors`
- [ ] **Step 5: Regression check.** `docker compose up -d`, then `cargo test -p icechunk --test main test_storage test_attribution test_key_layout test_flaky_connections`. All should pass unchanged.
- [ ] **Step 6: Commit.** `feat(s3): report unreachable or intercepted storage with host and hint`

---

### Task 4: object_store classifier (GCS) and wiring into ObjectStorage

**Files:**
- Modify: `icechunk-arrow-object-store/src/lib.rs`
- Modify: `icechunk-arrow-object-store/Cargo.toml` if `rustls` access is needed beyond Task 1's helper (it shouldn't be)
- Test: `icechunk/tests/test_network_errors.rs` (append)

**Interfaces:**
- Consumes: Task 1 items.
- Produces:
  - `pub fn classify_object_store_error(err: &object_store::Error) -> Option<NetworkFailure>`. Walk `source()` from `err` until a link downcasts to `object_store::client::HttpError`. If none is found → `None`. If found: when `error_chain_has_tls_failure(http_err)` → `Tls`; else `HttpErrorKind::Connect` → `Connect`; `Timeout` → `Timeout`; any other kind → `None`. There's no `UnexpectedResponse` here, because `RetryError` (which holds status and body) is `pub(crate)` in object_store 0.14.1. That's the spec's GCS fallback.
  - On `trait ObjectStoreBackend`: `fn endpoint_host(&self) -> Option<String> { None }`.
    - GCS backend: the host (and port) of the `google_base_url`/`base_url` config value if set, else `Some("storage.googleapis.com".into())`.
    - The S3 object_store backend: endpoint host, else the AWS default, computed the same way as `s3_endpoint_host`. Reuse it if the crate already depends on `icechunk-s3`; otherwise duplicate the 10 lines and leave a comment pointing at the twin.
    - All other backends keep `None`.
  - `fn os_error(&self, err: object_store::Error) -> StorageError` on `ObjectStorage`. Returns `unreachable_error(host, f, err)` when `endpoint_host()` is `Some` and the classifier matches. Otherwise it produces what the call site produced before (`StorageErrorKind::ObjectStore(Box::new(err))`). Use it at each terminal fallback that today boxes an `object_store::Error` into `ObjectStore`. Typed arms (`NotFound`, `Precondition`, `AlreadyExists`, `NotModified`, throttling) stay first and unchanged.

- [ ] **Step 1: Write failing tests** (GCS via `new_gcs_storage(bucket, Some("p"), Some(GcsCredentials::Anonymous), Some({"google_base_url": url}), vec![], vec![])`):
  - `gcs_closed_port_is_connect`: base URL `http://127.0.0.1:{closed}` → `Unreachable { failure: Connect, host: "127.0.0.1:{port}" }`, and the message contains `"firewall or proxy"`.
  - `gcs_html_403_is_not_unreachable`: base URL = mock server returning 403 `text/html` → `!is_unreachable()`. This documents the fallback.
  - `gcs_default_host_is_googleapis`: unit test in `icechunk-arrow-object-store`. A GCS backend with no base URL → `endpoint_host() == Some("storage.googleapis.com")`.
  - `classify_ignores_non_http_errors`: unit test. `object_store::Error::NotFound { .. }` → `None`.
- [ ] **Step 2: Run, expect failure.** `cargo test -p icechunk --test main test_network_errors::gcs` and `cargo test -p icechunk-arrow-object-store classify`
- [ ] **Step 3: Implement and wire.**
- [ ] **Step 4: Run, expect PASS** (same commands). Then run `cargo test -p icechunk-arrow-object-store` in full (no regressions).
- [ ] **Step 5: Commit.** `feat(object-store): report unreachable GCS storage with host and hint`

---

### Task 5: Virtual chunk fetchers

**Files:**
- Modify: `icechunk/src/virtual_chunks.rs` (`VirtualReferenceErrorKind`, `S3Fetcher`, `ObjectStoreFetcher`, the fetcher constructors)
- Modify: `icechunk-python/src/errors.rs` (classify the new virtual variant)
- Test: `icechunk/tests/test_network_errors.rs` (append)

**Interfaces:**
- Consumes: `classify_s3_error`, `s3_endpoint_host` (Task 2); `classify_object_store_error` (Task 4); Task 1 items.
- Produces:
  - `VirtualReferenceErrorKind::Unreachable { location: String, host: String, failure: NetworkFailure, #[source] cause: Box<dyn Error + Send + Sync> }` with Display `"could not fetch virtual chunk {location}: {StorageErrorKind-equivalent text}"`. The storage text comes from Task 1's `unreachable_message`.
  - `S3Fetcher` stores its `S3Options`. In `fetch_part`'s `other_err` / non-precondition arms, call `classify_s3_error(&e)`. On `Some(f)`, return `Unreachable { location: chunk_location.to_string(), host: s3_endpoint_host(&opts, &bucket_name), failure: f, cause }`. The `PreconditionFailed` and 409/412 arms stay first.
  - `ObjectStoreFetcher` gains `endpoint_host: Option<String>`, set by its constructor: the GCS base-URL host or `storage.googleapis.com` for GCS configs, the endpoint/AWS default for object_store-S3, and `None` otherwise. Its final `Err(err)` arm calls `classify_object_store_error`, which applies only when `endpoint_host` is `Some`. The `Precondition` arm stays first.
  - Python: `K::Unreachable { .. } => class("StorageError", codes::STORAGE_UNREACHABLE)` in the virtual-reference classifier, placed before the `_ =>` arm.

- [ ] **Step 1: Write failing tests.** Follow `read_virtual_chunk` in `icechunk/tests/test_attribution.rs`: a repo on in-memory storage, and an S3 virtual chunk container whose `S3Options` endpoint is the mock server.
  - `virtual_s3_html_403_is_unreachable`: mock returns 403 `text/html` for the chunk GET. The `get_chunk` error's kind chain contains `VirtualReferenceErrorKind::Unreachable { failure: UnexpectedResponse { status: 403, .. }, .. }`, and its string contains the mock's `host:port`, `"virtual chunk"` and `"firewall or proxy"`.
  - `virtual_s3_precondition_still_object_modified`: mock returns 412 with an XML body → still `ObjectModified`.
  - `virtual_gcs_closed_port_is_unreachable`: GCS container with `google_base_url` set to a closed port → `Unreachable { failure: Connect, .. }`.
- [ ] **Step 2: Run, expect failure.** `cargo test -p icechunk --test main test_network_errors::virtual`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run, expect PASS.** Then `cargo test -p icechunk --test main test_virtual_refs` (no regressions; it needs `docker compose up -d`).
- [ ] **Step 5: Commit.** `feat(virtual-chunks): classify unreachable virtual chunk hosts`

---

### Task 6: Python end-to-end test, changelogs, final checks

**Files:**
- Modify: `icechunk-python/tests/test_exception_tree.py`
- Modify: `Changelog.md`, `Changelog.python.md` (an unreleased-section entry each, following the existing format)
- Modify: the Python error docs, only if they list `ErrorKind` values explicitly (`grep -rn "throttled" docs/`)

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Write the test.**

```python
def test_storage_unreachable_closed_port() -> None:
    import socket
    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
    storage = ic.s3_storage(bucket="b", prefix="p", endpoint_url=f"http://127.0.0.1:{port}",
                            allow_http=True, region="us-east-1", access_key_id="k", secret_access_key="s")
    with pytest.raises(ic.StorageError) as exc:
        ic.Repository.open(storage)
    assert exc.value.kind == ic.ErrorKind.STORAGE_UNREACHABLE
    assert f"127.0.0.1:{port}" in str(exc.value)
    assert "firewall or proxy" in str(exc.value)
```

If the default retries make this slow (> ~10 s), pass storage settings that shorten the timeouts, using the same fields as Task 3.

- [ ] **Step 2: Run.** Build the extension, then run `pytest icechunk-python/tests/test_exception_tree.py -k "unreachable or in_sync"`. Expect PASS.
- [ ] **Step 3: Add changelog entries.** One sentence each, e.g. "Storage errors caused by unreachable hosts or by responses that did not come from the storage service (for example a proxy block page) now name the host and suggest checking firewall/proxy settings. In Python they have `kind == ErrorKind.STORAGE_UNREACHABLE`."
- [ ] **Step 4: Full checks.** Run `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings` (or the repo's `just` lint recipe, if there is one), `cargo deny check` (for the new direct `rustls` dependency), the full `cargo test --workspace --all-features` with compose running, and the Python test suite plus `mypy` as configured in the repo.
- [ ] **Step 5: Customer-name scrub.** This is done by the coordinating session, not the implementer, because only that session knows which names to search for. Grep the full branch diff and every commit message for customer identifiers.
- [ ] **Step 6: Commit.** `docs: changelog and Python test for storage-unreachable errors`
