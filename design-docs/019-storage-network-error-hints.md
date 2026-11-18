# Actionable errors for unreachable or intercepted storage

> Working design doc. It will be removed from the PR before merge.

## The problem

When a firewall or proxy blocks or intercepts traffic between Icechunk and
object storage, users get errors that tell them nothing. In one recent case,
a user behind a corporate proxy could open a repository in one bucket but not
another. The failing open produced:

```
icechunk.StorageError: object store error service error: unhandled error: Error
  context: icechunk::refs::fetch_branch with name="main" ...
  |-> object store error service error
  |-> service error
  |-> unhandled error
  `-> Error
```

The error doesn't name the host, the HTTP status, or what went wrong. Users
can't tell "my network blocks this domain" apart from "my credentials are
wrong" or "Icechunk is broken". That turns into long support threads over a
problem that only the user's IT team can fix.

That error comes from the AWS SDK. It got an HTTP response it couldn't parse
as an S3 error, so it returned `SdkError::ServiceError` wrapping an
`Unhandled` error whose message is just "Error". So *something answered*, most
likely a proxy block page or a TLS-intercepting middlebox. The connection
itself didn't fail.

We can't assume any particular proxy's behaviour. Proxies differ: some refuse
the connection, some reset it, some answer with HTML 403/407 pages, and some
re-sign TLS with a corporate CA. The design has to work from what the SDKs
tell us, not from any one proxy's quirks.

## Goals

* When a storage request **cannot get through** or gets a **response that
  isn't a valid storage-service response**, the error states the facts:
  * the **host** Icechunk was trying to reach
  * the **operation** and **object key/prefix**, where available
  * the **failure shape** (see below), with the HTTP status and
    `Content-Type` for unexpected responses
* The error adds a **hedged hint** that a firewall or proxy may be blocking
  or intercepting traffic to that host, and suggests checking with a network
  administrator. Icechunk is the right place for this hint. The wording is
  "this can happen when…", never "your firewall is blocking…".
* Python callers can match these errors on a **stable error code**, so
  downstream tools (e.g. Arraylake) can add their own context, such as the
  full list of domains to allowlist, without parsing strings.
* Backends in scope: **S3**, **S3-compatible R2** (both via `icechunk-s3`),
  and **GCS** (via `icechunk-arrow-object-store`).

## Non-goals

* Azure, plain HTTP, local filesystem, and other object_store backends. They
  can adopt the same kind later.
* A connectivity-check command or any proactive probing.
* Downstream (Arraylake) handling. That's a follow-up once the error code
  exists.

## Anti-goals (would be a failure even if it "works")

* **A firewall hint on real storage errors.** Well-formed `AccessDenied`,
  `NoSuchKey`/404, `PreconditionFailed`, throttling (`Throttled`), the R2
  404/304 special cases, and `NotModified` must behave exactly as they do
  today. A wrong hint sends people to IT for nothing.
* **Changing retry or timeout behaviour, or adding network calls.** This is
  only about classifying and wording the final error.
* **Leaking secrets.** Never include request headers, signed URLs, query
  strings, or credentials in the error. Response body snippets (if included)
  must be short and sanitized.
* **Losing information.** The original SDK error must stay in the
  `source()` chain, so `Debug`/tracing output loses nothing.

## Current state

* `StorageErrorKind` (`icechunk-storage/src/storage.rs`) is an exhaustive
  enum. SDK failures mostly end up in `ObjectStore(Box<dyn Error>)` via
  `obj_store_error` / `obj_store_error_res`, which keep only the SDK's
  `Display` chain.
* `icechunk-s3/src/lib.rs`: each operation matches on `SdkError`, handles a
  few cases (not-found, 304, precondition, throttling, R2 404s via
  `raw_response()`), and sends the rest to `_ => obj_store_error_res(sdk_err)`.
  At that point `sdk_err.raw_response()` still has the status and headers,
  and `DispatchFailure`/`TimeoutError` are still typed, but all of it gets
  flattened. Examples: `get_object_range_conditional`, `head_etag`, the put
  and copy paths, and listing (`.map_err(obj_store_error)`).
* `icechunk-arrow-object-store/src/lib.rs`: GCS errors arrive as
  `object_store::Error` (mostly `Generic { source }` wrapping the retry/HTTP
  error). These carry an HTTP status where there is one, plus an HTTP error
  kind (connect / timeout / request / decode, etc.); the builder should
  confirm the exact API for the pinned `object_store` 0.14.
* Python: `classify_storage` (`icechunk-python/src/errors.rs`) maps each
  `StorageErrorKind` to a `StorageError` class plus a string error code from
  the `codes` table.

## Design

### Failure shapes

A new `StorageErrorKind` variant (name is the builder's call; a sketch is
below) is used when a request fails in one of these ways:

| Shape | Typical causes | Detected from |
|---|---|---|
| **Connect** | DNS failure, connection refused/reset, no route | S3: `SdkError::DispatchFailure` with an IO connector error. GCS: HTTP connect error kind |
| **Timeout** | Silent drop by a firewall | S3: `SdkError::TimeoutError` or a timeout connector error. GCS: timeout error kind |
| **TLS** | Untrusted certificate, typically a re-signing proxy | `source()` chain contains a TLS/certificate error (rustls/hyper). Detection is best-effort; if we can't tell, fall back to Connect |
| **Unexpected response** | Proxy block page (403/407 HTML), captive portal, wrong endpoint | S3: `ServiceError` whose error has no S3 error code (`Unhandled`/`meta().code()` is `None`), or `ResponseError` (unparsable response). GCS: status present but body isn't a GCS JSON error, where detectable |

Anything else (a well-formed service error with a code, throttling, or the
existing not-found/precondition paths) keeps its current handling.

**Unexpected response requires positive evidence.** (My call, made to
protect the anti-goal.) A missing S3 error code isn't enough on its own,
because S3 HEAD responses never have a body: a real 403 AccessDenied or a
301/400 wrong-region reply on `head_object` also comes back `Unhandled` with
no code. So the unexpected-response shape fires only when:

* the status is **407** (Proxy Authentication Required), on any request
  including HEAD; or
* the response has a body whose `Content-Type` is **not** XML (for S3) or
  JSON (for GCS), e.g. `text/html` or `text/plain`. This includes proxy
  502/503/504 HTML pages and HTML bodies on a 200.

A bodiless HEAD error with any other status stays exactly as it is today.

**All S3-compatible endpoints.** (My call.) The classifier applies to every
store reached through `icechunk-s3` (AWS, R2, MinIO, rustfs, Tigris, Ceph,
the Hugging Face gateway, etc.), with no gating by endpoint. The
positive-evidence rule keeps that safe: those stores return XML errors. The
regression tests (below) cover MinIO/rustfs and assert that their ordinary
errors carry no hint.

**GCS fallback.** (My call.) If `object_store` 0.14 doesn't expose the
response `Content-Type`/body for a 4xx/5xx in a way we can inspect without
parsing `Display` text, GCS skips the unexpected-response shape and leaves
those errors as `ObjectStore` with no hint. Connect, timeout and TLS shapes
still apply to GCS. Never classify based on `Display` strings.

Sketch (my call, adjust freely):

```rust
#[error("could not reach storage at {host}: {shape}{hint}")]
StorageUnreachable {
    host: String,
    shape: NetworkFailure,      // Connect | Timeout | Tls | UnexpectedResponse { status, content_type, snippet }
    #[source] cause: Box<dyn Error + Send + Sync>,
}
```

The operation and key are already on the `ICError` context/span trace for
most paths. Where they aren't, the builder may add them to the message.

### Host

The host should be the one actually contacted:

* S3/R2: the configured `endpoint_url` host if set (always the case for R2).
  Otherwise the AWS default for the bucket/region (e.g.
  `<bucket>.s3.<region>.amazonaws.com`); approximating the SDK's endpoint
  resolution is acceptable if documented. If the raw HTTP request is available
  on the error, use its URI host (without the query string).
* GCS: `storage.googleapis.com` or the configured endpoint.

### Message

The facts come first, then the hint. Example for an unexpected response:

```
could not reach storage at abc123.r2.cloudflarestorage.com:
  got an unexpected response (HTTP 403, Content-Type: text/html) instead of an S3 response
  while reading refs/branch.main/ref.json.
This can happen when a firewall or proxy blocks or intercepts traffic to
abc123.r2.cloudflarestorage.com. If you are on a corporate or institutional
network, ask your network administrator to allow access to this host.
```

For the TLS shape, the hint should also mention configuring the proxy's CA
certificate as a common fix (in neutral terms; the exact env var/config
pointer is the builder's call based on what Icechunk's HTTP clients honour).

Snippet: optional, up to ~200 printable characters of the response body,
only for non-XML/non-JSON bodies, and only if available without an extra
read. Never include headers other than `Content-Type` (and possibly
`Server`/`Via`, which are informational and contain no secrets).

### Python

* New code in the `codes` table, e.g. `STORAGE_UNREACHABLE =>
  "storage-unreachable"`, mapped in `classify_storage` under the existing
  `StorageError` class. Same Python exception class, so existing `except
  StorageError` code still works.
* Expose the host and shape on the Python exception if the existing error
  machinery supports structured attributes. Otherwise the code alone is
  enough for now.
* Update the `.pyi` stub and Python error docs if codes are documented there.

### Virtual chunks

**In scope.** Third-party virtual-chunk hosts are among the most likely
targets of a firewall block.

Virtual chunks are *not* fetched through the repo `Storage` backends.
`S3Fetcher` and `ObjectStoreFetcher` in `icechunk/src/virtual_chunks.rs`
call `aws_sdk_s3::Client::get_object` / `object_store` `get_opts` directly.
They share the client construction with the backends (`mk_client`), but each
has its own error mapping, which ends in `VirtualReferenceErrorKind::FetchError`
and the Python code `virtual-chunk-fetch`. That split exists because a
virtual chunk container spans arbitrary buckets and URLs and enforces
per-chunk checksums, which doesn't fit the repo-shaped `Storage` trait.

Design:

* The classifiers live in the backend crates as **public helpers**: one for
  `SdkError` in `icechunk-s3`, one for `object_store::Error` in
  `icechunk-arrow-object-store`. Both the repo storage paths and the
  virtual-chunk fetchers call them. There must be a single implementation of
  the detection rules.
* When a virtual-chunk fetch failure matches a shape, it surfaces with the
  **same Python code, `storage-unreachable`**. The message also says it was
  a virtual chunk and names the chunk's host, which is taken from the
  chunk's URL. How it's represented on the Rust side (a new
  `VirtualReferenceErrorKind` variant wrapping the storage kind, or reusing
  `StorageErrorKind`) is the builder's call. What matters is the Python code
  and the message.
* Existing virtual-chunk semantics are unchanged: checksum/precondition
  failures still produce "virtual chunk modified", not-found stays as it is,
  and so on.
* Tests: at least one simulated shape (e.g. HTML 403) through a virtual
  chunk read, asserting the code and that the message contains the host and
  the hint.

## Testing

Don't depend on any real proxy. Simulate each shape locally in Rust
integration tests against both backends where feasible:

* **Connect:** a closed localhost port. **DNS:** a `.invalid` host.
* **Timeout:** a listener that accepts and never responds (short client
  timeout).
* **TLS:** a local TLS server with a self-signed certificate.
* **Unexpected response:** a tiny HTTP server returning `403` with
  `Content-Type: text/html` and an HTML body. Also a `200` with an HTML body
  for GETs, a `502` HTML page, and a `407` (including on HEAD).
* **Must not hint:** a bodiless `403` on HEAD from the same tiny server
  (simulating a real S3 AccessDenied on HEAD).
* **Regressions:** against the existing compose stores (MinIO/rustfs, the
  fake GCS server), confirm that real `NoSuchKey`, `AccessDenied`,
  precondition failures and throttling are classified exactly as before, with
  no hint.
* **Python:** one test asserting the error code and that the message contains
  the host and the hint.
* Make sure no test fixture or doc mentions a real customer or real
  bucket/account names.

## Left to the builder

* Variant and field names; whether `NetworkFailure` is a separate enum.
* Helper signatures, e.g. `classify_s3_error(sdk_err, host) ->
  Option<NetworkFailure>`, used in place of the scattered
  `_ => obj_store_error_res(sdk_err)` fallbacks and in the virtual-chunk
  fetchers.
* Exact TLS detection technique (downcasting vs. walking `source()`).
* Whether to include the body snippet at all if getting it is awkward.
* Changelog entries (`Changelog.md`, `Changelog.python.md`).
