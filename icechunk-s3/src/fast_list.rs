//! S3 page fetcher for the shared fast-listing engine
//! ([`icechunk_storage::fast_list`]).
//!
//! The AWS SDK's typed paginator materializes a struct per object and drains one
//! stream per prefix. This module instead signs bare `ListObjectsV2` `GET`
//! requests with `aws-sigv4` over `reqwest`, then scans each body chunk as it
//! is decrypted with a `memchr` substring search that touches only `<Size>` —
//! no XML tokenizer, no per-object allocation, no page-sized buffer.
//!
//! Everything above the wire format — the retry loop with jittered backoff, the
//! single-page probe, the Crockford fan-out, and the adaptive worker pool —
//! lives in the provider-neutral engine; [`SignedLister`] only implements one
//! page attempt ([`ListPageFetcher`]): `SigV4` signing, URL building, and
//! streaming the response body through [`PageScan`].

use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use aws_config::default_provider::credentials::default_provider;
use aws_credential_types::Credentials;
use aws_credential_types::provider::ProvideCredentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest,
    SigningSettings, UriPathNormalizationMode, sign,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use chrono::{DateTime, Utc};
use icechunk_storage::fast_list::{
    CONCURRENCY_CAP, ListPageFetcher, PageAttempt, is_transient_status,
};
use icechunk_storage::s3_config::{S3Credentials, S3CredentialsFetcher};
use icechunk_storage::{Settings, StorageResult, other_error, s3_config::S3Options};
use memchr::memmem;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use tokio::sync::{Mutex, OnceCell};
use url::Url;

const UNRESERVED: &AsciiSet =
    &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

const MAX_KEYS: usize = 1000;

/// Fallback whole-request timeout when [`Settings::timeouts`] carries no
/// per-attempt bound.
const DEFAULT_TIMEOUT_SECS: u64 = 120;

/// Re-fetch refreshable credentials this long before their stated expiry so a
/// page is never signed with an identity about to lapse in flight.
const CRED_REFRESH_MARGIN: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub(crate) struct SignedLister {
    http: reqwest::Client,
    scheme: String,
    authority: String,
    key_path_prefix: String,
    host_header: String,
    region: String,
    signer: Signer,
}

impl SignedLister {
    pub(crate) fn new(
        config: &S3Options,
        bucket: &str,
        region: String,
        credentials: &S3Credentials,
        settings: &Settings,
    ) -> StorageResult<Self> {
        let http = build_http_client(settings)?;
        let endpoint = resolve_endpoint(config, &region)?;

        // The AWS SDK addresses buckets that are not virtual-hostable over TLS
        // (dotted names break the `*.s3.<region>.amazonaws.com` wildcard cert)
        // path-style; the fast path must match or it hits a cert mismatch and
        // retry-storms.
        let use_path_style =
            config.force_path_style || !bucket_is_virtual_host_safe(bucket);
        let (authority, bucket_path) = if use_path_style {
            (endpoint.host, format!("/{bucket}"))
        } else {
            (format!("{bucket}.{}", endpoint.host), String::new())
        };
        let key_path_prefix = format!("{}{bucket_path}", endpoint.base_path);

        Ok(Self {
            http,
            scheme: endpoint.scheme,
            host_header: authority.clone(),
            authority,
            key_path_prefix,
            region,
            signer: Signer::new(config, credentials),
        })
    }

    fn build_url(&self, list_prefix: &str, token: Option<&str>) -> String {
        fn push(q: &mut String, k: &str, v: &str) {
            if !q.is_empty() {
                q.push('&');
            }
            q.push_str(k);
            q.push('=');
            q.push_str(&utf8_percent_encode(v, UNRESERVED).to_string());
        }
        let mut q = String::new();
        push(&mut q, "list-type", "2");
        push(&mut q, "prefix", list_prefix);
        push(&mut q, "max-keys", &MAX_KEYS.to_string());
        if let Some(t) = token {
            push(&mut q, "continuation-token", t);
        }
        format!("{}://{}{}?{}", self.scheme, self.authority, self.key_path_prefix, q)
    }

    fn sign_headers(
        &self,
        url: &str,
        identity: &Identity,
    ) -> StorageResult<http::HeaderMap> {
        let mut settings = SigningSettings::default();
        settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
        settings.percent_encoding_mode = PercentEncodingMode::Single;
        settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;

        let signing_params: aws_sigv4::http_request::SigningParams<'_> =
            v4::SigningParams::builder()
                .identity(identity)
                .region(&self.region)
                .name("s3")
                .time(SystemTime::now())
                .settings(settings)
                .build()
                .map_err(|e| other_error(format!("building sigv4 params: {e}")))?
                .into();

        let headers = [("host", self.host_header.as_str())];
        let signable = SignableRequest::new(
            "GET",
            url,
            headers.iter().map(|(k, v)| (*k, *v)),
            SignableBody::Bytes(&[]),
        )
        .map_err(|e| other_error(format!("building signable request: {e}")))?;

        let (instructions, _sig) = sign(signable, &signing_params)
            .map_err(|e| other_error(format!("sigv4 sign: {e}")))?
            .into_parts();

        let mut req = http::Request::builder()
            .method("GET")
            .uri(url)
            .body(())
            .map_err(|e| other_error(format!("building request for signing: {e}")))?;
        instructions.apply_to_request_http1x(&mut req);
        Ok(req.into_parts().0.headers)
    }
}

/// One attempt at one signed `ListObjectsV2` page, scanning the body
/// chunk-by-chunk as it is decrypted: aggregating into a page buffer first
/// re-copies every body byte and then scans it after it has gone cold in cache
/// — together ~25% of CPU on a saturated 2-core run. A mid-body read error
/// discards the partial [`PageScan`] and is retryable, so the engine re-fetches
/// the whole request.
#[async_trait]
impl ListPageFetcher for SignedLister {
    async fn attempt_page(&self, list_prefix: &str, token: Option<&str>) -> PageAttempt {
        match self.send_once(list_prefix, token).await {
            Outcome::Attempt(attempt) => attempt,
            // A mid-run credential rotation fails every in-flight request at
            // once with `ExpiredToken`. That is an auth event, not congestion:
            // surfacing each as Retryable would feed the engine's error-burst
            // signal and permanently halve the shared concurrency for reasons
            // unrelated to store capacity. So force a single-flight refresh and
            // retry once inside this attempt — a refresh that succeeds never
            // reaches the congestion signal. Exactly one in-attempt retry: if
            // the freshly signed request is still expired it is a genuine
            // failure and legitimately surfaces as Retryable.
            Outcome::AuthExpiry { generation, .. } => {
                self.signer.invalidate(generation).await;
                match self.send_once(list_prefix, token).await {
                    Outcome::Attempt(attempt) => attempt,
                    Outcome::AuthExpiry { url, .. } => {
                        PageAttempt::Retryable(other_error(format!(
                            "S3 list credentials still expired after refresh for {}",
                            redact(&url)
                        )))
                    }
                }
            }
        }
    }
}

/// One signed request's outcome: a classified [`PageAttempt`], or an auth-expiry
/// signal that [`SignedLister::attempt_page`] handles by refreshing credentials
/// and retrying once, keeping credential rotation out of the engine's
/// congestion signal.
enum Outcome {
    Attempt(PageAttempt),
    AuthExpiry { generation: u64, url: String },
}

impl SignedLister {
    /// Sign, send, and classify one `ListObjectsV2` request.
    async fn send_once(&self, list_prefix: &str, token: Option<&str>) -> Outcome {
        let url = self.build_url(list_prefix, token);
        // A transient failure resolving refreshable credentials (STS/IMDS blip)
        // is retryable, matching the SDK path that re-resolves transparently.
        let identity = match self.signer.identity().await {
            Ok(identity) => identity,
            Err(e) => return Outcome::Attempt(PageAttempt::Retryable(e)),
        };
        let generation = identity.as_ref().map(|(_, generation)| *generation);

        let mut rb = self.http.get(&url);
        if let Some((identity, _)) = &identity {
            match self.sign_headers(&url, identity) {
                Ok(headers) => rb = rb.headers(headers),
                Err(e) => return Outcome::Attempt(PageAttempt::Fatal(e)),
            }
        }

        match rb.send().await {
            Ok(resp) => self.classify_response(resp, url, generation).await,
            Err(e) if e.is_timeout() || e.is_connect() || e.is_request() => {
                Outcome::Attempt(PageAttempt::Retryable(other_error(format!(
                    "S3 list transport error for {}: {e}",
                    redact(&url)
                ))))
            }
            Err(e) => Outcome::Attempt(PageAttempt::Fatal(other_error(format!(
                "S3 list request error for {}: {e}",
                redact(&url)
            )))),
        }
    }

    async fn classify_response(
        &self,
        mut resp: reqwest::Response,
        url: String,
        generation: Option<u64>,
    ) -> Outcome {
        let status = resp.status().as_u16();
        if status == 200 {
            return Outcome::Attempt(match read_page_body(&mut resp).await {
                Ok(PageEnd::Framed { bytes, next_token }) => {
                    PageAttempt::Page { bytes, next_token }
                }
                Ok(PageEnd::TruncatedWithoutToken { cause, .. }) => {
                    PageAttempt::Retryable(other_error(format!(
                        "S3 list {cause} for {}",
                        redact(&url)
                    )))
                }
                Err(e) => PageAttempt::Retryable(other_error(format!(
                    "S3 list body read error for {}: {e}",
                    redact(&url)
                ))),
            });
        }
        if is_transient_status(status) || is_s3_extra_retryable_status(status) {
            return Outcome::Attempt(PageAttempt::Retryable(other_error(format!(
                "S3 list HTTP {status} for {}",
                redact(&url)
            ))));
        }
        // A signed request whose credentials expired mid-run comes back as a
        // 400 `ExpiredToken` (or a 403 expired-signature). Report it as an auth
        // event so `attempt_page` can refresh and retry once, rather than
        // aborting the sum or surfacing it as congestion.
        if self.signer.can_refresh()
            && is_auth_expiry_status(status)
            && let Some(generation) = generation
            && body_signals_expiry(resp).await
        {
            return Outcome::AuthExpiry { generation, url };
        }
        Outcome::Attempt(PageAttempt::Fatal(other_error(format!(
            "S3 list HTTP {status} for {}",
            redact(&url)
        ))))
    }
}

/// HTTP statuses under which an expired session token / signature surfaces.
fn is_auth_expiry_status(status: u16) -> bool {
    status == 400 || status == 403
}

/// Statuses the fast path retries on top of the shared [`is_transient_status`]
/// set (429 + 5xx), mirroring the SDK client this crate builds, which registers
/// a retry classifier for 408, 429, and 499 (`RETRY_CODES` in `lib.rs`). 429 is
/// already transient; 408 (Request Timeout) and 499 (Tigris "Client Closed
/// Request" — Tigris drives buckets through this fast path via its
/// `t3.storage.dev` endpoint) are the S3-local additions, kept out of
/// [`is_transient_status`] so the portable GCS/Azure fetchers are unaffected.
fn is_s3_extra_retryable_status(status: u16) -> bool {
    status == 408 || status == 499
}

/// S3 error `<Code>`s that mean the request credentials have expired and a
/// refresh is worth retrying with. `ExpiredToken` also matches the
/// `ExpiredTokenException` some S3-compatible stores return.
const EXPIRY_CODES: [&str; 2] = ["ExpiredToken", "TokenRefreshRequired"];

async fn body_signals_expiry(resp: reqwest::Response) -> bool {
    match resp.text().await {
        Ok(body) => EXPIRY_CODES.iter().any(|code| body.contains(code)),
        Err(_) => false,
    }
}

/// Signs [`SignedLister`] requests, holding whatever credential source the
/// backend was configured with. Static and anonymous requests sign from a fixed
/// identity; refreshable and environment credentials are cached with
/// single-flight refresh so a mid-run expiry re-fetches instead of failing the
/// sum ([`DynamicSigner`]).
#[derive(Debug)]
enum Signer {
    Unsigned,
    /// Static keys: an `expires_after` is informational only — there is nothing
    /// to re-fetch — so it is signed from one fixed identity, as the SDK does.
    Fixed(Identity),
    Dynamic(DynamicSigner),
}

impl Signer {
    fn new(config: &S3Options, credentials: &S3Credentials) -> Self {
        if config.anonymous {
            return Signer::Unsigned;
        }
        match credentials {
            S3Credentials::Anonymous => Signer::Unsigned,
            S3Credentials::Static(c) => Signer::Fixed(static_identity(
                c.access_key_id.clone(),
                c.secret_access_key.clone(),
                c.session_token.clone(),
                c.expires_after,
                "icechunk-static",
            )),
            S3Credentials::Refreshable(fetcher) => Signer::Dynamic(DynamicSigner::new(
                CredentialSource::Fetcher(Arc::clone(fetcher)),
            )),
            S3Credentials::FromEnv => Signer::Dynamic(DynamicSigner::new(
                CredentialSource::Env(OnceCell::new()),
            )),
        }
    }

    fn can_refresh(&self) -> bool {
        matches!(self, Signer::Dynamic(_))
    }

    /// The identity to sign the next request with, paired with the cache
    /// generation it came from (for [`Self::invalidate`]). `None` is unsigned.
    async fn identity(&self) -> StorageResult<Option<(Identity, u64)>> {
        match self {
            Signer::Unsigned => Ok(None),
            Signer::Fixed(identity) => Ok(Some((identity.clone(), 0))),
            Signer::Dynamic(dynamic) => dynamic.identity().await.map(Some),
        }
    }

    async fn invalidate(&self, generation: u64) {
        if let Signer::Dynamic(dynamic) = self {
            dynamic.invalidate(generation).await;
        }
    }
}

#[derive(Debug)]
struct DynamicSigner {
    source: CredentialSource,
    cache: Mutex<CredCache>,
}

#[derive(Debug, Default)]
struct CredCache {
    identity: Option<Identity>,
    generation: u64,
}

#[derive(Debug)]
enum CredentialSource {
    Fetcher(Arc<dyn S3CredentialsFetcher>),
    Env(OnceCell<Box<dyn ProvideCredentials>>),
}

impl DynamicSigner {
    fn new(source: CredentialSource) -> Self {
        Self { source, cache: Mutex::new(CredCache::default()) }
    }

    /// Return a live identity, re-fetching under the cache lock if the cached
    /// one is absent or within [`CRED_REFRESH_MARGIN`] of expiry. Holding the
    /// async lock across the fetch is the single-flight: concurrent workers
    /// block on it and reuse whatever the winner fetched instead of stampeding
    /// the credential source.
    async fn identity(&self) -> StorageResult<(Identity, u64)> {
        let mut cache = self.cache.lock().await;
        if let Some(identity) = &cache.identity
            && !expired(identity)
        {
            return Ok((identity.clone(), cache.generation));
        }
        let identity = self.source.fetch().await?;
        cache.generation += 1;
        cache.identity = Some(identity.clone());
        Ok((identity, cache.generation))
    }

    /// Drop the cached identity after an auth-expiry error so the next
    /// [`Self::identity`] re-fetches — but only if no other worker has already
    /// rotated past `used_generation`, so a burst of expiry errors triggers one
    /// refresh, not one per worker.
    async fn invalidate(&self, used_generation: u64) {
        let mut cache = self.cache.lock().await;
        if cache.generation == used_generation {
            cache.identity = None;
        }
    }
}

impl CredentialSource {
    async fn fetch(&self) -> StorageResult<Identity> {
        match self {
            CredentialSource::Fetcher(fetcher) => {
                let c = fetcher.get().await.map_err(|e| {
                    other_error(format!("fetching refreshable S3 credentials: {e}"))
                })?;
                Ok(static_identity(
                    c.access_key_id,
                    c.secret_access_key,
                    c.session_token,
                    c.expires_after,
                    "icechunk-refreshable",
                ))
            }
            CredentialSource::Env(cell) => {
                let provider = cell
                    .get_or_init(|| async {
                        Box::new(default_provider().await) as Box<dyn ProvideCredentials>
                    })
                    .await;
                let creds = provider.provide_credentials().await.map_err(|e| {
                    other_error(format!("resolving default S3 credentials: {e}"))
                })?;
                Ok(creds.into())
            }
        }
    }
}

fn static_identity(
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    expires_after: Option<DateTime<Utc>>,
    provider_name: &'static str,
) -> Identity {
    Credentials::new(
        access_key_id,
        secret_access_key,
        session_token,
        expires_after.map(Into::into),
        provider_name,
    )
    .into()
}

fn expired(identity: &Identity) -> bool {
    identity
        .expiration()
        .is_some_and(|exp| exp <= SystemTime::now() + CRED_REFRESH_MARGIN)
}

/// The resolved list endpoint: transport, host authority, and any base path
/// carried by a path-prefixed endpoint (`https://gateway.example.com/minio`).
struct Endpoint {
    scheme: String,
    host: String,
    base_path: String,
}

fn resolve_endpoint(config: &S3Options, region: &str) -> StorageResult<Endpoint> {
    match endpoint_override(config) {
        Some(ep) => {
            let u = Url::parse(&ep)
                .map_err(|e| other_error(format!("parsing endpoint_url {ep:?}: {e}")))?;
            let host = u
                .host_str()
                .ok_or_else(|| other_error(format!("endpoint_url {ep:?} has no host")))?;
            let host = match u.port() {
                Some(p) => format!("{host}:{p}"),
                None => host.to_string(),
            };
            Ok(Endpoint {
                scheme: u.scheme().to_string(),
                host,
                base_path: u.path().trim_end_matches('/').to_string(),
            })
        }
        None => Ok(Endpoint {
            scheme: "https".to_string(),
            host: format!("s3.{region}.amazonaws.com"),
            base_path: String::new(),
        }),
    }
}

/// The endpoint the SDK client would use: an explicit `endpoint_url`, else the
/// `AWS_ENDPOINT_URL_S3`/`AWS_ENDPOINT_URL` env vars the SDK also honors. Without
/// this the fast path would list `s3.<region>.amazonaws.com` while every other
/// operation went to the env-configured host (MinIO/LocalStack/gov-cloud).
fn endpoint_override(config: &S3Options) -> Option<String> {
    config.endpoint_url.clone().or_else(env_endpoint)
}

fn env_endpoint() -> Option<String> {
    if env_ignore_configured_endpoint_urls() {
        return None;
    }
    ["AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL"]
        .into_iter()
        .find_map(|k| std::env::var(k).ok().filter(|v| !v.is_empty()))
}

/// The SDK consults `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS` before honoring any
/// environment-configured endpoint; when it is truthy the SDK talks to the
/// regional default regardless of `AWS_ENDPOINT_URL[_S3]`. The fast path skips
/// the same env endpoints or it would list a different host than every other
/// operation (silently wrong sums if a same-named bucket exists on the env
/// endpoint, e.g. a localstack running beside real AWS). Parsed exactly as the
/// SDK's `parse_bool`: case-insensitive `true`, nothing else — notably not `1`,
/// unlike the FIPS/dualstack flags [`env_flag_enabled`] handles.
fn env_ignore_configured_endpoint_urls() -> bool {
    std::env::var("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS")
        .is_ok_and(|v| v.eq_ignore_ascii_case("true"))
}

/// Whether the signed-lister fast path can faithfully reproduce the SDK's
/// endpoint for `config`. The SDK also resolves FIPS/dualstack hosts from
/// `AWS_USE_FIPS_ENDPOINT`/`AWS_USE_DUALSTACK_ENDPOINT`, which the fast path does
/// not replicate; when either is requested and no explicit endpoint overrides
/// them, fall back to the portable listing path rather than list the wrong host.
pub(crate) fn signed_list_supported(config: &S3Options) -> bool {
    if endpoint_override(config).is_some() {
        return true;
    }
    !(env_flag_enabled("AWS_USE_FIPS_ENDPOINT")
        || env_flag_enabled("AWS_USE_DUALSTACK_ENDPOINT"))
}

fn env_flag_enabled(key: &str) -> bool {
    std::env::var(key)
        .ok()
        .is_some_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1"))
}

/// Whether `bucket` can be addressed virtual-host style over TLS. A dot is the
/// dominant disqualifier (it breaks the `*.s3.<region>.amazonaws.com` wildcard
/// certificate); the remaining checks are the S3 DNS-compatible bucket rules.
fn bucket_is_virtual_host_safe(bucket: &str) -> bool {
    !bucket.contains('.')
        && (3..=63).contains(&bucket.len())
        && bucket
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !bucket.starts_with('-')
        && !bucket.ends_with('-')
}

fn build_http_client(settings: &Settings) -> StorageResult<reqwest::Client> {
    let mut builder = reqwest::Client::builder().pool_max_idle_per_host(CONCURRENCY_CAP);
    let mut per_attempt = Duration::from_secs(DEFAULT_TIMEOUT_SECS);
    if let Some(timeouts) = settings.timeouts() {
        if let Some(ms) = timeouts.connect_timeout_ms {
            builder = builder.connect_timeout(Duration::from_millis(u64::from(ms)));
        }
        if let Some(ms) = timeouts.read_timeout_ms {
            builder = builder.read_timeout(Duration::from_millis(u64::from(ms)));
        }
        // The engine owns per-page retries, so a single reqwest request is one
        // attempt: the whole-request timeout maps to the per-attempt knob.
        // `operation_timeout_ms` (whole operation across retries) has no reqwest
        // equivalent here and is intentionally not applied.
        if let Some(ms) = timeouts.operation_attempt_timeout_ms {
            per_attempt = Duration::from_millis(u64::from(ms));
        }
    }
    builder
        .timeout(per_attempt)
        .build()
        .map_err(|e| other_error(format!("building reqwest client: {e}")))
}

const SIZE_OPEN: &[u8] = b"<Size>";
const TRUNCATED_TRUE: &[u8] = b"<IsTruncated>true";
const TOKEN_OPEN: &[u8] = b"<NextContinuationToken>";

const PATTERNS: [(&[u8], Pat); 3] =
    [(SIZE_OPEN, Pat::Size), (TRUNCATED_TRUE, Pat::Truncated), (TOKEN_OPEN, Pat::Token)];

static FINDERS: LazyLock<[memmem::Finder<'static>; 3]> =
    LazyLock::new(|| PATTERNS.map(|(pattern, _)| memmem::Finder::new(pattern)));

/// A match can straddle a chunk boundary by at most one byte short of the
/// longest pattern, so [`PageScan`] carries this many trailing bytes between
/// chunks.
const TAIL_MAX: usize = TOKEN_OPEN.len() - 1;

/// Corruption guard on token capture: real continuation tokens are well under
/// 1KB, so a longer capture means a scrambled body. The capture is abandoned
/// and the token treated as absent.
const TOKEN_MAX: usize = 8 * 1024;

#[derive(Debug, Clone, Copy)]
enum Pat {
    Size,
    Truncated,
    Token,
}

#[derive(Debug, Default)]
enum Mode {
    #[default]
    Normal,
    Digits(u64),
    Token(Vec<u8>),
}

/// Incremental scanner for one `ListObjectsV2` XML page, fed the response body
/// chunk by chunk so each chunk is scanned while still hot in cache and no
/// page-sized buffer is ever aggregated. Matches may straddle chunk boundaries
/// anywhere, down to one-byte chunks, and the element order within the page is
/// not assumed (S3, `MinIO`, rustfs, and R2 order `IsTruncated`,
/// `NextContinuationToken`, and `Contents` differently).
///
/// `Digits`/`Token` modes carry a partially consumed element value across the
/// boundary explicitly. In `Normal` mode the seam is covered by keeping the
/// last [`TAIL_MAX`] raw bytes of prior data: a straddling match must start in
/// that tail and end within the first [`TAIL_MAX`] bytes of the next chunk, so
/// scanning `tail ++ chunk-prefix` finds every straddle, and requiring the
/// match to end past the tail rejects re-matches of anything a previous chunk
/// already processed. Keeping raw bytes (even ones consumed as element values)
/// is sound because element text cannot contain a literal `<`, so a value byte
/// never starts a pattern; the same assumption as the whole-page scan this
/// replaces (keys cannot contain a literal `<Size>`).
#[derive(Debug, Default)]
struct PageScan {
    mode: Mode,
    total: u64,
    truncated: bool,
    token: Option<Vec<u8>>,
    token_given_up: bool,
    tail: [u8; TAIL_MAX],
    tail_len: usize,
}

impl PageScan {
    fn feed(&mut self, chunk: &[u8]) {
        let pos = match std::mem::replace(&mut self.mode, Mode::Normal) {
            Mode::Normal => self.resume_normal(chunk),
            Mode::Digits(value) => self.eat_digits(value, chunk, 0),
            Mode::Token(buf) => self.eat_token(buf, chunk, 0),
        };
        if matches!(self.mode, Mode::Normal) && pos < chunk.len() {
            self.scan_from(chunk, pos);
        }
        self.push_tail(chunk);
    }

    /// Resolve the scanned page. Digits still open at end of body are flushed
    /// (same as the whole-page scan folding to end of buffer).
    ///
    /// A page claiming truncation (`IsTruncated=true`) but carrying no usable
    /// continuation token is reported as [`PageEnd::TruncatedWithoutToken`], not
    /// silently completed: returning `next_token=None` there would make the
    /// engine treat the shard as finished and under-report the sum. The token is
    /// unusable when it was never seen, abandoned by the [`TOKEN_MAX`] guard, or
    /// still being captured at a cleanly-framed EOF.
    fn finish(self) -> PageEnd {
        let bytes = match &self.mode {
            Mode::Digits(value) => self.total.saturating_add(*value),
            _ => self.total,
        };
        if !self.truncated {
            return PageEnd::Framed { bytes, next_token: None };
        }
        if self.token_given_up {
            return PageEnd::TruncatedWithoutToken {
                bytes,
                cause: "truncated page with an oversized continuation token",
            };
        }
        if matches!(self.mode, Mode::Token(_)) {
            return PageEnd::TruncatedWithoutToken {
                bytes,
                cause: "truncated page with an unterminated continuation token",
            };
        }
        match self.token {
            Some(token) => PageEnd::Framed {
                bytes,
                next_token: Some(String::from_utf8_lossy(&token).into_owned()),
            },
            None => PageEnd::TruncatedWithoutToken {
                bytes,
                cause: "truncated page without a continuation token",
            },
        }
    }

    /// Seam handling on entering a chunk in `Normal` mode: dispatch the match
    /// that started in the tail and completes in this chunk, if any, and
    /// return the chunk position where in-chunk scanning resumes. At most one
    /// straddle can exist per boundary — a straddling match ends inside the
    /// chunk, so no later match can also start in the tail.
    fn resume_normal(&mut self, chunk: &[u8]) -> usize {
        if self.tail_len == 0 {
            return 0;
        }
        let mut seam = [0u8; TAIL_MAX * 2];
        let take = chunk.len().min(TAIL_MAX);
        seam[..self.tail_len].copy_from_slice(&self.tail[..self.tail_len]);
        seam[self.tail_len..self.tail_len + take].copy_from_slice(&chunk[..take]);
        let seam = &seam[..self.tail_len + take];
        for start in 0..self.tail_len {
            for (pattern, kind) in PATTERNS {
                if start + pattern.len() > self.tail_len
                    && seam[start..].starts_with(pattern)
                {
                    let value_at = start + pattern.len() - self.tail_len;
                    return self.on_match(kind, chunk, value_at);
                }
            }
        }
        0
    }

    /// In-chunk scan: repeatedly dispatch the earliest cached pattern match.
    /// Consumed value bytes can never contain a pattern start (element text
    /// has no literal `<`), so a dispatched match invalidates only its own
    /// cached position. The truncated flag and token each resolve at most once
    /// per page, so their finders stop running the moment they are settled —
    /// on real S3 both sit at the top of the document, and rescanning every
    /// later ~64KB chunk for them costs two full extra memmem passes over the
    /// body (measured as a net CPU regression vs the whole-page scan).
    fn scan_from(&mut self, chunk: &[u8], from: usize) {
        let find = |k: usize, at: usize| FINDERS[k].find(&chunk[at..]).map(|o| at + o);
        let mut next = [
            find(0, from),
            if self.truncated { None } else { find(1, from) },
            if self.token.is_some() || self.token_given_up {
                None
            } else {
                find(2, from)
            },
        ];
        loop {
            let mut earliest: Option<(usize, usize)> = None;
            for (k, at) in next.iter().enumerate() {
                if let Some(at) = *at
                    && earliest.is_none_or(|(e, _)| at < e)
                {
                    earliest = Some((at, k));
                }
            }
            let Some((at, k)) = earliest else { return };
            let (pattern, kind) = PATTERNS[k];
            let pos = self.on_match(kind, chunk, at + pattern.len());
            if !matches!(self.mode, Mode::Normal) {
                return;
            }
            next[k] = match kind {
                Pat::Truncated => None,
                Pat::Size => find(k, pos),
                Pat::Token if self.token.is_some() || self.token_given_up => None,
                Pat::Token => find(k, pos),
            };
        }
    }

    fn on_match(&mut self, kind: Pat, chunk: &[u8], value_at: usize) -> usize {
        match kind {
            Pat::Size => self.eat_digits(0, chunk, value_at),
            Pat::Truncated => {
                self.truncated = true;
                value_at
            }
            Pat::Token if self.token.is_none() && !self.token_given_up => {
                self.eat_token(Vec::new(), chunk, value_at)
            }
            Pat::Token => value_at,
        }
    }

    fn eat_digits(&mut self, mut value: u64, chunk: &[u8], start: usize) -> usize {
        let mut at = start;
        while at < chunk.len() && chunk[at].is_ascii_digit() {
            value = value.saturating_mul(10).saturating_add(u64::from(chunk[at] - b'0'));
            at += 1;
        }
        if at < chunk.len() {
            self.total = self.total.saturating_add(value);
        } else {
            self.mode = Mode::Digits(value);
        }
        at
    }

    fn eat_token(&mut self, mut buf: Vec<u8>, chunk: &[u8], start: usize) -> usize {
        let end = memchr::memchr(b'<', &chunk[start..]).map(|o| start + o);
        let value = &chunk[start..end.unwrap_or(chunk.len())];
        if !self.token_given_up {
            if buf.len() + value.len() > TOKEN_MAX {
                self.token_given_up = true;
            } else {
                buf.extend_from_slice(value);
            }
        }
        match end {
            Some(end) => {
                if !self.token_given_up {
                    self.token = Some(buf);
                }
                end
            }
            None => {
                self.mode = Mode::Token(buf);
                chunk.len()
            }
        }
    }

    fn push_tail(&mut self, chunk: &[u8]) {
        if chunk.len() >= TAIL_MAX {
            self.tail.copy_from_slice(&chunk[chunk.len() - TAIL_MAX..]);
            self.tail_len = TAIL_MAX;
        } else {
            let keep = (TAIL_MAX - chunk.len()).min(self.tail_len);
            self.tail.copy_within(self.tail_len - keep..self.tail_len, 0);
            self.tail[keep..keep + chunk.len()].copy_from_slice(chunk);
            self.tail_len = keep + chunk.len();
        }
    }
}

/// How a scanned `ListObjectsV2` page ended.
#[derive(Debug, PartialEq, Eq)]
enum PageEnd {
    /// The page framed cleanly. `next_token` is `Some` only when the page was
    /// truncated and carried a usable continuation token.
    Framed { bytes: u64, next_token: Option<String> },
    /// `IsTruncated=true` but the continuation token was missing, abandoned, or
    /// unterminated. Surfaced as retryable so the sum fails loudly instead of
    /// silently under-reporting.
    TruncatedWithoutToken { bytes: u64, cause: &'static str },
}

/// Reads a 200 response body to completion, scanning each chunk as it arrives.
/// A mid-body error surfaces as `Err` so the attempt can be classified as
/// retryable.
async fn read_page_body(resp: &mut reqwest::Response) -> reqwest::Result<PageEnd> {
    let mut scan = PageScan::default();
    while let Some(chunk) = resp.chunk().await? {
        scan.feed(&chunk);
    }
    Ok(scan.finish())
}

fn redact(url: &str) -> String {
    match url.split_once('?') {
        Some((base, _)) => format!("{base}?<query>"),
        None => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Mutex as StdMutex, MutexGuard};

    use icechunk_storage::TimeoutSettings;
    use icechunk_storage::fast_list::sum;
    use icechunk_storage::s3_config::S3StaticCredentials;
    use serde::{Deserialize, Serialize};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    fn scan_parts(parts: &[&[u8]]) -> PageEnd {
        let mut scan = PageScan::default();
        for part in parts {
            scan.feed(part);
        }
        scan.finish()
    }

    fn scan_whole(doc: &[u8]) -> PageEnd {
        scan_parts(&[doc])
    }

    fn framed(bytes: u64, token: Option<&str>) -> PageEnd {
        PageEnd::Framed { bytes, next_token: token.map(str::to_string) }
    }

    const REALISTIC_PAGE: &[u8] = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
        <Name>my-bucket</Name><Prefix>some/repo/chunks/</Prefix>\
        <KeyCount>3</KeyCount><MaxKeys>1000</MaxKeys>\
        <IsTruncated>false</IsTruncated>\
        <Contents><Key>some/repo/chunks/0A1B2C3D4E5F6G7H</Key>\
        <LastModified>2026-07-01T12:34:56.000Z</LastModified>\
        <ETag>&quot;9bb58f26192e4ba00f01e2e7b136bbd8&quot;</ETag>\
        <Size>1234</Size><StorageClass>STANDARD</StorageClass></Contents>\
        <Contents><Key>some/repo/chunks/8J9K0M1N2P3Q4R5S</Key>\
        <LastModified>2026-07-01T12:34:57.000Z</LastModified>\
        <ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag>\
        <Size>0</Size><StorageClass>STANDARD</StorageClass></Contents>\
        <Contents><Key>some/repo/chunks/6T7V8W9X0Y1Z2A3B</Key>\
        <LastModified>2026-07-01T12:34:58.000Z</LastModified>\
        <ETag>&quot;a3f5905aa2c94b21ac1c08b541e0c4d1&quot;</ETag>\
        <Size>987654321</Size><StorageClass>STANDARD</StorageClass></Contents>\
        </ListBucketResult>";

    const TRUNCATED_PAGE: &[u8] = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
        <Name>my-bucket</Name><Prefix>chunks/</Prefix>\
        <KeyCount>2</KeyCount><MaxKeys>1000</MaxKeys>\
        <IsTruncated>true</IsTruncated>\
        <Contents><Key>chunks/A1</Key><Size>40</Size></Contents>\
        <Contents><Key>chunks/B2</Key><Size>6</Size></Contents>\
        <NextContinuationToken>1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=\
        </NextContinuationToken></ListBucketResult>";

    const TOKEN_NOT_TRUNCATED_PAGE: &[u8] = b"<ListBucketResult>\
        <IsTruncated>false</IsTruncated>\
        <Contents><Key>chunks/A1</Key><Size>11</Size></Contents>\
        <NextContinuationToken>abc123</NextContinuationToken>\
        </ListBucketResult>";

    const TOKEN_BEFORE_CONTENTS_PAGE: &[u8] = b"<ListBucketResult>\
        <Name>b</Name><Prefix>chunks/</Prefix>\
        <NextContinuationToken>opaque+token/123==</NextContinuationToken>\
        <Contents><Key>chunks/A1</Key><Size>7</Size></Contents>\
        <Contents><Key>chunks/B2</Key><Size>9</Size></Contents>\
        <IsTruncated>true</IsTruncated></ListBucketResult>";

    const ENDS_AT_SIZE_CLOSE_PAGE: &[u8] = b"<ListBucketResult>\
        <IsTruncated>false</IsTruncated>\
        <Contents><Key>chunks/A1</Key><Size>77</Size>";

    const EXTREME_SIZES_PAGE: &[u8] = b"<ListBucketResult>\
        <IsTruncated>false</IsTruncated>\
        <Contents><Key>a</Key><Size>0</Size></Contents>\
        <Contents><Key>b</Key><Size>18446744073709551615</Size></Contents>\
        <Contents><Key>c</Key><Size>1000000</Size></Contents>\
        </ListBucketResult>";

    const SMALL_PAGE: &[u8] = b"<R><IsTruncated>true</IsTruncated>\
        <Contents><Size>42</Size></Contents>\
        <NextContinuationToken>tok+7/=</NextContinuationToken></R>";

    const OPEN_DIGITS_AT_EOF_PAGE: &[u8] = b"<Contents><Size>123";

    fn fixtures() -> Vec<(&'static [u8], PageEnd)> {
        vec![
            (REALISTIC_PAGE, framed(1234 + 987_654_321, None)),
            (
                TRUNCATED_PAGE,
                framed(46, Some("1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=")),
            ),
            (TOKEN_NOT_TRUNCATED_PAGE, framed(11, None)),
            (TOKEN_BEFORE_CONTENTS_PAGE, framed(16, Some("opaque+token/123=="))),
            (ENDS_AT_SIZE_CLOSE_PAGE, framed(77, None)),
            (EXTREME_SIZES_PAGE, framed(u64::MAX, None)),
            (SMALL_PAGE, framed(42, Some("tok+7/="))),
            (OPEN_DIGITS_AT_EOF_PAGE, framed(123, None)),
        ]
    }

    #[test]
    fn page_scan_reference_documents() {
        for (doc, expected) in fixtures() {
            assert_eq!(
                scan_whole(doc),
                expected,
                "fixture: {}",
                String::from_utf8_lossy(doc)
            );
        }
    }

    #[test]
    fn page_scan_survives_every_two_way_split() {
        for (doc, expected) in fixtures() {
            for i in 0..=doc.len() {
                assert_eq!(
                    scan_parts(&[&doc[..i], &doc[i..]]),
                    expected,
                    "split at {i} of {}",
                    String::from_utf8_lossy(doc)
                );
            }
            let bytes_one_at_a_time: Vec<&[u8]> = doc.chunks(1).collect();
            assert_eq!(scan_parts(&bytes_one_at_a_time), expected, "1-byte chunks");
        }
    }

    #[test]
    fn page_scan_survives_every_three_way_split() {
        let expected = scan_whole(SMALL_PAGE);
        assert_eq!(expected, framed(42, Some("tok+7/=")));
        for i in 0..=SMALL_PAGE.len() {
            for j in i..=SMALL_PAGE.len() {
                assert_eq!(
                    scan_parts(&[&SMALL_PAGE[..i], &SMALL_PAGE[i..j], &SMALL_PAGE[j..]]),
                    expected,
                    "three-way split at ({i}, {j})"
                );
            }
        }
    }

    #[test]
    fn page_scan_sums_only_size_elements() {
        let xml = br#"<ListBucketResult>
            <Contents><Key>chunks/A1</Key><Size>4</Size></Contents>
            <Contents><Key>chunks/B2</Key><Size>6</Size></Contents>
            <Contents><Key>chunks/C3</Key><Size>1</Size></Contents>
        </ListBucketResult>"#;
        assert_eq!(scan_whole(xml), framed(11, None));
    }

    #[test]
    fn page_scan_token_only_when_truncated() {
        let truncated = br#"<ListBucketResult><IsTruncated>true</IsTruncated>
            <NextContinuationToken>abc123</NextContinuationToken></ListBucketResult>"#;
        assert_eq!(scan_whole(truncated), framed(0, Some("abc123")));

        let done = br#"<ListBucketResult><IsTruncated>false</IsTruncated>
            <NextContinuationToken>abc123</NextContinuationToken></ListBucketResult>"#;
        assert_eq!(scan_whole(done), framed(0, None));
    }

    #[test]
    fn page_scan_gives_up_on_oversized_token() {
        let build = |n: usize| {
            let mut doc =
                b"<IsTruncated>true</IsTruncated><NextContinuationToken>".to_vec();
            doc.extend(std::iter::repeat_n(b'a', n));
            doc.extend_from_slice(b"</NextContinuationToken>");
            doc
        };
        let at_cap = build(TOKEN_MAX);
        assert_eq!(scan_whole(&at_cap), framed(0, Some(&"a".repeat(TOKEN_MAX))));

        // Beyond the cap the token is abandoned; a truncated page then has no
        // usable token and must be flagged rather than reported complete.
        let over_cap = build(TOKEN_MAX + 1);
        assert!(matches!(scan_whole(&over_cap), PageEnd::TruncatedWithoutToken { .. }));
        let parts: Vec<&[u8]> = over_cap.chunks(97).collect();
        assert!(matches!(scan_parts(&parts), PageEnd::TruncatedWithoutToken { .. }));
    }

    #[test]
    fn page_scan_flushes_open_digits_at_eof() {
        assert_eq!(scan_whole(b"<Contents><Size>123"), framed(123, None));
    }

    #[test]
    fn truncated_page_without_usable_token_is_flagged() {
        // IsTruncated=true with a continuation token that is missing entirely,
        // abandoned by the size guard, or unterminated at EOF must surface as an
        // inconsistency across every chunk split — never as a completed page,
        // which would silently under-report the sum.
        let missing =
            b"<IsTruncated>true</IsTruncated><Contents><Size>5</Size></Contents>"
                .to_vec();
        let unterminated =
            b"<IsTruncated>true</IsTruncated><NextContinuationToken>abc".to_vec();
        let mut abandoned =
            b"<IsTruncated>true</IsTruncated><NextContinuationToken>".to_vec();
        abandoned.extend(std::iter::repeat_n(b'a', TOKEN_MAX + 1));
        abandoned.extend_from_slice(b"</NextContinuationToken>");

        for doc in [&missing, &unterminated, &abandoned] {
            assert!(
                matches!(scan_whole(doc), PageEnd::TruncatedWithoutToken { .. }),
                "whole: {}",
                String::from_utf8_lossy(doc)
            );
            for i in 0..=doc.len() {
                assert!(
                    matches!(
                        scan_parts(&[&doc[..i], &doc[i..]]),
                        PageEnd::TruncatedWithoutToken { .. }
                    ),
                    "split {i}: {}",
                    String::from_utf8_lossy(doc)
                );
            }
        }
    }

    fn lister_for(
        base: &str,
        credentials: &S3Credentials,
        settings: &Settings,
    ) -> SignedLister {
        let config = S3Options::default()
            .with_endpoint_url(base)
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_force_path_style(true);
        SignedLister::new(
            &config,
            "test-bucket",
            "us-east-1".to_string(),
            credentials,
            settings,
        )
        .unwrap()
    }

    #[test]
    fn dotted_bucket_uses_path_style() {
        let config = S3Options::default()
            .with_endpoint_url("https://s3.us-east-1.amazonaws.com")
            .with_region("us-east-1");
        let creds = S3Credentials::Anonymous;

        let dotted = SignedLister::new(
            &config,
            "my.dotted.bucket",
            "us-east-1".into(),
            &creds,
            &Settings::default(),
        )
        .unwrap();
        let url = dotted.build_url("chunks/", None);
        assert!(
            url.contains("s3.us-east-1.amazonaws.com/my.dotted.bucket"),
            "url: {url}"
        );
        assert!(!url.contains("my.dotted.bucket.s3"), "url: {url}");

        let plain = SignedLister::new(
            &config,
            "plainbucket",
            "us-east-1".into(),
            &creds,
            &Settings::default(),
        )
        .unwrap();
        let url = plain.build_url("chunks/", None);
        assert!(url.contains("plainbucket.s3.us-east-1.amazonaws.com"), "url: {url}");
    }

    #[test]
    fn endpoint_path_prefix_is_preserved_in_requests() {
        let config = S3Options::default()
            .with_endpoint_url("http://gateway.example.com/minio")
            .with_region("us-east-1");
        let creds = S3Credentials::Anonymous;

        let path_style = SignedLister::new(
            &config.clone().with_force_path_style(true),
            "test-bucket",
            "us-east-1".into(),
            &creds,
            &Settings::default(),
        )
        .unwrap();
        let url = path_style.build_url("chunks/", None);
        assert!(url.contains("gateway.example.com/minio/test-bucket?"), "url: {url}");

        let vhost = SignedLister::new(
            &config,
            "test-bucket",
            "us-east-1".into(),
            &creds,
            &Settings::default(),
        )
        .unwrap();
        let url = vhost.build_url("chunks/", None);
        assert!(url.contains("test-bucket.gateway.example.com/minio?"), "url: {url}");
    }

    #[test]
    fn signature_covers_the_endpoint_path() {
        // SigV4 signs the canonical URI, so signing two URLs that differ only in
        // path must produce different signatures — proof the base path is signed.
        let lister = lister_for(
            "http://gateway.example.com/minio",
            &S3Credentials::Anonymous,
            &Settings::default(),
        );
        let identity = static_identity(
            "AKIDEXAMPLE".to_string(),
            "SECRETKEY".to_string(),
            None,
            None,
            "test",
        );
        let with_path = lister
            .sign_headers(
                "http://gateway.example.com/minio/test-bucket?list-type=2",
                &identity,
            )
            .unwrap();
        let without_path = lister
            .sign_headers("http://gateway.example.com/test-bucket?list-type=2", &identity)
            .unwrap();
        assert_ne!(with_path.get("authorization"), without_path.get("authorization"));
    }

    #[test]
    fn bucket_virtual_host_rules() {
        for ok in ["plainbucket", "a-b-c", "abc", "bucket123"] {
            assert!(bucket_is_virtual_host_safe(ok), "{ok} should be vhost-safe");
        }
        for bad in ["has.dot", "ab", "-lead", "trail-", "UPPER", "under_score"] {
            assert!(!bucket_is_virtual_host_safe(bad), "{bad} should not be vhost-safe");
        }
    }

    static ENV_LOCK: StdMutex<()> = StdMutex::new(());

    fn env_lock() -> MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Overwrite `key` (or remove it when `value` is `None`).
    #[expect(
        unsafe_code,
        reason = "env mutation is unsafe in edition 2024; all env-mutating \
                  tests are serialized behind ENV_LOCK so no concurrent env \
                  access races this"
    )]
    fn write_env(key: &str, value: Option<&str>) {
        match value {
            // SAFETY: serialized by ENV_LOCK; no other thread touches the env.
            Some(v) => unsafe { std::env::set_var(key, v) },
            // SAFETY: serialized by ENV_LOCK; no other thread touches the env.
            None => unsafe { std::env::remove_var(key) },
        }
    }

    struct EnvVarGuard {
        key: String,
        prev: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &str, value: &str) -> Self {
            let prev = std::env::var(key).ok();
            write_env(key, Some(value));
            Self { key: key.to_string(), prev }
        }

        fn remove(key: &str) -> Self {
            let prev = std::env::var(key).ok();
            write_env(key, None);
            Self { key: key.to_string(), prev }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            write_env(&self.key, self.prev.as_deref());
        }
    }

    #[test]
    fn env_endpoint_override_is_honored() {
        let _lock = env_lock();
        let _e = EnvVarGuard::set("AWS_ENDPOINT_URL_S3", "https://minio.internal:9000");
        let _e2 = EnvVarGuard::remove("AWS_ENDPOINT_URL");
        let _ignore = EnvVarGuard::remove("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS");

        let config = S3Options::default().with_region("us-east-1");
        let lister = SignedLister::new(
            &config,
            "test-bucket",
            "us-east-1".into(),
            &S3Credentials::Anonymous,
            &Settings::default(),
        )
        .unwrap();
        let url = lister.build_url("chunks/", None);
        assert!(url.contains("minio.internal:9000"), "url: {url}");
        assert!(!url.contains("amazonaws.com"), "url: {url}");
    }

    #[test]
    fn env_endpoint_skipped_when_ignore_flag_set() {
        let _lock = env_lock();
        let _e = EnvVarGuard::set("AWS_ENDPOINT_URL_S3", "https://minio.internal:9000");
        let _e2 = EnvVarGuard::remove("AWS_ENDPOINT_URL");

        let config = S3Options::default().with_region("us-east-1");
        let list_url = || {
            SignedLister::new(
                &config,
                "test-bucket",
                "us-east-1".into(),
                &S3Credentials::Anonymous,
                &Settings::default(),
            )
            .unwrap()
            .build_url("chunks/", None)
        };

        // Truthy (case-insensitive `true`) suppresses the env endpoint and the
        // fast path lists the regional default, exactly as the SDK does.
        for truthy in ["true", "TRUE", "True"] {
            let _ignore = EnvVarGuard::set("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", truthy);
            let url = list_url();
            assert!(url.contains("s3.us-east-1.amazonaws.com"), "{truthy}: {url}");
            assert!(!url.contains("minio.internal"), "{truthy}: {url}");
        }

        // The SDK's parse_bool accepts only `true`/`false`, so `1` (and other
        // non-boolean values) do not suppress the env endpoint.
        for non_truthy in ["1", "false", "yes"] {
            let _ignore =
                EnvVarGuard::set("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", non_truthy);
            let url = list_url();
            assert!(url.contains("minio.internal:9000"), "{non_truthy}: {url}");
        }

        // An explicit endpoint set in config (not via env) is unaffected.
        let _ignore = EnvVarGuard::set("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "true");
        let explicit = S3Options::default()
            .with_endpoint_url("https://explicit.example.com")
            .with_region("us-east-1");
        let url = SignedLister::new(
            &explicit,
            "test-bucket",
            "us-east-1".into(),
            &S3Credentials::Anonymous,
            &Settings::default(),
        )
        .unwrap()
        .build_url("chunks/", None);
        assert!(url.contains("explicit.example.com"), "url: {url}");
    }

    #[test]
    fn signed_list_supported_falls_back_for_fips_and_dualstack() {
        let _lock = env_lock();
        let _clear_ep = EnvVarGuard::remove("AWS_ENDPOINT_URL_S3");
        let _clear_ep2 = EnvVarGuard::remove("AWS_ENDPOINT_URL");
        let _clear_fips = EnvVarGuard::remove("AWS_USE_FIPS_ENDPOINT");
        let _clear_dual = EnvVarGuard::remove("AWS_USE_DUALSTACK_ENDPOINT");

        let config = S3Options::default().with_region("us-east-1");
        assert!(signed_list_supported(&config));

        {
            let _f = EnvVarGuard::set("AWS_USE_FIPS_ENDPOINT", "true");
            assert!(!signed_list_supported(&config));
        }
        {
            let _d = EnvVarGuard::set("AWS_USE_DUALSTACK_ENDPOINT", "1");
            assert!(!signed_list_supported(&config));
        }
        // An explicit endpoint overrides FIPS/dualstack, as the SDK resolver does.
        {
            let _f = EnvVarGuard::set("AWS_USE_FIPS_ENDPOINT", "true");
            let with_endpoint = S3Options::default()
                .with_endpoint_url("https://s3.example.com")
                .with_region("us-east-1");
            assert!(signed_list_supported(&with_endpoint));
        }
    }

    const EXPIRED_TOKEN_BODY: &str = "<?xml version=\"1.0\"?><Error>\
        <Code>ExpiredToken</Code><Message>The token has expired.</Message></Error>";

    fn ok_page(size: u64) -> String {
        format!(
            "<ListBucketResult><IsTruncated>false</IsTruncated>\
             <Contents><Size>{size}</Size></Contents></ListBucketResult>"
        )
    }

    /// Drain one HTTP/1.1 request head off `sock` (up to the blank line), so the
    /// client's send completes before the response is written. Returns whether a
    /// request was seen.
    async fn drain_request(sock: &mut tokio::net::TcpStream) -> bool {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 2048];
        loop {
            let Ok(n) = sock.read(&mut tmp).await else { return false };
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() > 64 * 1024 {
                break;
            }
        }
        !buf.is_empty()
    }

    /// Minimal HTTP/1.1 server for the fast-list fetcher. `handler` produces each
    /// response as `(status, body)`; `delay` is applied before writing so timeout
    /// behavior is testable. Returns the base URL.
    fn spawn_fake_s3<H>(delay: Duration, handler: H) -> String
    where
        H: Fn() -> (u16, String) + Send + Sync + 'static,
    {
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        std_listener.set_nonblocking(true).unwrap();
        let addr = std_listener.local_addr().unwrap();
        let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();
        let handler = Arc::new(handler);
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { break };
                let handler = Arc::clone(&handler);
                tokio::spawn(async move {
                    if !drain_request(&mut sock).await {
                        return;
                    }
                    let (status, body) = handler();
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    let resp = format!(
                        "HTTP/1.1 {status} S\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        format!("http://{addr}")
    }

    /// Counts credential fetches through a per-instance handle, so tests running
    /// in parallel do not race a shared counter. The `fetches` field is skipped
    /// by serde (it is only meaningful in-process) so the typetag `S3Credentials`
    /// serialization the trait requires still round-trips.
    #[derive(Debug, Default, Serialize, Deserialize)]
    struct CountingFetcher {
        #[serde(skip)]
        fetches: Arc<AtomicUsize>,
    }

    impl CountingFetcher {
        fn new() -> (Arc<Self>, Arc<AtomicUsize>) {
            let fetches = Arc::new(AtomicUsize::new(0));
            (Arc::new(Self { fetches: Arc::clone(&fetches) }), fetches)
        }
    }

    #[async_trait]
    #[typetag::serde(name = "icechunk-s3-fastlist-test-fetcher")]
    impl S3CredentialsFetcher for CountingFetcher {
        async fn get(&self) -> Result<S3StaticCredentials, String> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            Ok(S3StaticCredentials {
                access_key_id: "AKIDEXAMPLE".to_string(),
                secret_access_key: "SECRETKEY".to_string(),
                session_token: Some("SESSIONTOKEN".to_string()),
                expires_after: None,
            })
        }
    }

    #[tokio::test]
    async fn refreshable_credentials_survive_mid_run_expiry() {
        let seen = Arc::new(AtomicUsize::new(0));
        let handler_seen = Arc::clone(&seen);
        let base = spawn_fake_s3(Duration::ZERO, move || {
            if handler_seen.fetch_add(1, Ordering::SeqCst) == 0 {
                (400, EXPIRED_TOKEN_BODY.to_string())
            } else {
                (200, ok_page(42))
            }
        });

        let (fetcher, fetches) = CountingFetcher::new();
        let creds = S3Credentials::Refreshable(fetcher);
        let lister = lister_for(&base, &creds, &Settings::default());

        // The expiry is refreshed and retried inside the single attempt, so it
        // resolves to a Page and never surfaces as Retryable — an ExpiredToken
        // burst at a credential rotation must not reach the engine's congestion
        // signal and halve the shared concurrency.
        let attempt = lister.attempt_page("chunks/", None).await;
        assert!(
            matches!(attempt, PageAttempt::Page { bytes: 42, .. }),
            "expiry must refresh-and-retry to a Page, got: {attempt:?}"
        );
        // Two requests inside the one attempt (the doomed first, the refreshed
        // retry) and two fetches (signing the first, re-resolving after expiry):
        // the refresh is single-flight, not once per in-flight request.
        assert_eq!(seen.load(Ordering::SeqCst), 2);
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn refresh_failure_surfaces_retryable() {
        let seen = Arc::new(AtomicUsize::new(0));
        let handler_seen = Arc::clone(&seen);
        let base = spawn_fake_s3(Duration::ZERO, move || {
            handler_seen.fetch_add(1, Ordering::SeqCst);
            (400, EXPIRED_TOKEN_BODY.to_string())
        });

        let (fetcher, fetches) = CountingFetcher::new();
        let creds = S3Credentials::Refreshable(fetcher);
        let lister = lister_for(&base, &creds, &Settings::default());

        // The in-attempt refresh gets exactly one retry; a retry that is still
        // expired is a genuine failure and legitimately surfaces as Retryable
        // (which does count toward the congestion signal).
        let attempt = lister.attempt_page("chunks/", None).await;
        assert!(
            matches!(attempt, PageAttempt::Retryable(_)),
            "a refresh that stays expired must surface Retryable, got: {attempt:?}"
        );
        // Exactly one in-attempt retry, no loop: two requests, two fetches.
        assert_eq!(seen.load(Ordering::SeqCst), 2);
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn transient_507_is_retried_then_succeeds() {
        let seen = Arc::new(AtomicUsize::new(0));
        let handler_seen = Arc::clone(&seen);
        let base = spawn_fake_s3(Duration::ZERO, move || {
            if handler_seen.fetch_add(1, Ordering::SeqCst) == 0 {
                (507, "<Error><Code>InsufficientStorage</Code></Error>".to_string())
            } else {
                (200, ok_page(9))
            }
        });

        let lister = lister_for(&base, &S3Credentials::Anonymous, &Settings::default());
        let total =
            sum(Arc::new(lister) as Arc<dyn ListPageFetcher>, "chunks", false, 3).await;

        assert_eq!(total.ok(), Some(9));
        assert!(seen.load(Ordering::SeqCst) >= 2, "the 507 must have been retried");
    }

    #[tokio::test]
    async fn transient_499_is_retried_then_succeeds() {
        // 499 ("Client Closed Request") is fatal in the shared classifier but
        // the SDK client this crate builds retries it (Tigris occasionally
        // sends it); the fast path must match.
        let seen = Arc::new(AtomicUsize::new(0));
        let handler_seen = Arc::clone(&seen);
        let base = spawn_fake_s3(Duration::ZERO, move || {
            if handler_seen.fetch_add(1, Ordering::SeqCst) == 0 {
                (499, "<Error><Code>ClientClosedRequest</Code></Error>".to_string())
            } else {
                (200, ok_page(21))
            }
        });

        let lister = lister_for(&base, &S3Credentials::Anonymous, &Settings::default());
        let total =
            sum(Arc::new(lister) as Arc<dyn ListPageFetcher>, "chunks", false, 3).await;

        assert_eq!(total.ok(), Some(21));
        assert!(seen.load(Ordering::SeqCst) >= 2, "the 499 must have been retried");
    }

    #[tokio::test]
    async fn configured_attempt_timeout_is_applied() {
        let base = spawn_fake_s3(Duration::from_secs(30), || (200, ok_page(1)));
        let settings = Settings {
            timeouts: Some(TimeoutSettings {
                operation_attempt_timeout_ms: Some(50),
                ..Default::default()
            }),
            ..Default::default()
        };
        let lister = lister_for(&base, &S3Credentials::Anonymous, &settings);
        let attempt = lister.attempt_page("chunks/", None).await;
        assert!(
            matches!(attempt, PageAttempt::Retryable(_)),
            "a 50ms per-attempt timeout against a 30s-slow server must trip: {attempt:?}"
        );
    }
}
