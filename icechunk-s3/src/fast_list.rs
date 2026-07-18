//! High-throughput object-size listing over raw signed `ListObjectsV2` requests.
//!
//! The AWS SDK's typed paginator materializes a struct per object and drains one
//! stream per prefix. This module instead signs bare `GET` requests with
//! `aws-sigv4` over `reqwest`, then scans each XML page with a `memchr` substring
//! search that touches only `<Size>` — no XML tokenizer, no per-object
//! allocation. Chunk/manifest/snapshot/transaction ids are Crockford base32, so a
//! shardable prefix that spans more than one page is fanned out across the 1024
//! two-character leading prefixes as concurrent disjoint listings; prefixes that
//! fit in a single page are summed with one request.
//!
//! The fan-out is drained by a worker pool whose admission limit adapts at
//! runtime, BBR-startup style. No fixed limit works everywhere: hosts near S3
//! with wide NICs need hundreds of concurrent listings to approach the S3
//! per-partition request ceiling, while consumer connections lose goodput 3-5x
//! past a few dozen — silently, with no 503s and no timeouts, so error-driven
//! adaptation cannot see it. A [`ConcurrencyController`] instead watches goodput
//! (pages/sec): it doubles the limit while each step improves goodput by at
//! least 15%, reverts to the best-seen limit when the curve flattens, and
//! afterwards reacts only to bursts of retryable errors by halving. Individual
//! requests keep their jittered backoff; the controller is a coarser mechanism
//! layered on top, the way BBR coexists with retransmits.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest,
    SigningSettings, UriPathNormalizationMode, sign,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use icechunk_storage::{StorageResult, other_error, s3_config::S3Options};
use memchr::memmem;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior};
use url::Url;

const UNRESERVED: &AsciiSet =
    &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

/// Crockford base32 alphabet, in lexicographic order. Every object id icechunk
/// writes under `chunks`/`manifests`/`snapshots`/`transactions` starts with one
/// of these characters, so the 32 single-character sub-prefixes are disjoint and
/// cover the whole keyspace.
pub(crate) const CROCKFORD: &str = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

const MAX_KEYS: usize = 1000;
const TIMEOUT_SECS: u64 = 120;

/// Worker-pool bounds. `CONCURRENCY_START` is small enough that the narrowest
/// links survive the first measurement window; `CONCURRENCY_CAP` sits past the
/// S3 per-partition request ceiling so wide links are never probe-limited.
const CONCURRENCY_FLOOR: usize = 8;
const CONCURRENCY_START: usize = 16;
const CONCURRENCY_CAP: usize = 1024;

/// Whole runs last from sub-second to a few minutes, so the controller has to
/// converge within seconds: 500ms sampling windows, judged only once they carry
/// enough pages to mean something, but never deferred past ~2s.
const SAMPLE_INTERVAL: Duration = Duration::from_millis(500);
const MIN_JUDGE_PAGES: u64 = 24;
const MAX_WINDOW_SECS: f64 = 2.0;
const GOODPUT_IMPROVEMENT_FACTOR: f64 = 1.15;
const ERROR_BURST_FRACTION: f64 = 0.02;
const BEST_GOODPUT_DECAY: f64 = 0.5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Probing,
    Holding,
}

/// Pure BBR-startup-style governor for the fan-out worker limit.
///
/// Goodput (pages/sec) is the control signal because the failure mode this
/// exists for — congestion collapse on narrow links — emits no error signal at
/// all. The driver in [`SignedLister::sum`] feeds sampled counter deltas
/// through [`Self::observe`]; the struct never reads a clock and spawns
/// nothing, so tests drive it with synthetic windows.
///
/// While probing, the limit doubles as long as each step improves goodput by
/// ≥15%; the first flat or regressed window reverts to the best-seen limit and
/// holds. Holding ignores goodput on purpose — the end-of-run tail drain sinks
/// goodput, and shrinking then would be pointless — reacting only to bursts of
/// retryable errors (>2% of a window's pages) by halving the limit. An
/// error-halving also ends probing rather than restarting it: regrowing toward
/// a throttle ceiling the run just hit would oscillate for the rest of a short
/// run, and the goodput-vs-concurrency plateau is broad enough that the halved
/// limit stays within it.
#[derive(Debug)]
struct ConcurrencyController {
    limit: usize,
    phase: Phase,
    best_goodput: f64,
    best_limit: usize,
    pending_pages: u64,
    pending_errors: u64,
    pending_secs: f64,
    discard_next: bool,
}

impl ConcurrencyController {
    fn new() -> Self {
        Self {
            limit: CONCURRENCY_START,
            phase: Phase::Probing,
            best_goodput: 0.0,
            best_limit: CONCURRENCY_START,
            pending_pages: 0,
            pending_errors: 0,
            pending_secs: 0.0,
            discard_next: false,
        }
    }

    /// Feed one sampling window of counter deltas; returns the limit to apply.
    ///
    /// Windows under [`MIN_JUDGE_PAGES`] pages carry too little signal to judge
    /// and accumulate into the next call, but never past [`MAX_WINDOW_SECS`].
    /// The first judged window after any limit change is discarded outright:
    /// connection warm-up (or drain, after a shrink) makes it unrepresentative
    /// of the new limit, for errors as much as for goodput.
    fn observe(&mut self, pages: u64, errors: u64, secs: f64) -> usize {
        self.pending_pages += pages;
        self.pending_errors += errors;
        self.pending_secs += secs;
        if self.pending_pages < MIN_JUDGE_PAGES && self.pending_secs < MAX_WINDOW_SECS {
            return self.limit;
        }
        let (pages, errors, secs) =
            (self.pending_pages, self.pending_errors, self.pending_secs);
        self.pending_pages = 0;
        self.pending_errors = 0;
        self.pending_secs = 0.0;

        if self.discard_next {
            self.discard_next = false;
            return self.limit;
        }

        if errors as f64 > ERROR_BURST_FRACTION * pages as f64 {
            let halved = (self.limit / 2).max(CONCURRENCY_FLOOR);
            if halved != self.limit {
                self.limit = halved;
                self.discard_next = true;
            }
            self.best_goodput *= BEST_GOODPUT_DECAY;
            self.best_limit = self.limit;
            self.phase = Phase::Holding;
            return self.limit;
        }

        if self.phase == Phase::Probing {
            let goodput = if secs > 0.0 { pages as f64 / secs } else { 0.0 };
            if goodput >= GOODPUT_IMPROVEMENT_FACTOR * self.best_goodput {
                self.best_goodput = goodput;
                self.best_limit = self.limit;
                if self.limit >= CONCURRENCY_CAP {
                    self.phase = Phase::Holding;
                } else {
                    self.limit = (self.limit * 2).min(CONCURRENCY_CAP);
                    self.discard_next = true;
                }
            } else {
                if self.limit != self.best_limit {
                    self.limit = self.best_limit;
                    self.discard_next = true;
                }
                self.phase = Phase::Holding;
            }
        }
        self.limit
    }
}

#[derive(Debug)]
pub(crate) struct SignedLister {
    http: reqwest::Client,
    scheme: String,
    authority: String,
    key_path_prefix: String,
    host_header: String,
    region: String,
    /// Pre-converted once: building an [`Identity`] clones the credential
    /// strings (an STS session token is ~1KB), which is pure waste per request.
    identity: Option<Identity>,
    max_retries: u32,
    /// Counter deltas sampled (and reset) every [`SAMPLE_INTERVAL`] by the
    /// controller driver in [`Self::sum`].
    pages_completed: AtomicU64,
    retryable_errors: AtomicU64,
}

impl SignedLister {
    pub(crate) fn new(
        config: &S3Options,
        bucket: &str,
        region: String,
        creds: Option<Credentials>,
        max_retries: u32,
    ) -> StorageResult<Self> {
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(CONCURRENCY_CAP)
            .timeout(Duration::from_secs(TIMEOUT_SECS))
            .build()
            .map_err(|e| other_error(format!("building reqwest client: {e}")))?;

        let (scheme, endpoint_host) = match config.endpoint_url.as_deref() {
            Some(ep) => {
                let u = Url::parse(ep).map_err(|e| {
                    other_error(format!("parsing endpoint_url {ep:?}: {e}"))
                })?;
                let host = u.host_str().ok_or_else(|| {
                    other_error(format!("endpoint_url {ep:?} has no host"))
                })?;
                let host = match u.port() {
                    Some(p) => format!("{host}:{p}"),
                    None => host.to_string(),
                };
                (u.scheme().to_string(), host)
            }
            None => ("https".to_string(), format!("s3.{region}.amazonaws.com")),
        };

        let (authority, key_path_prefix) = if config.force_path_style {
            (endpoint_host, format!("/{bucket}"))
        } else {
            (format!("{bucket}.{endpoint_host}"), String::new())
        };

        Ok(Self {
            http,
            scheme,
            host_header: authority.clone(),
            authority,
            key_path_prefix,
            region,
            identity: creds.map(Into::into),
            max_retries,
            pages_completed: AtomicU64::new(0),
            retryable_errors: AtomicU64::new(0),
        })
    }

    /// Sum object sizes under `base_prefix`. Shardable prefixes fan out over
    /// the 1024 two-character shards, drained by up to [`CONCURRENCY_CAP`]
    /// workers gated on a watch channel; the sampling loop here feeds the
    /// [`ConcurrencyController`] and publishes its limit to the workers.
    pub(crate) async fn sum(
        self: Arc<Self>,
        base_prefix: &str,
        shardable: bool,
    ) -> StorageResult<u64> {
        if !shardable {
            return self.sum_prefix(base_prefix.to_string()).await;
        }
        // Probe one page first: a prefix that fits in a single response is
        // summed with exactly one request. Only truncated (>1000-object)
        // prefixes pay for the fan-out below.
        let mut probe_buf = Vec::new();
        let (probe_bytes, next_token) =
            self.fetch_page(&format!("{base_prefix}/"), None, &mut probe_buf).await?;
        if next_token.is_none() {
            return Ok(probe_bytes);
        }
        // Ids are >= 2 Crockford characters, so the 1024 two-character
        // sub-prefixes are disjoint and cover the whole keyspace.
        let queue: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(
            CROCKFORD
                .chars()
                .flat_map(|a| {
                    CROCKFORD.chars().map(move |b| format!("{base_prefix}/{a}{b}"))
                })
                .collect(),
        ));

        let (limit_tx, limit_rx) = watch::channel(CONCURRENCY_START);
        let mut workers: JoinSet<StorageResult<u64>> = JoinSet::new();
        for index in 0..CONCURRENCY_CAP {
            workers.spawn(Arc::clone(&self).worker(
                Arc::clone(&queue),
                limit_rx.clone(),
                index,
            ));
        }
        drop(limit_rx);

        let mut controller = ConcurrencyController::new();
        let mut ticker =
            tokio::time::interval_at(Instant::now() + SAMPLE_INTERVAL, SAMPLE_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut last_sample = Instant::now();
        // The fan-out re-lists the whole keyspace, so the truncated probe
        // page's bytes must not be counted a second time.
        let mut total = 0u64;
        let mut draining = false;

        let result = loop {
            tokio::select! {
                joined = workers.join_next() => match joined {
                    None => break Ok(total),
                    Some(Ok(Ok(subtotal))) => {
                        total = total.saturating_add(subtotal);
                        // A worker exits cleanly only after seeing the queue
                        // empty, so parked workers can never be needed again:
                        // admit everyone (they observe the empty queue and
                        // exit) and freeze the limit so no worker re-parks
                        // mid-prefix with no future admission coming.
                        if !draining {
                            draining = true;
                            let _ = limit_tx.send(CONCURRENCY_CAP);
                        }
                    }
                    Some(Ok(Err(e))) => break Err(e),
                    Some(Err(join_error)) => {
                        if join_error.is_panic() {
                            std::panic::resume_unwind(join_error.into_panic());
                        }
                    }
                },
                _ = ticker.tick(), if !draining => {
                    let pages = self.pages_completed.swap(0, Ordering::Relaxed);
                    let errors = self.retryable_errors.swap(0, Ordering::Relaxed);
                    let now = Instant::now();
                    let secs = now.duration_since(last_sample).as_secs_f64();
                    last_sample = now;
                    let limit = controller.observe(pages, errors, secs);
                    if *limit_tx.borrow() != limit {
                        let _ = limit_tx.send(limit);
                    }
                },
            }
        };
        if result.is_err() {
            workers.abort_all();
        }
        while workers.join_next().await.is_some() {}
        result
    }

    /// One fan-out worker: pops shard prefixes and pages through them until the
    /// queue is empty. Admission is re-checked between pages, not prefixes — a
    /// busy shard of a large repo holds hundreds of pages, and a shrink must
    /// take effect after the in-flight request, not minutes later.
    async fn worker(
        self: Arc<Self>,
        queue: Arc<Mutex<VecDeque<String>>>,
        mut limit_rx: watch::Receiver<usize>,
        index: usize,
    ) -> StorageResult<u64> {
        let mut subtotal = 0u64;
        let mut buf = Vec::new();
        loop {
            wait_admitted(index, &mut limit_rx).await;
            let popped = queue
                .lock()
                .map_err(|_| other_error("shard queue mutex poisoned"))?
                .pop_front();
            let Some(prefix) = popped else {
                return Ok(subtotal);
            };
            let mut token: Option<String> = None;
            loop {
                let (bytes, next) =
                    self.fetch_page(&prefix, token.as_deref(), &mut buf).await?;
                subtotal = subtotal.saturating_add(bytes);
                match next {
                    Some(t) => token = Some(t),
                    None => break,
                }
                wait_admitted(index, &mut limit_rx).await;
            }
        }
    }

    async fn sum_prefix(&self, list_prefix: String) -> StorageResult<u64> {
        let mut total = 0u64;
        let mut buf = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let (bytes, next) =
                self.fetch_page(&list_prefix, token.as_deref(), &mut buf).await?;
            total = total.saturating_add(bytes);
            match next {
                Some(t) => token = Some(t),
                None => return Ok(total),
            }
        }
    }

    /// `buf` is caller-owned scratch reused across pages: a ~330KB list page
    /// allocated fresh each time is above glibc's mmap threshold, so per-page
    /// allocation means an mmap/munmap syscall pair per request, serialized on
    /// the process-wide mmap lock across workers.
    async fn fetch_page(
        &self,
        list_prefix: &str,
        token: Option<&str>,
        buf: &mut Vec<u8>,
    ) -> StorageResult<(u64, Option<String>)> {
        let url = self.build_url(list_prefix, token);
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            match self.send_once(&url).await? {
                Ok(mut resp) => {
                    let status = resp.status().as_u16();
                    match status {
                        200 => {
                            buf.clear();
                            while let Some(chunk) = resp.chunk().await.map_err(|e| {
                                other_error(format!("reading S3 list body: {e}"))
                            })? {
                                buf.extend_from_slice(&chunk);
                            }
                            self.pages_completed.fetch_add(1, Ordering::Relaxed);
                            let bytes = scan_sizes(buf);
                            let next = next_token(buf);
                            return Ok((bytes, next));
                        }
                        503 | 429 | 500 | 502 | 504 => {
                            self.retryable_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        other => {
                            return Err(other_error(format!(
                                "S3 list HTTP {other} for {}",
                                redact(&url)
                            )));
                        }
                    }
                }
                Err(e) if e.is_timeout() || e.is_connect() || e.is_request() => {
                    self.retryable_errors.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    return Err(other_error(format!(
                        "S3 list request error for {}: {e}",
                        redact(&url)
                    )));
                }
            }
            if attempt > self.max_retries {
                return Err(other_error(format!(
                    "S3 list giving up after {attempt} attempts for {}",
                    redact(&url)
                )));
            }
            tokio::time::sleep(backoff(attempt)).await;
        }
    }

    async fn send_once(
        &self,
        url: &str,
    ) -> StorageResult<reqwest::Result<reqwest::Response>> {
        let mut rb = self.http.get(url);
        if self.identity.is_some() {
            rb = rb.headers(self.sign_headers(url)?);
        }
        Ok(rb.send().await)
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

    fn sign_headers(&self, url: &str) -> StorageResult<http::HeaderMap> {
        let identity = self
            .identity
            .as_ref()
            .ok_or_else(|| other_error("sign_headers called without credentials"))?;

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

/// Park until `index` is below the published worker limit. A closed channel
/// means the driving [`SignedLister::sum`] is gone (all workers are being torn
/// down); treating it as admission lets the worker run to its natural exit
/// instead of hanging.
async fn wait_admitted(index: usize, limit_rx: &mut watch::Receiver<usize>) {
    while index >= *limit_rx.borrow_and_update() {
        if limit_rx.changed().await.is_err() {
            return;
        }
    }
}

fn fold_digits(xml: &[u8], start: usize) -> (u64, usize) {
    let end = memchr::memchr(b'<', &xml[start..]).map(|r| start + r).unwrap_or(xml.len());
    let mut v = 0u64;
    for &b in &xml[start..end] {
        v = v * 10 + u64::from(b - b'0');
    }
    (v, end)
}

fn scan_sizes(xml: &[u8]) -> u64 {
    let finder = memmem::Finder::new(b"<Size>");
    let mut bytes = 0u64;
    let mut pos = 0usize;
    while let Some(off) = finder.find(&xml[pos..]) {
        let (v, end) = fold_digits(xml, pos + off + 6);
        bytes = bytes.saturating_add(v);
        pos = end;
    }
    bytes
}

fn next_token(xml: &[u8]) -> Option<String> {
    let truncated = memmem::find(xml, b"<IsTruncated>")
        .map(|i| xml[i + 13..].starts_with(b"true"))
        .unwrap_or(false);
    if !truncated {
        return None;
    }
    memmem::find(xml, b"<NextContinuationToken>").map(|i| {
        let s = i + 23;
        let e = memmem::find(&xml[s..], b"</NextContinuationToken>")
            .map(|o| s + o)
            .unwrap_or(s);
        String::from_utf8_lossy(&xml[s..e]).into_owned()
    })
}

fn backoff(attempt: u32) -> Duration {
    let base_ms: u64 = 100;
    let cap_ms: u64 = 30_000;
    let exp = base_ms.saturating_mul(1u64 << attempt.min(20)).min(cap_ms);
    let low = exp / 2;
    let high = exp.max(low + 1);
    Duration::from_millis(rand::random_range(low..=high))
}

fn redact(url: &str) -> String {
    match url.split_once('?') {
        Some((base, _)) => format!("{base}?<query>"),
        None => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crockford_covers_32_sorted_chars() {
        assert_eq!(CROCKFORD.len(), 32);
        let mut sorted: Vec<char> = CROCKFORD.chars().collect();
        let orig = sorted.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, orig);
        for banned in ['I', 'L', 'O', 'U'] {
            assert!(!CROCKFORD.contains(banned));
        }
    }

    #[test]
    fn scan_sizes_sums_only_size_elements() {
        let xml = br#"<ListBucketResult>
            <Contents><Key>chunks/A1</Key><Size>4</Size></Contents>
            <Contents><Key>chunks/B2</Key><Size>6</Size></Contents>
            <Contents><Key>chunks/C3</Key><Size>1</Size></Contents>
        </ListBucketResult>"#;
        assert_eq!(scan_sizes(xml), 11);
    }

    #[test]
    fn next_token_only_when_truncated() {
        let truncated = br#"<ListBucketResult><IsTruncated>true</IsTruncated>
            <NextContinuationToken>abc123</NextContinuationToken></ListBucketResult>"#;
        assert_eq!(next_token(truncated).as_deref(), Some("abc123"));

        let done = br#"<ListBucketResult><IsTruncated>false</IsTruncated>
            <NextContinuationToken>abc123</NextContinuationToken></ListBucketResult>"#;
        assert_eq!(next_token(done), None);
    }

    #[test]
    fn controller_doubles_while_goodput_improves_then_holds_at_cap() {
        let mut c = ConcurrencyController::new();
        let mut limit = CONCURRENCY_START;
        let mut pages = 100u64;
        for _ in 0..6 {
            assert_eq!(c.observe(pages, 0, 0.5), limit * 2);
            limit *= 2;
            assert_eq!(c.observe(pages, 0, 0.5), limit);
            pages *= 2;
        }
        assert_eq!(limit, CONCURRENCY_CAP);
        assert_eq!(c.observe(pages, 0, 0.5), CONCURRENCY_CAP);
        assert_eq!(c.observe(24, 0, 0.5), CONCURRENCY_CAP);
        assert_eq!(c.observe(100_000, 0, 0.5), CONCURRENCY_CAP);
    }

    #[test]
    fn controller_reverts_to_best_when_goodput_flattens() {
        let mut c = ConcurrencyController::new();
        assert_eq!(c.observe(100, 0, 0.5), 32);
        assert_eq!(c.observe(100, 0, 0.5), 32);
        assert_eq!(c.observe(200, 0, 0.5), 64);
        assert_eq!(c.observe(200, 0, 0.5), 64);
        // 420 pages/s is < 15% over the best 400 pages/s: park at 32.
        assert_eq!(c.observe(210, 0, 0.5), 32);
        assert_eq!(c.observe(210, 0, 0.5), 32);
        assert_eq!(c.observe(5_000, 0, 0.5), 32);
        assert_eq!(c.observe(24, 0, 0.5), 32);
    }

    #[test]
    fn controller_reverts_to_best_when_goodput_regresses() {
        let mut c = ConcurrencyController::new();
        assert_eq!(c.observe(100, 0, 0.5), 32);
        assert_eq!(c.observe(100, 0, 0.5), 32);
        assert_eq!(c.observe(200, 0, 0.5), 64);
        assert_eq!(c.observe(200, 0, 0.5), 64);
        assert_eq!(c.observe(50, 0, 0.5), 32);
        assert_eq!(c.observe(400, 0, 0.5), 32);
        assert_eq!(c.observe(400, 0, 0.5), 32);
    }

    #[test]
    fn controller_halves_on_error_burst_down_to_floor() {
        let mut c = ConcurrencyController::new();
        assert_eq!(c.observe(100, 0, 0.5), 32);
        assert_eq!(c.observe(100, 0, 0.5), 32);
        assert_eq!(c.observe(100, 3, 0.5), 16);
        assert_eq!(c.observe(100, 50, 0.5), 16);
        assert_eq!(c.observe(100, 3, 0.5), CONCURRENCY_FLOOR);
        assert_eq!(c.observe(100, 3, 0.5), CONCURRENCY_FLOOR);
        assert_eq!(c.observe(100, 50, 0.5), CONCURRENCY_FLOOR);
        assert_eq!(c.observe(100, 50, 0.5), CONCURRENCY_FLOOR);
    }

    #[test]
    fn controller_ignores_two_percent_error_noise() {
        let mut c = ConcurrencyController::new();
        assert_eq!(c.observe(100, 0, 0.5), 32);
        assert_eq!(c.observe(100, 0, 0.5), 32);
        // Exactly 2% errors is noise, not a burst: probing continues.
        assert_eq!(c.observe(400, 8, 0.5), 64);
    }

    #[test]
    fn controller_accumulates_low_signal_windows() {
        let mut c = ConcurrencyController::new();
        assert_eq!(c.observe(10, 0, 0.5), CONCURRENCY_START);
        assert_eq!(c.observe(10, 0, 0.5), CONCURRENCY_START);
        assert_eq!(c.observe(10, 0, 0.5), 32);
    }

    #[test]
    fn controller_judges_a_starved_window_after_two_seconds() {
        let mut c = ConcurrencyController::new();
        assert_eq!(c.observe(2, 0, 0.6), CONCURRENCY_START);
        assert_eq!(c.observe(2, 0, 0.6), CONCURRENCY_START);
        assert_eq!(c.observe(2, 0, 0.6), CONCURRENCY_START);
        assert_eq!(c.observe(2, 0, 0.6), 32);
    }

    #[test]
    fn controller_discards_first_window_after_a_limit_change() {
        let mut c = ConcurrencyController::new();
        assert_eq!(c.observe(100, 0, 0.5), 32);
        // Collapsed goodput right after the change is warm-up noise: ignored.
        assert_eq!(c.observe(24, 0, 0.5), 32);
        // The same collapse in a judged window reverts to the best limit.
        assert_eq!(c.observe(24, 0, 0.5), 16);
    }
}
