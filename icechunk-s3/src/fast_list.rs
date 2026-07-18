//! High-throughput object-size listing over raw signed `ListObjectsV2` requests.
//!
//! The AWS SDK's typed paginator materializes a struct per object and drains one
//! stream per prefix. This module instead signs bare `GET` requests with
//! `aws-sigv4` over `reqwest`, then scans each body chunk as it is decrypted
//! with a `memchr` substring search that touches only `<Size>` — no XML
//! tokenizer, no per-object allocation, no page-sized buffer.
//! Chunk/manifest/snapshot/transaction ids are Crockford base32, so a shardable
//! prefix that spans more than one page is fanned out across the 1024
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
use std::sync::{Arc, LazyLock, Mutex};
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
        let (probe_bytes, next_token) =
            self.fetch_page(&format!("{base_prefix}/"), None).await?;
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
                let (bytes, next) = self.fetch_page(&prefix, token.as_deref()).await?;
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
        let mut token: Option<String> = None;
        loop {
            let (bytes, next) = self.fetch_page(&list_prefix, token.as_deref()).await?;
            total = total.saturating_add(bytes);
            match next {
                Some(t) => token = Some(t),
                None => return Ok(total),
            }
        }
    }

    /// Fetch one list page, scanning the body chunk-by-chunk as it is
    /// decrypted: aggregating into a page buffer first re-copies every body
    /// byte and then scans it after it has gone cold in cache — together ~25%
    /// of CPU on a saturated 2-core run. A mid-body read error discards the
    /// partial [`PageScan`] and retries the whole request; a page counts as
    /// completed only once its body has been scanned to the end.
    async fn fetch_page(
        &self,
        list_prefix: &str,
        token: Option<&str>,
    ) -> StorageResult<(u64, Option<String>)> {
        let url = self.build_url(list_prefix, token);
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            match self.send_once(&url).await? {
                Ok(mut resp) => {
                    let status = resp.status().as_u16();
                    match status {
                        200 => match read_page_body(&mut resp).await {
                            Ok(page) => {
                                self.pages_completed.fetch_add(1, Ordering::Relaxed);
                                return Ok(page);
                            }
                            Err(_) => {
                                self.retryable_errors.fetch_add(1, Ordering::Relaxed);
                            }
                        },
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

    /// Digits still open at end of body are flushed (same as the whole-page
    /// scan folding to end of buffer); a token capture still open is malformed
    /// and treated as absent.
    fn finish(self) -> (u64, Option<String>) {
        let mut total = self.total;
        if let Mode::Digits(value) = self.mode {
            total = total.saturating_add(value);
        }
        let token = if self.truncated {
            self.token.map(|t| String::from_utf8_lossy(&t).into_owned())
        } else {
            None
        };
        (total, token)
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

/// Reads a 200 response body to completion, scanning each chunk as it arrives.
/// A mid-body error surfaces as `Err` so [`SignedLister::fetch_page`] can
/// retry the whole request.
async fn read_page_body(
    resp: &mut reqwest::Response,
) -> reqwest::Result<(u64, Option<String>)> {
    let mut scan = PageScan::default();
    while let Some(chunk) = resp.chunk().await? {
        scan.feed(&chunk);
    }
    Ok(scan.finish())
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

    fn scan_parts(parts: &[&[u8]]) -> (u64, Option<String>) {
        let mut scan = PageScan::default();
        for part in parts {
            scan.feed(part);
        }
        scan.finish()
    }

    fn scan_whole(doc: &[u8]) -> (u64, Option<String>) {
        scan_parts(&[doc])
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

    const OPEN_TOKEN_AT_EOF_PAGE: &[u8] =
        b"<IsTruncated>true</IsTruncated><NextContinuationToken>abc";

    fn fixtures() -> Vec<(&'static [u8], u64, Option<&'static str>)> {
        vec![
            (REALISTIC_PAGE, 1234 + 987_654_321, None),
            (TRUNCATED_PAGE, 46, Some("1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=")),
            (TOKEN_NOT_TRUNCATED_PAGE, 11, None),
            (TOKEN_BEFORE_CONTENTS_PAGE, 16, Some("opaque+token/123==")),
            (ENDS_AT_SIZE_CLOSE_PAGE, 77, None),
            (EXTREME_SIZES_PAGE, u64::MAX, None),
            (SMALL_PAGE, 42, Some("tok+7/=")),
            (OPEN_DIGITS_AT_EOF_PAGE, 123, None),
            (OPEN_TOKEN_AT_EOF_PAGE, 0, None),
        ]
    }

    #[test]
    fn page_scan_reference_documents() {
        for (doc, bytes, token) in fixtures() {
            assert_eq!(
                scan_whole(doc),
                (bytes, token.map(str::to_string)),
                "fixture: {}",
                String::from_utf8_lossy(doc)
            );
        }
    }

    #[test]
    fn page_scan_survives_every_two_way_split() {
        for (doc, bytes, token) in fixtures() {
            let expected = (bytes, token.map(str::to_string));
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
        assert_eq!(expected, (42, Some("tok+7/=".to_string())));
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
        assert_eq!(scan_whole(xml), (11, None));
    }

    #[test]
    fn page_scan_token_only_when_truncated() {
        let truncated = br#"<ListBucketResult><IsTruncated>true</IsTruncated>
            <NextContinuationToken>abc123</NextContinuationToken></ListBucketResult>"#;
        assert_eq!(scan_whole(truncated).1.as_deref(), Some("abc123"));

        let done = br#"<ListBucketResult><IsTruncated>false</IsTruncated>
            <NextContinuationToken>abc123</NextContinuationToken></ListBucketResult>"#;
        assert_eq!(scan_whole(done).1, None);
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
        assert_eq!(scan_whole(&at_cap), (0, Some("a".repeat(TOKEN_MAX))));

        let over_cap = build(TOKEN_MAX + 1);
        assert_eq!(scan_whole(&over_cap), (0, None));
        let parts: Vec<&[u8]> = over_cap.chunks(97).collect();
        assert_eq!(scan_parts(&parts), (0, None));
    }

    #[test]
    fn page_scan_end_of_body_flushes_digits_and_drops_open_token() {
        assert_eq!(scan_whole(b"<Contents><Size>123"), (123, None));
        assert_eq!(
            scan_whole(b"<IsTruncated>true</IsTruncated><NextContinuationToken>abc"),
            (0, None)
        );
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
