//! Azure Blob page fetcher for the shared fast-listing engine
//! ([`icechunk_storage::fast_list`]).
//!
//! Azure's List Blobs API (`?restype=container&comp=list`) takes a raw string
//! `prefix` like S3 and GCS, so the engine's Crockford sub-prefix sharding is
//! valid, and it serves up to 5000 blobs per page — 5x S3's page size, so a
//! full drain needs 5x fewer requests. The response is XML with one
//! `<Content-Length>` per blob (inside `<Properties>`) and a `<NextMarker>`
//! that is non-empty only when the listing is truncated (absent, self-closing,
//! or empty on the last page).
//!
//! Everything above the wire format — retries, backoff, probe, fan-out, the
//! adaptive worker pool — lives in the engine; [`AzureLister`] only implements
//! one page attempt ([`ListPageFetcher`]): URL building, authentication, and
//! streaming the response body through [`AzureScan`]. Three credential shapes
//! are supported: a SAS token appended to the query string, a bearer token
//! header, and account-key `SharedKey` signing (hand-rolled here because the
//! request is a fixed-shape `GET` — the string-to-sign mirrors
//! `object_store`'s implementation, which azurite validates in CI).

use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use base64::prelude::{BASE64_STANDARD, Engine as _};
use chrono::Utc;
use hmac::{Hmac, Mac as _};
use icechunk_storage::fast_list::{ListPageFetcher, PageAttempt, is_transient_status};
use icechunk_storage::{StorageResult, other_error};
use memchr::memmem;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use sha2::Sha256;
use url::Url;

use crate::{AzureRefreshableCredential, AzureRefreshableCredentialProvider};

const UNRESERVED: &AsciiSet =
    &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

/// Matches the `x-ms-version` `object_store` 0.14 sends, so the fast path and
/// the portable path see the same service behavior.
const AZURE_VERSION: &str = "2023-11-03";
const RFC1123_FMT: &str = "%a, %d %b %Y %H:%M:%S GMT";
const EMULATOR_DEFAULT_ENDPOINT: &str = "http://127.0.0.1:10000";
const MAX_RESULTS: usize = 5000;

/// SAS tokens arrive with or without a leading `?`/`&` depending on how they
/// were minted; normalize to the bare fragment appended to every list URL.
fn normalize_sas(token: &str) -> String {
    token.trim_start_matches(['?', '&']).to_string()
}

fn decode_access_key(key: &str) -> StorageResult<Vec<u8>> {
    BASE64_STANDARD
        .decode(key)
        .map_err(|e| other_error(format!("invalid Azure access key (base64): {e}")))
}

/// How one page attempt authenticates. Anonymous, SAS, bearer, and account-key
/// are fixed for the run; `Refreshable` keeps a live handle to the credential
/// provider so a token that expires mid-run is re-resolved (and force-refreshed
/// on a 401/403) rather than failing the sum. Each attempt resolves its stored
/// auth into a [`ConcreteAuth`] (refreshing if needed).
#[derive(Debug)]
pub(crate) enum AzureListAuth {
    Anonymous,
    /// A pre-signed query fragment, appended verbatim to every list URL.
    Sas(String),
    Bearer(String),
    /// The base64-decoded storage account key.
    SharedKey(Vec<u8>),
    Refreshable(Arc<AzureRefreshableCredentialProvider>),
}

impl AzureListAuth {
    pub(crate) fn sas(token: &str) -> Self {
        Self::Sas(normalize_sas(token))
    }

    pub(crate) fn shared_key(key: &str) -> StorageResult<Self> {
        decode_access_key(key).map(Self::SharedKey)
    }
}

/// The concrete auth applied to one request, after any refresh. Unlike
/// [`AzureListAuth`] it never carries a provider handle, so URL building and
/// signing branch on a fixed shape.
#[derive(Debug)]
enum ConcreteAuth {
    Anonymous,
    Sas(String),
    Bearer(String),
    SharedKey(Vec<u8>),
}

fn concrete_from_refreshable(
    cred: &AzureRefreshableCredential,
) -> StorageResult<ConcreteAuth> {
    Ok(match cred {
        AzureRefreshableCredential::SASToken { token, .. } => {
            ConcreteAuth::Sas(normalize_sas(token))
        }
        AzureRefreshableCredential::BearerToken { bearer, .. } => {
            ConcreteAuth::Bearer(bearer.clone())
        }
        AzureRefreshableCredential::AccessKey { key, .. } => {
            ConcreteAuth::SharedKey(decode_access_key(key)?)
        }
    })
}

#[derive(Debug)]
pub(crate) struct AzureLister {
    /// Client built by the gating with the run's `allow_http` and timeout
    /// settings applied.
    http: reqwest::Client,
    /// `{endpoint}/{container}` — for azurite (`use_emulator`) the account is
    /// part of the path, matching `object_store`'s path-style emulator URLs.
    container_url: String,
    /// `/{account}{url_path}` — the canonicalized-resource base for `SharedKey`
    /// signing (the account appears twice for the emulator, by design).
    canonical_path: String,
    account: String,
    auth: AzureListAuth,
}

impl AzureLister {
    pub(crate) fn new(
        endpoint: Option<&str>,
        use_emulator: bool,
        account: &str,
        container: &str,
        auth: AzureListAuth,
        http: reqwest::Client,
    ) -> StorageResult<Self> {
        // Mirrors object_store's endpoint resolution: the emulator base comes
        // from the environment (ignoring any Endpoint config) and gets
        // path-style `{account}/{container}`; a custom endpoint keeps its own
        // path and gets `/{container}`.
        let container_url = if use_emulator {
            let base = std::env::var("AZURITE_BLOB_STORAGE_URL")
                .unwrap_or_else(|_| EMULATOR_DEFAULT_ENDPOINT.to_string());
            format!("{}/{account}/{container}", base.trim_end_matches('/'))
        } else {
            match endpoint {
                Some(ep) => format!("{}/{container}", ep.trim_end_matches('/')),
                None => format!("https://{account}.blob.core.windows.net/{container}"),
            }
        };
        let parsed = Url::parse(&container_url).map_err(|e| {
            other_error(format!("parsing Azure container URL {container_url:?}: {e}"))
        })?;
        let canonical_path = format!("/{account}{}", parsed.path());
        Ok(Self {
            http,
            container_url,
            canonical_path,
            account: account.to_string(),
            auth,
        })
    }

    fn build_url(
        &self,
        list_prefix: &str,
        token: Option<&str>,
        auth: &ConcreteAuth,
    ) -> String {
        let mut url = format!(
            "{}?comp=list&restype=container&prefix={}&maxresults={MAX_RESULTS}",
            self.container_url,
            utf8_percent_encode(list_prefix, UNRESERVED),
        );
        if let Some(t) = token {
            url.push_str("&marker=");
            url.push_str(&utf8_percent_encode(t, UNRESERVED).to_string());
        }
        if let ConcreteAuth::Sas(sas) = auth {
            url.push('&');
            url.push_str(sas);
        }
        url
    }

    /// Resolve the stored auth into the concrete shape for one attempt,
    /// refreshing a rotating credential if needed. The returned credential (only
    /// for the refreshable case) is what an auth-expiry response invalidates.
    async fn resolve(
        &self,
    ) -> StorageResult<(ConcreteAuth, Option<AzureRefreshableCredential>)> {
        match &self.auth {
            AzureListAuth::Anonymous => Ok((ConcreteAuth::Anonymous, None)),
            AzureListAuth::Sas(sas) => Ok((ConcreteAuth::Sas(sas.clone()), None)),
            AzureListAuth::Bearer(bearer) => {
                Ok((ConcreteAuth::Bearer(bearer.clone()), None))
            }
            AzureListAuth::SharedKey(key) => {
                Ok((ConcreteAuth::SharedKey(key.clone()), None))
            }
            AzureListAuth::Refreshable(provider) => {
                let cred = provider.get_or_update_credentials().await?;
                Ok((concrete_from_refreshable(&cred)?, Some(cred)))
            }
        }
    }

    /// `SharedKey` string-to-sign for one list `GET`:
    /// <https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key>.
    /// The 11 standard-header lines are empty (no body, date carried in
    /// `x-ms-date`), and the canonicalized resource's query values are the raw
    /// (percent-decoded) strings, sorted by parameter name.
    fn string_to_sign(
        &self,
        date: &str,
        list_prefix: &str,
        token: Option<&str>,
    ) -> String {
        let marker = token.map(|t| format!("marker:{t}\n")).unwrap_or_default();
        format!(
            "GET\n\n\n\n\n\n\n\n\n\n\n\n\
             x-ms-date:{date}\nx-ms-version:{AZURE_VERSION}\n\
             {}\ncomp:list\n{marker}maxresults:{MAX_RESULTS}\nprefix:{list_prefix}\nrestype:container",
            self.canonical_path,
        )
    }

    fn shared_key_authorization(
        &self,
        key: &[u8],
        date: &str,
        list_prefix: &str,
        token: Option<&str>,
    ) -> StorageResult<String> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key)
            .map_err(|e| other_error(format!("building SharedKey hmac: {e}")))?;
        mac.update(self.string_to_sign(date, list_prefix, token).as_bytes());
        let sig = BASE64_STANDARD.encode(mac.finalize().into_bytes());
        Ok(format!("SharedKey {}:{sig}", self.account))
    }
}

#[async_trait]
impl ListPageFetcher for AzureLister {
    async fn attempt_page(&self, list_prefix: &str, token: Option<&str>) -> PageAttempt {
        let (concrete, resolved) = match self.resolve().await {
            Ok(resolved) => resolved,
            Err(e) => {
                return PageAttempt::Retryable(other_error(format!(
                    "resolving refreshable Azure credentials for fast list: {e}"
                )));
            }
        };
        let url = self.build_url(list_prefix, token, &concrete);
        let mut rb = self.http.get(&url).header("x-ms-version", AZURE_VERSION);
        match &concrete {
            ConcreteAuth::Anonymous | ConcreteAuth::Sas(_) => {}
            ConcreteAuth::Bearer(t) => rb = rb.bearer_auth(t),
            ConcreteAuth::SharedKey(key) => {
                let date = Utc::now().format(RFC1123_FMT).to_string();
                match self.shared_key_authorization(key, &date, list_prefix, token) {
                    Ok(authorization) => {
                        rb = rb
                            .header("x-ms-date", date)
                            .header("authorization", authorization);
                    }
                    Err(e) => return PageAttempt::Fatal(e),
                }
            }
        }
        match rb.send().await {
            Ok(mut resp) => {
                let status = resp.status().as_u16();
                if status == 200 {
                    match read_page_body(&mut resp).await {
                        Ok(outcome) => outcome.into_attempt(&url),
                        Err(e) => PageAttempt::Retryable(other_error(format!(
                            "Azure list body read error for {}: {e}",
                            redact(&url)
                        ))),
                    }
                } else if (status == 401 || status == 403)
                    && let (AzureListAuth::Refreshable(provider), Some(cred)) =
                        (&self.auth, &resolved)
                {
                    // A refreshable credential rejected mid-run (bearer 401, SAS
                    // or SharedKey 403): drop it so the retry re-resolves a fresh
                    // one instead of failing the sum.
                    provider.invalidate_if_matches(cred).await;
                    PageAttempt::Retryable(other_error(format!(
                        "Azure list HTTP {status} for {} (credential expired; refreshing)",
                        redact(&url)
                    )))
                } else if is_transient_status(status) {
                    PageAttempt::Retryable(other_error(format!(
                        "Azure list HTTP {status} for {}",
                        redact(&url)
                    )))
                } else {
                    PageAttempt::Fatal(other_error(format!(
                        "Azure list HTTP {status} for {}",
                        redact(&url)
                    )))
                }
            }
            Err(e) if e.is_timeout() || e.is_connect() || e.is_request() => {
                PageAttempt::Retryable(other_error(format!(
                    "Azure list transport error for {}: {e}",
                    redact(&url)
                )))
            }
            Err(e) => PageAttempt::Fatal(other_error(format!(
                "Azure list request error for {}: {e}",
                redact(&url)
            ))),
        }
    }
}

/// What scanning one complete 200 page body concluded.
#[derive(Debug)]
enum ScanOutcome {
    /// Clean end with no continuation: this shard is fully listed.
    LastPage { bytes: u64 },
    /// A continuation token to resume the shard from.
    Continues { bytes: u64, token: String },
    /// The body advertised a `NextMarker` that could not be recovered — the
    /// capture was abandoned as oversized, or left unterminated at end of body.
    /// Reporting `LastPage` here would silently drop the rest of the shard, so
    /// the fetcher retries the page and ultimately fails loudly. The bytes seen
    /// so far are dropped: the retry re-lists the whole page.
    ContinuationLost,
}

impl ScanOutcome {
    fn into_attempt(self, url: &str) -> PageAttempt {
        match self {
            ScanOutcome::LastPage { bytes } => {
                PageAttempt::Page { bytes, next_token: None }
            }
            ScanOutcome::Continues { bytes, token } => {
                PageAttempt::Page { bytes, next_token: Some(token) }
            }
            ScanOutcome::ContinuationLost => {
                PageAttempt::Retryable(other_error(format!(
                    "Azure list page for {} advertised a NextMarker that could not \
                     be parsed (oversized or truncated body); retrying rather than \
                     under-count",
                    redact(url)
                )))
            }
        }
    }
}

const LENGTH_OPEN: &[u8] = b"<Content-Length>";
const MARKER_OPEN: &[u8] = b"<NextMarker>";

const PATTERNS: [(&[u8], Pat); 2] =
    [(LENGTH_OPEN, Pat::Length), (MARKER_OPEN, Pat::Marker)];

static FINDERS: LazyLock<[memmem::Finder<'static>; 2]> =
    LazyLock::new(|| PATTERNS.map(|(pattern, _)| memmem::Finder::new(pattern)));

/// A match can straddle a chunk boundary by at most one byte short of the
/// longest pattern, so [`AzureScan`] carries this many trailing bytes between
/// chunks.
const TAIL_MAX: usize = LENGTH_OPEN.len() - 1;

/// Corruption guard on marker capture: real continuation markers are well
/// under 1KB, so a longer capture means a scrambled body. The capture is
/// abandoned and the marker treated as absent.
const TOKEN_MAX: usize = 8 * 1024;

#[derive(Debug, Clone, Copy)]
enum Pat {
    Length,
    Marker,
}

#[derive(Debug, Default)]
enum Mode {
    #[default]
    Normal,
    Digits(u64),
    Token(Vec<u8>),
}

/// Incremental scanner for one List Blobs XML page, fed the response body
/// chunk by chunk so each chunk is scanned while still hot in cache and no
/// page-sized buffer is ever aggregated. Matches may straddle chunk boundaries
/// anywhere, down to one-byte chunks. Same seam/carry structure as
/// icechunk-s3's `PageScan`.
///
/// There is no `IsTruncated` in this API: a *non-empty* completed
/// `<NextMarker>` is the truncation signal. The last page carries no marker at
/// all, a self-closing `<NextMarker />` (which the exact-tag pattern never
/// matches), or an empty `<NextMarker></NextMarker>` (captured empty, mapped
/// to the last page in [`Self::finish`]).
///
/// `<Content-Length>` appears exactly once per blob, inside `<Properties>`;
/// the full-tag pattern (with the closing `>`) cannot match the sibling
/// `Content-Language`/`Content-Type` elements. Blob names cannot contain a
/// literal `<Content-Length>` because element text XML-escapes `<` — the same
/// invariant `PageScan` relies on for `<Size>`, and what makes keeping raw
/// consumed bytes in the seam tail sound (a value byte never starts a
/// pattern). `<Marker>` (the request-echo element) does not match the
/// `<NextMarker>` pattern.
#[derive(Debug, Default)]
struct AzureScan {
    mode: Mode,
    total: u64,
    token: Option<Vec<u8>>,
    token_given_up: bool,
    tail: [u8; TAIL_MAX],
    tail_len: usize,
}

impl AzureScan {
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

    /// Digits still open at end of body are flushed (matching `PageScan`'s XML
    /// end-of-buffer fold). A completed-but-empty `<NextMarker></NextMarker>` is
    /// the last-page signal. A marker we saw but could not produce — abandoned
    /// as oversized, or still mid-capture at end of body — is truncation
    /// evidence with no usable token, so it becomes a lost continuation rather
    /// than being silently reported complete.
    fn finish(self) -> ScanOutcome {
        let AzureScan { mode, total, token, token_given_up, .. } = self;
        let total = match &mode {
            Mode::Digits(value) => total.saturating_add(*value),
            _ => total,
        };
        match token {
            Some(t) if !t.is_empty() => ScanOutcome::Continues {
                bytes: total,
                token: String::from_utf8_lossy(&t).into_owned(),
            },
            Some(_) => ScanOutcome::LastPage { bytes: total },
            None if token_given_up || matches!(mode, Mode::Token(_)) => {
                ScanOutcome::ContinuationLost
            }
            None => ScanOutcome::LastPage { bytes: total },
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
    /// cached position. The marker resolves at most once per page, so its
    /// finder stops running the moment it is settled.
    fn scan_from(&mut self, chunk: &[u8], from: usize) {
        let find = |k: usize, at: usize| FINDERS[k].find(&chunk[at..]).map(|o| at + o);
        let mut next = [
            find(0, from),
            if self.token.is_some() || self.token_given_up {
                None
            } else {
                find(1, from)
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
                Pat::Length => find(k, pos),
                Pat::Marker if self.token.is_some() || self.token_given_up => None,
                Pat::Marker => find(k, pos),
            };
        }
    }

    fn on_match(&mut self, kind: Pat, chunk: &[u8], value_at: usize) -> usize {
        match kind {
            Pat::Length => self.eat_digits(0, chunk, value_at),
            Pat::Marker if self.token.is_none() && !self.token_given_up => {
                self.eat_token(Vec::new(), chunk, value_at)
            }
            Pat::Marker => value_at,
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
/// A mid-body error surfaces as `Err` so the attempt can be classified as
/// retryable.
async fn read_page_body(resp: &mut reqwest::Response) -> reqwest::Result<ScanOutcome> {
    let mut scan = AzureScan::default();
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

/// Build an [`AzureLister`] fetcher for the engine, shared with the gating
/// logic in `AzureObjectStoreBackend::fast_list_fetcher`.
pub(crate) fn make_fetcher(
    endpoint: Option<&str>,
    use_emulator: bool,
    account: &str,
    container: &str,
    auth: AzureListAuth,
    http: reqwest::Client,
) -> StorageResult<Arc<dyn ListPageFetcher>> {
    Ok(Arc::new(AzureLister::new(
        endpoint,
        use_emulator,
        account,
        container,
        auth,
        http,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan_outcome(parts: &[&[u8]]) -> ScanOutcome {
        let mut scan = AzureScan::default();
        for part in parts {
            scan.feed(part);
        }
        scan.finish()
    }

    /// Well-formed fixtures resolve to a byte total and an optional continuation
    /// marker; a lost continuation is a bug in those cases, so it panics here and
    /// is asserted for explicitly in the dedicated tests.
    fn scan_parts(parts: &[&[u8]]) -> (u64, Option<String>) {
        match scan_outcome(parts) {
            ScanOutcome::LastPage { bytes } => (bytes, None),
            ScanOutcome::Continues { bytes, token } => (bytes, Some(token)),
            ScanOutcome::ContinuationLost => panic!("unexpected lost continuation"),
        }
    }

    fn scan_whole(doc: &[u8]) -> (u64, Option<String>) {
        scan_parts(&[doc])
    }

    const REALISTIC_PAGE: &[u8] = b"\xef\xbb\xbf<?xml version=\"1.0\" encoding=\"utf-8\"?>\
        <EnumerationResults ServiceEndpoint=\"https://myaccount.blob.core.windows.net/\" ContainerName=\"mycontainer\">\
        <Prefix>some/repo/chunks/</Prefix><MaxResults>5000</MaxResults><Blobs>\
        <Blob><Name>some/repo/chunks/0A1B2C3D4E5F6G7H</Name><Properties>\
        <Creation-Time>Tue, 01 Jul 2026 12:34:56 GMT</Creation-Time>\
        <Last-Modified>Tue, 01 Jul 2026 12:34:56 GMT</Last-Modified>\
        <Etag>0x8CBFF45D8A29A19</Etag>\
        <Content-Length>1234</Content-Length>\
        <Content-Type>application/octet-stream</Content-Type>\
        <Content-Encoding /><Content-Language>en-US</Content-Language>\
        <Content-CRC64 /><Content-MD5>m1t8AK9zQCLYJCcJmVGiCw==</Content-MD5>\
        <Cache-Control /><Content-Disposition />\
        <BlobType>BlockBlob</BlobType><AccessTier>Hot</AccessTier>\
        <AccessTierInferred>true</AccessTierInferred>\
        <LeaseStatus>unlocked</LeaseStatus><LeaseState>available</LeaseState>\
        <ServerEncrypted>true</ServerEncrypted></Properties><OrMetadata /></Blob>\
        <Blob><Name>some/repo/chunks/8J9K0M1N2P3Q4R5S</Name><Properties>\
        <Etag>0x8CBFF45D8A29A20</Etag><Content-Length>0</Content-Length>\
        <Content-Language /><BlobType>BlockBlob</BlobType></Properties></Blob>\
        <Blob><Name>some/repo/chunks/6T7V8W9X0Y1Z2A3B</Name><Properties>\
        <Content-Length>987654321</Content-Length>\
        <Content-Type>application/octet-stream</Content-Type></Properties></Blob>\
        </Blobs><NextMarker /></EnumerationResults>";

    const TRUNCATED_PAGE: &[u8] = b"<?xml version=\"1.0\" encoding=\"utf-8\"?>\
        <EnumerationResults ServiceEndpoint=\"https://a.blob.core.windows.net/\" ContainerName=\"c\">\
        <Prefix>chunks/</Prefix><Marker>2!72!prev-marker</Marker><MaxResults>5000</MaxResults><Blobs>\
        <Blob><Name>chunks/A1</Name><Properties><Content-Length>40</Content-Length></Properties></Blob>\
        <Blob><Name>chunks/B2</Name><Properties><Content-Length>6</Content-Length></Properties></Blob>\
        </Blobs><NextMarker>2!108!MDAwMDM1IWNodW5rcy9CMiEwMDAwMjghOTk5OS0xMi0zMVQyMzo1OTo1OS45OTk5OTk5WiE=</NextMarker>\
        </EnumerationResults>";

    const ABSENT_MARKER_PAGE: &[u8] = b"<EnumerationResults><Blobs>\
        <Blob><Name>chunks/A1</Name><Properties><Content-Length>11</Content-Length></Properties></Blob>\
        </Blobs></EnumerationResults>";

    const EMPTY_ELEMENT_MARKER_PAGE: &[u8] = b"<EnumerationResults><Blobs>\
        <Blob><Name>chunks/A1</Name><Properties><Content-Length>7</Content-Length></Properties></Blob>\
        </Blobs><NextMarker></NextMarker></EnumerationResults>";

    const ZERO_BLOBS_PAGE: &[u8] = b"<?xml version=\"1.0\" encoding=\"utf-8\"?>\
        <EnumerationResults ServiceEndpoint=\"https://a.blob.core.windows.net/\" ContainerName=\"c\">\
        <Prefix>snapshots/</Prefix><MaxResults>5000</MaxResults><Blobs />\
        <NextMarker /></EnumerationResults>";

    const EXTREME_SIZES_PAGE: &[u8] = b"<EnumerationResults><Blobs>\
        <Blob><Name>a</Name><Properties><Content-Length>0</Content-Length></Properties></Blob>\
        <Blob><Name>b</Name><Properties><Content-Length>18446744073709551615</Content-Length></Properties></Blob>\
        <Blob><Name>c</Name><Properties><Content-Length>1000000</Content-Length></Properties></Blob>\
        </Blobs></EnumerationResults>";

    const SMALL_PAGE: &[u8] = b"<E><Blobs><Blob><Properties>\
        <Content-Length>42</Content-Length></Properties></Blob></Blobs>\
        <NextMarker>2!8!bWFyaw==</NextMarker></E>";

    const OPEN_DIGITS_AT_EOF_PAGE: &[u8] = b"<Properties><Content-Length>123";

    fn fixtures() -> Vec<(&'static [u8], u64, Option<&'static str>)> {
        vec![
            (REALISTIC_PAGE, 1234 + 987_654_321, None),
            (
                TRUNCATED_PAGE,
                46,
                Some(
                    "2!108!MDAwMDM1IWNodW5rcy9CMiEwMDAwMjghOTk5OS0xMi0zMVQyMzo1OTo1OS45OTk5OTk5WiE=",
                ),
            ),
            (ABSENT_MARKER_PAGE, 11, None),
            (EMPTY_ELEMENT_MARKER_PAGE, 7, None),
            (ZERO_BLOBS_PAGE, 0, None),
            (EXTREME_SIZES_PAGE, u64::MAX, None),
            (SMALL_PAGE, 42, Some("2!8!bWFyaw==")),
            (OPEN_DIGITS_AT_EOF_PAGE, 123, None),
        ]
    }

    #[test]
    fn azure_scan_reference_documents() {
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
    fn azure_scan_survives_every_two_way_split() {
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
    fn azure_scan_survives_every_three_way_split() {
        let expected = scan_whole(SMALL_PAGE);
        assert_eq!(expected, (42, Some("2!8!bWFyaw==".to_string())));
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
    fn azure_scan_sums_only_content_length_elements() {
        // Content-Language and the request-echo Marker must not contribute.
        let xml = b"<EnumerationResults><Marker>77</Marker><Blobs>\
            <Blob><Name>a</Name><Properties><Content-Length>4</Content-Length>\
            <Content-Language>12</Content-Language></Properties></Blob>\
            <Blob><Name>b</Name><Properties><Content-Length>6</Content-Length></Properties></Blob>\
            </Blobs></EnumerationResults>";
        assert_eq!(scan_whole(xml), (10, None));
    }

    #[test]
    fn azure_scan_first_marker_wins() {
        let doc = b"<E><NextMarker>first</NextMarker>\
            <Blobs><Blob><Properties><Content-Length>3</Content-Length></Properties></Blob></Blobs>\
            <NextMarker>second</NextMarker></E>";
        assert_eq!(scan_whole(doc), (3, Some("first".to_string())));
    }

    #[test]
    fn azure_scan_gives_up_on_oversized_marker() {
        let build = |n: usize| {
            let mut doc = b"<NextMarker>".to_vec();
            doc.extend(std::iter::repeat_n(b'a', n));
            doc.extend_from_slice(b"</NextMarker>");
            doc
        };
        let at_cap = build(TOKEN_MAX);
        assert_eq!(scan_whole(&at_cap), (0, Some("a".repeat(TOKEN_MAX))));

        // Over the cap the marker is abandoned, but a NextMarker *was* present:
        // the page must report a lost continuation, not silently look complete.
        let over_cap = build(TOKEN_MAX + 1);
        assert!(matches!(scan_outcome(&[&over_cap]), ScanOutcome::ContinuationLost));
        let parts: Vec<&[u8]> = over_cap.chunks(97).collect();
        assert!(matches!(scan_outcome(&parts), ScanOutcome::ContinuationLost));
    }

    #[test]
    fn azure_scan_open_values_at_end_of_body() {
        // Open digits carry no continuation evidence: the shard is reported
        // complete with the flushed size.
        assert_eq!(scan_whole(b"<Content-Length>123"), (123, None));
        // A NextMarker left open at end of body is a lost continuation: the
        // listing advertised more pages we could not resume.
        assert!(matches!(
            scan_outcome(&[b"<NextMarker>abc"]),
            ScanOutcome::ContinuationLost
        ));
    }

    fn lister(
        endpoint: Option<&str>,
        use_emulator: bool,
        auth: AzureListAuth,
    ) -> Option<AzureLister> {
        AzureLister::new(
            endpoint,
            use_emulator,
            "myacct",
            "mycontainer",
            auth,
            reqwest::Client::new(),
        )
        .ok()
    }

    #[test]
    fn azure_lister_builds_cloud_urls() {
        let url = lister(None, false, AzureListAuth::Anonymous)
            .map(|l| l.build_url("some/repo/chunks/AB", None, &ConcreteAuth::Anonymous))
            .unwrap_or_default();
        assert_eq!(
            url,
            "https://myacct.blob.core.windows.net/mycontainer\
             ?comp=list&restype=container\
             &prefix=some%2Frepo%2Fchunks%2FAB&maxresults=5000"
        );
        let url = lister(None, false, AzureListAuth::Anonymous)
            .map(|l| l.build_url("p", Some("2!8!bWFyaw=="), &ConcreteAuth::Anonymous))
            .unwrap_or_default();
        assert_eq!(
            url,
            "https://myacct.blob.core.windows.net/mycontainer\
             ?comp=list&restype=container&prefix=p&maxresults=5000\
             &marker=2%218%21bWFyaw%3D%3D"
        );
    }

    #[test]
    fn azure_lister_appends_sas_with_or_without_leading_separator() {
        for raw in ["sv=1&sig=s%3D", "?sv=1&sig=s%3D", "&sv=1&sig=s%3D"] {
            let auth = ConcreteAuth::Sas(normalize_sas(raw));
            let url = lister(None, false, AzureListAuth::Anonymous)
                .map(|l| l.build_url("p", None, &auth))
                .unwrap_or_default();
            assert_eq!(
                url,
                "https://myacct.blob.core.windows.net/mycontainer\
                 ?comp=list&restype=container&prefix=p&maxresults=5000\
                 &sv=1&sig=s%3D",
                "raw sas: {raw}"
            );
        }
    }

    #[test]
    fn azure_lister_maps_custom_endpoints_and_emulator() {
        let (url, canonical) =
            lister(Some("http://localhost:8888/base/"), false, AzureListAuth::Anonymous)
                .map(|l| {
                    (
                        l.build_url("p", None, &ConcreteAuth::Anonymous),
                        l.canonical_path.clone(),
                    )
                })
                .unwrap_or_default();
        assert!(url.starts_with("http://localhost:8888/base/mycontainer?"), "got: {url}");
        assert_eq!(canonical, "/myacct/base/mycontainer");

        // The emulator uses path-style URLs: the account is in the path, and
        // the canonicalized resource repeats it.
        let (url, canonical) = lister(None, true, AzureListAuth::Anonymous)
            .map(|l| {
                (
                    l.build_url("p", None, &ConcreteAuth::Anonymous),
                    l.canonical_path.clone(),
                )
            })
            .unwrap_or_default();
        assert!(
            url.starts_with("http://127.0.0.1:10000/myacct/mycontainer?"),
            "got: {url}"
        );
        assert_eq!(canonical, "/myacct/myacct/mycontainer");
    }

    #[test]
    fn azure_lister_string_to_sign_shape() {
        let sts = lister(None, true, AzureListAuth::Anonymous)
            .map(|l| {
                l.string_to_sign("Fri, 17 Jul 2026 01:02:03 GMT", "repo/chunks/AB", None)
            })
            .unwrap_or_default();
        assert_eq!(
            sts,
            "GET\n\n\n\n\n\n\n\n\n\n\n\n\
             x-ms-date:Fri, 17 Jul 2026 01:02:03 GMT\n\
             x-ms-version:2023-11-03\n\
             /myacct/myacct/mycontainer\n\
             comp:list\nmaxresults:5000\nprefix:repo/chunks/AB\nrestype:container"
        );
        let sts = lister(None, false, AzureListAuth::Anonymous)
            .map(|l| l.string_to_sign("d", "p", Some("2!8!bWFyaw==")))
            .unwrap_or_default();
        assert_eq!(
            sts,
            "GET\n\n\n\n\n\n\n\n\n\n\n\n\
             x-ms-date:d\nx-ms-version:2023-11-03\n\
             /myacct/mycontainer\n\
             comp:list\nmarker:2!8!bWFyaw==\nmaxresults:5000\nprefix:p\nrestype:container"
        );
    }

    mod fetcher {
        use std::collections::VecDeque;
        use std::sync::Mutex;
        use std::time::Duration;

        use icechunk_macros::tokio_test;

        use super::*;
        use crate::AzureCredentialsFetcher;
        use crate::FastListHttpConfig;
        use crate::fast_list_test_server::{FakeResponse, FakeServer};

        /// A refreshable fetcher that yields each scripted credential once, then
        /// repeats the last. The shared call counter lets a test assert how many
        /// times a token was minted.
        #[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
        struct SequenceFetcher {
            #[serde(skip)]
            state: Arc<Mutex<SequenceState>>,
        }

        #[derive(Debug, Default)]
        struct SequenceState {
            remaining: VecDeque<AzureRefreshableCredential>,
            calls: usize,
        }

        impl SequenceFetcher {
            fn new(
                creds: Vec<AzureRefreshableCredential>,
            ) -> (Arc<Self>, Arc<Mutex<SequenceState>>) {
                let state = Arc::new(Mutex::new(SequenceState {
                    remaining: creds.into(),
                    calls: 0,
                }));
                (Arc::new(Self { state: Arc::clone(&state) }), state)
            }
        }

        #[async_trait]
        #[typetag::serde]
        impl AzureCredentialsFetcher for SequenceFetcher {
            async fn get(&self) -> Result<AzureRefreshableCredential, String> {
                let mut state = self.state.lock().expect("poisoned");
                state.calls += 1;
                let cred = if state.remaining.len() > 1 {
                    state.remaining.pop_front()
                } else {
                    state.remaining.front().cloned()
                };
                cred.ok_or_else(|| "no scripted credential".to_string())
            }
        }

        fn client(request_timeout: Duration) -> reqwest::Client {
            FastListHttpConfig {
                allow_http: true,
                connect_timeout: None,
                request_timeout,
                read_timeout: None,
            }
            .build_client()
            .expect("client builds")
        }

        fn lister(
            server: &FakeServer,
            auth: AzureListAuth,
            timeout: Duration,
        ) -> AzureLister {
            AzureLister::new(
                Some(&server.base_url()),
                false,
                "acct",
                "cont",
                auth,
                client(timeout),
            )
            .expect("lister builds")
        }

        fn bearer_cred(token: &str) -> AzureRefreshableCredential {
            AzureRefreshableCredential::BearerToken {
                bearer: token.to_string(),
                expires_after: Some(Utc::now() + chrono::TimeDelta::hours(1)),
            }
        }

        #[tokio_test]
        async fn abandoned_marker_page_fails_loudly() {
            let mut body = b"<EnumerationResults><Blobs></Blobs><NextMarker>".to_vec();
            body.extend(std::iter::repeat_n(b'a', TOKEN_MAX + 1));
            body.extend_from_slice(b"</NextMarker></EnumerationResults>");
            let server = FakeServer::start(move |_| FakeResponse::ok(body.clone())).await;
            let lister =
                lister(&server, AzureListAuth::Anonymous, Duration::from_secs(30));
            // A page whose NextMarker cannot be parsed must not be reported as a
            // completed shard (which would silently under-count); it retries.
            let attempt = lister.attempt_page("chunks/00", None).await;
            assert!(matches!(attempt, PageAttempt::Retryable(_)), "got {attempt:?}");
        }

        #[tokio_test]
        async fn refreshes_credential_after_mid_run_auth_failure() {
            // 403 is the SAS-expiry status the portable path would refresh past;
            // a refreshable credential must survive it mid-run.
            let server = FakeServer::start(|req| match req.authorization.as_deref() {
                Some("Bearer fresh") => FakeResponse::ok(
                    b"<EnumerationResults><Blobs><Blob><Properties>\
                      <Content-Length>42</Content-Length></Properties></Blob></Blobs>\
                      </EnumerationResults>"
                        .to_vec(),
                ),
                _ => FakeResponse::status(403),
            })
            .await;
            let (fetcher, state) =
                SequenceFetcher::new(vec![bearer_cred("stale"), bearer_cred("fresh")]);
            let provider = Arc::new(AzureRefreshableCredentialProvider::new(fetcher));
            let lister = lister(
                &server,
                AzureListAuth::Refreshable(provider),
                Duration::from_secs(30),
            );

            let first = lister.attempt_page("chunks/00", None).await;
            assert!(matches!(first, PageAttempt::Retryable(_)), "got {first:?}");
            let second = lister.attempt_page("chunks/00", None).await;
            assert!(
                matches!(second, PageAttempt::Page { bytes: 42, next_token: None }),
                "got {second:?}"
            );
            assert_eq!(state.lock().expect("poisoned").calls, 2);
        }

        #[tokio_test]
        async fn applies_request_timeout() {
            let server = FakeServer::start(|_| FakeResponse::stall()).await;
            let lister =
                lister(&server, AzureListAuth::Anonymous, Duration::from_millis(150));
            let started = std::time::Instant::now();
            let attempt = lister.attempt_page("chunks/00", None).await;
            assert!(matches!(attempt, PageAttempt::Retryable(_)), "got {attempt:?}");
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "configured request timeout should have fired promptly"
            );
        }
    }
}
