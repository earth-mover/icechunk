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

use std::sync::LazyLock;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest,
    SigningSettings, UriPathNormalizationMode, sign,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use icechunk_storage::fast_list::{CONCURRENCY_CAP, ListPageFetcher, PageAttempt};
use icechunk_storage::{StorageResult, other_error, s3_config::S3Options};
use memchr::memmem;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use url::Url;

const UNRESERVED: &AsciiSet =
    &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

const MAX_KEYS: usize = 1000;
const TIMEOUT_SECS: u64 = 120;

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
}

impl SignedLister {
    pub(crate) fn new(
        config: &S3Options,
        bucket: &str,
        region: String,
        creds: Option<Credentials>,
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
        })
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

/// One attempt at one signed `ListObjectsV2` page, scanning the body
/// chunk-by-chunk as it is decrypted: aggregating into a page buffer first
/// re-copies every body byte and then scans it after it has gone cold in cache
/// — together ~25% of CPU on a saturated 2-core run. A mid-body read error
/// discards the partial [`PageScan`] and is retryable, so the engine re-fetches
/// the whole request.
#[async_trait]
impl ListPageFetcher for SignedLister {
    async fn attempt_page(&self, list_prefix: &str, token: Option<&str>) -> PageAttempt {
        let url = self.build_url(list_prefix, token);
        match self.send_once(&url).await {
            Ok(Ok(mut resp)) => {
                let status = resp.status().as_u16();
                match status {
                    200 => match read_page_body(&mut resp).await {
                        Ok((bytes, next_token)) => {
                            PageAttempt::Page { bytes, next_token }
                        }
                        Err(e) => PageAttempt::Retryable(other_error(format!(
                            "S3 list body read error for {}: {e}",
                            redact(&url)
                        ))),
                    },
                    503 | 429 | 500 | 502 | 504 => PageAttempt::Retryable(other_error(
                        format!("S3 list HTTP {status} for {}", redact(&url)),
                    )),
                    other => PageAttempt::Fatal(other_error(format!(
                        "S3 list HTTP {other} for {}",
                        redact(&url)
                    ))),
                }
            }
            Ok(Err(e)) if e.is_timeout() || e.is_connect() || e.is_request() => {
                PageAttempt::Retryable(other_error(format!(
                    "S3 list transport error for {}: {e}",
                    redact(&url)
                )))
            }
            Ok(Err(e)) => PageAttempt::Fatal(other_error(format!(
                "S3 list request error for {}: {e}",
                redact(&url)
            ))),
            Err(e) => PageAttempt::Fatal(e),
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
/// A mid-body error surfaces as `Err` so the attempt can be classified as
/// retryable.
async fn read_page_body(
    resp: &mut reqwest::Response,
) -> reqwest::Result<(u64, Option<String>)> {
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
    use super::*;

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
}
