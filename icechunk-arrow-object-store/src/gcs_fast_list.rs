//! GCS page fetcher for the shared fast-listing engine
//! ([`icechunk_storage::fast_list`]).
//!
//! GCS's JSON list API supports response field projection, which is a huge
//! win over XML listings: requesting `fields=items(size),nextPageToken`
//! returns essentially `{"items":[{"size":"1234"},...],"nextPageToken":"..."}`
//! — ~16 bytes per object instead of ~330. Like S3 (and unlike `object_store`),
//! the API's `prefix` is a raw string prefix, so the engine's Crockford
//! sub-prefix sharding is valid.
//!
//! Everything above the wire format — retries, backoff, probe, fan-out, the
//! adaptive worker pool — lives in the engine; [`GcsLister`] only implements
//! one page attempt ([`ListPageFetcher`]): URL building, the bearer header,
//! and streaming the response body through [`JsonScan`].

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use async_trait::async_trait;
use icechunk_storage::fast_list::{
    CONCURRENCY_CAP, ListPageFetcher, PageAttempt, is_transient_status,
};
use icechunk_storage::{StorageResult, other_error};
use memchr::memmem;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};

const UNRESERVED: &AsciiSet =
    &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

const DEFAULT_GCS_ENDPOINT: &str = "https://storage.googleapis.com";
const MAX_RESULTS: usize = 1000;
const TIMEOUT_SECS: u64 = 120;

#[derive(Debug)]
pub(crate) struct GcsLister {
    http: reqwest::Client,
    /// `{endpoint}/storage/v1/b/{bucket}/o`, the JSON API objects listing for
    /// the bucket. A custom endpoint maps here directly (that is how
    /// fake-gcs-server serves the JSON API too).
    list_url_base: String,
    bearer: Option<String>,
}

impl GcsLister {
    pub(crate) fn new(
        endpoint: Option<&str>,
        bucket: &str,
        bearer: Option<String>,
    ) -> StorageResult<Self> {
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(CONCURRENCY_CAP)
            .timeout(Duration::from_secs(TIMEOUT_SECS))
            .build()
            .map_err(|e| other_error(format!("building reqwest client: {e}")))?;
        let endpoint =
            endpoint.unwrap_or(DEFAULT_GCS_ENDPOINT).trim_end_matches('/').to_string();
        let list_url_base = format!(
            "{endpoint}/storage/v1/b/{}/o",
            utf8_percent_encode(bucket, UNRESERVED)
        );
        Ok(Self { http, list_url_base, bearer })
    }

    fn build_url(&self, list_prefix: &str, token: Option<&str>) -> String {
        let mut url = format!(
            "{}?prefix={}&fields=items(size),nextPageToken&maxResults={MAX_RESULTS}&prettyPrint=false",
            self.list_url_base,
            utf8_percent_encode(list_prefix, UNRESERVED),
        );
        if let Some(t) = token {
            url.push_str("&pageToken=");
            url.push_str(&utf8_percent_encode(t, UNRESERVED).to_string());
        }
        url
    }
}

#[async_trait]
impl ListPageFetcher for GcsLister {
    async fn attempt_page(&self, list_prefix: &str, token: Option<&str>) -> PageAttempt {
        let url = self.build_url(list_prefix, token);
        let mut rb = self.http.get(&url);
        if let Some(bearer) = &self.bearer {
            rb = rb.bearer_auth(bearer);
        }
        match rb.send().await {
            Ok(mut resp) => {
                let status = resp.status().as_u16();
                if status == 200 {
                    match read_page_body(&mut resp).await {
                        Ok((bytes, next_token)) => {
                            PageAttempt::Page { bytes, next_token }
                        }
                        Err(e) => PageAttempt::Retryable(other_error(format!(
                            "GCS list body read error for {}: {e}",
                            redact(&url)
                        ))),
                    }
                } else if is_transient_status(status) {
                    PageAttempt::Retryable(other_error(format!(
                        "GCS list HTTP {status} for {}",
                        redact(&url)
                    )))
                } else {
                    PageAttempt::Fatal(other_error(format!(
                        "GCS list HTTP {status} for {}",
                        redact(&url)
                    )))
                }
            }
            Err(e) if e.is_timeout() || e.is_connect() || e.is_request() => {
                PageAttempt::Retryable(other_error(format!(
                    "GCS list transport error for {}: {e}",
                    redact(&url)
                )))
            }
            Err(e) => PageAttempt::Fatal(other_error(format!(
                "GCS list request error for {}: {e}",
                redact(&url)
            ))),
        }
    }
}

const SIZE_KEY: &[u8] = b"\"size\"";
const TOKEN_KEY: &[u8] = b"\"nextPageToken\"";

const PATTERNS: [(&[u8], Pat); 2] = [(SIZE_KEY, Pat::Size), (TOKEN_KEY, Pat::Token)];

static FINDERS: LazyLock<[memmem::Finder<'static>; 2]> =
    LazyLock::new(|| PATTERNS.map(|(pattern, _)| memmem::Finder::new(pattern)));

/// A match can straddle a chunk boundary by at most one byte short of the
/// longest pattern, so [`JsonScan`] carries this many trailing bytes between
/// chunks.
const TAIL_MAX: usize = TOKEN_KEY.len() - 1;

/// Corruption guard on token capture: real page tokens are well under 1KB, so
/// a longer capture means a scrambled body. The capture is abandoned and the
/// token treated as absent.
const TOKEN_MAX: usize = 8 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pat {
    Size,
    Token,
}

#[derive(Debug, Default)]
enum Mode {
    #[default]
    Normal,
    Prelude {
        kind: Pat,
        seen_colon: bool,
    },
    Digits(u64),
    Token {
        buf: Vec<u8>,
        escaped: bool,
    },
}

/// Incremental scanner for one GCS JSON list page, fed the response body chunk
/// by chunk so each chunk is scanned while still hot in cache and no page-sized
/// buffer is ever aggregated. Matches may straddle chunk boundaries anywhere,
/// down to one-byte chunks, and the key order within the page is not assumed
/// (`items` vs `nextPageToken` may come in either order). Same seam/carry
/// structure as icechunk-s3's `PageScan`.
///
/// A match is anchored on the exact quoted key (`"size"` / `"nextPageToken"`)
/// followed by optional JSON whitespace, `:`, optional whitespace, and an
/// opening `"`; anything else abandons the match without consuming a value, so
/// the key appearing as a string *value* (`{"name":"size"}`) or with an
/// unquoted value never counts. GCS serializes `size` as a quoted decimal
/// string, and the object resource has exactly one such field per item.
///
/// Because we always request `fields=items(size),nextPageToken`, the projected
/// response contains no other keys and no collision is possible. The scanner
/// still behaves sensibly on unprojected responses (emulators that ignore
/// `fields`): digit-bearing fields like `generation` have different keys, and
/// the one known false positive is a *user metadata* entry literally named
/// `size` with an all-digit string value (`"metadata":{"size":"123"}`), which
/// would be added to the sum — an accepted limitation outside the projected
/// request.
///
/// `Prelude`/`Digits`/`Token` modes carry partially consumed state across
/// chunk boundaries explicitly; the `Normal`-mode seam keeps the last
/// [`TAIL_MAX`] raw bytes, sound under the assumption that value bytes contain
/// no raw `"` (sizes are digit runs, page tokens are URL-safe base64), the
/// JSON analogue of `PageScan`'s no-`<`-in-element-text invariant. A
/// backslash-quote inside a token capture is defensively treated as content
/// (not a terminator); no JSON unescaping is performed.
///
/// Unlike `PageScan` (where XML bodies were historically folded to the end of
/// the buffer), a value still open at end of body is malformed JSON — the
/// transport retries truncated reads — and is dropped, not flushed.
#[derive(Debug, Default)]
struct JsonScan {
    mode: Mode,
    total: u64,
    token: Option<Vec<u8>>,
    token_given_up: bool,
    tail: [u8; TAIL_MAX],
    tail_len: usize,
}

impl JsonScan {
    fn feed(&mut self, chunk: &[u8]) {
        let pos = match std::mem::replace(&mut self.mode, Mode::Normal) {
            Mode::Normal => self.resume_normal(chunk),
            Mode::Prelude { kind, seen_colon } => {
                self.eat_prelude(kind, seen_colon, chunk, 0)
            }
            Mode::Digits(value) => self.eat_digits(value, chunk, 0),
            Mode::Token { buf, escaped } => self.eat_token(buf, escaped, chunk, 0),
        };
        if matches!(self.mode, Mode::Normal) && pos < chunk.len() {
            self.scan_from(chunk, pos);
        }
        self.push_tail(chunk);
    }

    /// The presence of a completed `nextPageToken` is the truncation signal;
    /// there is no separate `IsTruncated` in the JSON API.
    fn finish(self) -> (u64, Option<String>) {
        let token = self.token.map(|t| String::from_utf8_lossy(&t).into_owned());
        (self.total, token)
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
    /// Unlike `PageScan`, an abandoned prelude can leave another pattern's
    /// cached position behind the resume point (inside bytes just consumed),
    /// so stale cached positions are re-found from the resume point.
    fn scan_from(&mut self, chunk: &[u8], from: usize) {
        let find = |k: usize, at: usize| FINDERS[k].find(&chunk[at..]).map(|o| at + o);
        let token_settled = |scan: &Self| scan.token.is_some() || scan.token_given_up;
        let mut next =
            [find(0, from), if token_settled(self) { None } else { find(1, from) }];
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
            for (j, cached) in next.iter_mut().enumerate() {
                if j == k || cached.is_some_and(|a| a < pos) {
                    *cached = match PATTERNS[j].1 {
                        Pat::Token if token_settled(self) => None,
                        _ => find(j, pos),
                    };
                }
            }
        }
    }

    fn on_match(&mut self, kind: Pat, chunk: &[u8], value_at: usize) -> usize {
        match kind {
            Pat::Size => self.eat_prelude(Pat::Size, false, chunk, value_at),
            Pat::Token if self.token.is_none() && !self.token_given_up => {
                self.eat_prelude(Pat::Token, false, chunk, value_at)
            }
            Pat::Token => value_at,
        }
    }

    /// Consume `[ws] ':' [ws] '"'` between a matched key and its value. Any
    /// other byte abandons the match, and scanning resumes at the offending
    /// byte (it may open a genuine key).
    fn eat_prelude(
        &mut self,
        kind: Pat,
        mut seen_colon: bool,
        chunk: &[u8],
        start: usize,
    ) -> usize {
        let mut at = start;
        while at < chunk.len() {
            match chunk[at] {
                b' ' | b'\t' | b'\r' | b'\n' => {}
                b':' if !seen_colon => seen_colon = true,
                b'"' if seen_colon => {
                    return match kind {
                        Pat::Size => self.eat_digits(0, chunk, at + 1),
                        Pat::Token => self.eat_token(Vec::new(), false, chunk, at + 1),
                    };
                }
                _ => return at,
            }
            at += 1;
        }
        self.mode = Mode::Prelude { kind, seen_colon };
        at
    }

    /// The value of `"size"`: a quoted run of decimal digits, committed at the
    /// closing quote. Any other byte abandons without summing.
    fn eat_digits(&mut self, mut value: u64, chunk: &[u8], start: usize) -> usize {
        let mut at = start;
        while at < chunk.len() {
            let b = chunk[at];
            if b.is_ascii_digit() {
                value = value.saturating_mul(10).saturating_add(u64::from(b - b'0'));
                at += 1;
            } else if b == b'"' {
                self.total = self.total.saturating_add(value);
                return at + 1;
            } else {
                return at;
            }
        }
        self.mode = Mode::Digits(value);
        at
    }

    fn eat_token(
        &mut self,
        mut buf: Vec<u8>,
        mut escaped: bool,
        chunk: &[u8],
        start: usize,
    ) -> usize {
        let mut at = start;
        while at < chunk.len() {
            let b = chunk[at];
            at += 1;
            if !escaped && b == b'"' {
                if !self.token_given_up {
                    self.token = Some(buf);
                }
                return at;
            }
            escaped = !escaped && b == b'\\';
            if self.token_given_up {
                continue;
            }
            if buf.len() >= TOKEN_MAX {
                self.token_given_up = true;
            } else {
                buf.push(b);
            }
        }
        self.mode = Mode::Token { buf, escaped };
        at
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
    let mut scan = JsonScan::default();
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

/// Build a [`GcsLister`] fetcher for the engine, shared with the gating logic
/// in `GcsObjectStoreBackend::fast_list_fetcher`.
pub(crate) fn make_fetcher(
    endpoint: Option<&str>,
    bucket: &str,
    bearer: Option<String>,
) -> StorageResult<Arc<dyn ListPageFetcher>> {
    Ok(Arc::new(GcsLister::new(endpoint, bucket, bearer)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan_parts(parts: &[&[u8]]) -> (u64, Option<String>) {
        let mut scan = JsonScan::default();
        for part in parts {
            scan.feed(part);
        }
        scan.finish()
    }

    fn scan_whole(doc: &[u8]) -> (u64, Option<String>) {
        scan_parts(&[doc])
    }

    const PROJECTED_PAGE: &[u8] =
        br#"{"items":[{"size":"1234"},{"size":"0"},{"size":"987654321"}]}"#;

    const PROJECTED_PAGE_WITH_TOKEN: &[u8] = br#"{"items":[{"size":"40"},{"size":"6"}],"nextPageToken":"CaE1ueGcxLPRx1TrXYExHnhbYLgveDs2Jwm36Hy4vbOwM="}"#;

    const EMPTY_PAGE: &[u8] = br#"{"kind":"storage#objects"}"#;

    const TOKEN_BEFORE_ITEMS_PAGE: &[u8] =
        br#"{"nextPageToken":"opaque-token_123=","items":[{"size":"7"},{"size":"9"}]}"#;

    const WHITESPACE_PAGE: &[u8] = b"{\n  \"items\": [\n    { \"size\" : \"42\" },\n    { \"size\"\t:\t\"8\" }\n  ],\n  \"nextPageToken\" : \"tok\"\n}\n";

    /// An emulator ignoring `fields=`: full object resources. Only the items'
    /// `"size"` fields may be summed — `generation`/`metageneration` are
    /// digit-valued, `"name":"size"` is the key as a *value*, and an unquoted
    /// number is not GCS's serialization of `size`.
    const UNPROJECTED_PAGE: &[u8] = br#"{"kind":"storage#objects","nextPageToken":"CkVjaHVua3MvMEExQjJDM0Q0RTVGNkc3SA==","items":[{"kind":"storage#object","id":"my-bucket/some/repo/chunks/0A1B2C3D4E5F6G7H/1752700000000001","selfLink":"https://www.googleapis.com/storage/v1/b/my-bucket/o/some%2Frepo%2Fchunks%2F0A1B2C3D4E5F6G7H","mediaLink":"https://storage.googleapis.com/download/storage/v1/b/my-bucket/o/some%2Frepo%2Fchunks%2F0A1B2C3D4E5F6G7H?generation=1752700000000001&alt=media","name":"some/repo/chunks/0A1B2C3D4E5F6G7H","bucket":"my-bucket","generation":"1752700000000001","metageneration":"1","contentType":"application/octet-stream","storageClass":"STANDARD","size":"1234","md5Hash":"m1t8AK9zQCLYJCcJmVGiCw==","crc32c":"UJyH0g==","etag":"CJab6vHEs44DEAE=","timeCreated":"2026-07-01T12:34:56.000Z","updated":"2026-07-01T12:34:56.000Z","timeStorageClassUpdated":"2026-07-01T12:34:56.000Z"},{"kind":"storage#object","id":"my-bucket/some/repo/chunks/8J9K0M1N2P3Q4R5S/1752700000000002","name":"size","bucket":"my-bucket","generation":"1752700000000002","metageneration":"7","storageClass":"STANDARD","size":"987654321","fakeSize":42,"etag":"CJab6vHEs44DEAE=","timeCreated":"2026-07-01T12:34:57.000Z"}]}"#;

    const EXTREME_SIZES_PAGE: &[u8] =
        br#"{"items":[{"size":"0"},{"size":"18446744073709551615"},{"size":"1000000"}]}"#;

    const SMALL_PAGE: &[u8] = br#"{"items":[{"size":"42"}],"nextPageToken":"t-7_="}"#;

    const KEY_AS_VALUE_PAGE: &[u8] =
        br#"{"items":[{"name":"size"},{"size":"5"},{"note":"a \"size\" pun"}]}"#;

    const UNQUOTED_NUMBER_PAGE: &[u8] = br#"{"items":[{"size":123},{"size":"6"}]}"#;

    const ESCAPED_QUOTE_TOKEN_PAGE: &[u8] =
        br#"{"nextPageToken":"a\"b","items":[{"size":"5"}]}"#;

    const NON_DIGIT_SIZE_PAGE: &[u8] =
        br#"{"items":[{"size":"12a"},{"size":""},{"size":"9"}]}"#;

    fn fixtures() -> Vec<(&'static [u8], u64, Option<&'static str>)> {
        vec![
            (PROJECTED_PAGE, 1234 + 987_654_321, None),
            (
                PROJECTED_PAGE_WITH_TOKEN,
                46,
                Some("CaE1ueGcxLPRx1TrXYExHnhbYLgveDs2Jwm36Hy4vbOwM="),
            ),
            (EMPTY_PAGE, 0, None),
            (TOKEN_BEFORE_ITEMS_PAGE, 16, Some("opaque-token_123=")),
            (WHITESPACE_PAGE, 50, Some("tok")),
            (
                UNPROJECTED_PAGE,
                1234 + 987_654_321,
                Some("CkVjaHVua3MvMEExQjJDM0Q0RTVGNkc3SA=="),
            ),
            (EXTREME_SIZES_PAGE, u64::MAX, None),
            (SMALL_PAGE, 42, Some("t-7_=")),
            (KEY_AS_VALUE_PAGE, 5, None),
            (UNQUOTED_NUMBER_PAGE, 6, None),
            (ESCAPED_QUOTE_TOKEN_PAGE, 5, Some("a\\\"b")),
            (NON_DIGIT_SIZE_PAGE, 9, None),
        ]
    }

    #[test]
    fn json_scan_reference_documents() {
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
    fn json_scan_survives_every_two_way_split() {
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
    fn json_scan_survives_every_three_way_split() {
        let expected = scan_whole(SMALL_PAGE);
        assert_eq!(expected, (42, Some("t-7_=".to_string())));
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
    fn json_scan_first_token_wins() {
        let doc = br#"{"nextPageToken":"first","items":[{"size":"3"}],"nextPageToken":"second"}"#;
        assert_eq!(scan_whole(doc), (3, Some("first".to_string())));
    }

    #[test]
    fn json_scan_gives_up_on_oversized_token() {
        let build = |n: usize| {
            let mut doc = br#"{"nextPageToken":""#.to_vec();
            doc.extend(std::iter::repeat_n(b'a', n));
            doc.extend_from_slice(br#"","items":[{"size":"5"}]}"#);
            doc
        };
        let at_cap = build(TOKEN_MAX);
        assert_eq!(scan_whole(&at_cap), (5, Some("a".repeat(TOKEN_MAX))));

        let over_cap = build(TOKEN_MAX + 1);
        assert_eq!(scan_whole(&over_cap), (5, None));
        let parts: Vec<&[u8]> = over_cap.chunks(97).collect();
        assert_eq!(scan_parts(&parts), (5, None));
    }

    #[test]
    fn json_scan_drops_values_open_at_end_of_body() {
        assert_eq!(scan_whole(br#"{"items":[{"size":"123"#), (0, None));
        assert_eq!(scan_whole(br#"{"nextPageToken":"abc"#), (0, None));
        assert_eq!(scan_whole(br#"{"items":[{"size""#), (0, None));
    }

    #[test]
    fn gcs_lister_builds_projected_urls() {
        let lister = GcsLister::new(None, "my.bucket", None).ok().map(|l| {
            (l.build_url("some/repo/chunks/AB", None), l.build_url("p", Some("tok+/=")))
        });
        let (plain, with_token) = lister.unwrap_or_default();
        assert_eq!(
            plain,
            "https://storage.googleapis.com/storage/v1/b/my.bucket/o\
             ?prefix=some%2Frepo%2Fchunks%2FAB\
             &fields=items(size),nextPageToken&maxResults=1000&prettyPrint=false"
        );
        assert_eq!(
            with_token,
            "https://storage.googleapis.com/storage/v1/b/my.bucket/o\
             ?prefix=p\
             &fields=items(size),nextPageToken&maxResults=1000&prettyPrint=false\
             &pageToken=tok%2B%2F%3D"
        );
    }

    #[test]
    fn gcs_lister_maps_custom_endpoints() {
        let url = GcsLister::new(Some("http://localhost:4443/"), "b", None)
            .ok()
            .map(|l| l.build_url("p", None))
            .unwrap_or_default();
        assert!(
            url.starts_with("http://localhost:4443/storage/v1/b/b/o?prefix=p&"),
            "got: {url}"
        );
    }
}
