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

use std::time::{Duration, SystemTime};

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest,
    SigningSettings, UriPathNormalizationMode, sign,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use futures::{StreamExt as _, TryStreamExt as _, stream};
use icechunk_storage::{StorageResult, other_error, s3_config::S3Options};
use memchr::memmem;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
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

pub(crate) const DEFAULT_LIST_CONCURRENCY: usize = 256;

#[derive(Debug)]
pub(crate) struct SignedLister {
    http: reqwest::Client,
    scheme: String,
    authority: String,
    key_path_prefix: String,
    host_header: String,
    region: String,
    creds: Option<Credentials>,
    max_retries: u32,
    concurrency: usize,
}

impl SignedLister {
    pub(crate) fn new(
        config: &S3Options,
        bucket: &str,
        region: String,
        creds: Option<Credentials>,
        max_retries: u32,
        concurrency: usize,
    ) -> StorageResult<Self> {
        let concurrency = concurrency.max(1);
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(concurrency)
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
            creds,
            max_retries,
            concurrency,
        })
    }

    pub(crate) async fn sum(
        &self,
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
        let prefixes: Vec<String> = CROCKFORD
            .chars()
            .flat_map(|a| CROCKFORD.chars().map(move |b| format!("{base_prefix}/{a}{b}")))
            .collect();
        stream::iter(prefixes)
            .map(|p| self.sum_prefix(p))
            .buffer_unordered(self.concurrency)
            .try_fold(0u64, |acc, b| async move { Ok(acc.saturating_add(b)) })
            .await
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
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    match status {
                        200 => {
                            let body = resp.bytes().await.map_err(|e| {
                                other_error(format!("reading S3 list body: {e}"))
                            })?;
                            let bytes = scan_sizes(&body);
                            let next = next_token(&body);
                            return Ok((bytes, next));
                        }
                        503 | 429 | 500 | 502 | 504 => {}
                        other => {
                            return Err(other_error(format!(
                                "S3 list HTTP {other} for {}",
                                redact(&url)
                            )));
                        }
                    }
                }
                Err(e) if e.is_timeout() || e.is_connect() || e.is_request() => {}
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
        if self.creds.is_some() {
            let signed = self.sign_headers(url)?;
            for (name, value) in signed.iter() {
                rb = rb.header(name.clone(), value.clone());
            }
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
        let creds = self
            .creds
            .as_ref()
            .ok_or_else(|| other_error("sign_headers called without credentials"))?;

        let mut settings = SigningSettings::default();
        settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
        settings.percent_encoding_mode = PercentEncodingMode::Single;
        settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;

        let identity: Identity = creds.clone().into();
        let signing_params: aws_sigv4::http_request::SigningParams<'_> =
            v4::SigningParams::builder()
                .identity(&identity)
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
        Ok(req.headers().clone())
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
}
