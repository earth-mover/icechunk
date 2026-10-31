//! Per-request attribution rendered into the `User-Agent` header.

use std::borrow::Cow;

use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Longest accepted value for any [`Attribution`] label, in bytes.
pub const MAX_LABEL_BYTES: usize = 128;
/// Longest rendered `array=` value, in bytes, after encoding.
pub const MAX_ARRAY_BYTES: usize = 256;
/// Longest rendered `chunk=` value, in bytes.
pub const MAX_CHUNK_BYTES: usize = 64;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AttributionError {
    #[error("attribution `{field}` is longer than {MAX_LABEL_BYTES} bytes")]
    TooLong { field: &'static str },
    #[error("attribution `{field}` contains the forbidden character {ch:?}")]
    ForbiddenChar { field: &'static str, ch: char },
    #[error(
        "attribution `client` must be `name` or `name/version` in token characters, got {value:?}"
    )]
    InvalidClient { value: String },
}

/// Caller-supplied labels attached to every request of a repository.
///
/// They are rendered into the `User-Agent` header so the bucket owner can
/// break traffic down in access logs by three questions:
///
/// - `client`: which software is talking to the bucket. The product token of
///   the application or library that embeds icechunk, e.g. `weatherlib/0.9`.
/// - `workload`: what it is doing. The job, pipeline or deployment that runs
///   the requests, e.g. `nightly-ingest` or `dashboard-v2`.
/// - `principal`: on whose behalf. A user id, service account or tenant,
///   set by services that act for many users.
///
/// The array path and chunk coordinates are added per request by icechunk
/// itself and need no configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Attribution {
    client: Option<String>,
    workload: Option<String>,
    principal: Option<String>,
}

impl Attribution {
    pub fn new() -> Self {
        Self::default()
    }

    /// Which software is talking to the bucket: the product token of the
    /// application or library that embeds icechunk, `name` or `name/version`
    /// in HTTP token characters, e.g. `weatherlib/0.9`.
    pub fn with_client(
        mut self,
        client: impl Into<String>,
    ) -> Result<Self, AttributionError> {
        let client = client.into();
        validate_client(&client)?;
        self.client = Some(client);
        Ok(self)
    }

    /// What the requests are for: the job, pipeline or deployment running
    /// them, e.g. `nightly-ingest`. Printable ASCII, at most 128 bytes, none
    /// of `(`, `)`, `\`, `;`, `=`, `"`.
    pub fn with_workload(
        mut self,
        workload: impl Into<String>,
    ) -> Result<Self, AttributionError> {
        let workload = workload.into();
        validate_label("workload", &workload)?;
        self.workload = Some(workload);
        Ok(self)
    }

    /// On whose behalf the requests are made: a user id, service account or
    /// tenant, for services that act for many users. Same rules as
    /// [`Attribution::with_workload`].
    pub fn with_principal(
        mut self,
        principal: impl Into<String>,
    ) -> Result<Self, AttributionError> {
        let principal = principal.into();
        validate_label("principal", &principal)?;
        self.principal = Some(principal);
        Ok(self)
    }

    pub fn client(&self) -> Option<&str> {
        self.client.as_deref()
    }

    pub fn workload(&self) -> Option<&str> {
        self.workload.as_deref()
    }

    pub fn principal(&self) -> Option<&str> {
        self.principal.as_deref()
    }
}

fn validate_label(field: &'static str, value: &str) -> Result<(), AttributionError> {
    if value.len() > MAX_LABEL_BYTES {
        return Err(AttributionError::TooLong { field });
    }
    let forbidden = |c: &char| !(' '..='~').contains(c) || "()\\;=\"".contains(*c);
    match value.chars().find(forbidden) {
        Some(ch) => Err(AttributionError::ForbiddenChar { field, ch }),
        None => Ok(()),
    }
}

fn is_tchar(c: char) -> bool {
    c.is_ascii_alphanumeric() || "!#$%&'*+-.^_`|~".contains(c)
}

fn validate_client(value: &str) -> Result<(), AttributionError> {
    if value.len() > MAX_LABEL_BYTES {
        return Err(AttributionError::TooLong { field: "client" });
    }
    let invalid = || AttributionError::InvalidClient { value: value.to_string() };
    let (name, version) = match value.split_once('/') {
        Some((name, version)) => (name, Some(version)),
        None => (value, None),
    };
    let token = |s: &str| !s.is_empty() && s.chars().all(is_tchar);
    if !token(name) || version.is_some_and(|v| !token(v)) {
        return Err(invalid());
    }
    Ok(())
}

/// The per-repository part of the header, rendered once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttributionLabels {
    /// Product tokens, e.g. `weatherlib/0.9 icechunk/2.3.0`.
    products: Cow<'static, str>,
    /// Comment fields, e.g. `workload=x; principal=y`. Empty when none is set.
    fields: Cow<'static, str>,
}

/// Labels of a caller that set no attribution: the icechunk product token only.
pub static UNATTRIBUTED_LABELS: AttributionLabels = AttributionLabels {
    products: Cow::Borrowed(icechunk_types::user_agent_product()),
    fields: Cow::Borrowed(""),
};

impl Default for AttributionLabels {
    fn default() -> Self {
        UNATTRIBUTED_LABELS.clone()
    }
}

impl From<&Attribution> for AttributionLabels {
    fn from(a: &Attribution) -> Self {
        let products = match a.client() {
            Some(client) => {
                Cow::Owned(format!("{client} {}", icechunk_types::user_agent_product()))
            }
            None => Cow::Borrowed(icechunk_types::user_agent_product()),
        };
        let fields = [("workload", a.workload()), ("principal", a.principal())]
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| format!("{k}={v}")))
            .collect::<Vec<_>>()
            .join("; ");
        Self { products, fields: Cow::Owned(fields) }
    }
}

/// What one storage request is attributed to.
#[derive(Debug, Clone, Copy)]
pub struct RequestAttribution<'a> {
    pub labels: &'a AttributionLabels,
    /// Node path without its leading `/`.
    pub array: Option<&'a str>,
    /// Chunk coordinates.
    pub chunk: Option<&'a [u32]>,
}

impl<'a> RequestAttribution<'a> {
    pub fn without_node(labels: &'a AttributionLabels) -> Self {
        Self { labels, array: None, chunk: None }
    }

    /// The icechunk part of the `User-Agent` header: product tokens, then a
    /// parenthesised comment when there is anything to say.
    pub fn user_agent_fragment(&self) -> String {
        let mut parts: Vec<Cow<'_, str>> = Vec::with_capacity(3);
        if !self.labels.fields.is_empty() {
            parts.push(Cow::Borrowed(&self.labels.fields));
        }
        if let Some(array) = self.array {
            let value = truncate_middle(encode(array), MAX_ARRAY_BYTES);
            parts.push(Cow::Owned(format!("array={value}")));
        }
        if let Some(chunk) = self.chunk {
            let rendered = chunk.iter().map(u32::to_string).collect::<Vec<_>>().join("/");
            let value = truncate_middle(rendered, MAX_CHUNK_BYTES);
            parts.push(Cow::Owned(format!("chunk={value}")));
        }
        if parts.is_empty() {
            self.labels.products.to_string()
        } else {
            format!("{} ({})", self.labels.products, parts.join("; "))
        }
    }
}

/// Bytes a header comment cannot carry (controls and non-ASCII, via
/// [`CONTROLS`]), plus our own separators. `/` and space pass through.
const COMMENT_ESCAPES: &AsciiSet =
    &CONTROLS.add(b'(').add(b')').add(b'\\').add(b';').add(b'=').add(b'%');

fn encode(value: &str) -> String {
    utf8_percent_encode(value, COMMENT_ESCAPES).to_string()
}

/// Keep the first and last `(cap - 3) / 2` bytes joined by `...`, moving the
/// cut points outward off any `%XX` escape they would split. Input is ASCII
/// (the output of [`encode`], or digits and slashes), so byte slicing is safe.
fn truncate_middle(value: String, cap: usize) -> String {
    if value.len() <= cap {
        return value;
    }
    let bytes = value.as_bytes();
    let keep = (cap - 3) / 2;

    // the head may not end with `%` or `%X`
    let mut head_end = keep;
    while head_end > 0 && bytes[head_end.saturating_sub(2)..head_end].contains(&b'%') {
        head_end -= 1;
    }
    // the tail may not start inside an escape: `%` one or two bytes before
    // the cut means the cut splits `%XX`; step forward past it
    let mut tail_start = value.len() - keep;
    while tail_start < value.len()
        && bytes[tail_start.saturating_sub(2)..tail_start].contains(&b'%')
    {
        tail_start += 1;
    }
    format!("{}...{}", &value[..head_end], &value[tail_start..])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(a: &Attribution) -> AttributionLabels {
        AttributionLabels::from(a)
    }

    fn value_of(fragment: &str, field: &str) -> String {
        fragment
            .split(&format!("{field}="))
            .nth(1)
            .unwrap()
            .split([';', ')'])
            .next()
            .unwrap()
            .to_string()
    }

    #[test]
    fn empty_attribution_renders_only_the_product_token() {
        let l = labels(&Attribution::new());
        let r = RequestAttribution::without_node(&l);
        assert_eq!(r.user_agent_fragment(), icechunk_types::user_agent_product());
        assert_eq!(UNATTRIBUTED_LABELS, l);
        assert_eq!(AttributionLabels::default(), l);
    }

    #[test]
    fn labels_render_in_order_and_client_goes_first() {
        let a = Attribution::new()
            .with_client("weatherlib/0.9")
            .unwrap()
            .with_workload("nightly-ingest")
            .unwrap()
            .with_principal("u_123")
            .unwrap();
        let l = labels(&a);
        let r = RequestAttribution::without_node(&l);
        assert_eq!(
            r.user_agent_fragment(),
            format!(
                "weatherlib/0.9 {} (workload=nightly-ingest; principal=u_123)",
                icechunk_types::user_agent_product()
            )
        );
    }

    #[test]
    fn array_and_chunk_are_appended_after_labels() {
        let a = Attribution::new().with_workload("w").unwrap();
        let l = labels(&a);
        let r = RequestAttribution {
            labels: &l,
            array: Some("g/temperature"),
            chunk: Some(&[0, 1, 2]),
        };
        assert_eq!(
            r.user_agent_fragment(),
            format!(
                "{} (workload=w; array=g/temperature; chunk=0/1/2)",
                icechunk_types::user_agent_product()
            )
        );
    }

    #[test]
    fn array_only_without_labels() {
        let r = RequestAttribution {
            labels: &UNATTRIBUTED_LABELS,
            array: Some("a"),
            chunk: None,
        };
        assert_eq!(
            r.user_agent_fragment(),
            format!("{} (array=a)", icechunk_types::user_agent_product())
        );
    }

    #[test]
    fn reserved_characters_are_percent_encoded() {
        let r = RequestAttribution {
            labels: &UNATTRIBUTED_LABELS,
            array: Some("a(b)c\\d;e=f%g\u{e9} h"),
            chunk: None,
        };
        assert_eq!(
            r.user_agent_fragment(),
            format!(
                "{} (array=a%28b%29c%5Cd%3Be%3Df%25g%C3%A9 h)",
                icechunk_types::user_agent_product()
            )
        );
    }

    #[test]
    fn long_array_keeps_head_and_tail() {
        let long = format!("{}MIDDLE{}", "h".repeat(150), "t".repeat(150));
        let r = RequestAttribution {
            labels: &UNATTRIBUTED_LABELS,
            array: Some(&long),
            chunk: None,
        };
        let value = value_of(&r.user_agent_fragment(), "array");
        let keep = (MAX_ARRAY_BYTES - 3) / 2;
        assert_eq!(value, format!("{}...{}", "h".repeat(keep), "t".repeat(keep)));
        assert!(value.len() <= MAX_ARRAY_BYTES);
    }

    #[test]
    fn truncation_never_splits_a_percent_escape() {
        // "é" encodes to "%C3%A9": 100 of them is 600 bytes
        let long = "\u{e9}".repeat(100);
        let r = RequestAttribution {
            labels: &UNATTRIBUTED_LABELS,
            array: Some(&long),
            chunk: None,
        };
        let value = value_of(&r.user_agent_fragment(), "array");
        let (head, tail) = value.split_once("...").unwrap();
        assert_eq!(head.len() % 3, 0, "head ends mid-escape: {head}");
        assert_eq!(tail.len() % 3, 0, "tail starts mid-escape: {tail}");
        assert!(tail.starts_with('%'));
    }

    #[test]
    fn truncate_middle_isolated_escapes_at_both_cut_points() {
        // keep = (MAX_ARRAY_BYTES - 3) / 2 = 126: the head cut at 126 lands
        // inside "%41" (bytes 125..128), and the tail cut at len - 126 lands
        // one byte into "%42".
        let value =
            format!("{}%41{}%42{}", "a".repeat(125), "b".repeat(200), "c".repeat(125));
        let result = truncate_middle(value, MAX_ARRAY_BYTES);
        let (head, tail) = result.split_once("...").unwrap();
        assert_eq!(head, "a".repeat(125), "head kept part of the %41 escape");
        assert_eq!(tail, "c".repeat(125), "tail kept part of the %42 escape");
    }

    #[test]
    fn truncate_middle_never_leaves_an_orphaned_hex_digit_in_the_tail() {
        // Regression for a tail cut that landed one byte into "%41": the old
        // code only looked forward from the cut point, so it kept the
        // trailing "1" of the escape instead of stepping past it.
        let value = format!("{}%41{}", "A".repeat(50), "B".repeat(10));
        let result = truncate_middle(value, 25);
        assert_eq!(result, format!("{}...{}", "A".repeat(11), "B".repeat(10)));
    }

    #[test]
    fn long_chunk_is_truncated_at_its_own_cap() {
        let coords: Vec<u32> = (0..40).map(|i| 1_000_000 + i).collect();
        let r = RequestAttribution {
            labels: &UNATTRIBUTED_LABELS,
            array: None,
            chunk: Some(&coords),
        };
        let value = value_of(&r.user_agent_fragment(), "chunk");
        let keep = (MAX_CHUNK_BYTES - 3) / 2;
        assert_eq!(value.len(), 2 * keep + 3);
        assert!(value.starts_with("1000000/1000001/"));
        assert!(value.ends_with("/1000038/1000039"));
    }

    #[test]
    fn labels_reject_forbidden_characters_length_and_bad_clients() {
        for bad in ["a(b", "a)b", "a\\b", "a;b", "a=b", "a\"b", "a\u{e9}", "a\nb"] {
            assert!(Attribution::new().with_workload(bad).is_err(), "{bad:?}");
            assert!(Attribution::new().with_principal(bad).is_err(), "{bad:?}");
        }
        assert!(Attribution::new().with_workload("x".repeat(MAX_LABEL_BYTES)).is_ok());
        assert!(
            Attribution::new().with_workload("x".repeat(MAX_LABEL_BYTES + 1)).is_err()
        );
        assert!(Attribution::new().with_workload("spaces are fine").is_ok());
        for bad in ["", "a b", "a/", "/1", "a/b/c", "a(b)"] {
            assert!(Attribution::new().with_client(bad).is_err(), "{bad:?}");
        }
        for ok in ["weatherlib", "weatherlib/0.9.1", "my-lib_2/1.0-rc1"] {
            assert!(Attribution::new().with_client(ok).is_ok(), "{ok:?}");
        }
    }

    #[test]
    fn getters_return_what_was_set() {
        let a =
            Attribution::new().with_client("c/1").unwrap().with_principal("p").unwrap();
        assert_eq!(a.client(), Some("c/1"));
        assert_eq!(a.workload(), None);
        assert_eq!(a.principal(), Some("p"));
    }
}
