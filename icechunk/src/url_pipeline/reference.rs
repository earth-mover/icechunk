//! Parser for the body of an `icechunk:` URL pipeline segment.
//!
//! See the [`icechunk:` scheme specification](https://github.com/jbms/url-pipeline/blob/main/schemes/icechunk.md):
//!
//! ```text
//! icechunk-suffix =  "//" icechunk-ref [ "/" icechunk-path ]
//!                 /  [ "/" ] icechunk-path
//! icechunk-ref    =  "branch." branch-name / "tag." tag-name / snapshot-id
//! ```

use icechunk_types::ICResultExt as _;

use crate::{format::SnapshotId, repository::VersionInfo};

use super::{UrlPipelineErrorKind, UrlPipelineResult};

/// The branch used when an `icechunk:` segment does not name a ref.
pub const DEFAULT_BRANCH: &str = "main";

/// The Crockford base32 alphabet allowed in a snapshot id by the spec grammar.
///
/// The `base32` crate's decoder is lenient (it accepts lowercase and maps
/// `I`/`L`/`O` to digits), so the spelling is checked against this first.
const SNAPSHOT_ID_ALPHABET: &str = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Length of an encoded snapshot id.
const SNAPSHOT_ID_LEN: usize = 20;

/// Parse the body of an `icechunk:` segment (the text after `icechunk:`).
///
/// Returns the selected version and the residual path inside the repository.
///
/// - `""`, `"path"` and `"/path"` select the `main` branch (one leading `/`
///   is ignored).
/// - `"//branch.NAME[/path]"`, `"//tag.NAME[/path]"` and
///   `"//SNAPSHOTID[/path]"` select a ref.
///
/// Branch and tag names are percent-decoded and must not contain `/`, even
/// when it is percent-encoded as `%2F`. The residual path is returned
/// verbatim (it is *not* percent-decoded), with any trailing `/` removed;
/// an empty string means the repository root.
pub fn parse_icechunk_ref(body: &str) -> UrlPipelineResult<(VersionInfo, String)> {
    let invalid = |reason: String| {
        Err(UrlPipelineErrorKind::InvalidIcechunkSegment {
            body: body.to_string(),
            reason,
        })
        .capture()
    };

    let (version, path) = if let Some(rest) = body.strip_prefix("//") {
        let (reference, path) = match rest.split_once('/') {
            Some((reference, path)) => (reference, path),
            None => (rest, ""),
        };
        let version = if let Some(name) = reference.strip_prefix("branch.") {
            VersionInfo::BranchTipRef(decode_ref_name(body, "branch", name)?)
        } else if let Some(name) = reference.strip_prefix("tag.") {
            VersionInfo::TagRef(decode_ref_name(body, "tag", name)?)
        } else if reference.is_empty() {
            return invalid(
                "expected a ref after `//`: `branch.NAME`, `tag.NAME` or a snapshot id"
                    .to_string(),
            );
        } else {
            VersionInfo::SnapshotId(parse_snapshot_id(body, reference)?)
        };
        (version, path)
    } else {
        let path = body.strip_prefix('/').unwrap_or(body);
        (VersionInfo::BranchTipRef(DEFAULT_BRANCH.to_string()), path)
    };

    if path.starts_with('/') {
        return invalid("the path must not start with `//`".to_string());
    }
    Ok((version, path.trim_end_matches('/').to_string()))
}

fn decode_ref_name(body: &str, kind: &str, raw: &str) -> UrlPipelineResult<String> {
    let invalid = |reason: String| {
        Err(UrlPipelineErrorKind::InvalidIcechunkSegment {
            body: body.to_string(),
            reason,
        })
        .capture()
    };
    if raw.is_empty() {
        return invalid(format!("empty {kind} name"));
    }
    let Ok(name) = urlencoding::decode(raw) else {
        return invalid(format!(
            "{kind} name `{raw}` is not valid percent-encoded UTF-8"
        ));
    };
    if name.contains('/') {
        return invalid(format!(
            "{kind} name `{raw}` must not contain `/`, even percent-encoded"
        ));
    }
    Ok(name.into_owned())
}

fn parse_snapshot_id(body: &str, raw: &str) -> UrlPipelineResult<SnapshotId> {
    let invalid = || {
        Err(UrlPipelineErrorKind::InvalidIcechunkSegment {
            body: body.to_string(),
            reason: format!(
                "`{raw}` is not `branch.NAME`, `tag.NAME`, or a snapshot id \
                 ({SNAPSHOT_ID_LEN} uppercase Crockford base32 characters)"
            ),
        })
        .capture()
    };
    if raw.len() != SNAPSHOT_ID_LEN
        || !raw.chars().all(|c| SNAPSHOT_ID_ALPHABET.contains(c))
    {
        return invalid();
    }
    match SnapshotId::try_from(raw) {
        Ok(id) => Ok(id),
        Err(_) => invalid(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn branch(name: &str) -> VersionInfo {
        VersionInfo::BranchTipRef(name.to_string())
    }

    fn tag(name: &str) -> VersionInfo {
        VersionInfo::TagRef(name.to_string())
    }

    fn snap(id: &str) -> VersionInfo {
        VersionInfo::SnapshotId(SnapshotId::try_from(id).unwrap())
    }

    // Every example from `schemes/icechunk.md`, as the body that follows
    // `icechunk:` (the bare `icechunk` spelling has an empty body).
    #[rstest]
    #[case("", branch("main"), "")]
    #[case("//branch.main/", branch("main"), "")]
    #[case("path/to/node/", branch("main"), "path/to/node")]
    #[case("a", branch("main"), "a")]
    #[case("/path/to/node/", branch("main"), "path/to/node")]
    #[case("path/to/node/zarr.json", branch("main"), "path/to/node/zarr.json")]
    #[case("path/to/node/c/0/0/1", branch("main"), "path/to/node/c/0/0/1")]
    #[case("//branch.mybranch/path/to/node/", branch("mybranch"), "path/to/node")]
    #[case("//branch.a", branch("a"), "")]
    #[case("//tag.mytag/path/to/node/", tag("mytag"), "path/to/node")]
    #[case("//tag.a", tag("a"), "")]
    #[case(
        "//FWWFQGAW742XMX0F5MF0/path/to/node/",
        snap("FWWFQGAW742XMX0F5MF0"),
        "path/to/node"
    )]
    #[case(
        "//ABCDEFGHJKMNPQRSTVWX/path/to/node/",
        snap("ABCDEFGHJKMNPQRSTVWX"),
        "path/to/node"
    )]
    #[case("//branch.other/", branch("other"), "")]
    #[case("//tag.v5/", tag("v5"), "")]
    #[case("//4N0217AZA4VNPYD0HR0G/", snap("4N0217AZA4VNPYD0HR0G"), "")]
    // equivalences called out by the spec
    #[case("/", branch("main"), "")]
    #[case("//branch.main/a", branch("main"), "a")]
    #[case("//branch.main/path/to/node/", branch("main"), "path/to/node")]
    // not refs: without `//` these are plain paths
    #[case("branch.foo", branch("main"), "branch.foo")]
    #[case("/tag.foo/x", branch("main"), "tag.foo/x")]
    // names are percent-decoded; paths are not
    #[case("//branch.my%20branch/a%20b", branch("my branch"), "a%20b")]
    #[case("//tag.v1.0.0/x", tag("v1.0.0"), "x")]
    #[case("//branch.feat-x_y~z/", branch("feat-x_y~z"), "")]
    fn test_parse_valid(
        #[case] body: &str,
        #[case] expected_version: VersionInfo,
        #[case] expected_path: &str,
    ) {
        let (version, path) = parse_icechunk_ref(body).unwrap();
        assert_eq!(version, expected_version, "body: {body:?}");
        assert_eq!(path, expected_path, "body: {body:?}");
    }

    #[rstest]
    // empty ref
    #[case("//")]
    #[case("///path")]
    // empty names
    #[case("//branch.")]
    #[case("//branch./x")]
    #[case("//tag.")]
    // `/` in names, including percent-encoded
    #[case("//branch.a%2Fb")]
    #[case("//branch.a%2fb/x")]
    #[case("//tag.a%2Fb")]
    // bad percent-encoding
    #[case("//branch.%FF")]
    // not a ref and not a snapshot id
    #[case("//main")]
    #[case("//Branch.main")]
    #[case("//TAG.v1")]
    // snapshot ids: wrong length, lowercase, characters outside the alphabet
    #[case("//FWWFQGAW742XMX0F5MF")]
    #[case("//FWWFQGAW742XMX0F5MF00")]
    #[case("//fwwfqgaw742xmx0f5mf0")]
    #[case("//FWWFQGAW742XMX0F5MFI")]
    #[case("//FWWFQGAW742XMX0F5MFL")]
    #[case("//FWWFQGAW742XMX0F5MFO")]
    #[case("//FWWFQGAW742XMX0F5MFU")]
    // hex (the old adapter's regex) is not the encoding
    #[case("//0123456789abcdef0123")]
    // empty leading path component after the ref
    #[case("//branch.main//x")]
    fn test_parse_invalid(#[case] body: &str) {
        let err = parse_icechunk_ref(body).unwrap_err();
        assert!(
            matches!(err.kind, UrlPipelineErrorKind::InvalidIcechunkSegment { .. }),
            "body: {body:?}, err: {err}"
        );
    }

    #[test]
    fn test_snapshot_id_roundtrip() {
        let id = SnapshotId::random();
        let body = format!("//{id}/x");
        let (version, path) = parse_icechunk_ref(&body).unwrap();
        assert_eq!(version, VersionInfo::SnapshotId(id));
        assert_eq!(path, "x");
    }
}
