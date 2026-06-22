//! Lost-response recovery for conditional PUTs, shared across storage backends.
//!
//! A conditional PUT can land while its ack is lost; the client's retry then
//! trips the condition against the object we just wrote, faking a conflict.
//! Each conditional PUT stamps a unique [`WRITE_ID_METADATA_KEY`]; on conflict
//! the backend HEADs the object and checks whether the stored id is ours. It
//! builds [`ReadbackFacts`], calls [`classify_readback`], then a `finalize_*`
//! fn maps the outcome to a [`VersionedUpdateResult`].

use std::sync::Once;

use tracing::warn;

use crate::storage::{
    StorageError, StorageResult, VersionInfo, VersionedUpdateResult, other_error,
};

/// Per-PUT token stamped on conditional writes; underscores keep it portable
/// to Azure (C# identifier rules).
pub const WRITE_ID_METADATA_KEY: &str = "icechunk_write_id";

static CONDITIONAL_WITHOUT_METADATA_WARNED: Once = Once::new();

/// Warn once: conditional writes on but metadata off → no lost-response recovery.
pub fn warn_conditional_without_metadata_once() {
    CONDITIONAL_WITHOUT_METADATA_WARNED.call_once(|| {
        warn!(
            "conditional PUT is enabled but `unsafe_use_metadata` is \
             disabled — lost-response recovery for conditional writes \
             requires user metadata to stamp write-ids; without it, \
             transient PUT failures may surface as spurious conflicts \
             even when the write actually landed. See \
             icechunk_storage::Settings::unsafe_use_metadata."
        );
    });
}

/// Backend-neutral facts from a HEAD issued after a conditional PUT conflict.
#[derive(Debug)]
pub enum ReadbackFacts {
    Absent,
    Found { stored_write_id: Option<String>, version: VersionInfo },
}

/// `OurWrite` is universally success; the rest are mapped per-caller.
#[derive(Debug, PartialEq, Eq)]
pub enum ReadbackOutcome {
    OurWrite(VersionInfo),
    /// The landed object isn't (provably) ours: a different write-id, no
    /// write-id, or no object at all. All map identically per-caller.
    NotOurs,
    /// Our id matched but the object has no usable version identity.
    MissingVersion,
}

/// Classify a successful read-back. A failed read-back is inconclusive and
/// never reaches here — the backend returns the HEAD error instead.
pub fn classify_readback(our: &str, facts: &ReadbackFacts) -> ReadbackOutcome {
    match facts {
        ReadbackFacts::Absent => ReadbackOutcome::NotOurs,
        ReadbackFacts::Found { stored_write_id, version } => {
            if stored_write_id.as_deref() != Some(our) {
                ReadbackOutcome::NotOurs
            } else if version.is_create() {
                ReadbackOutcome::MissingVersion
            } else {
                ReadbackOutcome::OurWrite(version.clone())
            }
        }
    }
}

/// Finalize a precondition (412/409) conflict. Absent → genuine race
/// (`NotOnLatestVersion`); inconclusive read-back (`Err`) propagates — faking a
/// conflict would reintroduce the spurious-conflict bug.
pub fn finalize_precondition(
    readback: StorageResult<ReadbackOutcome>,
    key: &str,
) -> StorageResult<VersionedUpdateResult> {
    match readback? {
        ReadbackOutcome::OurWrite(new_version) => {
            warn!(
                key,
                "precondition failed but our write-id is stored; retried PUT, success"
            );
            Ok(VersionedUpdateResult::Updated { new_version })
        }
        ReadbackOutcome::NotOurs => Ok(VersionedUpdateResult::NotOnLatestVersion),
        ReadbackOutcome::MissingVersion => {
            Err(other_error("readback object is missing a version identity"))
        }
    }
}

/// Finalize a failure only our own landed write can rescue (S3 multipart
/// `NoSuchUpload`/404, or an `object_store` generic transport error). Anything
/// but `OurWrite` propagates `original`; a failed read-back propagates its error.
pub fn finalize_lost_response(
    readback: StorageResult<ReadbackOutcome>,
    key: &str,
    original: StorageError,
) -> StorageResult<VersionedUpdateResult> {
    match readback? {
        ReadbackOutcome::OurWrite(new_version) => {
            warn!(
                key,
                "PUT response lost but our write-id is stored; treating as success"
            );
            Ok(VersionedUpdateResult::Updated { new_version })
        }
        _ => Err(original),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{ETag, StorageErrorKind};

    fn found(write_id: Option<&str>, etag: Option<&str>) -> ReadbackFacts {
        ReadbackFacts::Found {
            stored_write_id: write_id.map(str::to_string),
            version: VersionInfo {
                etag: etag.map(|e| ETag(e.to_string())),
                generation: None,
            },
        }
    }

    #[test]
    fn classify_match_returns_readback_version() {
        // A match returns the read-back object's own version, not the previous one.
        assert_eq!(
            classify_readback("W", &found(Some("W"), Some("E1"))),
            ReadbackOutcome::OurWrite(VersionInfo::from_etag_only("E1".to_string()))
        );
    }

    #[test]
    fn classify_branches() {
        // A different write-id, no write-id, or no object all collapse to NotOurs.
        assert_eq!(
            classify_readback("W", &found(Some("OTHER"), Some("E1"))),
            ReadbackOutcome::NotOurs
        );
        assert_eq!(
            classify_readback("W", &found(None, Some("E1"))),
            ReadbackOutcome::NotOurs
        );
        assert_eq!(
            classify_readback("W", &ReadbackFacts::Absent),
            ReadbackOutcome::NotOurs
        );
        // Our id matched but no version identity.
        assert_eq!(
            classify_readback("W", &found(Some("W"), None)),
            ReadbackOutcome::MissingVersion
        );
    }

    #[test]
    fn precondition_absent_is_conflict_not_error() {
        // Regression: absent is a genuine race → NotOnLatestVersion, not an error.
        assert_eq!(
            finalize_precondition(Ok(ReadbackOutcome::NotOurs), "k").unwrap(),
            VersionedUpdateResult::NotOnLatestVersion
        );
    }

    #[test]
    fn precondition_inconclusive_readback_propagates() {
        // Regression: inconclusive read-back must Err, never fake a conflict —
        // else a lost-response write retries into a spurious conflict.
        let err = finalize_precondition(Err(other_error("head failed")), "k")
            .expect_err("inconclusive readback must propagate");
        assert!(matches!(err.kind, StorageErrorKind::Other(_)));
    }

    #[test]
    fn lost_response_inconclusive_propagates_head_error() {
        let err = finalize_lost_response(
            Err(other_error("head failed")),
            "k",
            other_error("original put error"),
        )
        .expect_err("inconclusive readback must propagate");
        assert!(matches!(err.kind, StorageErrorKind::Other(s) if s == "head failed"));
    }

    #[test]
    fn lost_response_non_ours_propagates_original() {
        let err = finalize_lost_response(
            Ok(ReadbackOutcome::NotOurs),
            "k",
            other_error("original put error"),
        )
        .expect_err("a write that did not land must propagate the original error");
        assert!(
            matches!(err.kind, StorageErrorKind::Other(s) if s == "original put error")
        );
    }
}
