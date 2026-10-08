use std::{fs::Metadata, os::unix::fs::MetadataExt as _};

use icechunk_storage::{ETag, Generation, VersionInfo};
use sha2::{Digest as _, Sha256};

/// Lowercase hex sha256 of `bytes`.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(crate) fn mtime_ns(meta: &Metadata) -> i128 {
    i128::from(meta.mtime()) * 1_000_000_000 + i128::from(meta.mtime_nsec())
}

pub(crate) fn ctime_ns(meta: &Metadata) -> i128 {
    i128::from(meta.ctime()) * 1_000_000_000 + i128::from(meta.ctime_nsec())
}

/// Identifies one state of a file, `{dev}:{ino}:{size}:{mtime_ns}:{ctime_ns}`.
/// Only ever compared for equality, never parsed.
pub(crate) fn generation_of(meta: &Metadata) -> Generation {
    Generation(format!(
        "{}:{}:{}:{}:{}",
        meta.dev(),
        meta.ino(),
        meta.size(),
        mtime_ns(meta),
        ctime_ns(meta)
    ))
}

/// The version of an object with this content: its sha256 is the identity.
/// `generation`, of the object's `fstat`, is for the read fast path.
pub(crate) fn version_of(bytes: &[u8], generation: Option<Generation>) -> VersionInfo {
    VersionInfo { etag: Some(ETag(sha256_hex(bytes))), generation }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn known_digest() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(version_of(b"abc", None).etag(), Some(&sha256_hex(b"abc")));
        assert_eq!(version_of(b"abc", None).generation, None);
    }

    #[test]
    fn generation_has_five_integer_fields_and_changes_on_rename() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repo");
        fs::write(&path, b"abc").unwrap();
        let meta = fs::metadata(&path).unwrap();
        let Generation(generation) = generation_of(&meta);
        let fields: Vec<&str> = generation.split(':').collect();
        assert_eq!(fields.len(), 5, "{generation}");
        assert!(fields.iter().all(|field| field.parse::<i128>().is_ok()), "{generation}");

        let staging = dir.path().join("staging");
        fs::write(&staging, b"abc").unwrap();
        fs::rename(&staging, &path).unwrap();
        assert_ne!(generation_of(&fs::metadata(&path).unwrap()).0, generation);
    }
}
