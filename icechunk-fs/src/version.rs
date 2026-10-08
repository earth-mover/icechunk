use icechunk_storage::VersionInfo;
use sha2::{Digest as _, Sha256};

/// Lowercase hex sha256 of `bytes`.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// The version of an object with this content: its sha256 is the identity.
pub(crate) fn content_version(bytes: &[u8]) -> VersionInfo {
    VersionInfo::from_etag_only(sha256_hex(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_digest() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(content_version(b"abc").etag(), Some(&sha256_hex(b"abc")));
        assert_eq!(content_version(b"abc").generation, None);
    }
}
