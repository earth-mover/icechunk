//! Parsing and formatting of the names of the `repo.d` protocol, see "Names"
//! in design-docs/020-filesystem-storage.md. Anything that doesn't parse
//! exactly is not a protocol name.

use std::io;

use icechunk_storage::{ETag, Generation, VersionInfo};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::path::parse_hyphenated_uuid;

/// The largest version number: `N < 10^15`.
pub(crate) const MAX_N: u64 = 999_999_999_999_999;
const K_WIDTH: usize = 15;
const HASH_LEN: usize = 64;

pub(crate) const REPO: &str = "repo";
pub(crate) const REPO_D: &str = "repo.d";
const TMP_DIR_PREFIX: &str = "repo.d.tmp.";
const STAGE_PREFIX: &str = "stage.";
const MIRROR_PREFIX: &str = "mirror.";
const LATEST_PREFIX: &str = "latest.";

/// Lowercase hex sha256 of `bytes`.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// A sha256 as 64 lowercase hex characters.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub(crate) struct Hash(String);

impl Hash {
    pub(crate) fn of(bytes: &[u8]) -> Self {
        Self(sha256_hex(bytes))
    }

    pub(crate) fn parse(s: &str) -> Option<Self> {
        (s.len() == HASH_LEN && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
            .then(|| Self(s.to_string()))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// Version `n` of `repo`, with bytes hashing to `hash`. `n <= MAX_N`.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct Version {
    n: u64,
    hash: Hash,
}

impl Version {
    /// `None` above [`MAX_N`].
    pub(crate) fn new(n: u64, hash: Hash) -> Option<Version> {
        (n <= MAX_N).then_some(Version { n, hash })
    }

    /// The first version of a new `repo.d`; `n` is the creating host's Unix
    /// time in milliseconds.
    pub(crate) fn initial(n: u64, hash: Hash) -> io::Result<Version> {
        Version::new(n, hash).ok_or_else(|| {
            io::Error::other(format!("the first version number {n} is above {MAX_N}"))
        })
    }

    pub(crate) fn n(&self) -> u64 {
        self.n
    }

    pub(crate) fn hash(&self) -> &Hash {
        &self.hash
    }

    /// `K`, the nines' complement of `n` in exactly 15 digits.
    fn k(&self) -> String {
        format!("{:0K_WIDTH$}", MAX_N - self.n)
    }

    /// `<N>.<h>`
    pub(crate) fn file_name(&self) -> String {
        format!("{}.{}", self.n, self.hash.as_str())
    }

    pub(crate) fn next(&self, hash: Hash) -> io::Result<Version> {
        Version::new(self.n + 1, hash).ok_or_else(|| {
            io::Error::other(format!("repo has reached the last version number, {MAX_N}"))
        })
    }

    pub(crate) fn to_version_info(&self) -> VersionInfo {
        VersionInfo {
            etag: Some(ETag(self.hash.as_str().to_string())),
            generation: Some(Generation(self.n.to_string())),
        }
    }

    pub(crate) fn from_version_info(v: &VersionInfo) -> Result<Version, String> {
        let etag = v.etag().ok_or("a version of repo needs an etag")?;
        let hash = Hash::parse(etag)
            .ok_or_else(|| format!("invalid etag for a version of repo: {etag:?}"))?;
        let Generation(generation) =
            v.generation.as_ref().ok_or("a version of repo needs a generation")?;
        parse_n(generation).and_then(|n| Version::new(n, hash)).ok_or_else(|| {
            format!("invalid generation for a version of repo: {generation:?}")
        })
    }
}

/// The empty file whose name says which version is current.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct Marker {
    pub(crate) version: Version,
}

impl Marker {
    /// `latest.<K>.<N>.<h>`
    pub(crate) fn file_name(&self) -> String {
        format!("{LATEST_PREFIX}{}.{}", self.version.k(), self.version.file_name())
    }
}

/// A protocol name inside `repo.d`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) enum RepoDName {
    Content(Version),
    Marker(Marker),
    Stage(Uuid),
    Mirror(Uuid),
}

pub(crate) fn parse_repo_d_name(name: &str) -> Option<RepoDName> {
    if let Some(id) = name.strip_prefix(STAGE_PREFIX) {
        return parse_hyphenated_uuid(id).map(RepoDName::Stage);
    }
    if let Some(id) = name.strip_prefix(MIRROR_PREFIX) {
        return parse_hyphenated_uuid(id).map(RepoDName::Mirror);
    }
    match name.strip_prefix(LATEST_PREFIX) {
        Some(rest) => {
            let (k, rest) = rest.split_once('.')?;
            let version = parse_version(rest)?;
            (k == version.k()).then_some(RepoDName::Marker(Marker { version }))
        }
        None => parse_version(name).map(RepoDName::Content),
    }
}

/// `stage.<id>`
pub(crate) fn stage_name(id: Uuid) -> String {
    format!("{STAGE_PREFIX}{id}")
}

/// `mirror.<id>`
pub(crate) fn mirror_name(id: Uuid) -> String {
    format!("{MIRROR_PREFIX}{id}")
}

/// `repo.d.tmp.<id>`
pub(crate) fn tmp_dir_name(id: Uuid) -> String {
    format!("{TMP_DIR_PREFIX}{id}")
}

/// A protocol name in the root, other than `repo`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum RootName {
    RepoD,
    TmpDir,
}

pub(crate) fn parse_root_name(name: &str) -> Option<RootName> {
    if name == REPO_D {
        return Some(RootName::RepoD);
    }
    parse_hyphenated_uuid(name.strip_prefix(TMP_DIR_PREFIX)?).map(|_| RootName::TmpDir)
}

/// A decimal `N <= MAX_N` without leading zeros.
fn parse_n(s: &str) -> Option<u64> {
    let canonical = !s.is_empty()
        && s.len() <= K_WIDTH
        && s.bytes().all(|b| b.is_ascii_digit())
        && (s == "0" || !s.starts_with('0'));
    if canonical { s.parse().ok() } else { None }
}

/// `<N>.<h>`
fn parse_version(s: &str) -> Option<Version> {
    let (n, hash) = s.split_once('.')?;
    Version::new(parse_n(n)?, Hash::parse(hash)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    fn hash() -> Hash {
        Hash::parse(H).unwrap()
    }

    fn version(n: u64) -> Version {
        Version::new(n, hash()).unwrap()
    }

    const NS: [u64; 4] = [0, 1, 1000, MAX_N];

    #[test]
    fn hash_of_is_lowercase_hex_sha256() {
        assert_eq!(Hash::of(b"abc").as_str(), H);
        assert_eq!(Hash::of(b"abc"), hash());
    }

    #[test]
    fn content_names_round_trip() {
        for n in NS {
            let v = version(n);
            let name = v.file_name();
            assert_eq!(name, format!("{n}.{H}"));
            assert_eq!(parse_repo_d_name(&name), Some(RepoDName::Content(v)));
        }
    }

    #[test]
    fn marker_names_round_trip() {
        for n in NS {
            let marker = Marker { version: version(n) };
            let name = marker.file_name();
            assert_eq!(parse_repo_d_name(&name), Some(RepoDName::Marker(marker)));
        }
    }

    #[test]
    fn stage_names_round_trip() {
        let id = Uuid::new_v4();
        let name = stage_name(id);
        assert_eq!(name, format!("stage.{id}"));
        assert_eq!(parse_repo_d_name(&name), Some(RepoDName::Stage(id)));
    }

    #[test]
    fn mirror_names_round_trip() {
        let id = Uuid::new_v4();
        let name = mirror_name(id);
        assert_eq!(name, format!("mirror.{id}"));
        assert_eq!(parse_repo_d_name(&name), Some(RepoDName::Mirror(id)));
    }

    #[test]
    fn initial_versions_start_at_n() {
        for n in NS {
            assert_eq!(Version::initial(n, hash()).unwrap(), version(n));
        }
        assert!(Version::initial(MAX_N + 1, hash()).is_err());
    }

    #[test]
    fn k_is_the_nines_complement_in_15_digits() {
        let latest = Marker { version: version(1000) };
        assert_eq!(latest.file_name(), format!("latest.999999999998999.1000.{H}"));
        let first = Marker { version: version(0) };
        assert_eq!(first.file_name(), format!("latest.999999999999999.0.{H}"));
        let last = Marker { version: version(MAX_N) };
        assert_eq!(last.file_name(), format!("latest.000000000000000.{MAX_N}.{H}"));
    }

    #[test]
    fn malformed_repo_d_names_are_ignored() {
        let upper = H.to_uppercase();
        let short = &H[1..];
        let id = Uuid::new_v4();
        for name in [
            // K disagrees with N
            format!("latest.999999999998998.1000.{H}"),
            // leading zeros in N, K of the wrong width
            format!("01.{H}"),
            format!("00.{H}"),
            format!("latest.999999999998999.01000.{H}"),
            format!("latest.99999999998999.1000.{H}"),
            format!("latest.0999999999998999.1000.{H}"),
            // N above MAX_N
            format!("1000000000000000.{H}"),
            format!("18446744073709551616.{H}"),
            // bad hashes
            format!("1.{upper}"),
            format!("1.{short}"),
            format!("1.{H}0"),
            format!("latest.999999999999998.1.{short}"),
            // bad stage ids
            format!("stage.{}", id.simple()),
            format!("stage.{}", id.hyphenated().to_string().to_uppercase()),
            format!("stage.{}", id.braced()),
            "stage.".to_string(),
            // other names
            ".nfs0001".to_string(),
            "latest".to_string(),
            "latest.999999999998999.1000".to_string(),
            format!("other.999999999998999.1000.{H}"),
            format!("frozen.999999999998999.1000.{H}"),
            format!("+1.{H}"),
            format!("-1.{H}"),
            String::new(),
            // trailing garbage
            format!("1.{H}.x"),
            format!("1.{H}x"),
            format!("latest.999999999998999.1000.{H}.x"),
            format!("stage.{id}.x"),
            // bad mirror ids
            format!("mirror.{}", id.simple()),
            format!("mirror.{}", id.hyphenated().to_string().to_uppercase()),
            format!("mirror.{}", id.braced()),
            "mirror.".to_string(),
            "mirror".to_string(),
            format!("mirror.{id}.x"),
            format!("mirror{id}"),
            format!("mirrors.{id}"),
            // removed names
            format!("frozen.999999999998999.1000.{H}"),
            format!("frozen.{id}"),
            format!("converted.999999999998999.1000.{H}"),
            format!("converted.{id}"),
            format!("converted.1000.{H}"),
            format!("repo.d.converting.{id}"),
            format!("repo.legacy.{id}"),
            format!("repo.stub.{id}"),
        ] {
            assert_eq!(parse_repo_d_name(&name), None, "{name}");
        }
    }

    #[test]
    fn root_names() {
        let id = Uuid::new_v4();
        assert_eq!(REPO, "repo");
        assert_eq!(parse_root_name(REPO_D), Some(RootName::RepoD));
        assert_eq!(tmp_dir_name(id), format!("repo.d.tmp.{id}"));
        assert_eq!(parse_root_name(&tmp_dir_name(id)), Some(RootName::TmpDir));
        for name in [
            "repo".to_string(),
            "repo.d.tmp.x".to_string(),
            "repo.d.tmp.".to_string(),
            "repo.d/".to_string(),
            "repo.dx".to_string(),
            format!("repo.d.tmp.{}", id.simple()),
            format!("repo.d.tmp.{id}/x"),
            // removed names
            format!("repo.legacy.{id}"),
            format!("repo.stub.{id}"),
            format!("repo.d.converting.{id}"),
            "repo.d.converting.".to_string(),
            format!("converted.{id}"),
            format!("converted.999999999998999.1000.{H}"),
            format!("frozen.{id}"),
            format!("frozen.999999999998999.1000.{H}"),
            // names inside repo.d aren't root names
            format!("mirror.{id}"),
            format!("stage.{id}"),
            format!("repo.mirror.{id}"),
        ] {
            assert_eq!(parse_root_name(&name), None, "{name}");
        }
    }

    #[test]
    fn version_info_round_trips() {
        for n in NS {
            let v = version(n);
            let info = v.to_version_info();
            assert_eq!(info.etag(), Some(&H.to_string()));
            assert_eq!(info.generation, Some(Generation(n.to_string())));
            assert_eq!(Version::from_version_info(&info), Ok(v));
        }
    }

    #[test]
    fn version_info_needs_a_valid_etag_and_generation() {
        let etag = || Some(ETag(H.to_string()));
        let generation = |g: &str| Some(Generation(g.to_string()));
        for info in [
            VersionInfo { etag: None, generation: generation("1") },
            VersionInfo { etag: etag(), generation: None },
            VersionInfo { etag: None, generation: None },
            VersionInfo { etag: etag(), generation: generation("x") },
            VersionInfo { etag: etag(), generation: generation("") },
            VersionInfo { etag: etag(), generation: generation("-1") },
            VersionInfo { etag: etag(), generation: generation("01") },
            VersionInfo { etag: etag(), generation: generation("1000000000000000") },
            VersionInfo {
                etag: Some(ETag("abc".to_string())),
                generation: generation("1"),
            },
        ] {
            assert!(Version::from_version_info(&info).is_err(), "{info:?}");
        }
    }

    #[test]
    fn next_increments_n_and_stops_at_max_n() {
        let other = Hash::of(b"other");
        assert_eq!(
            version(1000).next(other.clone()).unwrap(),
            Version::new(1001, other.clone()).unwrap()
        );
        assert!(version(MAX_N).next(other).is_err());
    }

    #[test]
    fn versions_above_max_n_cannot_be_built() {
        assert_eq!(version(MAX_N).n(), MAX_N);
        assert_eq!(version(MAX_N).hash(), &hash());
        for n in [MAX_N + 1, u64::MAX] {
            assert_eq!(Version::new(n, hash()), None, "{n}");
        }
    }
}
