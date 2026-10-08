//! Names and contents of the conditional-update protocol files, and lock
//! staleness. See "Names" and "Lock staleness" in
//! design-docs/020-filesystem-storage.md.

use std::{
    ffi::OsStr,
    fs, io,
    path::{Path, PathBuf},
    time::Duration,
};

use uuid::Uuid;

use crate::{
    path::parse_hyphenated_uuid,
    version::{ctime_ns, mtime_ns},
};

/// The protocol files of the object at `<dir>/<name>`, all in `<dir>`.
#[derive(Debug)]
pub(crate) struct ProtocolPaths {
    dir: PathBuf,
    name: String,
}

impl ProtocolPaths {
    pub(crate) fn new(target: &Path) -> io::Result<Self> {
        match (target.parent(), target.file_name().map(OsStr::to_str)) {
            (Some(dir), Some(Some(name))) => {
                Ok(Self { dir: dir.to_path_buf(), name: name.to_string() })
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{} has no parent or no UTF-8 file name to name protocol files after",
                    target.display()
                ),
            )),
        }
    }

    /// The commit ticket: a durable copy of the new content.
    pub(crate) fn ticket(&self, attempt: Uuid) -> PathBuf {
        self.dir.join(format!("{}.new_bytes.{}", self.name, attempt.hyphenated()))
    }

    /// Holds `attempt`; linked as the lock.
    pub(crate) fn lockinfo(&self, attempt: Uuid) -> PathBuf {
        self.dir.join(format!("{}.lockinfo.{}", self.name, attempt.hyphenated()))
    }

    /// The lock for replacing the version `hash`, generation `generation`.
    pub(crate) fn lock(&self, hash: &str, generation: u64) -> PathBuf {
        self.dir.join(format!("{}.lock.{hash}.{generation}", self.name))
    }

    /// Whether `file_name` is exactly the name of a lock for replacing `hash`.
    pub(crate) fn is_lock_of(&self, file_name: &OsStr, hash: &str) -> bool {
        file_name
            .to_str()
            .and_then(|name| name.strip_prefix(self.name.as_str()))
            .and_then(|rest| rest.strip_prefix(".lock."))
            .and_then(|rest| rest.strip_prefix(hash))
            .and_then(|rest| rest.strip_prefix('.'))
            .is_some_and(|generation| {
                generation
                    .parse::<u64>()
                    .is_ok_and(|parsed| parsed.to_string() == generation)
            })
    }

    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }
}

/// Whether `etag` can be a version: 64 lowercase hex characters, which also
/// makes it safe to put into a path.
pub(crate) fn is_valid_hash(etag: &str) -> bool {
    etag.len() == 64 && etag.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

#[derive(Debug)]
pub(crate) enum LockReadError {
    NotFound,
    /// Not exactly a hyphenated UUID. Lock files are fsynced before being
    /// linked, so this is corruption.
    Unparseable,
    Io(io::Error),
}

/// Reads the `attempt_id` held in `lock` through a new file descriptor.
pub(crate) fn read_lock_owner(lock: &Path) -> Result<Uuid, LockReadError> {
    let bytes = fs::read(lock).map_err(|err| match err.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory => LockReadError::NotFound,
        _ => LockReadError::Io(err),
    })?;
    std::str::from_utf8(&bytes)
        .ok()
        .and_then(parse_hyphenated_uuid)
        .ok_or(LockReadError::Unparseable)
}

/// A lock is stale once our lockinfo, just written, is more than
/// `stale_after` newer than the lock. Both are server timestamps, so client
/// clock skew doesn't matter.
pub(crate) fn is_stale(
    lock_ctime_ns: i128,
    our_mtime_ns: i128,
    stale_after: Duration,
) -> bool {
    let stale_after = i128::try_from(stale_after.as_nanos()).unwrap_or(i128::MAX);
    our_mtime_ns.saturating_sub(lock_ctime_ns) > stale_after
}

pub(crate) fn lock_is_stale(
    lock: &Path,
    our_lockinfo: &Path,
    stale_after: Duration,
) -> io::Result<bool> {
    let lock_ctime = ctime_ns(&fs::metadata(lock)?);
    let our_mtime = mtime_ns(&fs::metadata(our_lockinfo)?);
    Ok(is_stale(lock_ctime, our_mtime, stale_after))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::version::sha256_hex;

    #[test]
    fn protocol_paths_for_repo() {
        let paths = ProtocolPaths::new(Path::new("/r/repo")).unwrap();
        let attempt = Uuid::new_v4();
        let hash = sha256_hex(b"abc");
        assert_eq!(paths.dir(), Path::new("/r"));
        assert_eq!(
            paths.ticket(attempt),
            PathBuf::from(format!("/r/repo.new_bytes.{attempt}"))
        );
        assert_eq!(
            paths.lockinfo(attempt),
            PathBuf::from(format!("/r/repo.lockinfo.{attempt}"))
        );
        assert_eq!(paths.lock(&hash, 0), PathBuf::from(format!("/r/repo.lock.{hash}.0")));
    }

    #[test]
    fn protocol_paths_for_nested_key() {
        let paths = ProtocolPaths::new(Path::new("/r/a/config.yaml")).unwrap();
        let attempt = Uuid::new_v4();
        let hash = sha256_hex(b"abc");
        assert_eq!(paths.dir(), Path::new("/r/a"));
        assert_eq!(
            paths.ticket(attempt),
            PathBuf::from(format!("/r/a/config.yaml.new_bytes.{attempt}"))
        );
        assert_eq!(
            paths.lockinfo(attempt),
            PathBuf::from(format!("/r/a/config.yaml.lockinfo.{attempt}"))
        );
        assert_eq!(
            paths.lock(&hash, 3),
            PathBuf::from(format!("/r/a/config.yaml.lock.{hash}.3"))
        );
    }

    #[test]
    fn protocol_paths_need_a_file_name() {
        for target in ["/", "/r/.."] {
            let err = ProtocolPaths::new(Path::new(target)).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{target}");
        }
    }

    #[test]
    fn is_lock_of_matches_exact_lock_names() {
        let paths = ProtocolPaths::new(Path::new("/r/repo")).unwrap();
        let hash = sha256_hex(b"abc");
        let other = sha256_hex(b"def");
        let is_lock = |name: String| paths.is_lock_of(OsStr::new(&name), &hash);

        assert!(is_lock(format!("repo.lock.{hash}.17")));
        assert!(is_lock(format!("repo.lock.{hash}.0")));
        assert!(!is_lock(format!("repo.lock.{other}.0")));
        assert!(!is_lock(format!("repo.lock.{hash}.x")));
        assert!(!is_lock(format!("repo.lock.{hash}.")));
        assert!(!is_lock(format!("repo.lock.{hash}.+1")));
        assert!(!is_lock(format!("repo.lock.{hash}.01")));
        assert!(!is_lock(format!("repo.lockinfo.{}", Uuid::new_v4())));
        assert!(!is_lock(format!("other.lock.{hash}.0")));
    }

    #[test]
    fn valid_hashes() {
        let hash = sha256_hex(b"abc");
        assert!(is_valid_hash(&hash));
        assert!(!is_valid_hash(&hash.to_uppercase()));
        assert!(!is_valid_hash(&hash[1..]));
        assert!(!is_valid_hash(&format!("{hash}0")));
        assert!(!is_valid_hash("../x"));
        assert!(!is_valid_hash(""));
    }

    #[test]
    fn read_lock_owner_accepts_only_a_hyphenated_uuid() {
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("lock");
        let owner = Uuid::new_v4();

        fs::write(&lock, owner.to_string()).unwrap();
        assert_eq!(read_lock_owner(&lock).unwrap(), owner);

        for content in [
            String::new(),
            "garbage".to_string(),
            owner.simple().to_string(),
            format!("{owner}\n"),
        ] {
            fs::write(&lock, &content).unwrap();
            assert!(
                matches!(read_lock_owner(&lock), Err(LockReadError::Unparseable)),
                "{content:?}"
            );
        }
    }

    #[test]
    fn read_lock_owner_errors() {
        let dir = tempfile::tempdir().unwrap();
        assert!(matches!(
            read_lock_owner(&dir.path().join("missing")),
            Err(LockReadError::NotFound)
        ));
        assert!(matches!(
            read_lock_owner(dir.path()),
            Err(LockReadError::Io(err)) if err.kind() == io::ErrorKind::IsADirectory
        ));
    }

    #[test]
    fn staleness_is_strictly_greater_than_stale_after() {
        let stale_after = Duration::from_secs(60);
        let ctime = 1_000_000_000_000_000_000;
        let after = 60_000_000_000;
        assert!(!is_stale(ctime, ctime + after, stale_after));
        assert!(is_stale(ctime, ctime + after + 1, stale_after));
        assert!(!is_stale(ctime, ctime - 1, stale_after));
    }

    #[test]
    fn fresh_lock_is_not_stale() {
        let dir = tempfile::tempdir().unwrap();
        let lockinfo = dir.path().join("lockinfo");
        let lock = dir.path().join("lock");
        fs::write(&lockinfo, Uuid::new_v4().to_string()).unwrap();
        fs::hard_link(&lockinfo, &lock).unwrap();
        assert!(!lock_is_stale(&lock, &lockinfo, Duration::from_secs(60)).unwrap());
        assert!(
            lock_is_stale(&dir.path().join("missing"), &lockinfo, Duration::ZERO)
                .is_err()
        );
    }
}
