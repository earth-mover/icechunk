//! Names of the files the backend's own write protocols leave next to an
//! object. See "Names" in design-docs/020-filesystem-storage.md.

use std::ffi::OsStr;

use crate::{
    lock::is_valid_hash,
    path::{is_staging_name, parse_hyphenated_uuid},
    probe::{PROBE_LINK_SUFFIX, PROBE_PREFIX},
};

/// A file the backend's own write protocols create next to an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Leftover<'a> {
    /// `<name>.staging.<uuid>`
    Staging,
    /// `<name>.new_bytes.<uuid>`
    Ticket,
    /// `<name>.lockinfo.<uuid>`
    LockInfo,
    /// `<target>.lock.<hash>.<generation>`
    Lock { target: &'a str, hash: &'a str },
    /// `repo.probe.<uuid>` or `repo.probe.<uuid>.link`
    Probe,
}

/// Classifies `file_name` (no directory part). Returns `None`
/// if it doesn't match a known leftover.
pub(crate) fn parse_leftover(file_name: &str) -> Option<Leftover<'_>> {
    if is_staging_name(OsStr::new(file_name)) {
        return Some(Leftover::Staging);
    }
    parse_attempt_file(file_name)
        .or_else(|| parse_probe(file_name))
        .or_else(|| parse_lock(file_name))
}

/// `parse_leftover` on a UTF-8 name; a non-UTF-8 name is never a leftover.
pub(crate) fn is_leftover_name(file_name: &OsStr) -> bool {
    file_name.to_str().and_then(parse_leftover).is_some()
}

/// `<name>.new_bytes.<uuid>` or `<name>.lockinfo.<uuid>`
fn parse_attempt_file(file_name: &str) -> Option<Leftover<'_>> {
    let (rest, id) = file_name.rsplit_once('.')?;
    parse_hyphenated_uuid(id)?;
    [(".new_bytes", Leftover::Ticket), (".lockinfo", Leftover::LockInfo)]
        .into_iter()
        .find_map(|(suffix, kind)| {
            rest.strip_suffix(suffix).filter(|name| !name.is_empty()).map(|_| kind)
        })
}

fn parse_probe(file_name: &str) -> Option<Leftover<'_>> {
    file_name
        .strip_suffix(PROBE_LINK_SUFFIX)
        .unwrap_or(file_name)
        .strip_prefix(PROBE_PREFIX)
        .and_then(parse_hyphenated_uuid)
        .map(|_| Leftover::Probe)
}

fn parse_lock(file_name: &str) -> Option<Leftover<'_>> {
    let (rest, generation) = file_name.rsplit_once('.')?;
    generation.parse::<u64>().ok().filter(|parsed| parsed.to_string() == generation)?;
    let (rest, hash) = rest.rsplit_once('.')?;
    let target = rest.strip_suffix(".lock").filter(|target| !target.is_empty())?;
    is_valid_hash(hash).then_some(Leftover::Lock { target, hash })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        lock::ProtocolPaths,
        path::staging_path,
        probe::{probe_link_name, probe_name},
        version::sha256_hex,
    };
    use std::path::Path;
    use uuid::Uuid;

    fn name(path: &Path) -> String {
        path.file_name().unwrap().to_str().unwrap().to_string()
    }

    #[test]
    fn writer_names_round_trip() {
        let id = Uuid::new_v4();
        let hash = sha256_hex(b"v0");
        for target in ["repo", "config.yaml", "a.b.c"] {
            let paths = ProtocolPaths::new(&Path::new("/r").join(target)).unwrap();
            assert_eq!(parse_leftover(&name(&paths.ticket(id))), Some(Leftover::Ticket));
            assert_eq!(
                parse_leftover(&name(&paths.lockinfo(id))),
                Some(Leftover::LockInfo)
            );
            for generation in [0, 7, u64::MAX] {
                assert_eq!(
                    parse_leftover(&name(&paths.lock(&hash, generation))),
                    Some(Leftover::Lock { target, hash: hash.as_str() })
                );
            }
            assert_eq!(
                parse_leftover(&name(&staging_path(&Path::new("/r").join(target)))),
                Some(Leftover::Staging)
            );
        }
        assert_eq!(parse_leftover(&probe_name(id)), Some(Leftover::Probe));
        assert_eq!(parse_leftover(&probe_link_name(id)), Some(Leftover::Probe));
    }

    #[test]
    fn near_misses_are_not_leftovers() {
        let id = Uuid::new_v4();
        let hash = sha256_hex(b"v0");
        for name in [
            "repo".to_string(),
            "chunks".to_string(),
            "0123456789ABCDEFGHJK".to_string(), // an object id
            format!("repo.12345678901234.{}", "ABCDEFGH"), // backup-like
            format!(".new_bytes.{id}"),         // empty name
            format!("repo.new_bytes.{}", id.simple()), // not hyphenated
            format!("repo.new_bytes.{}", id.to_string().to_uppercase()),
            format!("repo.lockinfo.{id}x"),
            format!("repo.lock.{hash}.007"),
            format!("repo.lock.{hash}.+1"),
            format!("repo.lock.{hash}.-1"),
            format!("repo.lock.{hash}."),
            format!("repo.lock.{}.0", hash.to_uppercase()),
            format!("repo.lock.{}.0", &hash[1..]),
            format!(".lock.{hash}.0"),
            format!("repo.lock.{hash}"),
            format!("other.probe.{id}"),
            format!("repo.probe.{id}.link.link"),
            format!("repo.probe.{id}.lnk"),
        ] {
            assert_eq!(parse_leftover(&name), None, "{name}");
        }
    }

    #[test]
    fn non_utf8_is_not_a_leftover() {
        use std::os::unix::ffi::OsStrExt as _;
        assert!(!is_leftover_name(OsStr::from_bytes(b"repo.lockinfo.\xff")));
    }
}
