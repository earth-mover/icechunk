use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
};

use icechunk_storage::{StorageResult, other_error};
use uuid::Uuid;

use crate::names::parse_root_name;

const STAGING_INFIX: &str = ".staging.";

fn valid_component(component: &str) -> bool {
    !component.is_empty()
        && component != "."
        && component != ".."
        && !component.contains('\0')
}

/// Maps a `/`-separated object key to a path under `root`. Rejects any key
/// that could escape `root`, that doesn't name a file, or that is under a
/// protocol name of the root other than `repo`.
pub(crate) fn key_path(root: &Path, key: &str) -> StorageResult<PathBuf> {
    let protocol = key.split('/').next().and_then(parse_root_name).is_some();
    key.split('/')
        .try_fold(root.to_path_buf(), |mut path, component| {
            valid_component(component).then(|| {
                path.push(component);
                path
            })
        })
        .filter(|_| !protocol)
        .ok_or_else(|| {
            other_error(format!("invalid object key for FilesystemStorage: {key:?}"))
        })
}

/// Like [`key_path`], but `""` is the root and one trailing `/` is ignored.
pub(crate) fn prefix_path(root: &Path, prefix: &str) -> StorageResult<PathBuf> {
    let prefix = prefix.strip_suffix('/').unwrap_or(prefix);
    if prefix.is_empty() { Ok(root.to_path_buf()) } else { key_path(root, prefix) }
}

/// A unique sibling of `target` to write before renaming or linking it into place.
pub(crate) fn staging_path(target: &Path) -> PathBuf {
    let mut name = target.file_name().map(OsStr::to_os_string).unwrap_or_default();
    name.push(format!("{STAGING_INFIX}{}", Uuid::new_v4()));
    target.with_file_name(name)
}

pub(crate) fn is_staging_name(file_name: &OsStr) -> bool {
    file_name
        .to_str()
        .and_then(|name| name.rsplit_once(STAGING_INFIX))
        .and_then(|(_, id)| parse_hyphenated_uuid(id))
        .is_some()
}

/// Parses `s` only if it is exactly a lowercase hyphenated UUID, the form
/// `Uuid`'s `Display` writes.
pub(crate) fn parse_hyphenated_uuid(s: &str) -> Option<Uuid> {
    Uuid::try_parse(s).ok().filter(|uuid| uuid.hyphenated().to_string() == s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_key_maps_under_root() {
        let root = Path::new("/data/repo");
        assert_eq!(
            key_path(root, "chunks/ABC").unwrap(),
            PathBuf::from("/data/repo/chunks/ABC")
        );
        assert_eq!(key_path(root, "repo").unwrap(), PathBuf::from("/data/repo/repo"));
    }

    #[test]
    fn invalid_keys_are_rejected() {
        let root = Path::new("/data/repo");
        for key in [
            "..",
            "../x",
            "a/../b",
            ".",
            "a/./b",
            "a//b",
            "/etc/passwd",
            "a/",
            "",
            "a\0b",
        ] {
            assert!(key_path(root, key).is_err(), "{key:?}");
        }
    }

    #[test]
    fn root_protocol_names_are_rejected_only_at_the_root() {
        let root = Path::new("/data/repo");
        let id = Uuid::new_v4();
        for name in [
            "repo.d".to_string(),
            format!("repo.d.tmp.{id}"),
            format!("repo.legacy.{id}"),
            format!("repo.stub.{id}"),
        ] {
            assert!(key_path(root, &name).is_err(), "{name}");
            assert!(key_path(root, &format!("{name}/x")).is_err(), "{name}");
            assert!(prefix_path(root, &name).is_err(), "{name}");
            assert!(key_path(root, &format!("a/{name}")).is_ok(), "{name}");
        }
        for key in ["repo", "repo.dx", "repo.legacy.x", &format!("repo.staging.{id}")] {
            assert!(key_path(root, key).is_ok(), "{key}");
        }
    }

    #[test]
    fn prefix_paths() {
        let root = Path::new("/data/repo");
        assert_eq!(prefix_path(root, "").unwrap(), root);
        assert_eq!(
            prefix_path(root, "chunks/").unwrap(),
            prefix_path(root, "chunks").unwrap()
        );
        assert_eq!(
            prefix_path(root, "chunks").unwrap(),
            PathBuf::from("/data/repo/chunks")
        );
        assert!(prefix_path(root, "../x").is_err());
        assert!(prefix_path(root, "a//").is_err());
    }

    #[test]
    fn staging_names() {
        let target = Path::new("/data/repo/repo");
        let staging = staging_path(target);
        assert_eq!(staging.parent(), target.parent());
        assert_ne!(staging, staging_path(target));
        assert!(is_staging_name(staging.file_name().unwrap()));
        assert!(!is_staging_name(OsStr::new("repo")));
        assert!(!is_staging_name(OsStr::new("x.staging.notauuid")));
        let uuid = Uuid::new_v4();
        assert!(is_staging_name(OsStr::new(&format!("x.staging.{}", uuid.hyphenated()))));
        for id in
            [uuid.simple().to_string(), uuid.braced().to_string(), uuid.urn().to_string()]
        {
            assert!(!is_staging_name(OsStr::new(&format!("x.staging.{id}"))), "{id}");
        }
    }
}
