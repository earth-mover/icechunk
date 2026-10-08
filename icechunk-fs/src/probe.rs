//! Hard-link probe: checks that a directory supports exclusive hard links,
//! where `link` onto an existing name fails with `EEXIST`. Conditional writes
//! depend on that behaviour.

use std::{
    error::Error,
    fmt, fs, io,
    path::{Path, PathBuf},
};

use uuid::Uuid;

use crate::write::write_exclusive;

#[derive(Debug)]
struct ProbeFailed {
    root: PathBuf,
    source: io::Error,
}

impl fmt::Display for ProbeFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "hard-link probe failed in {}: {} (FilesystemStorage requires exclusive hard links)",
            self.root.display(),
            self.source
        )
    }
}

impl Error for ProbeFailed {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

/// Checks that `root` supports exclusive hard links. Leaves no files behind
/// unless it crashes or fails to remove them, which is reported as an error.
pub(crate) fn probe_hard_links(root: &Path) -> io::Result<()> {
    probe(root).map_err(|source| {
        io::Error::new(source.kind(), ProbeFailed { root: root.to_path_buf(), source })
    })
}

fn probe(root: &Path) -> io::Result<()> {
    let probe = root.join(format!("repo.probe.{}", Uuid::new_v4()));
    let mut link_name = probe.clone().into_os_string();
    link_name.push(".link");
    let link = PathBuf::from(link_name);

    write_exclusive(&probe, &[], false)?;
    let linked = link_exclusively(&probe, &link);
    let removed_link = match fs::remove_file(&link) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        removed => removed,
    };
    let removed_probe = fs::remove_file(&probe);
    linked.and(removed_link).and(removed_probe)
}

/// Links `probe` to `link`, then checks that linking again onto the existing
/// name fails with `EEXIST`.
fn link_exclusively(probe: &Path, link: &Path) -> io::Result<()> {
    fs::hard_link(probe, link)?;
    match fs::hard_link(probe, link) {
        Ok(()) => {
            Err(io::Error::new(io::ErrorKind::Unsupported, "link is not exclusive"))
        }
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_passes_and_leaves_nothing_behind() {
        let dir = tempfile::tempdir().unwrap();
        probe_hard_links(dir.path()).unwrap();
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn probe_on_missing_root_names_the_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("missing");
        let err = probe_hard_links(&root).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        let message = err.to_string();
        assert!(message.starts_with("hard-link probe failed in "), "{message}");
        assert!(message.contains(&root.display().to_string()), "{message}");
        assert!(message.contains("requires exclusive hard links"), "{message}");
    }

    #[test]
    fn probe_error_keeps_os_error() {
        let err = io::Error::new(
            io::ErrorKind::PermissionDenied,
            ProbeFailed {
                root: PathBuf::from("/mnt/x"),
                source: io::Error::from_raw_os_error(libc::EPERM),
            },
        );
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        assert!(
            err.to_string().starts_with("hard-link probe failed in /mnt/x: "),
            "{err}"
        );
        let inner = err.get_ref().unwrap();
        assert_eq!(
            inner.source().unwrap().downcast_ref::<io::Error>().unwrap().raw_os_error(),
            Some(libc::EPERM)
        );
    }
}
