//! Asks the client to refresh what it caches about a directory, see "Freshness
//! and the nudge" in design-docs/020-filesystem-storage.md. Never a safety
//! input: it only shortens how long a client can lag.

use std::path::Path;

/// Ignores every error, `ENOSYS` included.
pub(crate) fn nudge(dir: &Path) {
    #[cfg(target_os = "linux")]
    let _ignored = force_sync_stat(dir);
    #[cfg(not(target_os = "linux"))]
    let _ = dir;
}

/// `statx(dir, AT_STATX_FORCE_SYNC)`, through `syscall`: glibc has a `statx`
/// wrapper only since 2.28, newer than the manylinux2014 baseline (2.17).
#[cfg(target_os = "linux")]
fn force_sync_stat(dir: &Path) -> std::io::Result<()> {
    use std::{ffi::CString, io, mem::MaybeUninit, os::unix::ffi::OsStrExt as _};

    let c_path = CString::new(dir.as_os_str().as_bytes())?;
    let mut buf = MaybeUninit::<libc::statx>::uninit();
    // SAFETY: `c_path` is NUL-terminated and outlives the call, and `buf` is
    // writable and large enough for the `statx` the kernel writes into it.
    #[expect(unsafe_code)]
    let rc = unsafe {
        libc::syscall(
            libc::SYS_statx,
            libc::c_long::from(libc::AT_FDCWD),
            c_path.as_ptr(),
            libc::c_long::from(libc::AT_STATX_FORCE_SYNC),
            // `c_uint`, as in the kernel's signature; `c_long` has no
            // `From<c_uint>` where it is 32 bits.
            libc::STATX_BASIC_STATS,
            buf.as_mut_ptr(),
        )
    };
    if rc == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nudge_never_fails() {
        let dir = tempfile::tempdir().unwrap();
        nudge(dir.path());
        nudge(&dir.path().join("missing"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn force_sync_stat_reaches_the_kernel() {
        let dir = tempfile::tempdir().unwrap();
        force_sync_stat(dir.path()).unwrap();
        let err = force_sync_stat(&dir.path().join("missing")).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound, "{err}");
    }
}
