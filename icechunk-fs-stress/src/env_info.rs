//! What each supervisor records about its host and the filesystem under test.

use std::{ffi::CStr, fmt, path::Path};

use icechunk::format::format_constants::ICECHUNK_LIB_VERSION;
use serde::{Deserialize, Serialize};

use crate::{layout::is_tag_char, log::now_ms};

/// `hosts/<tag>.json`, written when a supervisor registers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostInfo {
    pub tag: String,
    pub hostname: String,
    pub pid: u32,
    /// `uname -r`.
    pub kernel: String,
    pub fs_type: String,
    /// The `/proc/self/mountinfo` line of the run root's filesystem, if
    /// there is one.
    #[serde(deserialize_with = "Option::deserialize")]
    pub mount_line: Option<String>,
    pub network_fs: bool,
    pub build: Build,
    pub registered_at_ms: i64,
}

/// The fs-stress binary that ran: the Icechunk version it was built from and
/// the commit, `"unknown"` if the build didn't set `FS_STRESS_COMMIT`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Build {
    pub icechunk_version: String,
    pub commit: String,
}

const COMMIT: &str = match option_env!("FS_STRESS_COMMIT") {
    Some(commit) if !commit.is_empty() => commit,
    _ => UNKNOWN,
};

impl Build {
    /// This binary.
    pub fn current() -> Self {
        Self {
            icechunk_version: ICECHUNK_LIB_VERSION.to_owned(),
            commit: COMMIT.to_owned(),
        }
    }
}

/// `Icechunk 2.3.0, commit 0123abc`.
impl fmt::Display for Build {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Icechunk {}, commit {}", self.icechunk_version, self.commit)
    }
}

/// `done/<tag>.json`, written when a supervisor finishes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostDone {
    pub tag: String,
    pub go_seen_at_ms: i64,
    pub stopped_at_ms: i64,
    pub workers: u32,
    pub incarnations_spawned: u64,
    pub external_kills: u64,
    /// Workers still running at the supervisor's deadline, then killed.
    pub stragglers: u32,
    /// Why the mirror sampler stopped logging, if it did: its samples end
    /// there.
    pub sampler_error: Option<String>,
}

impl HostDone {
    /// `a: 12 worker incarnations, 3 external kills, 0 stragglers`.
    pub fn line(&self) -> String {
        format!(
            "{}: {} worker incarnations, {} external kills, {} stragglers",
            self.tag, self.incarnations_spawned, self.external_kills, self.stragglers
        )
    }
}

const UNKNOWN: &str = "unknown";

const LOCAL_FILESYSTEMS: &[&str] = &[
    "ext2", "ext3", "ext4", "xfs", "btrfs", "zfs", "tmpfs", "apfs", "hfs", "f2fs",
    "bcachefs", "overlay", "vfat", "exfat", "jfs", "nilfs2",
];

/// Describes this host and the filesystem holding `root`. The filesystem type
/// comes from the mountinfo line on Linux, else from `statfs`. Whatever cannot
/// be determined is `"unknown"`, which counts as a network filesystem.
pub fn host_info(tag: &str, root: &Path) -> HostInfo {
    let (mount_line, fs_type) = match mount_of(root) {
        Some((line, fs_type)) => (Some(line), fs_type),
        None => (
            None,
            statfs_fs_type(root).unwrap_or_else(|| {
                tracing::warn!(
                    root = %root.display(),
                    "unknown filesystem type, treating it as a network filesystem"
                );
                UNKNOWN.to_owned()
            }),
        ),
    };
    HostInfo {
        tag: tag.to_owned(),
        hostname: hostname().unwrap_or_else(|| UNKNOWN.to_owned()),
        pid: std::process::id(),
        kernel: kernel_release().unwrap_or_else(|| UNKNOWN.to_owned()),
        network_fs: is_network_fs(&fs_type),
        fs_type,
        mount_line,
        build: Build::current(),
        registered_at_ms: now_ms(),
    }
}

/// `<hostname>-<pid>`, with characters not allowed in a tag replaced by `_`.
pub fn default_tag() -> String {
    let hostname = hostname().unwrap_or_else(|| UNKNOWN.to_owned());
    let hostname: String =
        hostname.chars().map(|c| if is_tag_char(c) { c } else { '_' }).collect();
    format!("{hostname}-{}", std::process::id())
}

/// Filesystems known to be local. Anything else, including an unknown type,
/// is treated as a network filesystem.
pub fn is_network_fs(fs_type: &str) -> bool {
    !LOCAL_FILESYSTEMS.contains(&fs_type)
}

/// Finds the `/proc/self/mountinfo` line whose `major:minor` is `dev` (a
/// Linux `st_dev`) and returns it with its filesystem type.
pub fn mount_line_for_dev(mountinfo: &str, dev: u64) -> Option<(String, String)> {
    // glibc and musl `major`/`minor`
    let major = ((dev >> 8) & 0xfff) | ((dev >> 32) & 0xffff_f000);
    let minor = (dev & 0xff) | ((dev >> 12) & 0xffff_ff00);
    let wanted = format!("{major}:{minor}");
    mountinfo.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        if fields.nth(2)? != wanted {
            return None;
        }
        // Optional fields end at a lone "-", followed by the filesystem type.
        let fs_type = fields.skip_while(|&field| field != "-").nth(1)?;
        Some((line.to_owned(), fs_type.to_owned()))
    })
}

/// The Linux `statfs` `f_type` magic of each local filesystem that has one.
/// Anything else is unknown, since mountinfo is the primary source on Linux.
pub fn fs_type_for_magic(magic: u32) -> Option<&'static str> {
    match magic {
        // ext2, ext3 and ext4 share a magic.
        0xEF53 => Some("ext4"),
        0x5846_5342 => Some("xfs"),
        0x9123_683E => Some("btrfs"),
        0x2FC1_2FC1 => Some("zfs"),
        0x0102_1994 => Some("tmpfs"),
        0xF2F5_2010 => Some("f2fs"),
        0xCA45_1A4E => Some("bcachefs"),
        0x794C_7630 => Some("overlay"),
        // msdos and vfat share a magic.
        0x4D44 => Some("vfat"),
        0x2011_BAB0 => Some("exfat"),
        0x3153_464A => Some("jfs"),
        0x3434 => Some("nilfs2"),
        _ => None,
    }
}

#[cfg(target_os = "linux")]
fn mount_of(root: &Path) -> Option<(String, String)> {
    use std::os::unix::fs::MetadataExt as _;

    let dev = std::fs::metadata(root).ok()?.dev();
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    mount_line_for_dev(&mountinfo, dev)
}

#[cfg(not(target_os = "linux"))]
fn mount_of(_root: &Path) -> Option<(String, String)> {
    None
}

#[cfg(any(target_os = "linux", target_vendor = "apple", target_os = "freebsd"))]
fn statfs(root: &Path) -> Option<libc::statfs> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt as _};

    let path = CString::new(root.as_os_str().as_bytes()).ok()?;
    // SAFETY: `statfs` holds only integers and integer arrays, so all zeros is
    // a valid value; `path` is NUL-terminated, and `statfs` writes only into
    // the struct it is given.
    #[expect(unsafe_code)]
    let (rc, stat) = unsafe {
        let mut stat: libc::statfs = std::mem::zeroed();
        (libc::statfs(path.as_ptr(), &mut stat), stat)
    };
    (rc == 0).then_some(stat)
}

#[cfg(target_os = "linux")]
fn statfs_fs_type(root: &Path) -> Option<String> {
    // `f_type`'s width and signedness vary by architecture and libc; the
    // magic is its low 32 bits.
    let magic = i128::from(statfs(root)?.f_type) & 0xFFFF_FFFF;
    fs_type_for_magic(u32::try_from(magic).ok()?).map(str::to_owned)
}

#[cfg(any(target_vendor = "apple", target_os = "freebsd"))]
fn statfs_fs_type(root: &Path) -> Option<String> {
    let name: Vec<u8> =
        statfs(root)?.f_fstypename.iter().map(|&c| c.to_ne_bytes()[0]).collect();
    let name = CStr::from_bytes_until_nul(&name).ok()?;
    Some(name.to_string_lossy().into_owned())
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple", target_os = "freebsd")))]
fn statfs_fs_type(_root: &Path) -> Option<String> {
    None
}

/// This host's name, or `unknown`.
pub fn host_name() -> String {
    hostname().unwrap_or_else(|| UNKNOWN.to_owned())
}

fn hostname() -> Option<String> {
    let mut buf = [0u8; 256];
    // SAFETY: `buf` is valid for writes of `buf.len()` bytes.
    #[expect(unsafe_code)]
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    if rc != 0 {
        return None;
    }
    let name = CStr::from_bytes_until_nul(&buf).ok()?;
    Some(name.to_string_lossy().into_owned())
}

fn kernel_release() -> Option<String> {
    // SAFETY: `utsname` holds only `c_char` arrays, so all zeros is a valid
    // value, and `uname` writes only into the struct it is given.
    #[expect(unsafe_code)]
    let (rc, uts) = unsafe {
        let mut uts: libc::utsname = std::mem::zeroed();
        (libc::uname(&mut uts), uts)
    };
    if rc != 0 {
        return None;
    }
    let release: Vec<u8> = uts.release.iter().map(|&c| c.to_ne_bytes()[0]).collect();
    let release = CStr::from_bytes_until_nul(&release).ok()?;
    Some(release.to_string_lossy().into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_filesystems_are_exactly_the_known_list() {
        for local in [
            "ext2", "ext3", "ext4", "xfs", "btrfs", "zfs", "tmpfs", "apfs", "hfs",
            "f2fs", "bcachefs", "overlay", "vfat", "exfat", "jfs", "nilfs2",
        ] {
            assert!(!is_network_fs(local), "{local}");
        }
        for network in [
            "nfs",
            "nfs4",
            "cifs",
            "smb3",
            "lustre",
            "gpfs",
            "ceph",
            "fuse.juicefs",
            "unknown",
            "",
            "EXT4",
            "ext4 ",
        ] {
            assert!(is_network_fs(network), "{network:?}");
        }
    }

    const MOUNTINFO: &str = "\
72 1 0:34 / / rw,relatime shared:1 - zfs rpool/system rw,xattr
36 72 0:6 / /dev rw,nosuid shared:14 - devtmpfs devtmpfs rw,size=3156388k
203 62 259:1 / /home/my\\040data rw,relatime shared:222 master:3 - ext4 /dev/nvme1n1p1 rw
299 203 0:66 /export /mnt/nfs rw,relatime - nfs4 server:/export rw,vers=4.2
";

    // glibc `makedev`
    fn makedev(major: u64, minor: u64) -> u64 {
        ((major & 0xfff) << 8)
            | ((major & !0xfff) << 32)
            | (minor & 0xff)
            | ((minor & !0xff) << 12)
    }

    #[test]
    fn mount_line_is_picked_by_device() {
        let (line, fs_type) = mount_line_for_dev(MOUNTINFO, makedev(259, 1)).unwrap();
        assert_eq!(
            line,
            "203 62 259:1 / /home/my\\040data rw,relatime shared:222 master:3 - ext4 \
             /dev/nvme1n1p1 rw"
        );
        assert_eq!(fs_type, "ext4");

        let (_, fs_type) = mount_line_for_dev(MOUNTINFO, makedev(0, 66)).unwrap();
        assert_eq!(fs_type, "nfs4");
        let (_, fs_type) = mount_line_for_dev(MOUNTINFO, makedev(0, 34)).unwrap();
        assert_eq!(fs_type, "zfs");

        assert_eq!(mount_line_for_dev(MOUNTINFO, makedev(0, 99)), None);
        assert_eq!(mount_line_for_dev(MOUNTINFO, makedev(1, 34)), None);
        assert_eq!(mount_line_for_dev("garbage\n\n1 2 3\n", makedev(0, 34)), None);
    }

    #[test]
    fn mount_line_handles_large_device_numbers() {
        let mountinfo = "10 1 4100:300000 / /big rw - xfs /dev/big rw\n";
        let (_, fs_type) = mount_line_for_dev(mountinfo, makedev(4100, 300_000)).unwrap();
        assert_eq!(fs_type, "xfs");
    }

    #[test]
    fn statfs_magic_maps_exactly_the_local_filesystems() {
        let known = [
            (0xEF53, "ext4"),
            (0x5846_5342, "xfs"),
            (0x9123_683E, "btrfs"),
            (0x2FC1_2FC1, "zfs"),
            (0x0102_1994, "tmpfs"),
            (0xF2F5_2010, "f2fs"),
            (0xCA45_1A4E, "bcachefs"),
            (0x794C_7630, "overlay"),
            (0x4D44, "vfat"),
            (0x2011_BAB0, "exfat"),
            (0x3153_464A, "jfs"),
            (0x3434, "nilfs2"),
        ];
        for (magic, name) in known {
            assert_eq!(fs_type_for_magic(magic), Some(name), "{magic:#x}");
            assert!(!is_network_fs(name));
        }
        // nfs, smb2, cifs, ceph, fuse, 0
        for magic in [0x6969, 0xFE53_4D42, 0xFF53_4D42, 0x00C3_6400, 0x6573_5546, 0] {
            assert_eq!(fs_type_for_magic(magic), None, "{magic:#x}");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn statfs_fallback_agrees_with_mountinfo() {
        let dir = tempfile::tempdir().unwrap();
        let Some((_, fs_type)) = mount_of(dir.path()) else { return };
        let expected = match fs_type.as_str() {
            "ext2" | "ext3" | "ext4" => Some("ext4"),
            "xfs" | "btrfs" | "zfs" | "tmpfs" | "f2fs" | "bcachefs" | "overlay"
            | "vfat" | "exfat" | "jfs" | "nilfs2" => Some(fs_type.as_str()),
            _ => None,
        };
        assert_eq!(statfs_fs_type(dir.path()).as_deref(), expected, "{fs_type}");
        assert_eq!(statfs_fs_type(Path::new("/proc")), None);
    }

    #[test]
    fn default_tag_is_a_valid_tag() {
        let tag = default_tag();
        assert!(crate::layout::is_valid_tag(&tag), "{tag:?}");
        assert!(tag.ends_with(&format!("-{}", std::process::id())), "{tag:?}");
    }

    #[test]
    fn build_is_the_icechunk_version_and_the_commit() {
        let build = Build::current();
        assert_eq!(build.icechunk_version, ICECHUNK_LIB_VERSION);
        assert_eq!(build.commit, COMMIT);
        assert!(!COMMIT.is_empty());
        assert_eq!(
            Build { icechunk_version: "2.3.0".to_owned(), commit: "0123abc".to_owned() }
                .to_string(),
            "Icechunk 2.3.0, commit 0123abc"
        );
    }

    #[test]
    fn host_info_describes_this_host() {
        let dir = tempfile::tempdir().unwrap();
        let info = host_info("t", dir.path());
        assert_eq!(info.tag, "t");
        assert_eq!(info.pid, std::process::id());
        assert_ne!(info.hostname, UNKNOWN);
        assert_ne!(info.kernel, UNKNOWN);
        assert!(info.registered_at_ms > 0);
        assert_eq!(info.build, Build::current());
        assert_eq!(info.network_fs, is_network_fs(&info.fs_type));
        #[cfg(target_os = "linux")]
        assert_ne!(info.fs_type, UNKNOWN);
    }
}
