//! Local filesystem store with compare-and-swap writes.

use std::{
    fmt::{self, Display},
    fs::File,
    ops::Range,
    path::{Path as StdPath, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt as _, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, RenameOptions, Result, UpdateVersion, local::LocalFileSystem,
    path::Path,
};
use url::Url;

/// `LocalFileSystem` plus `PutMode::Update`.
///
/// An update locks the target file's inode, compares the etag under the lock,
/// and then publishes with the inner store's staged rename. The rename creates a
/// new inode and the etag includes it, so a writer that still holds the lock on
/// the old inode fails the compare. Advisory locks do not work on NFS.
#[derive(Debug)]
pub(crate) struct ConditionalLocalFileSystem {
    inner: Arc<LocalFileSystem>,
    root: Url,
}

impl ConditionalLocalFileSystem {
    pub(crate) fn new_with_prefix(root: &StdPath) -> Result<Self> {
        let inner = Arc::new(LocalFileSystem::new_with_prefix(root)?);
        let root = Url::from_directory_path(root).map_err(|()| Error::Generic {
            store: "ConditionalLocalFileSystem",
            source: format!("{} is not an absolute path", root.display()).into(),
        })?;
        Ok(Self { inner, root })
    }

    fn filesystem_path(&self, location: &Path) -> Result<PathBuf> {
        let mut url = self.root.clone();
        url.path_segments_mut()
            .map_err(|()| generic("root url cannot be a base"))?
            .extend(location.parts().map(|p| p.as_ref().to_string()));
        url.to_file_path().map_err(|()| generic("url is not a file path"))
    }

    /// Open and lock the current inode. A missing file means the caller's
    /// version is stale.
    async fn lock(&self, location: &Path) -> Result<File> {
        let path = self.filesystem_path(location)?;
        let key = location.to_string();
        tokio::task::spawn_blocking(move || {
            let file = File::open(&path).map_err(|source| {
                if source.kind() == std::io::ErrorKind::NotFound {
                    Error::Precondition { path: key.clone(), source: source.into() }
                } else {
                    Error::Generic {
                        store: "ConditionalLocalFileSystem",
                        source: source.into(),
                    }
                }
            })?;
            file.lock().map_err(|source| Error::Generic {
                store: "ConditionalLocalFileSystem",
                source: source.into(),
            })?;
            Ok(file)
        })
        .await
        .map_err(|source| Error::Generic {
            store: "ConditionalLocalFileSystem",
            source: source.into(),
        })?
    }
}

fn generic(msg: &'static str) -> Error {
    Error::Generic { store: "ConditionalLocalFileSystem", source: msg.into() }
}

/// Name the network filesystem that holds `path`, if it is on one. `flock` may
/// guard one client only there, so a conditional put cannot be trusted.
pub(crate) fn network_filesystem(path: &StdPath) -> Option<&'static str> {
    // A repository is often created before its directory exists.
    let mut probe = path;
    loop {
        if probe.exists() {
            return network_filesystem_of(&statfs(probe)?);
        }
        probe = probe.parent()?;
    }
}

#[expect(unsafe_code)]
fn statfs(path: &StdPath) -> Option<libc::statfs> {
    use std::os::unix::ffi::OsStrExt as _;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `path` is a NUL-terminated C string, and `stat` points at enough
    // writable space for one `statfs`.
    let filled = unsafe { libc::statfs(path.as_ptr(), stat.as_mut_ptr()) } == 0;
    // SAFETY: a return of 0 means `statfs` initialized the struct.
    filled.then(|| unsafe { stat.assume_init() })
}

/// Linux reports a magic number. `i128` holds it on every target, because the
/// width of `f_type` follows the word size.
#[cfg(target_os = "linux")]
fn network_filesystem_of(stat: &libc::statfs) -> Option<&'static str> {
    network_filesystem_for_magic(stat.f_type.into())
}

/// The magic numbers that `statfs(2)` documents.
#[cfg(target_os = "linux")]
fn network_filesystem_for_magic(magic: i128) -> Option<&'static str> {
    match magic {
        0x6969 => Some("nfs"),
        0x517b => Some("smb"),
        0xff53_4d42 => Some("cifs"),
        0xfe53_4d42 => Some("smb2"),
        _ => None,
    }
}

/// BSD reports a name rather than a magic number.
#[cfg(not(target_os = "linux"))]
fn network_filesystem_of(stat: &libc::statfs) -> Option<&'static str> {
    let name: Vec<u8> =
        stat.f_fstypename.iter().take_while(|c| **c != 0).map(|c| *c as u8).collect();
    network_filesystem_for_name(std::str::from_utf8(&name).ok()?)
}

#[cfg(not(target_os = "linux"))]
fn network_filesystem_for_name(name: &str) -> Option<&'static str> {
    match name {
        "nfs" => Some("nfs"),
        "smbfs" => Some("smbfs"),
        "webdav" => Some("webdav"),
        _ => None,
    }
}

impl Display for ConditionalLocalFileSystem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConditionalLocalFileSystem({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ConditionalLocalFileSystem {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let PutMode::Update(UpdateVersion { e_tag: Some(expected), .. }) = &opts.mode
        else {
            return self.inner.put_opts(location, payload, opts).await;
        };
        let lock = self.lock(location).await?;
        let current = match self.inner.head(location).await {
            Ok(meta) => meta.e_tag,
            Err(Error::NotFound { path, source }) => {
                return Err(Error::Precondition { path, source });
            }
            Err(err) => return Err(err),
        };
        if current.as_deref() != Some(expected.as_str()) {
            return Err(Error::Precondition {
                path: location.to_string(),
                source: format!("etag {current:?} does not match {expected}").into(),
            });
        }
        let opts = PutOptions { mode: PutMode::Overwrite, ..opts };
        let res = self.inner.put_opts(location, payload, opts).await;
        drop(lock);
        res
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use bytes::Bytes;
    use futures::TryStreamExt as _;
    use icechunk_macros::tokio_test;
    use icechunk_storage::{Settings, Storage as _, VersionInfo, VersionedUpdateResult};
    use tempfile::TempDir;

    use crate::ObjectStorage;

    async fn put(
        store: &ObjectStorage,
        settings: &Settings,
        path: &str,
        content: &'static [u8],
        version: Option<&VersionInfo>,
    ) -> VersionedUpdateResult {
        store
            .put_object(
                settings,
                path,
                Bytes::from_static(content),
                None,
                Default::default(),
                version,
            )
            .await
            .unwrap()
    }

    fn updated(res: VersionedUpdateResult) -> VersionInfo {
        match res {
            VersionedUpdateResult::Updated { new_version } => new_version,
            VersionedUpdateResult::NotOnLatestVersion => {
                panic!("expected the write to land")
            }
        }
    }

    async fn get(
        store: &ObjectStorage,
        settings: &Settings,
        path: &str,
    ) -> (Vec<u8>, VersionInfo) {
        let (stream, version) = store
            .get_object_range_conditional(settings, path, None, None)
            .await
            .unwrap()
            .unwrap();
        let chunks: Vec<Bytes> = stream.try_collect().await.unwrap();
        (chunks.concat(), version)
    }

    #[tokio_test]
    async fn update_with_current_etag_lands_and_changes_etag() {
        let tmp = TempDir::new().unwrap();
        let store = ObjectStorage::new_local_filesystem(tmp.path()).await.unwrap();
        let settings = store.default_settings().await.unwrap();
        let path = "refs/branch.main/ref.json";

        let v1 = updated(
            put(&store, &settings, path, b"v1", Some(&VersionInfo::for_creation())).await,
        );
        let v2 = updated(put(&store, &settings, path, b"v2", Some(&v1)).await);

        assert!(v2.etag().is_some());
        assert_ne!(v1.etag(), v2.etag());
        assert_eq!(std::fs::read(tmp.path().join(path)).unwrap(), b"v2");
    }

    #[tokio_test]
    async fn update_after_delete_is_rejected() {
        let tmp = TempDir::new().unwrap();
        let store = ObjectStorage::new_local_filesystem(tmp.path()).await.unwrap();
        let settings = store.default_settings().await.unwrap();
        let path = "refs/branch.main/ref.json";

        let v1 = updated(
            put(&store, &settings, path, b"v1", Some(&VersionInfo::for_creation())).await,
        );
        std::fs::remove_file(tmp.path().join(path)).unwrap();

        let stale = put(&store, &settings, path, b"v2", Some(&v1)).await;
        assert!(matches!(stale, VersionedUpdateResult::NotOnLatestVersion));
        assert!(!tmp.path().join(path).exists());
    }

    /// Each task runs its own client and retries on conflict. Every increment
    /// must survive, so the final counter equals the total number of writes.
    #[tokio_test]
    async fn concurrent_updates_lose_no_increment() {
        const TASKS: usize = 8;
        const INCREMENTS: usize = 50;
        let tmp = TempDir::new().unwrap();
        let path = "counter";
        {
            let store = ObjectStorage::new_local_filesystem(tmp.path()).await.unwrap();
            let settings = store.default_settings().await.unwrap();
            updated(
                put(&store, &settings, path, b"0", Some(&VersionInfo::for_creation()))
                    .await,
            );
        }

        let root: &Path = tmp.path();
        let tasks: Vec<_> = (0..TASKS)
            .map(|_| {
                let root = root.to_path_buf();
                tokio::spawn(async move {
                    let store = ObjectStorage::new_local_filesystem(&root).await.unwrap();
                    let settings = store.default_settings().await.unwrap();
                    for _ in 0..INCREMENTS {
                        loop {
                            let (bytes, version) = get(&store, &settings, path).await;
                            let n: usize =
                                std::str::from_utf8(&bytes).unwrap().parse().unwrap();
                            let next = Bytes::from((n + 1).to_string());
                            let res = store
                                .put_object(
                                    &settings,
                                    path,
                                    next,
                                    None,
                                    Default::default(),
                                    Some(&version),
                                )
                                .await
                                .unwrap();
                            if matches!(res, VersionedUpdateResult::Updated { .. }) {
                                break;
                            }
                        }
                    }
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }

        let final_count = std::fs::read_to_string(root.join(path)).unwrap();
        assert_eq!(final_count, (TASKS * INCREMENTS).to_string());
    }

    #[test]
    fn a_local_directory_is_not_a_network_filesystem() {
        let dir = TempDir::new().unwrap();
        assert_eq!(super::network_filesystem(dir.path()), None);
        // A repository directory is often named before it exists.
        assert_eq!(super::network_filesystem(&dir.path().join("repo")), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn statfs_magic_numbers_name_the_network_filesystems() {
        use super::network_filesystem_for_magic as of;
        assert_eq!(of(0x6969), Some("nfs"));
        assert_eq!(of(0xff53_4d42), Some("cifs"));
        assert_eq!(of(0xfe53_4d42), Some("smb2"));
        assert_eq!(of(0x517b), Some("smb"));
        // ext4 and tmpfs must stay eligible for conditional updates.
        assert_eq!(of(0xef53), None);
        assert_eq!(of(0x0102_1994), None);
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn statfs_names_the_network_filesystems() {
        use super::network_filesystem_for_name as of;
        assert_eq!(of("nfs"), Some("nfs"));
        assert_eq!(of("smbfs"), Some("smbfs"));
        // apfs must stay eligible for conditional updates.
        assert_eq!(of("apfs"), None);
    }

    #[tokio_test]
    async fn update_with_stale_etag_is_rejected() {
        let tmp = TempDir::new().unwrap();
        let store = ObjectStorage::new_local_filesystem(tmp.path()).await.unwrap();
        let settings = store.default_settings().await.unwrap();
        let path = "refs/branch.main/ref.json";

        let v1 = updated(
            put(&store, &settings, path, b"v1", Some(&VersionInfo::for_creation())).await,
        );
        let _v2 = updated(put(&store, &settings, path, b"v2", Some(&v1)).await);

        let stale = put(&store, &settings, path, b"v3", Some(&v1)).await;
        assert!(matches!(stale, VersionedUpdateResult::NotOnLatestVersion));
        assert_eq!(std::fs::read(tmp.path().join(path)).unwrap(), b"v2");
    }
}
