//! `ObjectStore` wrapper that fails the Nth get (for testing).

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use futures::stream::BoxStream;
use icechunk_arrow_object_store::object_store::{
    self, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    path::Path as StorePath,
};

/// Delegates every call to `inner`, except `get_opts` lets the first
/// `succeed_first` calls through and returns `Error::Generic` for
/// every subsequent call. Pair with `concurrency = 1` for
/// deterministic failure points.
#[derive(Debug)]
pub struct FaultyStore {
    inner: Arc<dyn ObjectStore>,
    succeed_first: usize,
    gets: AtomicUsize,
}

impl FaultyStore {
    pub fn new(inner: Arc<dyn ObjectStore>, succeed_first: usize) -> Self {
        Self { inner, succeed_first, gets: AtomicUsize::new(0) }
    }
}

impl fmt::Display for FaultyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultyStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultyStore {
    async fn get_opts(
        &self,
        location: &StorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if self.gets.fetch_add(1, Ordering::Relaxed) >= self.succeed_first {
            return Err(object_store::Error::Generic {
                store: "FaultyStore",
                source: "simulated failure".into(),
            });
        }
        self.inner.get_opts(location, options).await
    }

    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<StorePath>>,
    ) -> BoxStream<'static, object_store::Result<StorePath>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    fn list_with_offset(
        &self,
        prefix: Option<&StorePath>,
        offset: &StorePath,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
