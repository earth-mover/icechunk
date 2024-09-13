use ::futures::{stream::BoxStream, TryStreamExt};
use async_tar::{Archive, Builder, HeaderMode};
use futures::Stream;
use rocksdb::{Env, OptimisticTransactionDB, Options, ThreadMode};
use std::{io::Write, path::Path as StdPath};
use tokio::{io::AsyncWrite, pin};
use tokio_util::compat::TokioAsyncWriteCompatExt;

use bytes::Bytes;

pub async fn extract_db<E: Into<std::io::Error>>(
    destination_path: &StdPath,
    db_bytes: impl Stream<Item = Result<Bytes, E>>,
) -> Result<(), std::io::Error> {
    pin!(db_bytes);
    let read = db_bytes.map_err(|e| e.into()).into_async_read();
    Archive::new(read).unpack(destination_path).await
}

pub async fn compress_db<W: AsyncWrite + Send + Sync + Unpin>(
    db_path: &StdPath,
    w: W,
) -> Result<Box<impl futures::io::AsyncWrite>, std::io::Error> {
    let mut b = Builder::new(w.compat_write());
    b.mode(HeaderMode::Deterministic);
    b.follow_symlinks(false);
    b.append_dir_all(".", db_path).await?;
    b.finish().await?;
    let w = b.into_inner().await?;
    Ok(Box::new(w))
}

pub fn open_db<T: ThreadMode>(
    db_path: &StdPath,
    db_env: &Env,
) -> Result<OptimisticTransactionDB<T>, rocksdb::Error> {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_env(db_env);
    // FIXME: more rocksdb configuration
    // FIXME: open for read only
    OptimisticTransactionDB::open(&options, db_path)
}
