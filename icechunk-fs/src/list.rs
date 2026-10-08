use std::{
    fs, io, mem,
    path::{Path, PathBuf},
};

use futures::{Stream, StreamExt as _, future::ready, stream};
use icechunk_storage::{DeleteObjectsResult, ListInfo};
use tokio::sync::mpsc;

use crate::{path::is_staging_name, storage::is_missing};

const BATCH_SIZE: usize = 1000;
const CHANNEL_CAPACITY: usize = 2;

type Batch = io::Result<Vec<ListInfo<String>>>;

/// Streams the regular files under `dir`, except in-flight staging files, with
/// ids relative to `dir`. A missing `dir` lists nothing, and entries deleted
/// while the walk runs are skipped. The walk runs on a blocking thread and
/// stops soon after the stream is dropped; an error ends the stream.
pub(crate) fn list_recursive(
    dir: PathBuf,
) -> impl Stream<Item = io::Result<ListInfo<String>>> + Send + 'static {
    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let walker = tokio::task::spawn_blocking(move || {
        if let Err(err) = walk(&dir, &tx) {
            // the receiver may be gone; then nobody needs the error
            drop(tx.blocking_send(Err(err)));
        }
    });
    stream::unfold(Some((rx, walker)), |state| async move {
        let (mut rx, walker) = state?;
        match rx.recv().await {
            Some(batch) => Some((batch, Some((rx, walker)))),
            // a walker that panicked must not look like a complete listing
            None => walker.await.err().map(|err| {
                (Err(io::Error::other(format!("listing task failed: {err}"))), None)
            }),
        }
    })
    .flat_map(|batch| match batch {
        Ok(infos) => stream::iter(infos.into_iter().map(Ok)).left_stream(),
        Err(err) => stream::once(ready(Err(err))).right_stream(),
    })
}

/// Returns early, without error, once the receiver is dropped.
fn walk(dir: &Path, tx: &mpsc::Sender<Batch>) -> io::Result<()> {
    let mut batch = Vec::new();
    let mut pending = vec![(dir.to_path_buf(), String::new())];
    while let Some((dir, id_prefix)) = pending.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if is_missing(&err) => continue,
            Err(err) => return Err(err),
        };
        for entry in entries {
            if tx.is_closed() {
                return Ok(());
            }
            let entry = entry?;
            let file_name = entry.file_name();
            let name = file_name.to_str().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("non-UTF-8 file name: {}", entry.path().display()),
                )
            })?;
            let id = format!("{id_prefix}{name}");
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(err) if is_missing(&err) => continue,
                Err(err) => return Err(err),
            };
            if file_type.is_dir() {
                pending.push((entry.path(), format!("{id}/")));
            } else if file_type.is_file() && !is_staging_name(&file_name) {
                let metadata = match entry.metadata() {
                    Ok(metadata) => metadata,
                    Err(err) if is_missing(&err) => continue,
                    Err(err) => return Err(err),
                };
                batch.push(ListInfo {
                    id,
                    created_at: metadata.modified()?.into(),
                    size_bytes: metadata.len(),
                });
                if batch.len() >= BATCH_SIZE
                    && tx.blocking_send(Ok(mem::take(&mut batch))).is_err()
                {
                    return Ok(());
                }
            }
        }
    }
    if !batch.is_empty() {
        drop(tx.blocking_send(Ok(batch)));
    }
    Ok(())
}

/// Removes each file, counting a missing one as deleted. Other failures are
/// logged and left out of the result.
pub(crate) fn delete_files(paths: Vec<(PathBuf, u64)>) -> DeleteObjectsResult {
    paths.into_iter().fold(DeleteObjectsResult::default(), |mut res, (path, size)| {
        match fs::remove_file(&path) {
            Err(err) if !is_missing(&err) => {
                tracing::error!(path = %path.display(), error = %err, "Error deleting object");
            }
            _ => {
                res.deleted_objects += 1;
                res.deleted_bytes += size;
            }
        }
        res
    })
}
