//! Blocking reads of object content and metadata.

use std::{
    fs::File,
    io::{self, Read as _, Seek as _, SeekFrom},
    ops::Range,
    path::Path,
};

use bytes::Bytes;
use chrono::{DateTime, Utc};

/// Reads `range` of the file, or all of it, through a newly opened file
/// descriptor, without mmap: on NFS only a fresh `open` is guaranteed to see
/// another client's rename. A range outside the file is an error.
pub(crate) fn read_range(path: &Path, range: Option<&Range<u64>>) -> io::Result<Bytes> {
    let Some(range) = range else { return read_all(path) };
    let mut file = File::open(path)?;
    let size = file.metadata()?.len();
    if range.start > range.end || range.end > size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "range {range:?} out of bounds for {} ({size} bytes)",
                path.display()
            ),
        ));
    }
    if range.is_empty() {
        return Ok(Bytes::new());
    }
    let len = usize::try_from(range.end - range.start).map_err(io::Error::other)?;
    let mut buf = vec![0; len];
    file.seek(SeekFrom::Start(range.start))?;
    file.read_exact(&mut buf)?;
    Ok(buf.into())
}

/// Reads the whole file through a newly opened file descriptor, like
/// [`read_range`].
pub(crate) fn read_all(path: &Path) -> io::Result<Bytes> {
    std::fs::read(path).map(Bytes::from)
}

pub(crate) fn last_modified(path: &Path) -> io::Result<DateTime<Utc>> {
    let metadata = std::fs::metadata(path)?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("{} is not a file", path.display()),
        ));
    }
    Ok(metadata.modified()?.into())
}
