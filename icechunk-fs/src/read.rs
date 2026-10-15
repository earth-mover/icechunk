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
    check_range(range, file.metadata()?.len(), path)?;
    if range.is_empty() {
        return Ok(Bytes::new());
    }
    let len = usize::try_from(range.end - range.start).map_err(io::Error::other)?;
    let mut buf = vec![0; len];
    file.seek(SeekFrom::Start(range.start))?;
    file.read_exact(&mut buf)?;
    Ok(buf.into())
}

/// `range` of `bytes`, the content of `path`, with the errors of [`read_range`].
pub(crate) fn slice_range(
    bytes: &Bytes,
    range: &Range<u64>,
    path: &Path,
) -> io::Result<Bytes> {
    check_range(range, bytes.len() as u64, path)?;
    let start = usize::try_from(range.start).map_err(io::Error::other)?;
    let end = usize::try_from(range.end).map_err(io::Error::other)?;
    Ok(bytes.slice(start..end))
}

fn check_range(range: &Range<u64>, size: u64, path: &Path) -> io::Result<()> {
    if range.start > range.end || range.end > size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "range {range:?} out of bounds for {} ({size} bytes)",
                path.display()
            ),
        ));
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slices_like_a_range_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x");
        std::fs::write(&path, b"0123456789").unwrap();
        let bytes = read_all(&path).unwrap();
        for range in [0..10, 2..5, 3..3, 10..10] {
            assert_eq!(
                slice_range(&bytes, &range, &path).unwrap(),
                read_range(&path, Some(&range)).unwrap(),
                "{range:?}"
            );
        }
        #[expect(clippy::reversed_empty_ranges)]
        for range in [5..11, 20..20, 5..4, 0..u64::MAX] {
            let sliced = slice_range(&bytes, &range, &path).unwrap_err();
            let read = read_range(&path, Some(&range)).unwrap_err();
            assert_eq!(sliced.kind(), io::ErrorKind::InvalidInput, "{range:?}");
            assert_eq!(sliced.to_string(), read.to_string(), "{range:?}");
        }
    }
}
