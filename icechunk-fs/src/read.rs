//! Blocking reads of object content and metadata.

use std::{
    fs::{File, Metadata},
    io::{self, Read as _, Seek as _, SeekFrom},
    ops::Range,
    path::Path,
};

use bytes::Bytes;
use chrono::{DateTime, Utc};

use icechunk_storage::Generation;

use crate::version::generation_of;

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

#[derive(Debug)]
pub(crate) enum FreshRead {
    /// The file's generation is the known one; nothing was read.
    Unchanged,
    Content {
        bytes: Bytes,
        generation: Generation,
    },
}

/// Opens `path` through a new file descriptor and `fstat`s it: `stat` can be
/// answered from the NFS attribute cache, `open` cannot. If the generation is
/// `known_generation` the file isn't read; otherwise it is read through the
/// same descriptor, so the content and metadata are of the same file.
pub(crate) fn read_fresh(
    path: &Path,
    known_generation: Option<&str>,
) -> io::Result<FreshRead> {
    let (mut file, meta) = open_fresh(path)?;
    let generation = generation_of(&meta);
    if known_generation.is_some_and(|known| known == generation.0) {
        return Ok(FreshRead::Unchanged);
    }
    let bytes = read_rest(&mut file, &meta)?;
    Ok(FreshRead::Content { bytes, generation })
}

/// Like [`read_fresh`], always reading the content.
pub(crate) fn read_with_metadata(path: &Path) -> io::Result<(Bytes, Metadata)> {
    let (mut file, meta) = open_fresh(path)?;
    Ok((read_rest(&mut file, &meta)?, meta))
}

fn open_fresh(path: &Path) -> io::Result<(File, Metadata)> {
    let file = File::open(path)?;
    let meta = file.metadata()?;
    Ok((file, meta))
}

fn read_rest(file: &mut File, meta: &Metadata) -> io::Result<Bytes> {
    let mut buf = Vec::new();
    buf.try_reserve_exact(usize::try_from(meta.len()).unwrap_or(0))
        .map_err(io::Error::other)?;
    file.read_to_end(&mut buf)?;
    Ok(buf.into())
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
    use std::fs;

    use super::*;

    #[test]
    fn fresh_read_skips_a_known_generation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repo");
        fs::write(&path, b"first").unwrap();

        let FreshRead::Content { bytes, generation } = read_fresh(&path, None).unwrap()
        else {
            panic!("no generation given, must read")
        };
        assert_eq!(bytes.as_ref(), b"first");
        let Generation(generation) = generation;
        assert!(matches!(
            read_fresh(&path, Some(&generation)).unwrap(),
            FreshRead::Unchanged
        ));

        let staging = dir.path().join("staging");
        fs::write(&staging, b"second").unwrap();
        fs::rename(&staging, &path).unwrap();
        let FreshRead::Content { bytes, generation: new_generation } =
            read_fresh(&path, Some(&generation)).unwrap()
        else {
            panic!("the file was replaced, must read")
        };
        assert_eq!(bytes.as_ref(), b"second");
        assert_ne!(new_generation.0, generation);
        assert_eq!(new_generation, generation_of(&fs::metadata(&path).unwrap()));
    }

    #[test]
    fn fresh_read_of_missing_file_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let err = read_fresh(&dir.path().join("repo"), None).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }
}
