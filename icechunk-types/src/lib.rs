pub mod error;

use core::fmt;
use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};
use serde_with::{TryFromInto, serde_as};
use thiserror::Error;
use typed_path::Utf8UnixPathBuf;

#[doc(hidden)]
pub mod sealed {
    pub trait Sealed {}
}

pub use error::{ICError, ICResultExt};

pub const DEFAULT_BRANCH: &str = "main";

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, PartialOrd, Ord)]
pub struct ETag(pub String);

/// A normalized Zarr path: absolute (starts with `/`) and no trailing slash.
#[serde_as]
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize)]
pub struct Path(#[serde_as(as = "TryFromInto<String>")] Utf8UnixPathBuf);

// The impl of Debug for Utf8UnixPathBuf is expensive and triggered often by tracing Spans
// This implemnetation is much cheaper, and removes formatting from our samply profiles.
impl Debug for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Errors when constructing a [`Path`].
#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum PathError {
    #[error("path must start with a `/` character")]
    NotAbsolute,
    #[error(r#"path must be cannonic, cannot include "." or "..""#)]
    NotCanonic,
}

impl Path {
    pub fn root() -> Path {
        Path(Utf8UnixPathBuf::from("/".to_string()))
    }

    // Fast-path unvalidated constructor for use when reading from Snapshots
    pub fn from_trusted(path: &str) -> Path {
        Path(Utf8UnixPathBuf::from(path))
    }

    pub fn new(path: &str) -> Result<Path, PathError> {
        let buf = Utf8UnixPathBuf::from(path);
        if !buf.is_absolute() {
            return Err(PathError::NotAbsolute);
        }

        if buf.normalize() != buf {
            return Err(PathError::NotCanonic);
        }
        Ok(Path(buf))
    }

    pub fn starts_with(&self, other: &Path) -> bool {
        self.0.starts_with(&other.0)
    }

    pub fn ancestors(&self) -> impl Iterator<Item = Path> + '_ {
        self.0.ancestors().map(|p| Path(p.to_owned()))
    }

    pub fn name(&self) -> Option<&str> {
        self.0.file_name()
    }

    pub fn buf(&self) -> &Utf8UnixPathBuf {
        &self.0
    }
}

impl TryFrom<&str> for Path {
    type Error = PathError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for Path {
    type Error = PathError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

impl TryFrom<String> for Path {
    type Error = PathError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Move {
    pub from: Path,
    pub to: Path,
}

/// Returns the user-agent string for icechunk HTTP requests.
///
/// Format: `icechunk-rust-<version>` (e.g., `icechunk-rust-2.0.0-alpha.4`).
pub fn user_agent() -> &'static str {
    concat!("icechunk-rust-", env!("CARGO_PKG_VERSION"))
}
