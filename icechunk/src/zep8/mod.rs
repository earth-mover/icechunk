//! ZEP 8 URL specification support for Icechunk.
//!
//! This module provides parsing and session creation utilities for
//! ZEP 8 pipe-chained URL syntax with icechunk adapters.

pub mod path_spec;
pub mod session_factory;
pub mod storage_factory;

pub use path_spec::{IcechunkPathSpec, ParseError, ReferenceType};
pub use session_factory::{ZepSessionError, create_readonly_session};
pub use storage_factory::{ZepStorageError, create_storage_from_url};
