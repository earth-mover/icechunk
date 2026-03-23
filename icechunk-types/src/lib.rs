pub mod error;

#[doc(hidden)]
pub mod sealed {
    pub trait Sealed {}
}

pub use error::{ICError, ICResultExt};

/// Returns the user-agent string for icechunk HTTP requests.
///
/// Format: `icechunk-rust-<version>` (e.g., `icechunk-rust-2.0.0-alpha.4`).
pub fn user_agent() -> &'static str {
    concat!("icechunk-rust-", env!("CARGO_PKG_VERSION"))
}
