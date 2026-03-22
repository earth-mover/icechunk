pub mod error;

#[doc(hidden)]
pub mod sealed {
    pub trait Sealed {}
}

pub use error::{ICError, ICResultExt};
