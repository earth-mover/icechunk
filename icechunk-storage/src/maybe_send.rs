//! `Send` bounds that relax in the browser.
//!
//! On `wasm32-unknown-unknown` there are no threads and browser futures
//! (`fetch`, JS promises) are not `Send`. Everywhere else these are exactly
//! `Send`, so native callers still see `Send` through any `impl Trait +
//! MaybeSend` they get back.

#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
mod imp {
    /// `Send`, except on `wasm32-unknown-unknown`, where every type implements it.
    pub trait MaybeSend: Send {}
    impl<T: Send + ?Sized> MaybeSend for T {}

    /// A `BoxStream`, except on `wasm32-unknown-unknown`, where it is a `LocalBoxStream`.
    pub type MaybeSendBoxStream<'a, T> = futures::stream::BoxStream<'a, T>;

    /// `.boxed()`, except on `wasm32-unknown-unknown`, where it is `.boxed_local()`.
    #[macro_export]
    macro_rules! maybe_send_boxed {
        ($e:expr) => {
            ($e).boxed()
        };
    }
}

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
mod imp {
    /// `Send`, except on `wasm32-unknown-unknown`, where every type implements it.
    pub trait MaybeSend {}
    impl<T: ?Sized> MaybeSend for T {}

    /// A `BoxStream`, except on `wasm32-unknown-unknown`, where it is a `LocalBoxStream`.
    pub type MaybeSendBoxStream<'a, T> = futures::stream::LocalBoxStream<'a, T>;

    /// `.boxed()`, except on `wasm32-unknown-unknown`, where it is `.boxed_local()`.
    #[macro_export]
    macro_rules! maybe_send_boxed {
        ($e:expr) => {
            ($e).boxed_local()
        };
    }
}

pub use imp::{MaybeSend, MaybeSendBoxStream};
