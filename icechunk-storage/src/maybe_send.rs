//! `Send` bounds that relax in the browser.
//!
//! On `wasm32-unknown-unknown` there are no threads and browser futures
//! (`fetch`, JS promises) are not `Send`. Everywhere else these are exactly
//! `Send`, so native callers still see `Send` through any `impl Trait +
//! MaybeSend` they get back.

/// `Send` everywhere except `wasm32-unknown-unknown`, where every type
/// implements it.
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
pub trait MaybeSend: Send {}
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
impl<T: Send + ?Sized> MaybeSend for T {}

/// `Send` everywhere except `wasm32-unknown-unknown`, where every type
/// implements it.
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
pub trait MaybeSend {}
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
impl<T: ?Sized> MaybeSend for T {}

/// [`BoxStream`](futures::stream::BoxStream) everywhere except `wasm32-unknown-unknown`, where it is a
/// [`LocalBoxStream`](futures::stream::LocalBoxStream).
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
pub type MaybeSendBoxStream<'a, T> = futures::stream::BoxStream<'a, T>;
/// [`BoxStream`](futures::stream::BoxStream) everywhere except `wasm32-unknown-unknown`, where it is a
/// [`LocalBoxStream`](futures::stream::LocalBoxStream).
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
pub type MaybeSendBoxStream<'a, T> = futures::stream::LocalBoxStream<'a, T>;

/// Box a future or stream as `Send` (`.boxed()`) everywhere except
/// `wasm32-unknown-unknown`, where it is boxed local (`.boxed_local()`).
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
#[macro_export]
macro_rules! maybe_send_boxed {
    ($e:expr) => {
        ($e).boxed()
    };
}

/// See the non-wasm definition.
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
#[macro_export]
macro_rules! maybe_send_boxed {
    ($e:expr) => {
        ($e).boxed_local()
    };
}
