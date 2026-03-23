//! Generic error wrapper with span tracing.
//!
//! [`ICError<E>`] wraps an error kind `E` with a [`SpanTrace`] captured at the
//! point of construction. Every error type in icechunk is an alias for
//! `ICError<SomeErrorKind>` (e.g. `type SessionError = ICError<SessionErrorKind>`).
//!
//! # Converting errors
//!
//! Prefer **`inject`** over `capture` when the error is already an [`ICError`],
//! because `inject` preserves the original span trace while `capture` replaces
//! it with a new one captured at the current call site.
//!
//! | Method | Input | Span trace | Use when |
//! |---|---|---|---|
//! | [`ICError::capture`] | bare error kind | new | constructing a fresh error |
//! | [`.capture()`](ICResultExt::capture) | `Result<T, E>` where `E: Into<Kind>` | new | converting a foreign (non-ICError) result |
//! | [`.capture_box()`](ICResultExt::capture_box) | `Result<T, E>` where `E: Error` | new | same, when Kind has `From<Box<dyn Error>>` |
//! | [`.inject()`](ICResultCtxExt::inject) | `Result<T, ICError<E>>` | **preserved** | propagating between `ICError` kinds |
//! | [`ICError::inject`] | `ICError<E>` | **preserved** | same, outside of Result |
//!
//! # Examples
//!
//! **Constructing a fresh error** — use the type alias, not `ICError` directly:
//! ```ignore
//! Err(SessionError::capture(SessionErrorKind::ReadOnlySession))
//! ```
//!
//! **Converting a foreign result** (e.g. serde, I/O):
//! ```ignore
//! serde_json::to_vec(&data).capture()?;          // E: Into<Kind>
//! builder.build().capture_box()?;                 // E: Error, Kind: From<Box<dyn Error>>
//! rmp_serde::to_vec(&data).map_err(Box::new).capture()?;  // Kind: From<Box<ConcreteError>>
//! ```
//!
//! **Propagating between `ICError` kinds** — preserves span trace:
//! ```ignore
//! session_fn().inject()?;   // SessionError → RepositoryError
//! ```

use std::fmt::Display;

use tracing_error::SpanTrace;

#[derive(Debug)]
pub struct ICError<E> {
    pub kind: E,
    pub context: SpanTrace,
}

impl<E> ICError<E> {
    /// Wrap `kind` in an [`ICError`] with a [`SpanTrace`] captured at the call site.
    pub fn capture(kind: E) -> Self {
        Self { kind, context: SpanTrace::capture() }
    }

    pub fn kind(&self) -> &E {
        &self.kind
    }

    pub fn span(&self) -> &SpanTrace {
        &self.context
    }

    /// Convert the error kind, preserving the original span trace.
    ///
    /// Prefer this over [`capture`](Self::capture) when you already have an
    /// `ICError` and want to change the kind without losing trace context.
    pub fn inject<F>(self) -> ICError<F>
    where
        E: Into<F>,
    {
        ICError { kind: self.kind.into(), context: self.context }
    }
}

impl<E: Display> Display for ICError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.kind.fmt(f)?;
        write!(f, "\n\ncontext:\n{}\n", self.context)?;
        Ok(())
    }
}

impl<E: std::error::Error + 'static> std::error::Error for ICError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.kind)
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
}

// We intentionally omit `From<E> for ICError<E>` to force callers to choose
// between `capture` (new span trace) and `inject` (preserved span trace).

/// Extension trait for converting `Result<T, E>` into `Result<T, ICError<Kind>>`,
/// capturing a new span trace.
pub trait ICResultExt<T, E> {
    /// Convert the error via `E: Into<Kind>`, capturing a new span trace.
    fn capture<Kind>(self) -> Result<T, ICError<Kind>>
    where
        E: Into<Kind>;

    /// Box the error into `Box<dyn Error>`, then convert via `Into<Kind>`,
    /// capturing a new span trace.
    ///
    /// Use this when the error kind has `#[from] Box<dyn Error + Send + Sync>`.
    /// When the kind instead has `#[from] Box<ConcreteError>`, use
    /// `.map_err(Box::new).capture()`.
    fn capture_box<Kind>(self) -> Result<T, ICError<Kind>>
    where
        E: std::error::Error + Send + Sync + 'static,
        Box<dyn std::error::Error + Send + Sync + 'static>: Into<Kind>;
}

impl<T, E> ICResultExt<T, E> for Result<T, E> {
    fn capture<Kind>(self) -> Result<T, ICError<Kind>>
    where
        E: Into<Kind>,
    {
        self.map_err(|e| ICError::capture(e.into()))
    }

    fn capture_box<Kind>(self) -> Result<T, ICError<Kind>>
    where
        E: std::error::Error + Send + Sync + 'static,
        Box<dyn std::error::Error + Send + Sync + 'static>: Into<Kind>,
    {
        self.map_err(|e| {
            let e: Box<dyn std::error::Error + Send + Sync + 'static> = Box::new(e);
            ICError::capture(e.into())
        })
    }
}

/// Extension trait for converting `Result<T, ICError<E>>` into
/// `Result<T, ICError<Kind>>`, **preserving** the original span trace.
///
/// Prefer this over [`ICResultExt`] when the error is already an [`ICError`].
pub trait ICResultCtxExt<T, E> {
    fn inject<Kind>(self) -> Result<T, ICError<Kind>>
    where
        E: Into<Kind>;
}

impl<T, E> ICResultCtxExt<T, E> for Result<T, ICError<E>> {
    fn inject<Kind>(self) -> Result<T, ICError<Kind>>
    where
        E: Into<Kind>,
    {
        self.map_err(|e| e.inject())
    }
}
