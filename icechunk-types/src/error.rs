//! Generic error wrapper with span tracing.
//!
//! [`ICError<E>`] wraps an error kind `E` with a [`SpanTrace`] for debugging.
//!
//! [`SpanTrace`]: tracing_error::SpanTrace

use std::fmt::Display;

use tracing_error::SpanTrace;

#[derive(Debug)]
pub struct ICError<E> {
    pub kind: E,
    pub context: SpanTrace,
}

impl<E> ICError<E> {
    // FIXME: delete in favor of self.inject
    pub fn no_context(kind: E) -> Self {
        Self { kind, context: SpanTrace::capture() }
    }

    pub fn kind(&self) -> &E {
        &self.kind
    }

    pub fn span(&self) -> &SpanTrace {
        &self.context
    }

    /// Convert the error kind, preserving the original span trace.
    pub fn with_ctx<F>(self) -> ICError<F>
    where
        E: Into<F>,
    {
        ICError { kind: self.kind.into(), context: self.context }
    }

    /// Convert the error kind, preserving the original span trace.
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

// We intentionally don't have this instance, to make sure we don't lose context by mistake
// impl<E> From<E> for ICError<E> {
//     fn from(kind: E) -> Self {
//         Self::new(kind)
//     }
// }

/// Extension trait for converting `Result<T, E>` to `Result<T, ICError<Kind>>`
/// where `E: Into<Kind>`.
pub trait ICResultExt<T, E> {
    fn ic_err<Kind>(self) -> Result<T, ICError<Kind>>
    where
        E: Into<Kind>;

    fn ic_err_box<Kind>(self) -> Result<T, ICError<Kind>>
    where
        E: std::error::Error + Send + Sync + 'static,
        Box<dyn std::error::Error + Send + Sync + 'static>: Into<Kind>;
}

impl<T, E> ICResultExt<T, E> for Result<T, E> {
    fn ic_err<Kind>(self) -> Result<T, ICError<Kind>>
    where
        E: Into<Kind>,
    {
        self.map_err(|e| ICError::no_context(e.into()))
    }

    fn ic_err_box<Kind>(self) -> Result<T, ICError<Kind>>
    where
        E: std::error::Error + Send + Sync + 'static,
        Box<dyn std::error::Error + Send + Sync + 'static>: Into<Kind>,
    {
        self.map_err(|e| {
            let e: Box<dyn std::error::Error + Send + Sync + 'static> = Box::new(e);
            ICError::no_context(e.into())
        })
    }
}

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
        self.map_err(|e| e.with_ctx())
    }
}
