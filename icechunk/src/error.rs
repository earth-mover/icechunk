use std::fmt::Display;

use tracing_error::SpanTrace;

#[derive(Debug)]
pub struct ICError<E> {
    pub kind: E,
    pub context: SpanTrace,
}

impl<E> ICError<E> {
    pub fn new(kind: E) -> Self {
        Self::with_context(kind, SpanTrace::capture())
    }

    pub fn with_context(kind: E, context: SpanTrace) -> Self {
        Self { kind, context }
    }

    pub fn kind(&self) -> &E {
        &self.kind
    }

    pub fn span(&self) -> &SpanTrace {
        &self.context
    }
}

impl<E: Display> std::fmt::Display for ICError<E> {
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
