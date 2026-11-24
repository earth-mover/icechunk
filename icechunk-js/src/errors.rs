pub(crate) trait IntoNapiResult<T> {
    fn map_napi_err(self) -> napi::Result<T>;
}

impl<T, E: std::error::Error + 'static> IntoNapiResult<T> for Result<T, E> {
    fn map_napi_err(self) -> napi::Result<T> {
        self.map_err(|e| to_napi_error(&e))
    }
}

pub(crate) fn to_napi_error(error: &(dyn std::error::Error + 'static)) -> napi::Error {
    napi::Error::from_reason(chain_message(error))
}

/// A one-line chain of causes (`outer: inner: root`), followed by the
/// `ICError` span-trace context blocks. `Display` alone only shows the
/// outermost message, which hides the actual cause, e.g. "error fetching
/// virtual reference" without the CORS problem or the JS callback's error.
/// Mirrors `chain_message` in the Python bindings.
pub(crate) fn chain_message(error: &(dyn std::error::Error + 'static)) -> String {
    const CTX_MARKER: &str = "\n\ncontext:\n";
    let mut parts: Vec<String> = Vec::new();
    let mut contexts: Vec<String> = Vec::new();
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(err) = current {
        let mut msg = err.to_string();
        if let Some(idx) = msg.find(CTX_MARKER) {
            let ctx = msg[idx + CTX_MARKER.len()..].trim_end().to_string();
            if !ctx.is_empty() && !contexts.contains(&ctx) {
                contexts.push(ctx);
            }
            msg.truncate(idx);
        }
        // #[from] variants often embed their source in their own message
        if !msg.is_empty() && parts.last().is_none_or(|prev| !prev.contains(&msg)) {
            parts.push(msg);
        }
        current = err.source();
    }
    let mut out = parts.join(": ");
    for ctx in contexts {
        out.push_str(CTX_MARKER);
        out.push_str(&ctx);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Outer(std::io::Error);
    impl std::fmt::Display for Outer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("error fetching virtual reference\n\ncontext:\n   0: span\n")
        }
    }
    impl std::error::Error for Outer {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn includes_causes_and_keeps_context() {
        let e = Outer(std::io::Error::other("missing Content-Range"));
        assert_eq!(
            chain_message(&e),
            "error fetching virtual reference: missing Content-Range\n\ncontext:\n   0: span"
        );
    }
}
