//! Global tokio runtime for blocking on async operations.

use std::sync::OnceLock;
use tokio::runtime::Runtime;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Get or lazily create the global tokio runtime.
#[allow(clippy::expect_used)]
pub(crate) fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Runtime::new().expect("failed to create tokio runtime")
    })
}

/// Run an async future on the global runtime, blocking the current thread.
pub(crate) fn block_on<F: std::future::Future>(future: F) -> F::Output {
    get_runtime().block_on(future)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_is_singleton() {
        let r1 = get_runtime() as *const Runtime;
        let r2 = get_runtime() as *const Runtime;
        assert_eq!(r1, r2);
    }

    #[test]
    fn test_block_on_runs_future() {
        let result = block_on(async { 42 });
        assert_eq!(result, 42);
    }
}
