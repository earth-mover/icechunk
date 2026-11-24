//! Task runtime shim.
//!
//! Everywhere except `wasm32-unknown-unknown` this is a straight re-export of
//! Tokio, so native behaviour is unchanged. In the browser there is no Tokio
//! runtime and no threads, so tasks run on the JS event loop via
//! `wasm_bindgen_futures::spawn_local`, "blocking" work runs inline, and
//! sleeps use JS timers.
//!
//! The wasm types mirror the subset of Tokio's API that icechunk uses, with
//! the same semantics: dropping a [`JoinHandle`] detaches the task, aborting
//! one makes it resolve to a cancelled [`JoinError`], and dropping a
//! [`JoinSet`] aborts every task still in it.

#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
pub use tokio::{
    spawn,
    task::{AbortHandle, JoinError, JoinHandle, JoinSet, spawn_blocking},
    time::sleep,
};

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
pub use self::browser::*;

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
mod browser {
    use std::{
        fmt,
        future::Future,
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    use futures::{
        FutureExt as _, StreamExt as _,
        channel::oneshot,
        future::{AbortHandle as FutAbortHandle, Abortable, Aborted},
        stream::FuturesUnordered,
    };

    /// A task failed to complete. In the browser a panic aborts the whole
    /// module, so the only way to get one is cancellation.
    #[derive(Debug)]
    pub struct JoinError(());

    impl JoinError {
        pub fn is_cancelled(&self) -> bool {
            true
        }
        pub fn is_panic(&self) -> bool {
            false
        }
    }

    impl fmt::Display for JoinError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("task was cancelled")
        }
    }

    impl std::error::Error for JoinError {}

    /// Cancels a spawned task.
    #[derive(Debug, Clone)]
    pub struct AbortHandle(FutAbortHandle);

    impl AbortHandle {
        pub fn abort(&self) {
            self.0.abort();
        }
    }

    /// Handle to a task spawned with [`spawn`]. Dropping it detaches the task.
    #[derive(Debug)]
    pub struct JoinHandle<T> {
        rx: oneshot::Receiver<Result<T, Aborted>>,
        abort: AbortHandle,
    }

    impl<T> JoinHandle<T> {
        pub fn abort(&self) {
            self.abort.abort();
        }

        pub fn abort_handle(&self) -> AbortHandle {
            self.abort.clone()
        }
    }

    impl<T> Future for JoinHandle<T> {
        type Output = Result<T, JoinError>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.rx.poll_unpin(cx).map(|res| match res {
                Ok(Ok(value)) => Ok(value),
                // aborted, or the task was dropped before it sent a result
                Ok(Err(Aborted)) | Err(oneshot::Canceled) => Err(JoinError(())),
            })
        }
    }

    /// Run `fut` on the browser event loop.
    pub fn spawn<F>(fut: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        let (abort, registration) = FutAbortHandle::new_pair();
        let (tx, rx) = oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let res = Abortable::new(fut, registration).await;
            // the handle may have been dropped (detached); that is fine
            let _ = tx.send(res);
        });
        JoinHandle { rx, abort: AbortHandle(abort) }
    }

    /// There are no threads to offload to, so the closure runs inline, right
    /// away. Long-running work blocks the event loop; run icechunk in a Web
    /// Worker if that matters.
    pub fn spawn_blocking<F, R>(f: F) -> std::future::Ready<Result<R, JoinError>>
    where
        F: FnOnce() -> R,
    {
        std::future::ready(Ok(f()))
    }

    pub async fn sleep(duration: Duration) {
        gloo_timers::future::sleep(duration).await;
    }

    /// A set of spawned tasks, polled for completion in any order.
    #[derive(Debug)]
    pub struct JoinSet<T> {
        tasks: FuturesUnordered<JoinHandle<T>>,
    }

    impl<T> Default for JoinSet<T> {
        fn default() -> Self {
            Self { tasks: FuturesUnordered::new() }
        }
    }

    impl<T: 'static> JoinSet<T> {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn spawn<F>(&mut self, fut: F) -> AbortHandle
        where
            F: Future<Output = T> + 'static,
        {
            let handle = spawn(fut);
            let abort = handle.abort_handle();
            self.tasks.push(handle);
            abort
        }

        pub async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
            self.tasks.next().await
        }

        pub fn len(&self) -> usize {
            self.tasks.len()
        }

        pub fn is_empty(&self) -> bool {
            self.tasks.is_empty()
        }

        pub fn abort_all(&mut self) {
            for task in self.tasks.iter() {
                task.abort();
            }
        }
    }

    impl<T> Drop for JoinSet<T> {
        fn drop(&mut self) {
            for task in self.tasks.iter() {
                task.abort();
            }
        }
    }
}
