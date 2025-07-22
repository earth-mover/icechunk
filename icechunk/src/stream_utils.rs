use std::{
    collections::HashSet,
    sync::atomic::{AtomicUsize, Ordering},
};

use futures::{Stream, TryStream, TryStreamExt};
use tokio::sync::Notify;
use tracing::trace;

/// Return the unique elements of the input stream
/// Unique is computed according to `f`, and only the first
/// matching element is yielded.
pub(crate) fn try_unique_stream<S, T, E, F, V>(
    f: F,
    stream: S,
) -> impl TryStream<Ok = T, Error = E>
where
    F: Fn(&S::Ok) -> V,
    S: TryStream<Ok = T, Error = E>,
    V: Eq + std::hash::Hash,
{
    let mut seen = HashSet::new();
    stream.try_filter(move |item| {
        let v = f(item);
        if seen.insert(v) {
            futures::future::ready(true)
        } else {
            futures::future::ready(false)
        }
    })
}

/// A component to limit the rate at which a stream produces items
/// Call `limit_stream` on your stream and the result will block as
/// needed to never exceed the max_usage.
/// Call `unlimit_stream` after the resources are no longer used, to return
/// "usage" to the stream. This will wake up the original stream once its
/// usage falls below max_usage.
#[derive(Debug)]
pub(crate) struct StreamLimiter {
    name: String,
    max_usage: usize,
    current_usage: AtomicUsize,
    waiter: Notify,
}

impl StreamLimiter {
    /// Create a new StreamLimiter
    /// Don't yield more than max_usage in-flight usage.
    /// Use `get_usage` to calculate the usage of an element in the stream.
    /// `name` is used purely for logging
    pub(crate) fn new(name: String, max_usage: usize /*, get_usage: F*/) -> Self {
        Self {
            name,
            //get_usage,
            max_usage,
            current_usage: AtomicUsize::new(0),
            waiter: Notify::new(),
        }
    }

    /// Get the current usage and number of in-flight items.
    pub(crate) fn current_usage(&self) -> usize {
        self.current_usage.load(Ordering::Relaxed)
    }

    /// Limit the input stream s according to the usage of each element
    /// get_usage is used to calculate how much each element needs
    /// The stream will block if the usage goes above the maximum declared
    /// in self.
    pub(crate) fn limit_stream<S, T, E, F>(
        &self,
        s: S,
        get_usage: F,
    ) -> impl Stream<Item = Result<T, E>>
    where
        S: TryStream<Ok = T, Error = E>,
        F: Fn(&T) -> usize + Copy,
    {
        s.and_then(move |elem| async move {
            let usage = get_usage(&elem);
            self.in_use(usage).await;
            Ok(elem)
        })
    }

    /// Free the usage of elements as they pass the stream.
    /// get_usage is used to calculate how much each element needs
    /// This can wake up a stream blocked by going above usage
    pub(crate) fn unlimit_stream<S, T, E, F>(
        &self,
        s: S,
        get_usage: F,
    ) -> impl Stream<Item = Result<T, E>>
    where
        S: TryStream<Ok = T, Error = E>,
        F: Fn(&T) -> usize + Copy,
    {
        s.and_then(move |elem| async move {
            let usage = get_usage(&elem);
            self.return_used(usage).await;
            Ok(elem)
        })
    }

    async fn in_use(&self, usage: usize) {
        loop {
            match self.current_usage.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| {
                    if current == 0 || current + usage <= self.max_usage {
                        Some(current + usage)
                    } else {
                        None
                    }
                },
            ) {
                Ok(old_usage) => {
                    trace!(
                        item_usage = usage,
                        current_usage = old_usage,
                        limiter = self.name,
                        "Stream rate limiter: Allowing item"
                    );
                    break;
                }
                Err(old_usage) => {
                    trace!(
                        item_usage = usage,
                        current_usage = old_usage,
                        limiter = self.name,
                        "Stream rate limiter: Blocking item"
                    );
                    self.waiter.notified().await;
                }
            }
        }
    }

    async fn return_used(&self, usage: usize) {
        let old_usage = self.current_usage.fetch_sub(usage, Ordering::Relaxed);
        trace!(
            item_usage = usage,
            current_usage = old_usage,
            limiter = self.name,
            "Stream rate limiter: Freeing item"
        );
        self.waiter.notify_waiters();
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{convert::Infallible, error::Error, future::ready, sync::Arc};

    use futures::{FutureExt, StreamExt, TryStreamExt, stream};
    use icechunk_macros::tokio_test;
    use tokio::pin;

    use super::*;

    #[tokio_test]
    async fn test_limiter_lets_everything_pass_if_under_limit()
    -> Result<(), Box<dyn Error>> {
        let limiter = &Arc::new(StreamLimiter::new("test".to_string(), 1_000_000));
        let stream = stream::iter(1..=100).map(Ok::<_, Infallible>);
        let res = limiter.limit_stream(stream, |n| *n).try_collect::<Vec<_>>().await?;
        assert_eq!(res, (1..=100).collect::<Vec<_>>());
        Ok(())
    }

    #[tokio_test]
    async fn test_limiter_limits() -> Result<(), Box<dyn Error>> {
        let limiter = &Arc::new(StreamLimiter::new("test".to_string(), 3));
        let stream = stream::repeat(1).map(Ok::<_, Infallible>);
        let stream = limiter.limit_stream(stream, |n| *n);

        pin!(stream);
        assert_eq!(stream.try_next().now_or_never(), Some(Ok(Some(1))));
        assert_eq!(stream.try_next().now_or_never(), Some(Ok(Some(1))));
        assert_eq!(stream.try_next().now_or_never(), Some(Ok(Some(1))));
        // 4th element blocks
        assert_eq!(stream.try_next().now_or_never(), None);
        Ok(())
    }

    #[tokio_test]
    async fn test_limiter_frees() -> Result<(), Box<dyn Error>> {
        let limiter = &Arc::new(StreamLimiter::new("test".to_string(), 3));
        let stream = stream::repeat(1).map(Ok::<_, Infallible>);
        let stream = limiter.limit_stream(stream, |n| *n);
        let stream = stream.map_ok(|n| n + 1);
        let res = limiter
            .unlimit_stream(stream, |n| n - 1)
            .take(100)
            .try_fold(0, |total, partial| ready(Ok(total + partial)))
            .await?;
        assert_eq!(res, 2 * 100);
        assert_eq!(limiter.current_usage(), 0);
        Ok(())
    }
}
