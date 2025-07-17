use std::{collections::HashSet, marker::PhantomData};

use futures::{TryStream, TryStreamExt};
use tokio::sync::{Mutex, Notify};
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

#[derive(Debug, Default)]
struct StreamLimiterState {
    // TODO: we may want to switch to atomic values instead
    current_usage: usize,
    in_flight: usize,
}

/// A component to limit the rate at which a stream produces items
/// Call `limit` on each element of the stream before they are processed.
/// Call `free` on each element of the stream once they are done.
/// No more than `max_usage` usage will be generated in total by the in-flight
/// yielded elements. Usage is measured according to the function passed to `new`.
/// Usually, this is used in combination with `TryStreamExt::try_buffer`.
///
/// Note: this could have a much better interface with a bit of extra work:
/// - We could simple wrap the stream and it would take care of mapping every element
/// - Elements could be freed automatically using the Drop instance
#[derive(Debug)]
pub(crate) struct StreamLimiter<T, F: Fn(&'_ T) -> usize> {
    name: String,
    get_usage: F,
    max_usage: usize,
    state: Mutex<StreamLimiterState>,
    waiter: Notify,
    _phantom: PhantomData<T>,
}

impl<T, F: Fn(&'_ T) -> usize> StreamLimiter<T, F> {
    /// Create a new StreamLimiter
    /// Don't yield more than max_usage in-flight usage.
    /// Use `get_usage` to calculate the usage of an element in the stream.
    /// `name` is used purely for logging
    pub(crate) fn new(name: String, max_usage: usize, get_usage: F) -> Self {
        Self {
            name,
            get_usage,
            max_usage,
            state: Mutex::new(Default::default()),
            waiter: Notify::new(),
            _phantom: Default::default(),
        }
    }

    /// Expend some usage. This should be called on every element
    /// of the input stream, before they are processed.
    pub(crate) async fn limit(&self, item: T) -> T {
        let this_item_usage = (self.get_usage)(&item);
        loop {
            let mut state = self.state.lock().await;
            if state.current_usage == 0
                || state.current_usage + this_item_usage <= self.max_usage
            {
                trace!(
                    item_usage = this_item_usage,
                    current_usage = state.current_usage,
                    in_flight = state.in_flight,
                    limiter = self.name,
                    "Stream rate limiter: Allowing item"
                );
                state.current_usage += this_item_usage;
                state.in_flight += 1;
                return item;
            } else {
                trace!(
                    item_usage = this_item_usage,
                    current_usage = state.current_usage,
                    in_flight = state.in_flight,
                    limiter = self.name,
                    "Stream rate limiter: Blocking item"
                );
                drop(state);
                self.waiter.notified().await;
            }
        }
    }

    /// Free some usage. This should be called on every element
    /// of the output stream, after they are processed.
    pub(crate) async fn free(&self, item: T) -> T {
        let this_item_usage = (self.get_usage)(&item);
        let mut state = self.state.lock().await;
        trace!(
            item_usage = this_item_usage,
            current_usage = state.current_usage,
            in_flight = state.in_flight,
            limiter = self.name,
            "Stream rate limiter: Freeing item"
        );
        state.current_usage -= this_item_usage;
        state.in_flight -= 1;
        self.waiter.notify_waiters();
        item
    }

    /// Get the current usage and number of in-flight items.
    pub(crate) async fn current_usage(&self) -> (usize, usize) {
        let state = self.state.lock().await;
        (state.current_usage, state.in_flight)
    }
}
