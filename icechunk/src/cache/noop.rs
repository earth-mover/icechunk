//! No-op cache backend implementation.
//!
//! This module provides a cache implementation that performs no caching at all.
//! All requests are passed directly through to the underlying storage backend.

use std::future::Future;
use std::hash::Hash;
use std::marker::PhantomData;
use std::pin::Pin;

use super::StorageCache;

/// A no-op cache that performs no caching.
///
/// This cache implementation is a pass-through that never stores any values.
/// All requests are forwarded directly to the fetcher function, making it
/// equivalent to having no cache at all.
///
/// # Use Cases
///
/// - **Debugging**: Disable caching to test storage backend performance
/// - **Testing**: Ensure tests exercise the full storage path
/// - **Low-memory environments**: Avoid cache overhead when memory is constrained
///
/// # Type Parameters
///
/// - `K`: The key type (not actually used, but required for trait compatibility)
/// - `V`: The value type (not actually used, but required for trait compatibility)
#[derive(Debug)]
pub struct NoOpCache<K, V> {
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> NoOpCache<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Creates a new no-op cache.
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<K, V> Default for NoOpCache<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> StorageCache<K, V> for NoOpCache<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + std::fmt::Debug + 'static,
    V: Clone + Send + Sync + std::fmt::Debug + 'static,
{
    fn get_or_fetch<'a>(
        &'a self,
        _key: K,
        fetcher: Pin<Box<dyn Future<Output = Result<V, Box<dyn std::error::Error + Send + Sync>>> + Send + 'a>>,
    ) -> Pin<Box<dyn Future<Output = Result<V, Box<dyn std::error::Error + Send + Sync>>> + Send + 'a>> {
        // Always fetch, never cache
        fetcher
    }

    fn insert(&self, _key: K, _value: V) {
        // No-op
    }

    fn remove(&self, _key: &K) {
        // No-op
    }

    fn clear(&self) {
        // No-op
    }
}

