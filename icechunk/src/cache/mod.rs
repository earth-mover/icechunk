//! Pluggable cache backend infrastructure for Icechunk.
//!
//! This module provides a trait-based architecture for cache backends that allows
//! different caching strategies to be used interchangeably. The cache is used by
//! the [`AssetManager`](crate::asset_manager::AssetManager) to cache manifests,
//! snapshots, transaction logs, and chunks.
//!
//! # Available Backends
//!
//! - **Ephemeral** ([`EphemeralCache`]): In-memory LRU cache with weight-based eviction.
//!   This is the default backend and provides fast access with configurable memory limits.
//!   Concurrent requests for the same uncached item are automatically deduplicated.
//!
//! - **NoOp** ([`NoOpCache`]): A pass-through cache that performs no caching. Useful for
//!   debugging or when caching is not desired.
//!
//! # Examples
//!
//! The cache backend is configured via [`CachingConfig`](crate::config::CachingConfig):
//!
//! ```rust,no_run
//! use icechunk::{config::CachingConfig, cache::CacheBackend};
//!
//! let config = CachingConfig {
//!     num_snapshot_nodes: Some(500_000),
//!     num_chunk_refs: Some(15_000_000),
//!     backend: CacheBackend::Ephemeral,
//!     ..Default::default()
//! };
//! ```
//!
//! # Implementing Custom Backends
//!
//! To implement a custom cache backend (e.g., Redis, filesystem), implement the
//! [`StorageCache`] trait and add a new variant to [`CacheBackend`].

use serde::{Deserialize, Serialize};
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;

pub mod ephemeral;
pub mod noop;

pub use ephemeral::EphemeralCache;
pub use noop::NoOpCache;

/// A trait for pluggable cache backends that support async operations,
/// weight-based eviction, and concurrent request deduplication.
///
/// This trait is object-safe (uses boxed futures) to allow dynamic dispatch
/// through `Arc<dyn StorageCache<K, V>>`.
///
/// # Type Parameters
///
/// - `K`: The cache key type (must be hashable and cloneable)
/// - `V`: The cached value type (must be cloneable)
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to allow use across async tasks.
pub trait StorageCache<K, V>: Send + Sync + std::fmt::Debug
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Gets a value from the cache, or fetches it using the provided future if not present.
    ///
    /// Concurrent requests for the same key should be deduplicated - only one fetch
    /// operation should occur, with all waiters receiving the same result.
    ///
    /// # Parameters
    ///
    /// - `key`: The cache key to look up
    /// - `fetcher`: An async function that fetches the value if not in cache
    ///
    /// # Returns
    ///
    /// The cached or fetched value, or an error if the fetch operation fails.
    fn get_or_fetch<'a>(
        &'a self,
        key: K,
        fetcher: Pin<Box<dyn Future<Output = Result<V, Box<dyn std::error::Error + Send + Sync>>> + Send + 'a>>,
    ) -> Pin<Box<dyn Future<Output = Result<V, Box<dyn std::error::Error + Send + Sync>>> + Send + 'a>>;

    /// Inserts a value into the cache with the given key.
    ///
    /// If the key already exists, the value is updated. If the cache is full,
    /// the implementation determines which entry to evict.
    fn insert(&self, key: K, value: V);

    /// Removes a value from the cache.
    ///
    /// If the key doesn't exist, this is a no-op.
    fn remove(&self, key: &K);

    /// Clears all entries from the cache.
    fn clear(&self);
}

/// Enum representing different cache backend implementations.
///
/// This enum is used in [`CachingConfig`](crate::config::CachingConfig) to select
/// which cache backend to use for the repository.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheBackend {
    /// In-memory LRU cache with weight-based eviction (default).
    ///
    /// Provides fast access with configurable memory limits. Cache entries are
    /// evicted based on their size (weight) using an LRU policy. This backend
    /// automatically deduplicates concurrent requests for the same uncached item.
    ///
    /// This is the default backend and provides the best performance for most use cases.
    Ephemeral,

    /// No-op cache that performs no caching.
    ///
    /// All requests are passed directly to the storage backend without caching.
    /// Useful for debugging, testing, or when caching overhead is not desired.
    NoOp,
}

impl Default for CacheBackend {
    fn default() -> Self {
        CacheBackend::Ephemeral
    }
}

impl CacheBackend {
    /// Returns a human-readable name for the cache backend.
    pub fn name(&self) -> &'static str {
        match self {
            CacheBackend::Ephemeral => "ephemeral",
            CacheBackend::NoOp => "noop",
        }
    }
}
