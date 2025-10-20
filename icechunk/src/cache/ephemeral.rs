//! Ephemeral (in-memory) cache backend implementation.
//!
//! This module provides an in-memory LRU cache with weight-based eviction using
//! the `quick_cache` library. This is the default cache backend for Icechunk.

use bytes::Bytes;
use quick_cache::{Weighter, sync::Cache};
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;

use super::StorageCache;
use crate::format::{manifest::Manifest, snapshot::Snapshot, transaction_log::TransactionLog};

/// A trait for types that can provide their byte size for cache weighting.
///
/// This trait is used to determine how much "weight" each cached item has,
/// which determines how many items can fit in the cache before eviction occurs.
pub trait Weighable {
    /// Returns the weight of this item in bytes.
    ///
    /// For most types, this is the actual byte size of the data structure.
    fn weight(&self) -> u64;
}

// Implement Weighable for the types used in AssetManager
impl Weighable for Arc<Manifest> {
    fn weight(&self) -> u64 {
        self.len() as u64
    }
}

impl Weighable for Arc<Snapshot> {
    fn weight(&self) -> u64 {
        self.len() as u64
    }
}

impl Weighable for Arc<TransactionLog> {
    fn weight(&self) -> u64 {
        self.len() as u64
    }
}

impl Weighable for Bytes {
    fn weight(&self) -> u64 {
        self.len() as u64
    }
}

/// An in-memory LRU cache with weight-based eviction.
///
/// This cache uses the `quick_cache` library to provide fast, thread-safe
/// access with automatic eviction based on item weights. Items are evicted
/// using a Least Recently Used (LRU) policy when the total weight exceeds
/// the configured capacity.
///
/// # Features
///
/// - **Weight-based eviction**: Items are weighted by their byte size, allowing
///   precise control over memory usage.
/// - **Concurrent request deduplication**: Multiple concurrent requests for the
///   same uncached key will only trigger one fetch operation.
/// - **Thread-safe**: Safe to use across multiple async tasks.
///
/// # Type Parameters
///
/// - `K`: The key type (must be `Hash + Eq + Clone`)
/// - `V`: The value type (must implement [`Weighable`])
#[derive(Debug)]
pub struct EphemeralCache<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Weighable + Clone + Send + Sync + 'static,
{
    cache: Cache<K, V, GenericWeighter>,
}

impl<K, V> EphemeralCache<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Weighable + Clone + Send + Sync + 'static,
{
    /// Creates a new ephemeral cache with the specified capacity.
    ///
    /// # Parameters
    ///
    /// - `capacity`: The maximum total weight (in bytes) of all cached items.
    ///   When this limit is exceeded, the least recently used items are evicted.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use icechunk::cache::EphemeralCache;
    ///
    /// // Create a cache that can hold up to 1GB of data
    /// let cache = EphemeralCache::new(1_000_000_000);
    /// ```
    pub fn new(capacity: u64) -> Self {
        Self {
            cache: Cache::with_weighter(
                capacity.try_into().unwrap_or(usize::MAX),
                capacity,
                GenericWeighter,
            ),
        }
    }
}

impl<K, V> StorageCache<K, V> for EphemeralCache<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + std::fmt::Debug + 'static,
    V: Weighable + Clone + Send + Sync + std::fmt::Debug + 'static,
{
    fn get_or_fetch<'a>(
        &'a self,
        key: K,
        fetcher: Pin<Box<dyn Future<Output = Result<V, Box<dyn std::error::Error + Send + Sync>>> + Send + 'a>>,
    ) -> Pin<Box<dyn Future<Output = Result<V, Box<dyn std::error::Error + Send + Sync>>> + Send + 'a>> {
        Box::pin(async move {
            match self.cache.get_value_or_guard_async(&key).await {
                Ok(value) => Ok(value),
                Err(guard) => {
                    let value = fetcher.await?;
                    let _ = guard.insert(value.clone());
                    Ok(value)
                }
            }
        })
    }

    fn insert(&self, key: K, value: V) {
        self.cache.insert(key, value);
    }

    fn remove(&self, key: &K) {
        self.cache.remove(key);
    }

    fn clear(&self) {
        self.cache.clear();
    }
}

/// A generic weighter that delegates to the Weighable trait.
#[derive(Debug, Clone)]
struct GenericWeighter;

impl<K, V> Weighter<K, V> for GenericWeighter
where
    K: Hash + Eq,
    V: Weighable,
{
    fn weight(&self, _key: &K, value: &V) -> u64 {
        value.weight()
    }
}

