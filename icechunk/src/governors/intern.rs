//! Process-wide interning of deserialized governors.
//!
//! Sessions serialized from one governor must rebind to a single shared
//! instance when deserialized into the same process, so their I/O stays
//! jointly governed. The table is keyed on the factory's serialized bytes
//! and holds weak references only, so it never keeps a governor alive.
//! It is consulted only at deserialization; instances injected through
//! `Repository::{create,open}` never touch it.

use std::sync::Arc;

use crate::storage::{GovernorFactory, IoGovernor};

#[cfg(not(feature = "shuttle"))]
use std::{
    collections::HashMap,
    sync::{LazyLock, Mutex, PoisonError, Weak},
};

/// Weak governor references keyed by their factory's serialized bytes.
#[cfg(not(feature = "shuttle"))]
type InternTable = HashMap<Vec<u8>, Weak<dyn IoGovernor>>;

#[cfg(not(feature = "shuttle"))]
static INTERN_TABLE: LazyLock<Mutex<InternTable>> = LazyLock::new(Mutex::default);

/// Builds a governor from a deserialized factory, rebinding to the live
/// instance if an equivalent factory was already built in this process.
#[cfg(not(feature = "shuttle"))]
pub(crate) fn intern_or_build(factory: &dyn GovernorFactory) -> Arc<dyn IoGovernor> {
    let Ok(key) = rmp_serde::to_vec(factory) else {
        // Can't key it: build unshared, weakening only the joint governing.
        tracing::warn!(?factory, "governor factory failed to serialize, not interning");
        return factory.build();
    };
    let mut table = INTERN_TABLE.lock().unwrap_or_else(PoisonError::into_inner);
    table.retain(|_, governor| governor.strong_count() > 0);
    if let Some(existing) = table.get(&key).and_then(Weak::upgrade) {
        tracing::info!(?factory, interned = true, "rebound governor");
        return existing;
    }
    let built = factory.build();
    table.insert(key, Arc::downgrade(&built));
    tracing::info!(?factory, interned = false, "rebound governor");
    built
}

/// A process-global table would leak state across shuttle executions
/// so shuttle builds always get a fresh instance.
#[cfg(feature = "shuttle")]
pub(crate) fn intern_or_build(factory: &dyn GovernorFactory) -> Arc<dyn IoGovernor> {
    factory.build()
}

#[cfg(test)]
#[cfg(not(feature = "shuttle"))]
mod tests {
    use super::*;
    use crate::governors::CompatGovernorConfig;

    /// The table is process-global; serialize the tests touching it so they
    /// can't prune each other's entries mid-assertion under `cargo test`.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    /// `None`: no entry for this recipe; `Some(alive)` otherwise.
    fn entry_state(factory: &dyn GovernorFactory) -> Option<bool> {
        let key = rmp_serde::to_vec(factory).unwrap();
        let table = INTERN_TABLE.lock().unwrap_or_else(PoisonError::into_inner);
        table.get(&key).map(|weak| weak.strong_count() > 0)
    }

    #[icechunk_macros::test]
    fn test_equivalent_factories_intern_to_one_instance() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let factory = CompatGovernorConfig { max_concurrent_requests: 41 };
        let g1 = intern_or_build(&factory);
        let g2 = intern_or_build(&factory);
        assert!(Arc::ptr_eq(&g1, &g2));

        let other = CompatGovernorConfig { max_concurrent_requests: 42 };
        let g3 = intern_or_build(&other);
        assert!(!Arc::ptr_eq(&g1, &g3));
    }

    #[icechunk_macros::test]
    fn test_dropped_governors_are_pruned() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let factory = CompatGovernorConfig { max_concurrent_requests: 43 };
        let governor = intern_or_build(&factory);
        assert_eq!(entry_state(&factory), Some(true));

        drop(governor);
        assert_eq!(entry_state(&factory), Some(false));

        // any later interning opportunistically prunes the dead entry
        let unrelated = CompatGovernorConfig { max_concurrent_requests: 44 };
        let _keep = intern_or_build(&unrelated);
        assert_eq!(entry_state(&factory), None);
    }
}
