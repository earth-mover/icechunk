use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::error;

use crate::config::DEFAULT_MAX_CONCURRENT_REQUESTS;
use crate::storage::{
    Direction, GovernorFactory, IoClass, IoGovernor, IoOutcome, IoPermit, MemoryPermit,
    MemoryState, PermitState,
};

/// Reproduces the legacy `AssetManager` request semaphore; the default when
/// no governor is passed.
///
/// One pool of `max_concurrent_requests` units shared by reads and writes:
/// a read takes one unit per logical fetch (in
/// [`reserve_memory`](IoGovernor::reserve_memory)), held until hand-off; a
/// write takes one unit per HTTP request (in
/// [`acquire`](IoGovernor::acquire)). Read requests and expected sizes are
/// ignored.
#[derive(Debug)]
pub struct CompatGovernor {
    semaphore: Arc<Semaphore>,
    max_concurrent_requests: u16,
}

impl CompatGovernor {
    pub fn new(config: &CompatGovernorConfig) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(config.max_concurrent_requests as usize)),
            max_concurrent_requests: config.max_concurrent_requests,
        }
    }

    pub fn max_concurrent_requests(&self) -> u16 {
        self.max_concurrent_requests
    }

    async fn acquire_unit(&self) -> Option<OwnedSemaphorePermit> {
        // The semaphore is never closed, so this can only fail on a bug;
        // failing open is better than failing the I/O operation.
        match Arc::clone(&self.semaphore).acquire_owned().await {
            Ok(permit) => Some(permit),
            Err(_) => {
                error!("CompatGovernor semaphore unexpectedly closed");
                None
            }
        }
    }
}

#[async_trait]
impl IoGovernor for CompatGovernor {
    async fn reserve_memory(
        &self,
        class: IoClass,
        _expected_total: Option<u64>,
    ) -> MemoryPermit {
        match class.direction {
            Direction::Read => match self.acquire_unit().await {
                Some(permit) => {
                    MemoryPermit::new(Box::new(UnitReservation { _permit: permit }))
                }
                None => MemoryPermit::noop(),
            },
            Direction::Write => MemoryPermit::noop(),
        }
    }

    async fn acquire(&self, class: IoClass, _expected_bytes: Option<u64>) -> IoPermit {
        match class.direction {
            Direction::Write => match self.acquire_unit().await {
                Some(permit) => IoPermit::new(Box::new(UnitPermit { _permit: permit })),
                None => IoPermit::noop(),
            },
            Direction::Read => IoPermit::noop(),
        }
    }

    fn factory(&self) -> Arc<dyn GovernorFactory> {
        Arc::new(CompatGovernorConfig {
            max_concurrent_requests: self.max_concurrent_requests,
        })
    }

    fn as_arc_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

/// The unit is released when the permit drops.
#[derive(Debug)]
struct UnitReservation {
    _permit: OwnedSemaphorePermit,
}

impl MemoryState for UnitReservation {
    fn adjust(&self, _actual_total: u64) {}
}

/// The unit is released when the permit drops.
#[derive(Debug)]
struct UnitPermit {
    _permit: OwnedSemaphorePermit,
}

impl PermitState for UnitPermit {
    fn complete(self: Box<Self>, _outcome: IoOutcome) {}
    fn abort(self: Box<Self>) {}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatGovernorConfig {
    pub max_concurrent_requests: u16,
}

impl Default for CompatGovernorConfig {
    fn default() -> Self {
        Self { max_concurrent_requests: DEFAULT_MAX_CONCURRENT_REQUESTS }
    }
}

#[typetag::serde(name = "compat")]
impl GovernorFactory for CompatGovernorConfig {
    fn build(&self) -> Arc<dyn IoGovernor> {
        Arc::new(CompatGovernor::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Asset, IoResult, UnlimitedGovernorConfig};
    use futures::{FutureExt as _, poll};

    const READ_CHUNK: IoClass =
        IoClass { direction: Direction::Read, asset: Asset::Chunk };
    const WRITE_CHUNK: IoClass =
        IoClass { direction: Direction::Write, asset: Asset::Chunk };

    fn governor(max_concurrent_requests: u16) -> CompatGovernor {
        CompatGovernor::new(&CompatGovernorConfig { max_concurrent_requests })
    }

    #[icechunk_macros::tokio_test]
    async fn read_reservations_block_at_limit() {
        let gov = governor(2);
        let r1 = gov.reserve_memory(READ_CHUNK, None).await;
        let _r2 = gov.reserve_memory(READ_CHUNK, Some(100)).await;

        let mut third = gov.reserve_memory(READ_CHUNK, None).boxed();
        assert!(poll!(third.as_mut()).is_pending());

        drop(r1);
        assert!(poll!(third.as_mut()).is_ready());
    }

    #[icechunk_macros::tokio_test]
    async fn write_acquires_block_at_limit() {
        let gov = governor(1);
        let w1 = gov.acquire(WRITE_CHUNK, Some(10)).await;

        let mut second = gov.acquire(WRITE_CHUNK, None).boxed();
        assert!(poll!(second.as_mut()).is_pending());

        w1.complete(IoOutcome { bytes: 10, result: IoResult::Ok });
        assert!(poll!(second.as_mut()).is_ready());
    }

    #[icechunk_macros::tokio_test]
    async fn read_acquires_are_free() {
        let gov = governor(1);
        // exhaust the pool
        let _w = gov.acquire(WRITE_CHUNK, None).await;
        for _ in 0..10 {
            let permit = gov.acquire(READ_CHUNK, None).await;
            permit.complete(IoOutcome { bytes: 1, result: IoResult::Ok });
        }
    }

    #[icechunk_macros::tokio_test]
    async fn write_reservations_are_free() {
        let gov = governor(1);
        let _r = gov.reserve_memory(READ_CHUNK, None).await;
        let _m1 = gov.reserve_memory(WRITE_CHUNK, None).await;
        let _m2 = gov.reserve_memory(WRITE_CHUNK, None).await;
    }

    #[icechunk_macros::tokio_test]
    async fn reads_and_writes_share_the_pool() {
        let gov = governor(1);
        let r = gov.reserve_memory(READ_CHUNK, None).await;

        let mut write = gov.acquire(WRITE_CHUNK, None).boxed();
        assert!(poll!(write.as_mut()).is_pending());

        drop(r);
        assert!(poll!(write.as_mut()).is_ready());
    }

    #[icechunk_macros::tokio_test]
    async fn aborted_write_releases_the_unit() {
        let gov = governor(1);
        let w = gov.acquire(WRITE_CHUNK, None).await;
        drop(w); // abort path

        let mut next = gov.acquire(WRITE_CHUNK, None).boxed();
        assert!(poll!(next.as_mut()).is_ready());
    }

    #[icechunk_macros::test]
    fn compat_factory_round_trips() {
        let config = CompatGovernorConfig { max_concurrent_requests: 7 };
        let factory: Box<dyn GovernorFactory> = Box::new(config.clone());

        let bytes = rmp_serde::to_vec(&factory).unwrap();
        let back: Box<dyn GovernorFactory> = rmp_serde::from_slice(&bytes).unwrap();
        let rebuilt = back.build().factory();
        assert_eq!(rmp_serde::to_vec(&rebuilt).unwrap(), bytes);

        let json = serde_json::to_string(&factory).unwrap();
        assert!(json.contains(r#""governor_type":"compat""#));
        let back: Box<dyn GovernorFactory> = serde_json::from_str(&json).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), json);
    }

    #[icechunk_macros::test]
    fn unlimited_factory_round_trips() {
        let factory: Box<dyn GovernorFactory> = Box::new(UnlimitedGovernorConfig {});
        let bytes = rmp_serde::to_vec(&factory).unwrap();
        let back: Box<dyn GovernorFactory> = rmp_serde::from_slice(&bytes).unwrap();
        let rebuilt = back.build().factory();
        assert_eq!(rmp_serde::to_vec(&rebuilt).unwrap(), bytes);
    }
}
