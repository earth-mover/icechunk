//! An externally injectable I/O controller.
//!
//! An [`IoGovernor`] is consulted before each logical fetch (to reserve
//! buffer memory) and again before each HTTP request (to admit it). The
//! trait vocabulary lives in this crate, concrete governors live in the `icechunk` crate.
//!
//! See `design-docs/019-better-io-control.md`.

use std::{
    any::Any,
    fmt::Debug,
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;

use crate::storage::Settings;

/// Direction of an I/O operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    Read,
    Write,
}

/// The kind of asset an I/O operation is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Asset {
    Snapshot,
    Manifest,
    TransactionLog,
    Ref,
    Chunk,
    VirtualChunk,
    Config,
    RepoInfo,
    Other,
}

/// Classifies an I/O operation for the governor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IoClass {
    pub direction: Direction,
    pub asset: Asset,
}

/// How a governed request ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoResult {
    Ok,
    /// The object store library retry loop gave up and the throttle error
    /// surfaced. Per-attempt throttles arrive through
    /// [`IoGovernor::record_throttle`] instead.
    Throttled(u16),
    Error,
    Aborted,
}

/// What the governor learns when a request finishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoOutcome {
    /// Actual bytes transferred.
    pub bytes: u64,
    pub result: IoResult,
}

/// Implemented by each governor for its own per-request bookkeeping.
pub trait PermitState: Debug + Send {
    /// The request finished (last byte); bytes and throttle signals feed
    /// the governor's accounting.
    fn complete(self: Box<Self>, outcome: IoOutcome);
    /// The request never finished; release resources.
    fn abort(self: Box<Self>);
}

/// Admission for one HTTP request.
///
/// Capacity is held from [`IoGovernor::acquire`] until the permit is
/// dropped. Exactly one of `complete` / `abort` reaches the governor:
/// dropping without calling [`IoPermit::complete`] reports an abort.
#[derive(Debug)]
#[must_use]
pub struct IoPermit {
    // `Some` until the outcome is reported; `None` tells `Drop` it already was.
    inner: Option<Box<dyn PermitState>>,
}

impl IoPermit {
    pub fn new(state: Box<dyn PermitState>) -> Self {
        Self { inner: Some(state) }
    }

    /// A permit that carries no governor bookkeeping.
    pub fn noop() -> Self {
        Self { inner: None }
    }

    pub fn complete(mut self, outcome: IoOutcome) {
        if let Some(s) = self.inner.take() {
            s.complete(outcome);
        }
    }

    /// Complete according to `res`: `Ok` reports `bytes` transferred, `Err`
    /// reports [`IoResult::Error`] with no bytes.
    pub fn complete_result<T, E>(self, res: &Result<T, E>, bytes: u64) {
        match res {
            Ok(_) => self.complete(IoOutcome { bytes, result: IoResult::Ok }),
            Err(_) => self.complete(IoOutcome { bytes: 0, result: IoResult::Error }),
        }
    }
}

impl Drop for IoPermit {
    fn drop(&mut self) {
        if let Some(s) = self.inner.take() {
            s.abort();
        }
    }
}

/// Implemented by each governor for its own per-logical-fetch bookkeeping.
///
/// The reservation is released when the state is dropped.
pub trait MemoryState: Debug + Send + Sync {
    /// True-up when the actual size becomes known. Down releases the
    /// difference; up debits it immediately without waiting.
    fn adjust(&self, actual_total: u64);
}

/// Buffer budget for one logical fetch; released on drop, at hand-off.
#[derive(Debug)]
#[must_use]
pub struct MemoryPermit {
    inner: Option<Box<dyn MemoryState>>,
    /// Whether an observed total size already trued the reservation up.
    observed: AtomicBool,
}

impl MemoryPermit {
    pub fn new(state: Box<dyn MemoryState>) -> Self {
        Self { inner: Some(state), observed: AtomicBool::new(false) }
    }

    /// A permit that carries no governor bookkeeping.
    pub const fn noop() -> Self {
        Self { inner: None, observed: AtomicBool::new(false) }
    }

    /// A shared no-op permit, for reads that are deliberately not
    /// memory-metered.
    pub fn unmetered() -> &'static MemoryPermit {
        static UNMETERED: MemoryPermit = MemoryPermit::noop();
        &UNMETERED
    }

    /// True-up when the actual size becomes known (e.g. from a
    /// content-length). Down releases the difference; up debits it
    /// immediately without waiting.
    pub fn adjust(&self, actual_total: u64) {
        if let Some(s) = &self.inner {
            s.adjust(actual_total);
        }
    }

    /// [`adjust`](Self::adjust), but at most once over the permit's life:
    /// retried requests can observe the same object size repeatedly, only
    /// the first observation trues the reservation up.
    pub fn observe_total_size(&self, actual_total: u64) {
        if !self.observed.swap(true, Ordering::Relaxed) {
            self.adjust(actual_total);
        }
    }
}

/// How much of an object a read fetches.
///
/// A reservation can only be observed by a whole-object read,
/// and every whole-object read must state its
/// reservation from [`reserve_memory`](IoGovernor::reserve_memory), or
/// [`ObjectRange::unmetered`] to declare the fetch deliberately
/// unmetered.
#[derive(Debug, Clone, Copy)]
pub enum ObjectRange<'a> {
    /// Fetch this byte range: the caller already knows the extent, so
    /// there is nothing for the response to reveal.
    Ranged(&'a Range<u64>),
    /// Fetch the whole object; the response's size trues up the logical
    /// fetch's memory reservation.
    Whole(&'a MemoryPermit),
}

impl ObjectRange<'static> {
    /// A whole-object read that is deliberately not memory-metered
    /// (e.g. ref files).
    pub fn unmetered() -> Self {
        ObjectRange::Whole(MemoryPermit::unmetered())
    }
}

impl ObjectRange<'_> {
    /// Bytes the read will transfer, when knowable up front.
    pub fn expected_bytes(&self) -> Option<u64> {
        match self {
            ObjectRange::Ranged(range) => Some(range.end.saturating_sub(range.start)),
            ObjectRange::Whole(_) => None,
        }
    }

    /// The byte range to request, `None` for the whole object.
    pub fn range(&self) -> Option<&Range<u64>> {
        match self {
            ObjectRange::Ranged(range) => Some(range),
            ObjectRange::Whole(_) => None,
        }
    }

    /// Report the total object size revealed by the response (e.g. its
    /// content-length), truing up the riding memory reservation at most
    /// once. A no-op on ranged reads, whose responses only describe the
    /// range.
    pub fn observe_total_size(&self, actual_total: u64) {
        if let ObjectRange::Whole(permit) = self {
            permit.observe_total_size(actual_total);
        }
    }
}

/// An externally injectable I/O controller.
///
/// Icechunk consults it before each logical fetch (to reserve buffer
/// memory) and again before each HTTP request (to admit it).
#[async_trait]
pub trait IoGovernor: Debug + Send + Sync {
    /// One atomic reservation for a whole logical fetch, held until the
    /// assembled bytes are handed off to the consumer. Reads only: writes
    /// don't reserve memory.
    async fn reserve_memory(
        &self,
        class: IoClass,
        expected_total: Option<u64>,
    ) -> MemoryPermit;

    /// Admit one HTTP request, after range-splitting.
    async fn acquire(&self, class: IoClass, expected_bytes: Option<u64>) -> IoPermit;

    /// A recipe for an equivalent governor, reflecting the *current*
    /// assigned settings. Runtime state is not part of the recipe.
    fn factory(&self) -> Arc<dyn GovernorFactory>;

    /// Side channel for per-attempt throttle signals observed below the
    /// object store library retry loops.
    fn record_throttle(&self, _class: IoClass) {}

    /// Downcast support: recover the concrete governor behind an
    /// `Arc<dyn IoGovernor>` (e.g. to reach knobs and metrics not part of
    /// this trait). Implementations return `self`.
    fn as_arc_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}

/// Serializable recipe for building a governor.
///
/// Rides serialized sessions via typetag; the concrete type must be linked
/// into any binary that deserializes such sessions
#[typetag::serde(tag = "governor_type")]
pub trait GovernorFactory: Debug + Send + Sync {
    fn build(&self) -> Arc<dyn IoGovernor>;
}

/// Everything a [`Storage`](crate::Storage) implementation needs to know
/// about the calling context of a single operation.
#[derive(Debug, Clone, Copy)]
pub struct StorageContext<'a> {
    pub settings: &'a Settings,
    pub governor: &'a Arc<dyn IoGovernor>,
    /// The kind of asset the operation is for. Callers know this;
    /// implementations use it to classify their requests.
    pub asset: Asset,
}

impl StorageContext<'_> {
    pub fn io_class(&self, direction: Direction) -> IoClass {
        IoClass { direction, asset: self.asset }
    }
}

/// A governor that admits everything immediately.
///
/// Useful in tests and as an explicit opt-out of I/O control.
#[derive(Debug)]
pub struct UnlimitedGovernor;

#[async_trait]
impl IoGovernor for UnlimitedGovernor {
    async fn reserve_memory(
        &self,
        _class: IoClass,
        _expected_total: Option<u64>,
    ) -> MemoryPermit {
        MemoryPermit::noop()
    }

    async fn acquire(&self, _class: IoClass, _expected_bytes: Option<u64>) -> IoPermit {
        IoPermit::noop()
    }

    fn factory(&self) -> Arc<dyn GovernorFactory> {
        Arc::new(UnlimitedGovernorConfig {})
    }

    fn as_arc_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct UnlimitedGovernorConfig {}

#[typetag::serde(name = "unlimited")]
impl GovernorFactory for UnlimitedGovernorConfig {
    fn build(&self) -> Arc<dyn IoGovernor> {
        Arc::new(UnlimitedGovernor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    };

    const PENDING: u8 = 0;
    const COMPLETED: u8 = 1;
    const ABORTED: u8 = 2;

    #[derive(Debug)]
    struct Recorder(Arc<AtomicU8>);

    impl PermitState for Recorder {
        fn complete(self: Box<Self>, _outcome: IoOutcome) {
            self.0.store(COMPLETED, Ordering::SeqCst);
        }
        fn abort(self: Box<Self>) {
            self.0.store(ABORTED, Ordering::SeqCst);
        }
    }

    #[test]
    fn complete_reports_exactly_once() {
        let state = Arc::new(AtomicU8::new(PENDING));
        let permit = IoPermit::new(Box::new(Recorder(Arc::clone(&state))));
        permit.complete(IoOutcome { bytes: 42, result: IoResult::Ok });
        // drop ran inside `complete` after taking the state: no abort overwrite
        assert_eq!(state.load(Ordering::SeqCst), COMPLETED);
    }

    #[test]
    fn drop_reports_abort() {
        let state = Arc::new(AtomicU8::new(PENDING));
        let permit = IoPermit::new(Box::new(Recorder(Arc::clone(&state))));
        drop(permit);
        assert_eq!(state.load(Ordering::SeqCst), ABORTED);
    }

    #[test]
    fn as_arc_any_recovers_the_concrete_governor() {
        let governor: Arc<dyn IoGovernor> = Arc::new(UnlimitedGovernor);
        let any = Arc::clone(&governor).as_arc_any();
        assert!(any.downcast::<UnlimitedGovernor>().is_ok());
    }

    #[test]
    fn noop_permit_is_silent() {
        let permit = IoPermit::noop();
        permit.complete(IoOutcome { bytes: 0, result: IoResult::Ok });
        drop(IoPermit::noop());
    }

    #[derive(Debug)]
    struct MemRecorder(Arc<AtomicU8>);

    impl MemoryState for MemRecorder {
        fn adjust(&self, actual_total: u64) {
            self.0.store(actual_total as u8, Ordering::SeqCst);
        }
    }

    #[test]
    fn memory_permit_forwards_adjust() {
        let state = Arc::new(AtomicU8::new(0));
        let permit = MemoryPermit::new(Box::new(MemRecorder(Arc::clone(&state))));
        permit.adjust(7);
        assert_eq!(state.load(Ordering::SeqCst), 7);
        MemoryPermit::noop().adjust(9);
    }

    #[test]
    fn observe_total_size_adjusts_at_most_once() {
        let state = Arc::new(AtomicU8::new(0));
        let permit = MemoryPermit::new(Box::new(MemRecorder(Arc::clone(&state))));
        permit.observe_total_size(7);
        assert_eq!(state.load(Ordering::SeqCst), 7);
        permit.observe_total_size(9); // a retried request observes again
        assert_eq!(state.load(Ordering::SeqCst), 7);
        // explicit adjust stays unrestricted
        permit.adjust(3);
        assert_eq!(state.load(Ordering::SeqCst), 3);
        MemoryPermit::noop().observe_total_size(9);
    }

    #[test]
    fn object_range_observation_reaches_the_reservation() {
        let state = Arc::new(AtomicU8::new(0));
        let permit = MemoryPermit::new(Box::new(MemRecorder(Arc::clone(&state))));

        // ranged reads have no reservation to reach, by construction
        ObjectRange::Ranged(&(0..10)).observe_total_size(9);
        ObjectRange::unmetered().observe_total_size(9);
        assert_eq!(state.load(Ordering::SeqCst), 0);

        let whole = ObjectRange::Whole(&permit);
        whole.observe_total_size(7);
        whole.observe_total_size(9); // at most once
        assert_eq!(state.load(Ordering::SeqCst), 7);
    }

    #[test]
    fn object_range_accessors() {
        let range = 3..10;
        let ranged = ObjectRange::Ranged(&range);
        assert_eq!(ranged.expected_bytes(), Some(7));
        assert_eq!(ranged.range(), Some(&range));

        let whole = ObjectRange::unmetered();
        assert_eq!(whole.expected_bytes(), None);
        assert_eq!(whole.range(), None);
    }
}
