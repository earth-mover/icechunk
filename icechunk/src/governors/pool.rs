//! A `u64`-weighted async admission pool.
//!
//! # Admission discipline
//!
//! A request for `weight` is granted `eff = weight.min(capacity)` when
//! `in_use == 0 || in_use + eff <= capacity`: an oversized request runs
//! alone rather than never. Waiters form a strict FIFO queue and only the
//! head is ever considered — a new request that would fit still queues
//! behind existing waiters, so a large request can't be starved by a
//! stream of small ones. Shrinking the capacity never cancels outstanding
//! grants; `in_use` simply stays above `capacity` (blocking all admission)
//! until enough weight is returned.
//!
//! Waiting is cancel-safe: dropping an un-granted waiter dequeues it (a
//! cancelled head immediately unblocks its followers); dropping one that
//! was granted concurrently returns the weight to the pool.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use tokio::sync::oneshot;
use tracing::error;

#[derive(Debug)]
pub struct WeightedPool {
    state: Arc<Mutex<PoolState>>,
}

#[derive(Debug)]
struct PoolState {
    capacity: u64,
    in_use: u64,
    next_waiter_id: u64,
    queue: VecDeque<Waiter>,
}

#[derive(Debug)]
struct Waiter {
    id: u64,
    weight: u64,
    /// Sends the effective (granted) weight; a failed send means the waiter
    /// was cancelled and the weight goes back to the pool.
    grant: oneshot::Sender<u64>,
}

fn lock(state: &Mutex<PoolState>) -> MutexGuard<'_, PoolState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

impl PoolState {
    fn admissible(&self, eff: u64) -> bool {
        self.in_use == 0 || self.in_use.saturating_add(eff) <= self.capacity
    }

    /// Admit waiters strictly from the head until one doesn't fit.
    fn wake_waiters(&mut self) {
        while let Some(head) = self.queue.front() {
            let eff = head.weight.min(self.capacity);
            if !self.admissible(eff) {
                break;
            }
            let Some(waiter) = self.queue.pop_front() else { break };
            self.in_use = self.in_use.saturating_add(eff);
            if waiter.grant.send(eff).is_err() {
                // cancelled before its Drop could dequeue it; undo and go on
                self.in_use = self.in_use.saturating_sub(eff);
            }
        }
    }
}

impl WeightedPool {
    pub fn new(capacity: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(PoolState {
                capacity,
                in_use: 0,
                next_waiter_id: 0,
                queue: VecDeque::new(),
            })),
        }
    }

    /// Waits until `weight` (capped at the pool capacity) is admissible,
    /// then debits it until the returned guard drops.
    pub async fn acquire(&self, weight: u64) -> PoolGuard {
        let (waiter_id, rx) = {
            let mut pool = lock(&self.state);
            let eff = weight.min(pool.capacity);
            // Immediate grant only when nobody is queued. Notice that small
            // requests can't pass before the head of the queue:
            // checking only for admissible is not enough.
            if pool.queue.is_empty() && pool.admissible(eff) {
                pool.in_use = pool.in_use.saturating_add(eff);
                return PoolGuard::new(&self.state, eff);
            }
            // we continue under the same lock, now we know admission failed
            // we need to enqueue
            let (tx, rx) = oneshot::channel();
            let waiter_id = pool.next_waiter_id;
            pool.next_waiter_id += 1;
            pool.queue.push_back(Waiter { id: waiter_id, weight, grant: tx });
            (waiter_id, rx)
        }; // lock released before awaiting

        // If this future is dropped mid-wait, `queued`'s Drop dequeues the
        // waiter, or repays a grant that raced with the cancellation. It
        // must own `rx` for that, so we await through `&mut`.
        let mut queued = QueuedWaiter { pool: self, id: waiter_id, rx, granted: false };
        let outcome = (&mut queued.rx).await;
        queued.granted = true; // disarm: from here the guard owns the debit
        match outcome {
            // the granting side already debited `eff` (capacity at grant
            // time, not enqueue time)
            Ok(eff) => PoolGuard::new(&self.state, eff),
            Err(_) => {
                // Unreachable by construction: the sender drops only after a
                // successful send (`wake_waiters`) or in our own
                // `QueuedWaiter::drop`, which can't run while we're here.
                debug_assert!(false, "weighted pool waiter lost its grant channel");
                error!("weighted pool waiter lost its grant channel");
                PoolGuard::new(&self.state, 0)
            }
        }
    }

    /// Change the capacity. Growing admits eligible waiters; shrinking
    /// below `in_use` just blocks admission until enough weight returns.
    pub fn set_capacity(&self, capacity: u64) {
        let mut pool = lock(&self.state);
        pool.capacity = capacity;
        pool.wake_waiters();
    }

    /// Debit `weight` unconditionally, without waiting for admission. The
    /// caller owes it back through a [`PoolGuard`] it is already holding
    /// (or a later explicit release mechanism).
    pub fn force_add(&self, weight: u64) {
        let mut pool = lock(&self.state);
        pool.in_use = pool.in_use.saturating_add(weight);
    }

    pub fn capacity(&self) -> u64 {
        lock(&self.state).capacity
    }

    pub fn in_use(&self) -> u64 {
        lock(&self.state).in_use
    }

    /// Number of queued waiters.
    pub fn queued(&self) -> usize {
        lock(&self.state).queue.len()
    }
}

/// Weight debited from a [`WeightedPool`], returned on drop.
#[derive(Debug)]
pub struct PoolGuard {
    state: Arc<Mutex<PoolState>>,
    debit: u64,
}

impl PoolGuard {
    fn new(state: &Arc<Mutex<PoolState>>, debit: u64) -> Self {
        Self { state: Arc::clone(state), debit }
    }

    /// The effective weight this guard holds.
    pub fn weight(&self) -> u64 {
        self.debit
    }

    /// True-up the held weight. Down releases the difference (admitting
    /// eligible waiters); up debits it immediately without waiting, even
    /// past the pool capacity — the overage is repaid when the guard drops.
    pub fn adjust_to(&mut self, new_weight: u64) {
        let mut pool = lock(&self.state);
        if new_weight < self.debit {
            pool.in_use = pool.in_use.saturating_sub(self.debit - new_weight);
            self.debit = new_weight;
            pool.wake_waiters();
        } else {
            pool.in_use = pool.in_use.saturating_add(new_weight - self.debit);
            self.debit = new_weight;
        }
    }
}

impl Drop for PoolGuard {
    fn drop(&mut self) {
        let mut pool = lock(&self.state);
        pool.in_use = pool.in_use.saturating_sub(self.debit);
        pool.wake_waiters();
    }
}

/// Cleans up after a cancelled `acquire`: lives across the grant await so
/// that dropping the acquire future dequeues the waiter (or repays a grant
/// that raced with the cancellation).
struct QueuedWaiter<'a> {
    pool: &'a WeightedPool,
    id: u64,
    rx: oneshot::Receiver<u64>,
    granted: bool,
}

impl Drop for QueuedWaiter<'_> {
    fn drop(&mut self) {
        if self.granted {
            return;
        }
        let mut pool = lock(&self.pool.state);
        let len_before = pool.queue.len();
        let id = self.id;
        pool.queue.retain(|waiter| waiter.id != id);
        if pool.queue.len() < len_before {
            // dequeued while still waiting; the head may have changed
            pool.wake_waiters();
        } else if let Ok(eff) = self.rx.try_recv() {
            // granted between our last poll and the drop: give it back
            pool.in_use = pool.in_use.saturating_sub(eff);
            pool.wake_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{FutureExt as _, poll};
    use std::task::Poll;

    #[icechunk_macros::tokio_test]
    async fn test_accounting() {
        let pool = WeightedPool::new(10);
        let g1 = pool.acquire(3).await;
        let g2 = pool.acquire(4).await;
        assert_eq!(pool.in_use(), 7);
        assert_eq!(g1.weight(), 3);

        drop(g1);
        assert_eq!(pool.in_use(), 4);
        drop(g2);
        assert_eq!(pool.in_use(), 0);
        assert_eq!(pool.capacity(), 10);
    }

    /// A queued large request is not starved: a later small request that
    /// would fit still queues behind it.
    #[icechunk_macros::tokio_test]
    async fn test_no_barging_past_waiters() {
        let pool = WeightedPool::new(10);
        let hold = pool.acquire(6).await;

        let mut large = pool.acquire(6).boxed();
        assert!(poll!(large.as_mut()).is_pending());

        // 6 + 3 <= 10 fits numerically, but FIFO queues it behind `large`
        let mut small = pool.acquire(3).boxed();
        assert!(poll!(small.as_mut()).is_pending());
        assert_eq!(pool.queued(), 2);

        drop(hold);
        assert!(poll!(large.as_mut()).is_ready());
        assert!(poll!(small.as_mut()).is_ready());
    }

    /// Waiters are granted strictly in arrival order.
    #[icechunk_macros::tokio_test]
    async fn test_fifo_grant_order() {
        let pool = WeightedPool::new(10);
        let hold = pool.acquire(10).await;

        let mut a = pool.acquire(7).boxed();
        assert!(poll!(a.as_mut()).is_pending());
        let mut b = pool.acquire(2).boxed();
        assert!(poll!(b.as_mut()).is_pending());
        let mut c = pool.acquire(2).boxed();
        assert!(poll!(c.as_mut()).is_pending());

        drop(hold); // a (7) and b (7+2<=10) admitted; c (9+2>10) still waits
        let Poll::Ready(ga) = poll!(a.as_mut()) else { panic!("a must be granted") };
        let Poll::Ready(_gb) = poll!(b.as_mut()) else { panic!("b must be granted") };
        assert!(poll!(c.as_mut()).is_pending());

        drop(ga); // in_use 2, c (2+2<=10) admitted
        assert!(poll!(c.as_mut()).is_ready());
    }

    /// An oversized request is capped at capacity and runs alone.
    #[icechunk_macros::tokio_test]
    async fn test_oversized_acquire_capped_and_solo() {
        let pool = WeightedPool::new(10);
        let g = pool.acquire(25).await;
        assert_eq!(g.weight(), 10);
        assert_eq!(pool.in_use(), 10);

        let mut next = pool.acquire(1).boxed();
        assert!(poll!(next.as_mut()).is_pending());

        drop(g);
        assert!(poll!(next.as_mut()).is_ready());
        assert_eq!(pool.in_use(), 0);
    }

    /// Shrinking below `in_use` cancels nothing; admission stays blocked
    /// until enough outstanding weight drains.
    #[icechunk_macros::tokio_test]
    async fn test_shrink_below_in_use_blocks_admission() {
        let pool = WeightedPool::new(10);
        let g1 = pool.acquire(4).await;
        let g2 = pool.acquire(4).await;

        pool.set_capacity(5);
        assert_eq!(pool.capacity(), 5);
        assert_eq!(pool.in_use(), 8);

        let mut next = pool.acquire(2).boxed();
        assert!(poll!(next.as_mut()).is_pending());

        drop(g1); // in_use 4; 4 + 2 > 5, still blocked
        assert!(poll!(next.as_mut()).is_pending());

        drop(g2); // in_use 0
        let Poll::Ready(g) = poll!(next.as_mut()) else { panic!("must be granted") };
        assert_eq!(g.weight(), 2);
        assert_eq!(pool.in_use(), 2);
    }

    #[icechunk_macros::tokio_test]
    async fn test_grow_wakes_waiters() {
        let pool = WeightedPool::new(4);
        let hold = pool.acquire(4).await;

        let mut next = pool.acquire(3).boxed();
        assert!(poll!(next.as_mut()).is_pending());

        pool.set_capacity(8);
        assert!(poll!(next.as_mut()).is_ready());
        drop(hold);
        assert_eq!(pool.in_use(), 0);
    }

    /// Dropping a queued waiter dequeues it and leaks no weight.
    #[icechunk_macros::tokio_test]
    async fn test_cancelled_waiter_leaks_nothing() {
        let pool = WeightedPool::new(4);
        let hold = pool.acquire(4).await;

        let mut a = pool.acquire(2).boxed();
        assert!(poll!(a.as_mut()).is_pending());
        let mut b = pool.acquire(2).boxed();
        assert!(poll!(b.as_mut()).is_pending());
        assert_eq!(pool.queued(), 2);

        drop(a);
        assert_eq!(pool.queued(), 1);

        drop(hold);
        let Poll::Ready(gb) = poll!(b.as_mut()) else { panic!("b must be granted") };
        assert_eq!(pool.in_use(), 2);
        drop(gb);
        assert_eq!(pool.in_use(), 0);
    }

    /// A cancelled head immediately unblocks its followers, with no
    /// release event needed.
    #[icechunk_macros::tokio_test]
    async fn test_cancelling_head_admits_follower() {
        let pool = WeightedPool::new(10);
        let hold = pool.acquire(6).await;

        let mut head = pool.acquire(6).boxed();
        assert!(poll!(head.as_mut()).is_pending());
        let mut follower = pool.acquire(3).boxed();
        assert!(poll!(follower.as_mut()).is_pending());

        drop(head);
        assert!(poll!(follower.as_mut()).is_ready());
        drop(hold);
        assert_eq!(pool.in_use(), 0);
    }

    /// A waiter granted concurrently with its cancellation returns the
    /// weight to the pool.
    #[icechunk_macros::tokio_test]
    async fn test_cancelled_after_grant_returns_weight() {
        let pool = WeightedPool::new(4);
        let hold = pool.acquire(4).await;

        let mut a = pool.acquire(2).boxed();
        assert!(poll!(a.as_mut()).is_pending());

        drop(hold); // grants a inside the release, before a is polled again
        assert_eq!(pool.in_use(), 2);

        drop(a); // never learns of the grant; its Drop must repay it
        assert_eq!(pool.in_use(), 0);
    }

    /// Adjusting a guard down releases the difference and wakes waiters.
    #[icechunk_macros::tokio_test]
    async fn test_adjust_to_down_wakes_waiters() {
        let pool = WeightedPool::new(10);
        let mut g = pool.acquire(8).await;

        let mut next = pool.acquire(5).boxed();
        assert!(poll!(next.as_mut()).is_pending());

        g.adjust_to(3);
        assert_eq!(g.weight(), 3);
        let Poll::Ready(gn) = poll!(next.as_mut()) else { panic!("must be granted") };
        assert_eq!(pool.in_use(), 8);

        drop(g);
        drop(gn);
        assert_eq!(pool.in_use(), 0);
    }

    /// Adjusting up debits immediately, past capacity if need be; new
    /// admissions block until the guard repays it.
    #[icechunk_macros::tokio_test]
    async fn test_adjust_to_up_debits_past_capacity() {
        let pool = WeightedPool::new(10);
        let mut g = pool.acquire(4).await;
        g.adjust_to(15);
        assert_eq!(g.weight(), 15);
        assert_eq!(pool.in_use(), 15);

        let mut next = pool.acquire(1).boxed();
        assert!(poll!(next.as_mut()).is_pending());

        drop(g);
        assert_eq!(pool.in_use(), 1, "waiter admitted once the overage is repaid");
        assert!(poll!(next.as_mut()).is_ready());
    }

    #[icechunk_macros::tokio_test]
    async fn test_force_add_debits_past_capacity() {
        let pool = WeightedPool::new(10);
        let g = pool.acquire(8).await;
        pool.force_add(5);
        assert_eq!(pool.in_use(), 13);

        let mut next = pool.acquire(1).boxed();
        assert!(poll!(next.as_mut()).is_pending());

        drop(g); // in_use 5; 5 + 1 <= 10
        assert!(poll!(next.as_mut()).is_ready());
    }

    #[icechunk_macros::tokio_test]
    async fn test_concurrent_hammer_preserves_invariants() {
        let pool = Arc::new(WeightedPool::new(50));
        let tasks: Vec<_> = (0..8u64)
            .map(|task| {
                let pool = Arc::clone(&pool);
                tokio::spawn(async move {
                    for i in 0..200u64 {
                        let weight = (task * 7 + i * 13) % 23 + 1;
                        let guard = pool.acquire(weight).await;
                        // no weight exceeds capacity, so in_use never does
                        assert!(pool.in_use() <= 50);
                        assert_eq!(guard.weight(), weight);
                        tokio::task::yield_now().await;
                    }
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(pool.in_use(), 0);
        assert_eq!(pool.queued(), 0);
    }
}
