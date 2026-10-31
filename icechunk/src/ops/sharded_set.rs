//! A hash set split into 256 independently locked shards, so many threads can
//! insert concurrently. Built for the tens of millions of chunk ids GC and
//! stats accumulate while decoding manifests in parallel.
//! This proved to be significantly faster than the dashmap crate.

use std::{
    collections::HashSet,
    hash::{BuildHasher, Hash, Hasher, RandomState},
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

const SHARDS: usize = 256;

/// Hasher for random 12-byte object ids: the hash is eight of the id's own
/// bytes, `id[1..9]` read little-endian, with no mixing at all. That is only
/// fast *and* well distributed because the ids are already uniformly random.
///
/// Three consumers each read their own slice of the 64-bit hash. They must
/// land on different id bytes, or two keys that collide in one of them would
/// collide in the others too:
///
/// | consumer | hash bits | id byte |
/// |---|---|---|
/// | hashbrown bucket index | lowest `log2(buckets)` | `id[1]`, then `id[2]`… as the table grows |
/// | `ShardedSet::shard_of` | 32..40 | `id[5]` |
/// | hashbrown control tag | top 7 | `id[8]` |
///
/// Any eight consecutive id bytes would satisfy this; `1..9` is simply the
/// window that was measured.
///
/// Meant for keys hashed in a single `write` call, as `ObjectId` is. A key
/// that makes several writes has them folded together (rotate and xor), not
/// concatenated, so the hash stays eight bytes wide.
#[derive(Debug, Default, Clone, Copy)]
pub struct IdHasherBuilder;

impl BuildHasher for IdHasherBuilder {
    type Hasher = IdHasher;
    fn build_hasher(&self) -> IdHasher {
        IdHasher(0)
    }
}

#[derive(Debug, Default)]
pub struct IdHasher(u64);

impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        if bytes.len() >= 9 {
            let mut eight = [0u8; 8];
            eight.copy_from_slice(&bytes[1..9]);
            // rotate by a number coprime with 8 so a second write cannot line
            // its bytes up with the first one's
            self.0 = self.0.rotate_left(29) ^ u64::from_le_bytes(eight);
        } else {
            // not an object id: fold whatever we got
            for b in bytes {
                self.0 = self.0.rotate_left(8) ^ u64::from(*b);
            }
        }
    }

    /// `[u8; N]::hash` writes the length first; the id bytes carry all the entropy.
    fn write_usize(&mut self, _: usize) {}
}

#[derive(Debug)]
pub struct ShardedSet<K, S = RandomState> {
    /// Picks the shard. One instance for the whole set, so every operation
    /// agrees on where a key lives (matters for `RandomState`).
    hasher: S,
    shards: Vec<Mutex<HashSet<K, S>>>,
    /// Staggers where consecutive `extend_weighted` calls start their sweep,
    /// so concurrent callers rarely want the same shard at the same time.
    rotor: AtomicUsize,
}

pub type ChunkIdSet = ShardedSet<crate::format::ChunkId, IdHasherBuilder>;

impl<K, S> Default for ShardedSet<K, S>
where
    K: Hash + Eq,
    S: BuildHasher + Default,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, S> ShardedSet<K, S>
where
    K: Hash + Eq,
    S: BuildHasher + Default,
{
    pub fn new() -> Self {
        Self {
            hasher: S::default(),
            shards: (0..SHARDS)
                .map(|_| Mutex::new(HashSet::with_hasher(S::default())))
                .collect(),
            rotor: AtomicUsize::new(0),
        }
    }

    /// Byte 4 of the hash, not the top one: hashbrown's control tag is the
    /// top 7 bits, and sharding on those would give every key in a shard the
    /// same tag, degrading its SIMD group probing to full key comparisons.
    fn shard_of(&self, key: &K) -> usize {
        ((self.hasher.hash_one(key) >> 32) & 0xFF) as usize
    }

    fn lock(&self, shard: usize) -> std::sync::MutexGuard<'_, HashSet<K, S>> {
        self.shards[shard].lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Whether `key` was newly inserted. Prefer `extend_weighted` for bulk work.
    pub fn insert(&self, key: K) -> bool {
        let shard = self.shard_of(&key);
        self.lock(shard).insert(key)
    }

    pub fn contains(&self, key: &K) -> bool {
        self.lock(self.shard_of(key)).contains(key)
    }

    pub fn len(&self) -> usize {
        (0..SHARDS).map(|s| self.lock(s).len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Insert every key, taking each shard's lock at most once. Returns the
    /// summed weight of the keys that were not already present.
    pub fn extend_weighted(&self, items: impl IntoIterator<Item = (K, u64)>) -> u64 {
        self.try_extend_weighted::<std::convert::Infallible>(items.into_iter().map(Ok))
            .unwrap_or_else(|never| match never {})
    }

    /// Like [`Self::extend_weighted`], but stops at the first `Err` and returns
    /// it. Items are bucketed as they arrive, so the caller need not collect
    /// them first.
    pub fn try_extend_weighted<E>(
        &self,
        items: impl IntoIterator<Item = Result<(K, u64), E>>,
    ) -> Result<u64, E> {
        let mut buckets: Vec<Vec<(K, u64)>> = (0..SHARDS).map(|_| Vec::new()).collect();
        for item in items {
            let (key, weight) = item?;
            buckets[self.shard_of(&key)].push((key, weight));
        }
        let start = (self.rotor.fetch_add(1, Ordering::Relaxed) * 8) % SHARDS;
        let mut new_weight = 0u64;
        for offset in 0..SHARDS {
            let shard = (start + offset) % SHARDS;
            let bucket = std::mem::take(&mut buckets[shard]);
            if bucket.is_empty() {
                continue;
            }
            let mut set = self.lock(shard);
            for (key, weight) in bucket {
                if set.insert(key) {
                    new_weight = new_weight.saturating_add(weight);
                }
            }
        }
        Ok(new_weight)
    }

    pub fn extend(&self, keys: impl IntoIterator<Item = K>) {
        self.extend_weighted(keys.into_iter().map(|k| (k, 0)));
    }

    /// Number of keys in every shard, for distribution checks.
    pub fn shard_lens(&self) -> Vec<usize> {
        (0..SHARDS).map(|s| self.lock(s).len()).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use proptest::prelude::*;

    use super::*;
    use crate::format::ChunkId;

    #[derive(Debug, Clone)]
    enum Op<K> {
        Insert(K),
        Contains(K),
        Extend(Vec<(K, u64)>),
        /// `try_extend_weighted` over the items with an `Err` injected at the
        /// given position (at most the batch length, so it may come last).
        TryExtend(Vec<(K, u64)>, usize),
    }

    fn ops<K: Debug + Clone + 'static>(
        key: BoxedStrategy<K>,
    ) -> impl Strategy<Value = Vec<Op<K>>> {
        let batch = prop::collection::vec((key.clone(), 0..1_000u64), 0..32);
        let op = prop_oneof![
            key.clone().prop_map(Op::Insert),
            key.prop_map(Op::Contains),
            batch.clone().prop_map(Op::Extend),
            (batch, 0..=32usize).prop_map(|(items, pos)| {
                let pos = pos.min(items.len());
                Op::TryExtend(items, pos)
            }),
        ];
        prop::collection::vec(op, 0..64)
    }

    /// Replay `ops` against the sharded set and a `HashSet` model, checking
    /// every return value and the aggregate views after each step.
    fn behaves_like_hash_set<K, S>(ops: Vec<Op<K>>) -> Result<(), TestCaseError>
    where
        K: Hash + Eq + Clone + Debug,
        S: BuildHasher + Default,
    {
        let set: ShardedSet<K, S> = ShardedSet::new();
        let mut model: HashSet<K> = HashSet::new();
        for op in ops {
            match op {
                Op::Insert(k) => {
                    prop_assert_eq!(set.insert(k.clone()), model.insert(k));
                }
                Op::Contains(k) => {
                    prop_assert_eq!(set.contains(&k), model.contains(&k));
                }
                Op::Extend(items) => {
                    // within one batch only the first occurrence of a key is new
                    let expected: u64 = items
                        .iter()
                        .filter(|(k, _)| model.insert(k.clone()))
                        .map(|(_, w)| w)
                        .sum();
                    prop_assert_eq!(set.extend_weighted(items), expected);
                }
                Op::TryExtend(items, pos) => {
                    let len_before = set.len();
                    let (head, tail) = items.split_at(pos);
                    let iter = head
                        .iter()
                        .cloned()
                        .map(Ok)
                        .chain(std::iter::once(Err(())))
                        .chain(tail.iter().cloned().map(Ok));
                    prop_assert_eq!(set.try_extend_weighted(iter), Err(()));
                    // a failed batch inserts nothing, not even the items before the error
                    prop_assert_eq!(set.len(), len_before);
                    for (k, _) in &items {
                        prop_assert_eq!(set.contains(k), model.contains(k));
                    }
                }
            }
            prop_assert_eq!(set.len(), model.len());
            prop_assert_eq!(set.is_empty(), model.is_empty());
            prop_assert_eq!(set.shard_lens().iter().sum::<usize>(), model.len());
        }
        for k in &model {
            prop_assert!(set.contains(k));
        }
        Ok(())
    }

    /// Ids as production produces them, plus two adversarial shapes: ids that
    /// `IdHasher` cannot tell apart (same bytes `1..9`, so identical hash,
    /// shard and bucket), and a tiny pool for many exact duplicates.
    fn chunk_id() -> BoxedStrategy<ChunkId> {
        prop_oneof![
            any::<[u8; 12]>(),
            (any::<u8>(), any::<u8>()).prop_map(|(head, tail)| {
                let mut bytes = [7u8; 12];
                bytes[0] = head;
                bytes[11] = tail;
                bytes
            }),
            (0..4u8).prop_map(|b| [b; 12]),
        ]
        .prop_map(ChunkId::new)
        .boxed()
    }

    proptest! {
        #[test]
        fn chunk_id_set_behaves_like_hash_set(ops in ops(chunk_id())) {
            behaves_like_hash_set::<ChunkId, IdHasherBuilder>(ops)?;
        }

        /// The generic instantiation, with `RandomState` picking the shard.
        /// Small keys so batches repeat keys often.
        #[test]
        fn generic_set_behaves_like_hash_set(ops in ops(any::<u16>().boxed())) {
            behaves_like_hash_set::<u16, RandomState>(ops)?;
        }
    }

    #[test]
    fn insert_contains_len() {
        let set = ChunkIdSet::new();
        let a = ChunkId::random();
        let b = ChunkId::random();
        assert!(set.insert(a.clone()));
        assert!(!set.insert(a.clone()));
        assert!(set.contains(&a));
        assert!(!set.contains(&b));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn extend_weighted_counts_only_new_keys() {
        let set = ChunkIdSet::new();
        let ids: Vec<ChunkId> = (0..1000).map(|_| ChunkId::random()).collect();
        let first = set.extend_weighted(ids.iter().cloned().map(|id| (id, 3)));
        assert_eq!(first, 3000);
        let again = set.extend_weighted(ids.iter().cloned().map(|id| (id, 3)));
        assert_eq!(again, 0);
        assert_eq!(set.len(), 1000);
        for id in &ids {
            assert!(set.contains(id));
        }
    }

    /// The error must surface, and nothing may be inserted: the bucketing
    /// loop stops before any shard is touched.
    #[test]
    fn try_extend_weighted_stops_at_the_first_error() {
        let set = ChunkIdSet::new();
        let good = ChunkId::random();
        let unreached = ChunkId::random();
        let items = vec![Ok((good.clone(), 7)), Err("boom"), Ok((unreached.clone(), 7))];
        assert_eq!(set.try_extend_weighted(items), Err("boom"));
        assert!(!set.contains(&good));
        assert!(!set.contains(&unreached));
        assert!(set.is_empty());
    }

    #[test]
    fn random_ids_spread_evenly_over_shards() {
        let set = ChunkIdSet::new();
        let n = 1_000_000usize;
        set.extend((0..n).map(|_| ChunkId::random()));
        assert_eq!(set.len(), n);
        let mean = n as f64 / SHARDS as f64;
        for (i, len) in set.shard_lens().into_iter().enumerate() {
            let ratio = len as f64 / mean;
            assert!((0.8..1.2).contains(&ratio), "shard {i} holds {len}, mean {mean}");
        }
    }

    #[test]
    fn concurrent_extend_from_many_threads() {
        let set = std::sync::Arc::new(ChunkIdSet::new());
        let shared: Vec<ChunkId> = (0..50_000).map(|_| ChunkId::random()).collect();
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let set = std::sync::Arc::clone(&set);
                let shared = shared.clone();
                std::thread::spawn(move || {
                    // every thread inserts the same shared ids plus 10k of its own
                    set.extend(shared);
                    set.extend((0..10_000).map(|_| ChunkId::random()));
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(set.len(), 50_000 + 16 * 10_000);
        for id in &shared {
            assert!(set.contains(id));
        }
    }

    #[test]
    fn generic_keys_use_random_state() {
        let set: ShardedSet<(String, u64)> = ShardedSet::new();
        assert_eq!(
            set.extend_weighted([
                (("a".to_string(), 1), 10),
                (("a".to_string(), 1), 10),
                (("b".to_string(), 2), 5),
            ]),
            15
        );
        assert!(set.contains(&("a".to_string(), 1)));
        assert_eq!(set.len(), 2);
    }
}
