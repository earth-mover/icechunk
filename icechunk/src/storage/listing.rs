//! Parallel listing of the typed object ids under a repository prefix.
//!
//! [`list_object_ids`] is the entry point: given a `Storage`, a prefix such as
//! `chunks` and a concurrency, it yields every object under the prefix as a
//! typed [`ListInfo`], unordered. On backends that list id prefixes natively
//! the work fans out over a pool of tasks, one id prefix at a time, so the
//! parsing of listing responses spreads over cores; elsewhere it is one plain
//! listing.

use std::{
    num::NonZeroU16,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_stream::try_stream;
use futures::{
    Stream, StreamExt as _, TryStreamExt as _, future::ready, stream::BoxStream,
};
use icechunk_types::{ICResultExt as _, error::ICResultCtxExt as _};
use tokio::{sync::mpsc, task::JoinSet};
use tracing::debug;

use crate::{
    Storage,
    format::{OBJECT_ID_ONE_CHAR_PREFIXES, OBJECT_ID_TWO_CHAR_PREFIXES},
    repository::RepositoryResult,
    storage::{self, ListInfo},
};

/// About ten listing streams keep one core busy parsing responses, so the
/// default scales with the machine, within [32, 256].
pub fn default_list_concurrency() -> NonZeroU16 {
    let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
    let n = (cores * 8).clamp(32, 256);
    NonZeroU16::new(n as u16).unwrap_or(NonZeroU16::MIN)
}

/// One S3 page.
const LIST_PAGE_SIZE: usize = 1_000;

/// The single id prefix [`list_object_ids`] probes to size its fan-out: `"00"`,
/// the lexicographically first two-character prefix, which is why the slice is
/// the head of the sorted list.
static PROBE_ID_PREFIX: LazyLock<&'static [String]> =
    LazyLock::new(|| &OBJECT_ID_TWO_CHAR_PREFIXES[..1]);

/// Unordered. Lists id prefixes on `concurrency` tasks.
///
/// A fixed concurrency doesn't work well, if you choose 1024
/// and the repo has < 1k objects, you wasted (and paid) for
/// 1023 request. So we probe with a two character prefix initially,
/// 00, if it comes full, it deserves all the concurrency.
///
/// Dropping the result stream drops all tasks.
pub(crate) async fn list_object_ids<'a, Id>(
    storage: &'a Arc<dyn Storage + Send + Sync>,
    settings: &'a storage::Settings,
    prefix: &str,
    concurrency: NonZeroU16,
) -> RepositoryResult<BoxStream<'a, RepositoryResult<ListInfo<Id>>>>
where
    Id: for<'b> TryFrom<&'b str> + Send + std::fmt::Debug + 'static,
{
    if !storage.lists_id_prefixes_natively() {
        // one pass over the prefix; fanning out would repeat the full listing
        let stream = storage.list_objects(settings, prefix).await.inject()?;
        return Ok(translate_list_infos(stream.map(|r| r.inject())));
    }

    let probe = storage
        .list_objects_with_id_prefixes(settings, prefix, *PROBE_ID_PREFIX)
        .await
        .inject()?;
    let probed = probe
        .take(LIST_PAGE_SIZE)
        .try_fold(0usize, |n, _| ready(Ok(n + 1)))
        .await
        .inject()?;
    let prefixes: &'static [String] = if probed >= LIST_PAGE_SIZE {
        OBJECT_ID_TWO_CHAR_PREFIXES.as_slice()
    } else {
        OBJECT_ID_ONE_CHAR_PREFIXES.as_slice()
    };
    debug!(
        prefix,
        probed,
        fan_out = prefixes.len(),
        concurrency = concurrency.get(),
        "listing object ids"
    );

    let storage = Arc::clone(storage);
    let settings = settings.clone();
    let prefix = prefix.to_string();

    let workers_n = (concurrency.get() as usize).min(prefixes.len());
    // room for every worker to have one page queued while it fetches the next
    let (tx, rx) = mpsc::channel::<Vec<ListInfo<Id>>>(2 * workers_n);
    let next = Arc::new(AtomicUsize::new(0));
    let mut workers: JoinSet<RepositoryResult<()>> = JoinSet::new();
    for _ in 0..workers_n {
        workers.spawn(list_prefixes_worker(
            Arc::clone(&storage),
            settings.clone(),
            prefix.clone(),
            prefixes,
            Arc::clone(&next),
            tx.clone(),
        ));
    }
    drop(tx);

    let stream = try_stream! {
        let mut workers = workers;
        let mut rx = rx;
        loop {
            let event = tokio::select! {
                biased;
                Some(joined) = workers.join_next() => ListEvent::Worker(joined),
                page = rx.recv() => ListEvent::Page(page),
            };
            match event {
                // a worker finishing means it ran out of prefixes, unless it failed
                ListEvent::Worker(joined) => { joined.capture()??; }
                ListEvent::Page(Some(page)) => {
                    for info in page {
                        yield info;
                    }
                }
                ListEvent::Page(None) => break,
            }
        }
        while let Some(joined) = workers.join_next().await {
            joined.capture()??;
        }
    };
    Ok(stream.boxed())
}

pub(crate) fn convert_list_item<Id>(item: &ListInfo<String>) -> Option<ListInfo<Id>>
where
    Id: for<'b> TryFrom<&'b str>,
{
    let id = Id::try_from(item.id.as_str()).ok()?;
    let created_at = item.created_at;
    Some(ListInfo { created_at, id, size_bytes: item.size_bytes })
}

pub(crate) fn translate_list_infos<'a, Id>(
    s: impl Stream<Item = RepositoryResult<ListInfo<String>>> + Send + 'a,
) -> BoxStream<'a, RepositoryResult<ListInfo<Id>>>
where
    Id: for<'b> TryFrom<&'b str> + Send + std::fmt::Debug + 'a,
{
    s.try_filter_map(|info| async move {
        let converted = convert_list_item(&info);
        if converted.is_none() {
            tracing::error!(list_info = ?info, "Error processing list item metadata");
        }
        Ok(converted)
    })
    .boxed()
}

/// One step of the listing fan-out loop. `select!` arms cannot `yield`, so
/// they hand the outcome to the loop body, which does.
enum ListEvent<T> {
    Worker(Result<RepositoryResult<()>, tokio::task::JoinError>),
    Page(Option<T>),
}

/// Lists one id prefix at a time from the shared queue until it is empty,
/// parsing ids and sending one `Vec` per page. Stops silently when the
/// receiver is gone: the consumer dropped the stream.
async fn list_prefixes_worker<Id>(
    storage: Arc<dyn Storage + Send + Sync>,
    settings: storage::Settings,
    prefix: String,
    prefixes: &'static [String],
    next: Arc<AtomicUsize>,
    tx: mpsc::Sender<Vec<ListInfo<Id>>>,
) -> RepositoryResult<()>
where
    Id: for<'b> TryFrom<&'b str> + Send + std::fmt::Debug + 'static,
{
    loop {
        let i = next.fetch_add(1, Ordering::Relaxed);
        let Some(id_prefix) = prefixes.get(i..=i) else { return Ok(()) };
        let stream = storage
            .list_objects_with_id_prefixes(&settings, &prefix, id_prefix)
            .await
            .inject()?;
        // the backend yields a whole page at once, so `ready_chunks` groups by page
        let mut pages = stream.ready_chunks(LIST_PAGE_SIZE);
        while let Some(items) = pages.next().await {
            let mut page = Vec::with_capacity(items.len());
            for item in items {
                let item = item.inject()?;
                match convert_list_item(&item) {
                    Some(info) => page.push(info),
                    None => {
                        tracing::error!(list_info = ?item, "Error processing list item metadata");
                    }
                }
            }
            if tx.send(page).await.is_err() {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use icechunk_macros::tokio_test;

    use super::*;
    use crate::{
        Repository,
        asset_manager::AssetManager,
        format::{ChunkId, format_constants::SpecVersionBin},
        storage::{
            latency::LatencyStorage, logging::LoggingStorage, new_in_memory_storage,
        },
    };

    /// A repo with `n` zero-byte chunks whose ids all start with `"0"`, or with
    /// `"00"` when `two_char`, which is the prefix the probe lists: so the probe
    /// page is full when `two_char` and `n >= 1000`.
    #[cfg(not(feature = "shuttle"))]
    async fn repo_with_chunks_under_first_prefix(
        backend: &Arc<dyn Storage + Send + Sync>,
        n: usize,
        two_char: bool,
    ) -> Arc<AssetManager> {
        let repo =
            Repository::create(None, Arc::clone(backend), Default::default(), None, true)
                .await
                .unwrap();
        let am = Arc::clone(repo.asset_manager());
        for _ in 0..n {
            let mut id = ChunkId::random();
            // base32 packs 5 bits per character: the first is byte 0's top 5
            // bits, the second spans byte 0's low 3 and byte 1's top 2
            if two_char {
                id.0[0] = 0;
                id.0[1] &= 0x3F;
            } else {
                id.0[0] &= 0x07;
            }
            am.write_chunk(id, Bytes::new()).await.unwrap();
        }
        am
    }

    #[cfg(not(feature = "shuttle"))]
    fn list_calls(logging: &LoggingStorage) -> usize {
        logging
            .fetch_operations()
            .into_iter()
            .filter(|(op, _)| op == "list_objects_with_id_prefixes")
            .count()
    }

    #[tokio_test]
    async fn small_repo_lists_with_32_prefixes() {
        let backend: Arc<dyn Storage + Send + Sync> =
            new_in_memory_storage().await.unwrap();
        let am = repo_with_chunks_under_first_prefix(&backend, 50, false).await;
        let (logging, lam) = crate::test_utils::fanning_out_logging_asset_manager(
            &backend,
            am.storage_settings().clone(),
            SpecVersionBin::V2,
        );
        let ids: Vec<ChunkId> = lam
            .list_chunks_with_concurrency(NonZeroU16::new(8).unwrap())
            .await
            .unwrap()
            .map_ok(|i| i.id)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(ids.len(), 50);
        assert_eq!(list_calls(&logging), 1 + 32, "probe plus one call per prefix");
    }

    #[tokio_test]
    async fn full_probe_page_lists_with_1024_prefixes() {
        let backend: Arc<dyn Storage + Send + Sync> =
            new_in_memory_storage().await.unwrap();
        let am = repo_with_chunks_under_first_prefix(&backend, 1000, true).await;
        let (logging, lam) = crate::test_utils::fanning_out_logging_asset_manager(
            &backend,
            am.storage_settings().clone(),
            SpecVersionBin::V2,
        );
        let ids: Vec<ChunkId> = lam
            .list_chunks_with_concurrency(NonZeroU16::new(64).unwrap())
            .await
            .unwrap()
            .map_ok(|i| i.id)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(ids.len(), 1000);
        assert_eq!(list_calls(&logging), 1 + 1024);
        // every id exactly once
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), 1000);
    }

    #[tokio_test]
    async fn concurrency_does_not_change_the_result() {
        let backend: Arc<dyn Storage + Send + Sync> =
            new_in_memory_storage().await.unwrap();
        let written = repo_with_chunks_under_first_prefix(&backend, 300, false).await;
        let (_logging, am) = crate::test_utils::fanning_out_logging_asset_manager(
            &backend,
            written.storage_settings().clone(),
            SpecVersionBin::V2,
        );
        let one: std::collections::HashSet<ChunkId> = am
            .list_chunks_with_concurrency(NonZeroU16::MIN)
            .await
            .unwrap()
            .map_ok(|i| i.id)
            .try_collect()
            .await
            .unwrap();
        let many: std::collections::HashSet<ChunkId> = am
            .list_chunks_with_concurrency(NonZeroU16::new(64).unwrap())
            .await
            .unwrap()
            .map_ok(|i| i.id)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(one.len(), 300);
        assert_eq!(one, many);
        let default_: std::collections::HashSet<ChunkId> =
            am.list_chunks().await.unwrap().map_ok(|i| i.id).try_collect().await.unwrap();
        assert_eq!(default_, many);
    }

    #[tokio_test]
    async fn dropping_the_stream_stops_the_workers() {
        let backend: Arc<dyn Storage + Send + Sync> =
            new_in_memory_storage().await.unwrap();
        let written = repo_with_chunks_under_first_prefix(&backend, 1000, false).await;
        // an in-memory listing finishes before the first page is consumed, so
        // slow it down: otherwise the workers are already gone and a dropped
        // stream proves nothing
        let slow: Arc<dyn Storage + Send + Sync> =
            Arc::new(LatencyStorage::new(Arc::clone(&backend), 0, 50));
        let (logging, am) = crate::test_utils::fanning_out_logging_asset_manager(
            &slow,
            written.storage_settings().clone(),
            SpecVersionBin::V2,
        );
        // every worker holds a clone of the storage `Arc`, so the count coming
        // back down is the observable proof that they were dropped
        let idle = Arc::strong_count(&logging);
        let mut stream =
            am.list_chunks_with_concurrency(NonZeroU16::new(16).unwrap()).await.unwrap();
        let first = stream.try_next().await.unwrap();
        assert!(first.is_some());
        assert!(
            Arc::strong_count(&logging) > idle,
            "workers should be holding storage clones while listing"
        );
        // the JoinSet lives inside the stream and aborts its tasks on drop, but
        // the runtime drops the aborted futures on its own schedule
        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&logging) != idle {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("aborted workers should drop their storage clones");

        let n = am
            .list_chunks()
            .await
            .unwrap()
            .try_fold(0usize, |n, _| async move { Ok(n + 1) })
            .await
            .unwrap();
        assert_eq!(n, 1000);
    }

    /// A backend that only filters a full listing must be listed once, not
    /// once per id prefix: the fan-out would repeat the whole listing.
    #[tokio_test]
    async fn backend_without_native_prefixes_lists_once() {
        let backend: Arc<dyn Storage + Send + Sync> =
            new_in_memory_storage().await.unwrap();
        let written = repo_with_chunks_under_first_prefix(&backend, 50, false).await;
        assert!(
            !backend.lists_id_prefixes_natively(),
            "the in-memory backend filters, it does not list prefixes server side"
        );
        let (logging, am) = crate::test_utils::logging_asset_manager(
            &backend,
            written.storage_settings().clone(),
            SpecVersionBin::V2,
        );
        let ids: Vec<ChunkId> = am
            .list_chunks_with_concurrency(NonZeroU16::new(64).unwrap())
            .await
            .unwrap()
            .map_ok(|i| i.id)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(ids.len(), 50);
        let ops = logging.fetch_operations();
        let plain = ops.iter().filter(|(op, _)| op == "list_objects").count();
        assert_eq!(plain, 1, "exactly one full listing");
        assert_eq!(list_calls(&logging), 0, "no prefixed listing, not even a probe");
    }

    /// The probe must be exactly one prefix, the smallest two-character one,
    /// so that "a full probe page" means every prefix holds at least a page.
    #[test]
    fn probe_prefix_is_the_smallest_two_char_prefix() {
        assert_eq!(*PROBE_ID_PREFIX, ["00".to_string()].as_slice());
        assert_eq!(
            PROBE_ID_PREFIX[0],
            *OBJECT_ID_TWO_CHAR_PREFIXES.iter().min().expect("non-empty table")
        );
        assert!(OBJECT_ID_TWO_CHAR_PREFIXES.is_sorted());
        assert_eq!(
            OBJECT_ID_TWO_CHAR_PREFIXES.len(),
            OBJECT_ID_ONE_CHAR_PREFIXES.len().pow(2)
        );
    }

    #[test]
    fn default_list_concurrency_is_clamped() {
        let n = default_list_concurrency().get();
        assert!((32..=256).contains(&n));
    }
}
