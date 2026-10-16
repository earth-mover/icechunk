//! A test storage that records the calls a wrapper forwards to it, and
//! [`assert_forwards`], which checks a wrapper forwards each of them.

use std::{fmt, ops::Range, pin::Pin, sync::Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt as _, stream::BoxStream};
use icechunk_format::writer_features::WriterFeature;
use icechunk_storage::{other_error, sealed};
use serde::{Deserialize, Serialize};

use super::{
    DeleteObjectsResult, GcPhase, GetModifiedResult, ListInfo, PutCondition, Settings,
    Storage, StorageContext, StorageError, StorageInfo, StorageResult,
    StorageWriterFeatures, VersionInfo, VersionedUpdateResult, WriteHint,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Call {
    WriterFeatures,
    GcHookBeforeDeletes { changed: Option<VersionInfo> },
    GcHookAfterDeletes { changed: Option<VersionInfo> },
    CreateLocationIfNeeded,
    RootIsClean,
    SupportsIc1Repos,
    ListsIdPrefixesNatively,
}

#[derive(Debug, PartialEq, Eq)]
enum Answer {
    WriterFeatures(StorageWriterFeatures),
    Unit,
    Bool(bool),
}

/// Answers every recorded method with the opposite of the trait's default
/// (or, for `root_is_clean`, of what the default computes from its empty
/// listing), so a wrapper that doesn't forward a method gives itself away by
/// its answer as well as by the missing call.
#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct RecordingStorage {
    #[serde(skip)]
    calls: Mutex<Vec<Call>>,
}

impl RecordingStorage {
    pub(crate) fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    fn record(&self, call: Call) {
        self.calls.lock().unwrap_or_else(|p| p.into_inner()).push(call);
    }

    fn clear(&self) {
        self.calls.lock().unwrap_or_else(|p| p.into_inner()).clear();
    }

    fn features() -> StorageWriterFeatures {
        StorageWriterFeatures::new([WriterFeature::FilesystemLayoutV1], [])
    }
}

fn not_recorded<T>() -> StorageResult<T> {
    Err(other_error("RecordingStorage records only the forwarded methods"))
}

impl fmt::Display for RecordingStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordingStorage")
    }
}

impl sealed::Sealed for RecordingStorage {}

#[async_trait]
#[typetag::serde]
impl Storage for RecordingStorage {
    fn storage_info(&self) -> StorageInfo {
        StorageInfo { backend_type: "recording", fields: vec![] }
    }

    async fn can_write(&self) -> StorageResult<bool> {
        Ok(true)
    }

    async fn writer_features(&self) -> StorageResult<StorageWriterFeatures> {
        self.record(Call::WriterFeatures);
        Ok(Self::features())
    }

    async fn gc_hook(
        &self,
        _ctx: &StorageContext<'_>,
        phase: GcPhase<'_>,
    ) -> StorageResult<()> {
        self.record(match phase {
            GcPhase::BeforeDeletes { changed } => {
                Call::GcHookBeforeDeletes { changed: changed.cloned() }
            }
            GcPhase::AfterDeletes { changed } => {
                Call::GcHookAfterDeletes { changed: changed.cloned() }
            }
        });
        Ok(())
    }

    async fn create_location_if_needed(&self) -> StorageResult<()> {
        self.record(Call::CreateLocationIfNeeded);
        Ok(())
    }

    async fn root_is_clean(&self, _ctx: &StorageContext<'_>) -> StorageResult<bool> {
        self.record(Call::RootIsClean);
        Ok(false)
    }

    fn supports_ic1_repos(&self) -> bool {
        self.record(Call::SupportsIc1Repos);
        false
    }

    fn lists_id_prefixes_natively(&self) -> bool {
        self.record(Call::ListsIdPrefixesNatively);
        true
    }

    async fn get_object_range(
        &self,
        _ctx: &StorageContext<'_>,
        _path: &str,
        _range: Option<&Range<u64>>,
    ) -> StorageResult<Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>>
    {
        not_recorded()
    }

    async fn put_object(
        &self,
        _ctx: &StorageContext<'_>,
        _path: &str,
        _bytes: Bytes,
        _content_type: Option<&str>,
        _metadata: Vec<(String, String)>,
        _hint: WriteHint,
    ) -> StorageResult<()> {
        not_recorded()
    }

    async fn put_object_conditional(
        &self,
        _ctx: &StorageContext<'_>,
        _path: &str,
        _bytes: Bytes,
        _content_type: Option<&str>,
        _metadata: Vec<(String, String)>,
        _condition: PutCondition<'_>,
    ) -> StorageResult<VersionedUpdateResult> {
        not_recorded()
    }

    async fn copy_object(
        &self,
        _ctx: &StorageContext<'_>,
        _from: &str,
        _to: &str,
        _content_type: Option<&str>,
        _version: &VersionInfo,
    ) -> StorageResult<VersionedUpdateResult> {
        not_recorded()
    }

    async fn list_objects<'a>(
        &'a self,
        _ctx: &StorageContext<'_>,
        _prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        Ok(futures::stream::empty().boxed())
    }

    async fn delete_batch(
        &self,
        _ctx: &StorageContext<'_>,
        _prefix: &str,
        _batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        not_recorded()
    }

    async fn get_object_last_modified(
        &self,
        _ctx: &StorageContext<'_>,
        _path: &str,
    ) -> StorageResult<DateTime<Utc>> {
        not_recorded()
    }

    async fn get_object_conditional(
        &self,
        _ctx: &StorageContext<'_>,
        _path: &str,
        _previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
        not_recorded()
    }
}

async fn ask(
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    call: &Call,
) -> Answer {
    match call {
        Call::WriterFeatures => Answer::WriterFeatures(
            storage.writer_features().await.expect("writer_features failed"),
        ),
        Call::GcHookBeforeDeletes { changed } => {
            let phase = GcPhase::BeforeDeletes { changed: changed.as_ref() };
            storage.gc_hook(ctx, phase).await.expect("gc_hook failed");
            Answer::Unit
        }
        Call::GcHookAfterDeletes { changed } => {
            let phase = GcPhase::AfterDeletes { changed: changed.as_ref() };
            storage.gc_hook(ctx, phase).await.expect("gc_hook failed");
            Answer::Unit
        }
        Call::CreateLocationIfNeeded => {
            storage
                .create_location_if_needed()
                .await
                .expect("create_location_if_needed failed");
            Answer::Unit
        }
        Call::RootIsClean => {
            Answer::Bool(storage.root_is_clean(ctx).await.expect("root_is_clean failed"))
        }
        Call::SupportsIc1Repos => Answer::Bool(storage.supports_ic1_repos()),
        Call::ListsIdPrefixesNatively => {
            Answer::Bool(storage.lists_id_prefixes_natively())
        }
    }
}

/// Calls each forwarded method on `wrapper` and checks that `inner` received
/// exactly that call and that `wrapper` returned `inner`'s answer.
pub(crate) async fn assert_forwards(
    wrapper: &(dyn Storage + Send + Sync),
    inner: &RecordingStorage,
) {
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    let changed = Some(VersionInfo::from_etag_only("changed".to_string()));
    let table = [
        (Call::WriterFeatures, Answer::WriterFeatures(RecordingStorage::features())),
        (Call::GcHookBeforeDeletes { changed: changed.clone() }, Answer::Unit),
        (Call::GcHookBeforeDeletes { changed: None }, Answer::Unit),
        (Call::GcHookAfterDeletes { changed }, Answer::Unit),
        (Call::GcHookAfterDeletes { changed: None }, Answer::Unit),
        (Call::CreateLocationIfNeeded, Answer::Unit),
        (Call::RootIsClean, Answer::Bool(false)),
        (Call::SupportsIc1Repos, Answer::Bool(false)),
        (Call::ListsIdPrefixesNatively, Answer::Bool(true)),
    ];
    for (call, expected) in table {
        inner.clear();
        assert_eq!(ask(wrapper, &ctx, &call).await, expected, "{wrapper}: {call:?}");
        assert_eq!(inner.calls(), vec![call.clone()], "{wrapper}: {call:?}");
    }
}
