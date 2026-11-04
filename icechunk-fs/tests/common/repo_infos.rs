//! Minimal repo-info files for the tests of the `repo.d` protocol, shared by
//! the unit tests and the integration tests: the steps that need the meaning
//! of a repo info decode it.

use chrono::{DateTime, Utc};
use icechunk_format::{
    SnapshotId,
    format_constants::SpecVersionBin,
    layout_v1::{FeatureChange, convert},
    repo_info::{DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE, RepoInfo, UpdateType},
    repo_info_file,
    snapshot::SnapshotInfo,
    writer_features::WriterFeature,
};

/// A first version listing `filesystem-layout-v1`, with different bytes for
/// every `seq`.
pub(crate) fn repo_info(seq: u32) -> Vec<u8> {
    minimal(seq, &[WriterFeature::FilesystemLayoutV1])
}

/// Like [`repo_info`] without writer features: what an implementation that
/// doesn't implement the layout writes (a fork), or the last version of a
/// conversion to the object store layout.
pub(crate) fn featureless_repo_info(seq: u32) -> Vec<u8> {
    minimal(seq, &[])
}

/// Like [`repo_info`] with a change after the creation: what an orphan mirror
/// holds, unlike an interrupted creation.
pub(crate) fn changed_repo_info(seq: u32) -> Vec<u8> {
    let changed_at = at(seq) + chrono::TimeDelta::seconds(1);
    convert(
        &featureless_repo_info(seq),
        FeatureChange::Add,
        "overwritten/changed",
        changed_at,
        DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE,
    )
    .unwrap()
}

/// The repo info after a commit on top of `bytes` by the attempt `attempt`:
/// like core, it records the attempt's backup path on the entry that was
/// newest before it. `seq` makes the snapshot and the time unique and orders
/// the commits; the ops log is paged at `per_file` entries.
pub(crate) fn committed_repo_info(
    bytes: &[u8],
    attempt: &str,
    seq: u32,
    per_file: u16,
) -> Vec<u8> {
    let info = repo_info_file::decode(bytes).unwrap();
    let mut id = [1; 12];
    id[..4].copy_from_slice(&seq.to_be_bytes());
    let id = SnapshotId::new(id);
    let time = at(1_000_000 + seq);
    let snapshot = SnapshotInfo {
        id: id.clone(),
        parent_id: Some(SnapshotId::new([0; 12])),
        flushed_at: time,
        message: format!("commit {seq}"),
        metadata: Default::default(),
        pruned_ancestor_tx_logs: vec![],
    };
    let committed = info
        .add_snapshot(
            SpecVersionBin::current(),
            snapshot,
            Some("main"),
            UpdateType::NewCommitUpdate { branch: "main".to_string(), new_snap_id: id },
            Some(time),
            attempt,
            per_file,
        )
        .unwrap();
    repo_info_file::encode(&committed, SpecVersionBin::current(), 3).unwrap()
}

fn minimal(seq: u32, features: &[WriterFeature]) -> Vec<u8> {
    let time = at(seq);
    let info = RepoInfo::initial(
        SpecVersionBin::current(),
        SnapshotInfo {
            id: SnapshotId::new([0; 12]),
            parent_id: None,
            flushed_at: time,
            message: "initial".to_string(),
            metadata: Default::default(),
            pruned_ancestor_tx_logs: vec![],
        },
        DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE,
        None::<&()>,
        Some(time),
        features,
    );
    repo_info_file::encode(&info, SpecVersionBin::current(), 3).unwrap()
}

fn at(seq: u32) -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000 + i64::from(seq), 0).unwrap()
}
