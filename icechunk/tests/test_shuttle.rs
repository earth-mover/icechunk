// Shuttle concurrency test for icechunk
//
// Only compiles when the `shuttle` feature is active:
//   cargo test -p icechunk --features shuttle --test test_shuttle -- --nocapture

// TODO: Make repo_info_num_updates tiny?
// TODO: assert that ops log contains Action
// 

#![cfg(feature = "shuttle")]

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use icechunk::format::manifest::ChunkPayload;
use icechunk::format::repo_info::UpdateType;
use icechunk::format::snapshot::ArrayShape;
use icechunk::format::{ChunkIndices, Path, SnapshotId};
use icechunk::repository::VersionInfo;
use icechunk::{Repository, new_in_memory_storage};
use proptest::collection::vec;
use proptest::prelude::*;
use shuttle::future::{block_on, spawn};
use shuttle::{Config, Runner, scheduler};
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;

fn assert_ops_log_invariants(log: &[(DateTime<Utc>, UpdateType, Option<String>)]) {
    // timestamps must be strictly decreasing (most recent first)
    log.windows(2).for_each(|window| {
        let (time_a, _, _) = &window[0];
        let (time_b, _, _) = &window[1];
        assert!(
            time_a > time_b,
            "ops log timestamps must be strictly decreasing: {time_a} should be > {time_b}"
        );
    });

    // all non-None backup paths must be unique
    let backup_paths: Vec<_> =
        log.iter().filter_map(|(_, _, path)| path.as_ref()).collect();
    let unique: HashSet<_> = backup_paths.iter().collect();
    assert_eq!(
        backup_paths.len(),
        unique.len(),
        "ops log backup paths must be unique, got duplicates in: {backup_paths:?}"
    );

    // last entry (earliest) should always be RepoInitializedUpdate
    if let Some((_, update_type, _)) = log.last() {
        assert!(
            matches!(update_type, UpdateType::RepoInitializedUpdate),
            "last ops log entry (earliest) should be RepoInitializedUpdate, got: {update_type:?}"
        );
    } else {
        unreachable!();
    }
}

/// Like `shuttle::check_random` but with 8MB stack to handle
/// icechunk's commit path (zstd + flatbuffers serialization).
fn check_random(f: impl Fn() + Send + Sync + 'static, iterations: usize) {
    let mut config = Config::default();
    config.stack_size = 0x80_0000; // 8MB
    let scheduler = scheduler::RandomScheduler::new(iterations);
    Runner::new(scheduler, config).run(f);
}

async fn mk_commit(
    repo: Arc<Repository>,
    path: Path,
    branch: &str,
    c: u32,
) -> Result<SnapshotId, Box<dyn Error + Send + Sync>> {
    let mut session = repo.writable_session(branch).await?;
    session
        .set_chunk_ref(
            path,
            ChunkIndices(vec![c]),
            Some(ChunkPayload::Inline("foo".into())),
        )
        .await?;
    Ok(session
        .commit_rebasing(
            &icechunk::conflicts::detector::ConflictDetector,
            3,
            "write chunk",
            None,
            |_| async {},
            |_| async {},
        )
        .await?)
}

async fn mk_concurrent_commits_same_branch() -> Result<(), Box<dyn Error + Send + Sync>> {
    let storage = new_in_memory_storage().await?;
    let repo = Arc::new(
        Repository::create(None, storage, Default::default(), None, false).await?,
    );

    let mut session = repo.writable_session("main").await?;
    let shape = ArrayShape::new(vec![(10, 1)]).unwrap();
    let path: Path = "/array".try_into()?;
    session.add_array(path.clone(), shape, None, Bytes::new()).await?;

    let mut snaps = vec![];
    snaps.push(session.commit("init array", None).await?);

    repo.create_branch("feature", &snaps[0]).await?;

    // for c in 0..3u32 {
    //     snaps.push(mk_commit(repo.clone(), path.clone(), c).await?);
    // }

    // repo.create_branch("feature", &snaps[2]).await?;

    // eprintln!("starting new run");

    let repo1 = repo.clone();
    let path1 = path.clone();
    let handle1 = spawn(async move {
        mk_commit(repo1, path1, "main", 2).await
        // repo1
        //     .diff(
        //         &VersionInfo::BranchTipRef("main".into()),
        //         &VersionInfo::BranchTipRef("feature".into()),
        //     )
        //     .await
    });

    let repo2 = repo.clone();
    let path2 = path.clone();
    let handle2 = spawn(async move {
        mk_commit(repo2, path2, "feature", 1).await
        // repo2.reset_branch("main", &snaps[1], None).await
    });

    handle1.await??;
    handle2.await??;

    let (stream, _, _) = repo.ops_log().await?;
    let log: Vec<_> = stream.try_collect().await?;
    assert_ops_log_invariants(&log);

    Ok(())
}

#[test]
fn concurrent_commits_same_branch() {
    check_random(
        || {
            block_on(mk_concurrent_commits_same_branch()).unwrap();
        },
        100,
    );
}

// ====

#[derive(Debug, Clone)]
enum Action {
    Commit,
    AddBranch,
    DeleteBranch,
    AddTag,
    DeleteTag,
    Amend,
    ResetBranch,
}

#[derive(Debug, Clone)]
enum ActionResult {
    Commit(String, SnapshotId),
    AddBranch(String, SnapshotId),
    DeleteBranch(String),
    AddTag(String, SnapshotId),
    DeleteTag(String),
    Amend(String, SnapshotId),
    ResetBranch(String, SnapshotId),
}

fn actions(
    range: impl Into<proptest::collection::SizeRange>,
) -> impl Strategy<Value = Vec<Action>> {
    use Action::*;
    vec(
        proptest::sample::select(vec![
            Commit,
            AddBranch,
            DeleteBranch,
            AddTag,
            DeleteTag,
            Amend,
            ResetBranch,
        ]),
        range,
    )
}

async fn setup_branches(
    repo: Arc<Repository>,
    actions: &[Action],
    branches: &[String],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let path: Path = "/array".try_into()?;
    for (action, branch) in actions.iter().zip(branches.iter()) {
        match action {
            Action::AddBranch => {}
            Action::Amend => {
                repo.create_branch(branch, &repo.lookup_branch("main").await?).await?;
                mk_commit(repo.clone(), path.clone(), branch, 3).await?;
            }
            Action::ResetBranch => {
                repo.create_branch(branch, &repo.lookup_branch("main").await?).await?;
                mk_commit(repo.clone(), path.clone(), branch, 2).await?;
                mk_commit(repo.clone(), path.clone(), branch, 3).await?;
            }
            _ => {
                repo.create_branch(branch, &repo.lookup_branch("main").await?).await?;
                repo.create_tag(
                    &format!("tag-to-delete-{branch}"),
                    &repo.lookup_branch("main").await?,
                )
                .await?;
            }
        }
    }
    Ok(())
}

async fn execute_action(
    repo: Arc<Repository>,
    action: Action,
    branch: String,
) -> Result<ActionResult, Box<dyn Error + Send + Sync>> {
    use Action::*;

    let res = match action {
        Commit => {
            let snap = mk_commit(repo, "/array".try_into()?, &branch, 0).await?;
            ActionResult::Commit(branch, snap)
        }
        AddBranch => {
            let snap = repo.lookup_branch("main").await?;
            repo.create_branch(&branch, &snap).await?;
            ActionResult::AddBranch(branch, snap)
        }
        DeleteBranch => {
            repo.delete_branch(&branch).await?;
            ActionResult::DeleteBranch(branch)
        }
        AddTag => {
            let snap = repo.lookup_branch(&branch).await?;
            // stick `branch` in to avoid conflicts
            let tag = format!("tag-to-create-{branch}");
            repo.create_tag(&tag, &snap).await?;
            ActionResult::AddTag(tag, snap)
        }
        DeleteTag => {
            // stick `branch` in to avoid conflicts
            let tag = format!("tag-to-delete-{branch}");
            repo.delete_tag(&tag).await?;
            ActionResult::DeleteTag(tag)
        }
        Amend => {
            let mut session = repo.writable_session(&branch).await?;
            session
                .set_chunk_ref(
                    "/array".try_into()?,
                    ChunkIndices(vec![4]),
                    Some(ChunkPayload::Inline("amend".into())),
                )
                .await?;
            let snap = session.amend("amend commit", None, false).await?;
            ActionResult::Amend(branch, snap)
        }
        ResetBranch => {
            let snap = repo.lookup_branch("main").await?;
            repo.reset_branch(&branch, &snap, None).await?;
            ActionResult::ResetBranch(branch, snap)
        }
    };

    Ok(res)
}

async fn assert_action_postcondition(
    repo: Arc<Repository>,
    action: ActionResult,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    use ActionResult::*;
    match action {
        Commit(branch, snap) => {
            let anc: HashSet<SnapshotId> = repo
                .ancestry(&VersionInfo::BranchTipRef(branch.clone()))
                .await?
                .map_ok(|info| info.id)
                .try_collect()
                .await?;
            assert!(
                anc.contains(&snap),
                "commit {snap:?} not found in ancestry of branch {branch:?}"
            );
        }
        AddBranch(branch, snap) => {
            assert!(repo.list_branches().await?.contains(&branch));
            assert_eq!(repo.lookup_branch(&branch).await?, snap);
        }
        DeleteBranch(branch) => {
            assert!(!repo.list_branches().await?.contains(&branch));
        }
        AddTag(tag, snap) => {
            assert!(repo.list_tags().await?.contains(&tag));
            assert_eq!(repo.lookup_tag(&tag).await?, snap);
        }
        DeleteTag(tag) => {
            assert!(!repo.list_tags().await?.contains(&tag));
        }
        Amend(branch, snap) => {
            let tip = repo.lookup_branch(&branch).await?;
            assert_eq!(tip, snap, "amend snapshot should be branch tip for {branch}");
        }
        ResetBranch(branch, snap) => {
            let tip = repo.lookup_branch(&branch).await?;
            assert_eq!(tip, snap, "branch {branch} should be reset to {snap:?}");
        }
    };
    Ok(())
}

async fn execute_concurrent_actions(
    actions: Vec<Action>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let branches: Vec<String> =
        (0..actions.len()).map(|i| format!("branch-{i}")).collect();
    let storage = new_in_memory_storage().await?;
    let repo = Arc::new(
        Repository::create(None, storage, Default::default(), None, false).await?,
    );

    let mut session = repo.writable_session("main").await?;
    let shape = ArrayShape::new(vec![(10, 1)]).unwrap();
    let path: Path = "/array".try_into()?;
    session.add_array(path.clone(), shape, None, Bytes::new()).await?;
    session.commit("foo", None).await?;

    setup_branches(repo.clone(), &actions, &branches).await?;

    let (stream, _, _) = repo.ops_log().await?;
    let ops_count_before = stream.try_collect::<Vec<_>>().await?.len();

    let handles = actions
        .iter()
        .zip(branches.iter())
        .map(|(action, branch)| {
            spawn({
                let repo = repo.clone();
                let branch = branch.clone();
                let action = action.clone();
                async move { execute_action(repo, action, branch).await }
            })
        })
        .collect::<Vec<_>>();

    let mut results = vec![];
    for handle in handles {
        results.push(handle.await??);
    }

    let (stream, _, _) = repo.ops_log().await?;
    let log: Vec<_> = stream.try_collect().await?;

    assert_eq!(log.len() - ops_count_before, actions.len());
    assert_ops_log_invariants(&log);

    for r in results {
        assert_action_postcondition(repo.clone(), r).await?;
    }
    Ok(())
}

proptest! {
    #[test]
    fn concurrent_actions(acts in actions(3..=5)) {
        let acts = acts.clone();
        check_random(move || {
            let acts = acts.clone();
            block_on(execute_concurrent_actions(acts)).unwrap();
        }, 100);
    }
}
