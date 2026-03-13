// Shuttle concurrency test for icechunk
//
// Only compiles when the `shuttle` feature is active:
//   cargo test -p icechunk --features shuttle --test test_shuttle -- --nocapture

#![cfg(feature = "shuttle")]

use bytes::Bytes;
use futures::TryStreamExt;
use icechunk::format::manifest::ChunkPayload;
use icechunk::format::snapshot::ArrayShape;
use icechunk::format::{ChunkIndices, Path, SnapshotId};
use icechunk::repository::VersionInfo;
use icechunk::{Repository, initialize_tracing, new_in_memory_storage};
use itertools::Itertools;
use shuttle::future::{block_on, spawn};
use shuttle::{Config, Runner, scheduler};
use std::sync::Arc;
use tracing::trace;

/// Like `shuttle::check_random` but with 8MB stack to handle
/// icechunk's commit path (zstd + flatbuffers serialization).
fn check_random(f: impl Fn() + Send + Sync + 'static, iterations: usize) {
    let mut config = Config::default();
    config.stack_size = 0x80_0000; // 8MB
    let scheduler = scheduler::RandomScheduler::new(iterations);
    Runner::new(scheduler, config).run(f);
}

async fn mk_concurrent_commits_same_branch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // TODO: setup repo + array
    // TODO: spawn 2 actors that each commit
    // TODO: assert results
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

    async fn mk_commit(
        repo: Arc<Repository>,
        path: Path,
        branch: &str,
        c: u32,
    ) -> Result<SnapshotId, Box<dyn std::error::Error + Send + Sync>> {
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

    // for c in 0..3u32 {
    //     snaps.push(mk_commit(repo.clone(), path.clone(), c).await?);
    // }

    // repo.create_branch("feature", &snaps[2]).await?;

    eprintln!("starting new run");
    initialize_tracing(Some("icechunk=trace"));

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

    log.windows(2).for_each(|window|  {
        let (time_a, _, _) = &window[0];
        let (time_b, _, _) = &window[1];
        assert!(
            time_a > time_b,
            "ops log timestamps must be strictly decreasing: {time_a} should be > {time_b}"
        );
    });

    initialize_tracing(None);

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
