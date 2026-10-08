use std::{collections::HashSet, error::Error, path::PathBuf, sync::Arc};

use bytes::Bytes;
use futures::TryStreamExt as _;
use icechunk::{
    Repository,
    conflicts::basic_solver::BasicConflictSolver,
    format::{
        ChunkIndices, Path, SnapshotId, manifest::ChunkPayload, snapshot::ArrayShape,
    },
    repository::VersionInfo,
    storage::new_filesystem_storage,
};
use icechunk_macros::tokio_test;
use pretty_assertions::assert_eq;
use tokio::{sync::Barrier, task::JoinSet};

const WRITERS: u32 = 16;
const COMMITS_PER_WRITER: u32 = 20;

/// Each writer opens its own `Repository` on the same directory, so the only
/// thing serializing their commits is the storage's conditional update of
/// `repo`. In every round all writers start from the same tip and commit at
/// once, so all but one must rebase.
#[tokio_test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_commits_from_independent_repositories()
-> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().to_path_buf();
    let initial = Repository::create(new_filesystem_storage(&root).await?)
        .execute()
        .await?
        .lookup_branch("main")
        .await?;

    let barrier = Arc::new(Barrier::new(WRITERS as usize));
    let mut set = JoinSet::new();
    for writer in 0..WRITERS {
        let root = root.clone();
        let barrier = Arc::clone(&barrier);
        set.spawn(async move { make_commits(root, writer, &barrier).await });
    }
    let mut acknowledged = HashSet::new();
    while let Some(res) = set.join_next().await {
        acknowledged.extend(res?.map_err(|e| e as Box<dyn Error>)?);
    }
    assert_eq!(acknowledged.len(), (WRITERS * COMMITS_PER_WRITER) as usize);

    let repo = Repository::open(new_filesystem_storage(&root).await?).execute().await?;
    let ancestry: Vec<SnapshotId> = repo
        .ancestry(&VersionInfo::BranchTipRef("main".to_string()))
        .await?
        .map_ok(|info| info.id)
        .try_collect()
        .await?;
    assert_eq!(ancestry.len(), (WRITERS * COMMITS_PER_WRITER + 1) as usize);
    assert_eq!(ancestry.last(), Some(&initial));
    let in_ancestry: HashSet<_> = ancestry.into_iter().collect();
    let missing: Vec<_> = acknowledged.difference(&in_ancestry).collect();
    assert_eq!(missing, Vec::<&SnapshotId>::new());

    let tip =
        repo.readonly_session(&VersionInfo::BranchTipRef("main".to_string())).await?;
    for writer in 0..WRITERS {
        let path: Path = format!("/writer_{writer}").try_into()?;
        for commit in 0..COMMITS_PER_WRITER {
            let chunk = tip.get_chunk_ref(&path, &ChunkIndices(vec![commit])).await?;
            assert_eq!(chunk, Some(payload(writer, commit)), "{path} chunk {commit}");
        }
    }

    let names = std::fs::read_dir(&root)?
        .map(|entry| entry.map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect::<Result<Vec<_>, _>>()?;
    let leftovers: Vec<String> = names
        .into_iter()
        .filter(|name| {
            name.starts_with("repo.new_bytes.") || name.starts_with("repo.lockinfo.")
        })
        .collect();
    assert_eq!(leftovers, Vec::<String>::new());

    Ok(())
}

fn payload(writer: u32, commit: u32) -> ChunkPayload {
    ChunkPayload::Inline(Bytes::from(format!("{writer}-{commit}")))
}

async fn make_commits(
    root: PathBuf,
    writer: u32,
    barrier: &Barrier,
) -> Result<Vec<SnapshotId>, Box<dyn Error + Send + Sync>> {
    let repo = Repository::open(new_filesystem_storage(&root).await?).execute().await?;
    let path: Path = format!("/writer_{writer}").try_into()?;
    let solver = BasicConflictSolver::default();
    let mut snapshots = Vec::new();
    for commit in 0..COMMITS_PER_WRITER {
        let mut session = repo.writable_session("main").await?;
        if commit == 0 {
            let shape =
                ArrayShape::new(vec![(COMMITS_PER_WRITER as u64, COMMITS_PER_WRITER)])
                    .ok_or("invalid shape")?;
            session.add_array(path.clone(), shape, None, Bytes::new()).await?;
        }
        session
            .set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![commit]),
                Some(payload(writer, commit)),
            )
            .await?;
        // A failing writer returns early and the test aborts the others, so
        // no one waits here forever.
        barrier.wait().await;
        // Every failed attempt is caused by another writer's commit landing,
        // so this many attempts always suffice.
        let attempts = (WRITERS * COMMITS_PER_WRITER) as u16;
        let snapshot = session
            .commit(format!("writer {writer} commit {commit}"))
            .rebase(&solver, attempts)
            .execute()
            .await?;
        snapshots.push(snapshot);
    }
    Ok(snapshots)
}
