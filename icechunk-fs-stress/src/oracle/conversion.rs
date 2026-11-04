//! The conversion scenario's rules, on the files a conversion leaves: it
//! changes only the writer features, the gate and the ops log (C2), a
//! finished one leaves the target layout and nothing else (C3), nothing
//! overwrites the converted `$ROOT/repo` afterwards (C5),
//! `repo.d` is deleted only once `$ROOT/repo` holds the conversion's version
//! without the feature (C6), and a writer during an interrupted conversion to
//! the object store layout fails with the error naming it (C7). C1 is the
//! repository oracle over every phase's logs, C4 rule I1 on the first version
//! a conversion from the object store layout builds.

use std::{fs, io, path::Path};

use anyhow::Context as _;
use icechunk::format::{
    OVERWRITTEN_FILES_PATH, layout_v1,
    repo_info::{RepoInfo, UpdateType},
    repo_info_file,
};

use crate::{
    log::{Direction, OpResult},
    storage_mode::REPO_KEY,
};

const REPO_D: &str = "repo.d";
const TMP_PREFIX: &str = "repo.d.tmp.";
const MARKER_PREFIX: &str = "latest.";

/// The error of every refused change while a conversion to the object store
/// layout is interrupted names this rerun.
pub const TO_OBJECT_STORE_RERUN: &str = "convert_to_object_store_layout()";

fn decode(bytes: &[u8], what: &str) -> Result<RepoInfo, String> {
    repo_info_file::decode(bytes)
        .map_err(|err| format!("{what} doesn't decode: {}", err.kind()))
}

fn read(path: &Path) -> anyhow::Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("reading {}", path.display())),
    }
}

fn exists(path: &Path) -> anyhow::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err).with_context(|| format!("checking {}", path.display())),
    }
}

/// The `repo.d.tmp.*` entries of the storage root.
fn tmp_dirs(storage_root: &Path) -> anyhow::Result<Vec<String>> {
    let mut names = Vec::new();
    for entry in fs::read_dir(storage_root)
        .with_context(|| format!("listing {}", storage_root.display()))?
    {
        let name = entry?.file_name().to_string_lossy().into_owned();
        if name.starts_with(TMP_PREFIX) {
            names.push(name);
        }
    }
    names.sort();
    Ok(names)
}

/// The backup the newest ops-log entry of `info` names on its predecessor:
/// what the change that made `info` backed up.
fn named_backup(bytes: &[u8]) -> Result<String, String> {
    layout_v1::change_info(bytes)
        .map_err(|err| format!("reading its ops log: {}", err.kind()))?
        .attempt
        .map(|attempt| attempt.id)
        .ok_or_else(|| "its ops log names no backup of its predecessor".to_owned())
}

/// C2: the defect of `after`, the repo info a conversion in `direction`
/// wrote, against `before`, the backup its newest ops-log entry names: it
/// must be `before` with only the writer feature added (from the object store
/// layout) or removed with the gate (to it), the same status, and exactly
/// one `RepoStatusChangedUpdate` on top of `before`'s ops log. `Err` only if
/// the backup can't be read.
pub fn conversion_defect(
    direction: Direction,
    after: &[u8],
    storage_root: &Path,
) -> anyhow::Result<Option<String>> {
    let backup = match named_backup(after) {
        Ok(backup) => backup,
        Err(detail) => return Ok(Some(detail)),
    };
    let path = storage_root.join(OVERWRITTEN_FILES_PATH).join(&backup);
    let Some(before) = read(&path)? else {
        return Ok(Some(format!(
            "its backup {OVERWRITTEN_FILES_PATH}/{backup} is missing"
        )));
    };
    Ok(compare(direction, &before, after).err())
}

fn compare(
    direction: Direction,
    before_bytes: &[u8],
    after_bytes: &[u8],
) -> Result<(), String> {
    let before = decode(before_bytes, "the backup")?;
    let after = decode(after_bytes, "the converted repo info")?;
    let field = |what: &str, err: icechunk::format::IcechunkFormatError| {
        format!("reading {what}: {}", err.kind())
    };
    let features = |info: &RepoInfo, which: &str| {
        info.raw_writer_features()
            .map(|names| {
                names.iter().map(|name| name.as_str().to_owned()).collect::<Vec<_>>()
            })
            .map_err(|err| field(&format!("the writer features of {which}"), err))
    };
    let (expected_before, expected_after) = match direction {
        Direction::FromObjectStore => (vec![], vec![layout_v1::FEATURE.to_owned()]),
        Direction::ToObjectStore => (vec![layout_v1::FEATURE.to_owned()], vec![]),
    };
    if features(&before, "the backup")? != expected_before {
        return Err(format!(
            "it backed up a repo info with the writer features {:?}, not {expected_before:?}",
            features(&before, "the backup")?
        ));
    }
    if features(&after, "the converted repo info")? != expected_after {
        return Err(format!(
            "it wrote the writer features {:?}, not {expected_after:?}",
            features(&after, "the converted repo info")?
        ));
    }
    if direction == Direction::ToObjectStore
        && !after
            .header()
            .map_err(|err| field("the header", err))?
            .writer_features()
            .is_empty()
    {
        return Err("it kept the gate".to_owned());
    }
    let status = |info: &RepoInfo| info.status().map_err(|err| field("the status", err));
    if status(&before)? != status(&after)? {
        return Err(format!(
            "it changed the status from {:?} to {:?}",
            status(&before)?,
            status(&after)?
        ));
    }
    macro_rules! same {
        ($what:literal, $get:expr) => {{
            let get = $get;
            let (old, new) = (get(&before), get(&after));
            if old != new {
                return Err(format!("it changed the {}: {old:?} became {new:?}", $what));
            }
        }};
    }
    same!("branches", |info: &RepoInfo| info
        .branches()
        .map(|refs| refs.map(|(name, id)| (name.to_owned(), id)).collect::<Vec<_>>())
        .map_err(|err| err.kind().to_string()));
    same!("tags", |info: &RepoInfo| info
        .tags()
        .map(|refs| refs.map(|(name, id)| (name.to_owned(), id)).collect::<Vec<_>>())
        .map_err(|err| err.kind().to_string()));
    same!("deleted tags", |info: &RepoInfo| info
        .deleted_tags()
        .map(|names| names.map(str::to_owned).collect::<Vec<_>>())
        .map_err(|err| err.kind().to_string()));
    same!("snapshots", |info: &RepoInfo| info
        .all_snapshots()
        .and_then(Iterator::collect::<Result<Vec<_>, _>>)
        .map_err(|err| err.kind().to_string()));
    same!("configuration", |info: &RepoInfo| info
        .config_bytes_raw()
        .map_err(|err| err.kind().to_string()));
    same!("metadata", |info: &RepoInfo| info
        .metadata()
        .map_err(|err| err.kind().to_string()));
    same!("enabled feature flags", |info: &RepoInfo| info
        .enabled_feature_flags()
        .map(|flags| flags.map(Iterator::collect::<Vec<_>>))
        .map_err(|err| err.kind().to_string()));
    same!("disabled feature flags", |info: &RepoInfo| info
        .disabled_feature_flags()
        .map(|flags| flags.map(Iterator::collect::<Vec<_>>))
        .map_err(|err| err.kind().to_string()));

    let updates = |info: &RepoInfo, which: &str| {
        info.latest_updates()
            .and_then(|updates| {
                updates
                    .map(|update| update.map(|(kind, time, _)| (kind, time)))
                    .collect::<Result<Vec<_>, _>>()
            })
            .map_err(|err| field(&format!("the ops log of {which}"), err))
    };
    let (old, new) =
        (updates(&before, "the backup")?, updates(&after, "the converted repo info")?);
    let Some(((newest, _), rest)) = new.split_first() else {
        return Err("its ops log is empty".to_owned());
    };
    if !matches!(newest, UpdateType::RepoStatusChangedUpdate { .. }) {
        return Err(format!(
            "its newest ops-log entry is {newest:?}, not a status change"
        ));
    }
    // The page may have moved the oldest entries out of this file.
    if old.len() < rest.len() || old[..rest.len()] != *rest {
        return Err(format!(
            "its ops log isn't the backup's with one entry on top: {} entries below the \
             new one, {} in the backup",
            rest.len(),
            old.len()
        ));
    }
    Ok(())
}

/// C3: what keeps the storage root from being in the layout `direction`
/// converts to, and nothing else: to the object store layout, a `$ROOT/repo`
/// without writer features or gate, no `repo.d` and no `repo.d.tmp.*`; from
/// it, `repo.d` with one marker whose version `$ROOT/repo` copies, listing
/// the feature, and no `repo.d.tmp.*`.
pub fn end_state_defect(
    direction: Direction,
    storage_root: &Path,
) -> anyhow::Result<Option<String>> {
    let tmp = tmp_dirs(storage_root)?;
    if !tmp.is_empty() {
        return Ok(Some(format!("it left {}", tmp.join(", "))));
    }
    let Some(mirror) = read(&storage_root.join(REPO_KEY))? else {
        return Ok(Some("there is no repo".to_owned()));
    };
    let repo_d = storage_root.join(REPO_D);
    match direction {
        Direction::ToObjectStore => {
            if exists(&repo_d)? {
                return Ok(Some("repo.d exists".to_owned()));
            }
            let info = match decode(&mirror, "repo") {
                Ok(info) => info,
                Err(detail) => return Ok(Some(detail)),
            };
            let listed = info.raw_writer_features().map(|names| names.is_empty());
            let gated = info.header().map(|header| !header.writer_features().is_empty());
            Ok(match (listed, gated) {
                (Ok(true), Ok(false)) => None,
                (Ok(_), Ok(_)) => {
                    Some("repo lists writer features or carries the gate".to_owned())
                }
                (Err(err), _) | (_, Err(err)) => {
                    Some(format!("reading repo: {}", err.kind()))
                }
            })
        }
        Direction::FromObjectStore => {
            if !exists(&repo_d)? {
                return Ok(Some("there is no repo.d".to_owned()));
            }
            let mut markers = Vec::new();
            for entry in fs::read_dir(&repo_d)
                .with_context(|| format!("listing {}", repo_d.display()))?
            {
                let name = entry?.file_name().to_string_lossy().into_owned();
                if let Some(marker) = name.strip_prefix(MARKER_PREFIX) {
                    markers.push(marker.to_owned());
                }
            }
            let [marker] = &markers[..] else {
                return Ok(Some(format!("repo.d has {} markers", markers.len())));
            };
            // `<K>.<N>.<h>`: the version file is `<N>.<h>`.
            let Some((_, version)) = marker.split_once('.') else {
                return Ok(Some(format!(
                    "repo.d has the malformed marker latest.{marker}"
                )));
            };
            let Some(current) = read(&repo_d.join(version))? else {
                return Ok(Some(format!("repo.d has no version {version}")));
            };
            if current != mirror {
                return Ok(Some(format!(
                    "repo isn't a copy of the current version {version}"
                )));
            }
            Ok(match layout_v1::lists_feature(&mirror) {
                Ok(true) => None,
                Ok(false) => Some(format!("repo doesn't list {}", layout_v1::FEATURE)),
                Err(err) => Some(format!("repo doesn't decode: {}", err.kind())),
            })
        }
    }
}

/// Whether the storage root is as a rerun of the conversion to the object store
/// layout leaves it when its listing of the storage root misses `repo.d`
/// (an accepted limit): `repo.d` is there, its current version, that of its
/// highest marker, lacks the feature, and `$ROOT/repo` is a copy of it.
pub fn left_by_stale_root_listing(storage_root: &Path) -> anyhow::Result<bool> {
    let repo_d = storage_root.join(REPO_D);
    let entries = match fs::read_dir(&repo_d) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(err).with_context(|| format!("listing {}", repo_d.display()));
        }
    };
    let mut current: Option<(u64, String)> = None;
    for entry in entries {
        let name = entry?.file_name().to_string_lossy().into_owned();
        // `latest.<K>.<N>.<h>`: the version file is `<N>.<h>`.
        let version = name
            .strip_prefix(MARKER_PREFIX)
            .and_then(|marker| marker.split_once('.'))
            .map(|(_, version)| version);
        let Some(version) = version else { continue };
        let Some(n) = version.split_once('.').and_then(|(n, _)| n.parse().ok()) else {
            continue;
        };
        if current.as_ref().is_none_or(|(highest, _)| n > *highest) {
            current = Some((n, version.to_owned()));
        }
    }
    let Some((_, version)) = current else { return Ok(false) };
    let (Some(current), Some(mirror)) =
        (read(&repo_d.join(version))?, read(&storage_root.join(REPO_KEY))?)
    else {
        return Ok(false);
    };
    Ok(current == mirror && matches!(layout_v1::lists_feature(&mirror), Ok(false)))
}

/// C5: what shows that something overwrote the repository after a conversion
/// to the object store layout: a `repo.d`, or a `$ROOT/repo` that is missing
/// or lists the feature.
pub fn object_store_overwritten(storage_root: &Path) -> anyhow::Result<Option<String>> {
    if exists(&storage_root.join(REPO_D))? {
        return Ok(Some("repo.d exists".to_owned()));
    }
    let Some(mirror) = read(&storage_root.join(REPO_KEY))? else {
        return Ok(Some("there is no repo".to_owned()));
    };
    Ok(match layout_v1::lists_feature(&mirror) {
        Ok(false) => None,
        Ok(true) => Some(format!("repo lists {}", layout_v1::FEATURE)),
        Err(err) => Some(format!("repo doesn't decode: {}", err.kind())),
    })
}

/// C6, after any run of a conversion to the object store layout, finished or
/// not: once `repo.d` is gone, `$ROOT/repo` must be that conversion's
/// version without the feature: it lists no feature, its newest entry is a
/// status change, and it names a backup of a version with the feature.
pub fn deleted_history(storage_root: &Path) -> anyhow::Result<Option<String>> {
    if exists(&storage_root.join(REPO_D))? {
        return Ok(None);
    }
    let gone = |detail: String| Ok(Some(format!("repo.d is gone, and {detail}")));
    let Some(mirror) = read(&storage_root.join(REPO_KEY))? else {
        return gone("there is no repo".to_owned());
    };
    match layout_v1::lists_feature(&mirror) {
        Ok(false) => {}
        Ok(true) => return gone(format!("repo still lists {}", layout_v1::FEATURE)),
        Err(err) => return gone(format!("repo doesn't decode: {}", err.kind())),
    }
    let backup = match named_backup(&mirror) {
        Ok(backup) => backup,
        Err(detail) => return gone(format!("repo: {detail}")),
    };
    let path = storage_root.join(OVERWRITTEN_FILES_PATH).join(&backup);
    Ok(match read(&path)?.map(|bytes| layout_v1::lists_feature(&bytes)) {
        Some(Ok(true)) => None,
        _ => Some(format!(
            "repo.d is gone, and repo's backup {OVERWRITTEN_FILES_PATH}/{backup} isn't a version \
             listing {}",
            layout_v1::FEATURE
        )),
    })
}

/// C7: what is wrong with the outcome of a writer attempt during an
/// interrupted conversion to the object store layout, which must be refused with
/// the error that names the conversion to rerun.
pub fn writer_defect(result: Option<&OpResult>) -> Option<String> {
    match result {
        Some(OpResult::ConversionInProgress { message })
            if message.contains(TO_OBJECT_STORE_RERUN) =>
        {
            None
        }
        Some(OpResult::ConversionInProgress { message }) => {
            Some(format!("refused without naming {TO_OBJECT_STORE_RERUN}: {message}"))
        }
        Some(result) if result.landed().is_some() => Some("it landed".to_owned()),
        Some(result) => Some(format!("it failed otherwise: {result:?}")),
        None => Some("it logged no outcome".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use icechunk::{Repository, storage::new_local_filesystem_storage};
    use icechunk_fs::FilesystemStorage;

    use super::*;

    /// A repository in the object store layout in `root` with one commit on
    /// `main`.
    async fn object_store_repo(root: &Path) {
        let storage = new_local_filesystem_storage(root).await.unwrap();
        let repo = Repository::create(storage).execute().await.unwrap();
        let mut session = repo.writable_session("main").await.unwrap();
        session
            .add_group(icechunk::format::Path::root(), bytes::Bytes::new())
            .await
            .unwrap();
        session.commit("one").execute().await.unwrap();
    }

    fn mirror(root: &Path) -> Vec<u8> {
        fs::read(root.join(REPO_KEY)).unwrap()
    }

    #[tokio::test]
    async fn real_conversions_pass_c2_c3_c5_and_c6() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        object_store_repo(root).await;
        assert_eq!(object_store_overwritten(root).unwrap(), None);
        let storage = FilesystemStorage::new(root).unwrap();
        storage.convert_to_filesystem_layout().await.unwrap();
        assert_eq!(
            conversion_defect(Direction::FromObjectStore, &mirror(root), root).unwrap(),
            None
        );
        assert_eq!(end_state_defect(Direction::FromObjectStore, root).unwrap(), None);
        assert!(end_state_defect(Direction::ToObjectStore, root).unwrap().is_some());
        assert!(object_store_overwritten(root).unwrap().is_some());
        assert_eq!(deleted_history(root).unwrap(), None);
        assert!(!left_by_stale_root_listing(root).unwrap(), "repo lists the feature");

        let storage: Arc<FilesystemStorage> =
            Arc::new(FilesystemStorage::new(root).unwrap());
        storage.convert_to_object_store_layout().await.unwrap();
        assert_eq!(
            conversion_defect(Direction::ToObjectStore, &mirror(root), root).unwrap(),
            None
        );
        assert_eq!(end_state_defect(Direction::ToObjectStore, root).unwrap(), None);
        assert_eq!(object_store_overwritten(root).unwrap(), None);
        assert_eq!(deleted_history(root).unwrap(), None);
        assert!(!left_by_stale_root_listing(root).unwrap(), "no repo.d");
        // The wrong direction is a mismatch.
        assert!(
            conversion_defect(Direction::FromObjectStore, &mirror(root), root)
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn c2_wants_the_backup_and_nothing_else_changed() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        object_store_repo(root).await;
        FilesystemStorage::new(root)
            .unwrap()
            .convert_to_filesystem_layout()
            .await
            .unwrap();
        let after = mirror(root);
        let backup = named_backup(&after).unwrap();
        let path = root.join(OVERWRITTEN_FILES_PATH).join(&backup);
        let before = fs::read(&path).unwrap();

        // A backup of another repo info: one with another ops log.
        let other = crate::fork::fork_bytes(&before, "repo.9.Z").unwrap();
        fs::write(&path, &other).unwrap();
        let defect = conversion_defect(Direction::FromObjectStore, &after, root).unwrap();
        assert!(
            defect
                .as_deref()
                .is_some_and(|d| d.contains("status") || d.contains("ops log")),
            "{defect:?}"
        );
        fs::remove_file(&path).unwrap();
        let defect = conversion_defect(Direction::FromObjectStore, &after, root).unwrap();
        assert!(defect.as_deref().is_some_and(|d| d.contains("missing")), "{defect:?}");
        fs::write(&path, &before).unwrap();
        assert_eq!(
            conversion_defect(Direction::FromObjectStore, &after, root).unwrap(),
            None
        );
        // A repo info that changed a branch as well.
        let info = repo_info_file::decode(&after).unwrap();
        let spec = info.spec_version().unwrap();
        let tagged = info
            .add_tag(
                spec,
                "extra",
                &icechunk::format::snapshot::Snapshot::INITIAL_SNAPSHOT_ID,
                &backup,
                50,
            )
            .unwrap();
        let tagged = repo_info_file::encode(&tagged, spec, 3).unwrap();
        let defect = compare(Direction::FromObjectStore, &before, &tagged);
        assert!(defect.is_err(), "{defect:?}");
    }

    #[tokio::test]
    async fn c3_wants_the_target_layout_and_nothing_else() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        object_store_repo(root).await;
        FilesystemStorage::new(root)
            .unwrap()
            .convert_to_filesystem_layout()
            .await
            .unwrap();
        #[expect(
            clippy::create_dir,
            reason = "plants a new directory in an existing parent"
        )]
        fs::create_dir(root.join(format!("{TMP_PREFIX}{}", uuid::Uuid::new_v4())))
            .unwrap();
        let defect = end_state_defect(Direction::FromObjectStore, root).unwrap();
        assert!(
            defect.as_deref().is_some_and(|d| d.contains("repo.d.tmp.")),
            "{defect:?}"
        );
        for entry in fs::read_dir(root).unwrap() {
            let path = entry.unwrap().path();
            if path.file_name().unwrap().to_string_lossy().starts_with(TMP_PREFIX) {
                fs::remove_dir(path).unwrap();
            }
        }
        // A mirror that isn't the current version.
        let mirror_bytes = mirror(root);
        let other = crate::fork::fork_bytes(&mirror_bytes, "repo.9.Z").unwrap();
        fs::write(root.join(REPO_KEY), other).unwrap();
        let defect = end_state_defect(Direction::FromObjectStore, root).unwrap();
        assert!(defect.as_deref().is_some_and(|d| d.contains("copy")), "{defect:?}");
    }

    /// The state the sabotage `DeleteDirBeforePublish` leaves: `repo.d` gone
    /// while `$ROOT/repo` still holds the mirror with the feature.
    #[tokio::test]
    async fn c6_c3_and_c5_catch_repo_d_deleted_before_the_publication() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        object_store_repo(root).await;
        FilesystemStorage::new(root)
            .unwrap()
            .convert_to_filesystem_layout()
            .await
            .unwrap();
        fs::remove_dir_all(root.join(REPO_D)).unwrap();
        let defect = deleted_history(root).unwrap();
        assert!(
            defect.as_deref().is_some_and(|d| d.contains("still lists")),
            "{defect:?}"
        );
        assert!(end_state_defect(Direction::ToObjectStore, root).unwrap().is_some());
        assert!(object_store_overwritten(root).unwrap().is_some());
    }

    #[test]
    fn c7_wants_the_rerun_error() {
        let refused = |message: &str| OpResult::ConversionInProgress {
            message: message.to_owned(),
        };
        assert_eq!(
            writer_defect(Some(&refused(
                "rerun convert_to_object_store_layout() to complete it"
            ))),
            None
        );
        for (result, expected) in [
            (Some(refused("in progress")), "without naming"),
            (Some(OpResult::Ack { snapshot: None }), "landed"),
            (Some(OpResult::Conflict), "failed otherwise"),
            (None, "no outcome"),
        ] {
            let defect = writer_defect(result.as_ref());
            assert!(
                defect.as_deref().is_some_and(|d| d.contains(expected)),
                "{result:?}: {defect:?}"
            );
        }
    }
}
