use std::{
    collections::{HashMap, HashSet},
    sync::LazyLock,
};

use icechunk_types::ICResultExt as _;

use crate::{
    change_set::ChangeSet,
    format::{IcechunkFormatErrorKind, IcechunkResult, repo_info::RepoInfo},
    session::CommitMethod,
};

#[derive(Debug, PartialEq, Eq)]
pub struct FeatureFlag {
    id: u16,
    name: &'static str,
    default_enabled: bool,
    setting: Option<bool>,
}

impl FeatureFlag {
    /// Behavior for setting:
    ///   * None means not set by the user
    ///   * Some(true) means enabled
    ///   * Some(false) means disabled
    pub(crate) fn new(
        id: u16,
        name: &'static str,
        default_enabled: bool,
        setting: Option<bool>,
    ) -> Self {
        Self { id, name, default_enabled, setting }
    }

    pub fn id(&self) -> u16 {
        self.id
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn default_enabled(&self) -> bool {
        self.default_enabled
    }

    pub fn default_disabled(&self) -> bool {
        !self.default_enabled()
    }

    pub fn setting(&self) -> Option<bool> {
        self.setting
    }

    pub fn in_default_state(&self) -> bool {
        self.setting.is_none()
    }

    pub fn enabled(&self) -> bool {
        self.setting.unwrap_or(self.default_enabled)
    }
}

// Feature flag ID constants.
pub const COMMIT_FLAG: u16 = 1;
pub const AMEND_FLAG: u16 = 2;
pub const MOVE_NODE_FLAG: u16 = 3;
pub const CREATE_TAG_FLAG: u16 = 4;
pub const DELETE_TAG_FLAG: u16 = 5;
pub const REBASE_FLAG: u16 = 6;
pub const CREATE_NEW_NODES_FLAG: u16 = 7;
pub const DELETE_NODES_FLAG: u16 = 8;
pub const UPDATE_CHUNKS_FLAG: u16 = 9;
pub const UPDATE_ARRAY_METADATA_FLAG: u16 = 10;
pub const UPDATE_GROUP_METADATA_FLAG: u16 = 11;
pub const CREATE_BRANCH_FLAG: u16 = 12;
pub const DELETE_BRANCH_FLAG: u16 = 13;
pub const RESET_BRANCH_FLAG: u16 = 14;
pub const GARBAGE_COLLECTION_FLAG: u16 = 15;
pub const EXPIRATION_FLAG: u16 = 16;
// ID 17 is reserved for upgrade_spec_version. No upgrade path starts from a
// repo that has repo info, so nothing can check it yet.
pub const UPDATE_CONFIG_FLAG: u16 = 18;
pub const SET_DEFAULT_COMMIT_METADATA_FLAG: u16 = 19;
pub const UPDATE_REPOSITORY_METADATA_FLAG: u16 = 20;
pub const REWRITE_MANIFESTS_FLAG: u16 = 21;

/// Query the repo info object and determine if the feature flag is enabled or not.
/// This function takes into account user settings in repo info object and the
/// default state of the given feature flag.
/// If this function returns `true` it means the feature must be enabled, either
/// because it's enabled by default or because the user enabled it by choice.
/// Same is true for `false` return values.
fn feature_flag_enabled(repo_info: &RepoInfo, flag_id: u16) -> IcechunkResult<bool> {
    repo_info
        .feature_flag_enabled(flag_id)?
        .map(Ok)
        .unwrap_or_else(|| find_flag_by_id(flag_id).map(|(_, default)| default))
}

pub fn raise_if_feature_flag_disabled(
    repo_info: &RepoInfo,
    flag_id: u16,
    feature_description: &str,
) -> IcechunkResult<()> {
    if feature_flag_enabled(repo_info, flag_id)? {
        Ok(())
    } else {
        let (name, _) = find_flag_by_id(flag_id)?;
        Err(IcechunkFormatErrorKind::FeatureFlagDisabled {
            feature_description: feature_description.to_string(),
            feature_flag: name.to_string(),
        })
        .capture()
    }
}

/// Flags a commit or flush must honor, in check order. Each entry pairs the
/// flag id with the noun for the error description.
pub(crate) fn commit_required_flags(
    change_set: &ChangeSet,
    commit_method: CommitMethod,
    rewrite_manifests: bool,
    has_default_commit_metadata: bool,
) -> Vec<(u16, &'static str)> {
    let mut flags = vec![(COMMIT_FLAG, "")];
    if commit_method == CommitMethod::Amend {
        flags.push((AMEND_FLAG, "amend"));
    }
    if rewrite_manifests {
        flags.push((REWRITE_MANIFESTS_FLAG, "manifest rewrite"));
    }
    if matches!(change_set, ChangeSet::Rearrange(_)) {
        flags.push((MOVE_NODE_FLAG, "rearrange session"));
    }
    if has_default_commit_metadata {
        flags.push((SET_DEFAULT_COMMIT_METADATA_FLAG, "default commit metadata"));
    }
    if change_set.new_nodes().next().is_some() {
        flags.push((CREATE_NEW_NODES_FLAG, "new nodes"));
    }
    if change_set.deleted_groups().next().is_some()
        || change_set.deleted_arrays().next().is_some()
    {
        flags.push((DELETE_NODES_FLAG, "node delete"));
    }
    if change_set
        .arrays_with_chunk_changes()
        .any(|node| change_set.has_chunk_changes(node))
    {
        flags.push((UPDATE_CHUNKS_FLAG, "chunk update"));
    }
    if change_set.updated_arrays().next().is_some() {
        flags.push((UPDATE_ARRAY_METADATA_FLAG, "array metadata update"));
    }
    if change_set.updated_groups().next().is_some() {
        flags.push((UPDATE_GROUP_METADATA_FLAG, "group metadata update"));
    }
    flags
}

/// Checks every entry of `required_flags`. `verb` is `commit` or `flush`.
pub(crate) fn raise_if_commit_flags_disabled(
    repo_info: &RepoInfo,
    verb: &str,
    required_flags: &[(u16, &'static str)],
) -> IcechunkResult<()> {
    for (flag_id, noun) in required_flags {
        let description =
            if noun.is_empty() { verb.to_string() } else { format!("{verb} {noun}") };
        raise_if_feature_flag_disabled(repo_info, *flag_id, &description)?;
    }
    Ok(())
}

pub fn find_feature_flag_id(flag: &str) -> IcechunkResult<u16> {
    FEATURE_FLAGS
        .get(flag)
        .map(|(id, _)| *id)
        .ok_or_else(|| IcechunkFormatErrorKind::InvalidFeatureFlagName {
            name: flag.to_string(),
        })
        .capture()
}

fn find_flag_by_id(flag_id: u16) -> IcechunkResult<(&'static str, bool)> {
    FEATURE_FLAGS
        .iter()
        .find(|(_, (id, _))| *id == flag_id)
        .map(|(name, (_, default))| (*name, *default))
        .ok_or(IcechunkFormatErrorKind::InvalidFeatureFlagId { id: flag_id })
        .capture()
}

pub(crate) static FEATURE_FLAGS: LazyLock<HashMap<&str, (u16, bool)>> =
    LazyLock::new(|| {
        let res = HashMap::from([
            // (name, (id, default_enabled))
            ("commit", (COMMIT_FLAG, true)),
            ("amend", (AMEND_FLAG, true)),
            ("move_node", (MOVE_NODE_FLAG, true)),
            ("create_tag", (CREATE_TAG_FLAG, true)),
            ("delete_tag", (DELETE_TAG_FLAG, true)),
            ("rebase", (REBASE_FLAG, true)),
            ("create_new_nodes", (CREATE_NEW_NODES_FLAG, true)),
            ("delete_nodes", (DELETE_NODES_FLAG, true)),
            ("update_chunks", (UPDATE_CHUNKS_FLAG, true)),
            ("update_array_metadata", (UPDATE_ARRAY_METADATA_FLAG, true)),
            ("update_group_metadata", (UPDATE_GROUP_METADATA_FLAG, true)),
            ("create_branch", (CREATE_BRANCH_FLAG, true)),
            ("delete_branch", (DELETE_BRANCH_FLAG, true)),
            ("reset_branch", (RESET_BRANCH_FLAG, true)),
            ("garbage_collection", (GARBAGE_COLLECTION_FLAG, true)),
            ("expiration", (EXPIRATION_FLAG, true)),
            ("update_config", (UPDATE_CONFIG_FLAG, true)),
            ("set_default_commit_metadata", (SET_DEFAULT_COMMIT_METADATA_FLAG, true)),
            ("update_repository_metadata", (UPDATE_REPOSITORY_METADATA_FLAG, true)),
            ("rewrite_manifests", (REWRITE_MANIFESTS_FLAG, true)),
        ]);
        //  check we didn't duplicate ids
        debug_assert_eq!(
            res.values().map(|(id, _)| id).collect::<HashSet<_>>().len(),
            res.len()
        );
        res
    });

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use bytes::Bytes;
    use futures::TryStreamExt as _;
    use icechunk_types::Path;

    use crate::{
        Repository, Storage,
        change_set::ChangeSet,
        format::{
            ChunkIndices, IcechunkFormatError, SnapshotId,
            format_constants::SpecVersionBin,
            manifest::ChunkPayload,
            repo_info::UpdateType,
            snapshot::{
                ArrayShape, NodeType, Snapshot, SnapshotInfo, SnapshotProperties,
            },
        },
        new_in_memory_storage,
        repository::{RepositoryError, RepositoryErrorKind},
        session::{CommitMethod, SessionError, SessionErrorKind},
    };

    use super::*;

    async fn new_repo() -> Repository {
        let storage: Arc<dyn Storage + Send + Sync> =
            new_in_memory_storage().await.unwrap();
        Repository::create(None, storage, HashMap::new(), None, true).await.unwrap()
    }

    /// Commits a root group and an array at `/array`. Returns the snapshot id.
    async fn commit_root_and_array(repo: &Repository) -> SnapshotId {
        let mut session = repo.writable_session("main").await.unwrap();
        session.add_group(Path::root(), Bytes::copy_from_slice(b"")).await.unwrap();
        session
            .add_array(
                "/array".try_into().unwrap(),
                ArrayShape::new(vec![(4, 4)]).unwrap(),
                Some(vec!["t".into()]),
                Bytes::from_static(br#"{"this":"array"}"#),
            )
            .await
            .unwrap();
        session.commit("root and array").execute().await.unwrap()
    }

    fn assert_flag_disabled_repo<T: std::fmt::Debug>(
        res: Result<T, RepositoryError>,
        flag: &str,
        description: &str,
    ) {
        match res {
            Err(RepositoryError {
                kind:
                    RepositoryErrorKind::FormatError(
                        IcechunkFormatErrorKind::FeatureFlagDisabled {
                            feature_description,
                            feature_flag,
                        },
                    ),
                ..
            }) => {
                assert_eq!(feature_flag, flag);
                assert_eq!(feature_description, description);
            }
            other => panic!("expected FeatureFlagDisabled({flag}), got {other:?}"),
        }
    }

    /// Accepts both the commit path shape (wrapped in RepositoryError) and the
    /// direct session shape.
    fn assert_flag_disabled_session<T: std::fmt::Debug>(
        res: Result<T, SessionError>,
        flag: &str,
        description: &str,
    ) {
        let (feature_flag, feature_description) = match res {
            Err(SessionError {
                kind:
                    SessionErrorKind::RepositoryError(RepositoryErrorKind::FormatError(
                        IcechunkFormatErrorKind::FeatureFlagDisabled {
                            feature_description,
                            feature_flag,
                        },
                    )),
                ..
            })
            | Err(SessionError {
                kind:
                    SessionErrorKind::FormatError(
                        IcechunkFormatErrorKind::FeatureFlagDisabled {
                            feature_description,
                            feature_flag,
                        },
                    ),
                ..
            }) => (feature_flag, feature_description),
            other => panic!("expected FeatureFlagDisabled({flag}), got {other:?}"),
        };
        assert_eq!(feature_flag, flag);
        assert_eq!(feature_description, description);
    }

    #[tokio::test]
    async fn all_flags_on_new_repo() {
        let storage: Arc<dyn Storage + Send + Sync> =
            new_in_memory_storage().await.unwrap();

        let repo =
            Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
                .await
                .unwrap();

        let all: Vec<_> = repo.feature_flags().await.unwrap().collect();
        assert_eq!(all.len(), FEATURE_FLAGS.len());

        for flag in &all {
            // Every flag should exist in FEATURE_FLAGS
            let (id, default) = FEATURE_FLAGS
                .get(flag.name())
                .unwrap_or_else(|| panic!("Unknown flag: {}", flag.name()));
            assert_eq!(flag.id(), *id);
            assert_eq!(flag.default_enabled(), *default);
            assert!(flag.in_default_state());
            // All current defaults are enabled
            assert!(
                flag.enabled(),
                "Flag {} should be enabled on a fresh repo",
                flag.name()
            );
        }

        assert_eq!(
            repo.enabled_feature_flags().await.unwrap().count(),
            FEATURE_FLAGS.len()
        );
        assert_eq!(repo.disabled_feature_flags().await.unwrap().count(), 0);
    }

    #[test]
    fn set_and_unset_flags_on_repo_info() {
        let initial = Snapshot::initial(SpecVersionBin::current()).unwrap();
        let ri = RepoInfo::initial(
            SpecVersionBin::current(),
            SnapshotInfo::from_snapshot_file(&initial).unwrap(),
            100,
            None::<&()>,
            None,
        );
        assert!(feature_flag_enabled(&ri, MOVE_NODE_FLAG).unwrap());
        assert!(feature_flag_enabled(&ri, CREATE_TAG_FLAG).unwrap());
        assert!(feature_flag_enabled(&ri, DELETE_TAG_FLAG).unwrap());
        assert!(matches!(
            feature_flag_enabled(&ri, 9999),
            Err(IcechunkFormatError { kind: IcechunkFormatErrorKind::InvalidFeatureFlagId { id }, ..}) if id == 9999
        ));

        let ri = ri
            .update_feature_flag(
                SpecVersionBin::current(),
                CREATE_TAG_FLAG,
                Some(false),
                "foo",
                100,
            )
            .unwrap();
        assert!(!feature_flag_enabled(&ri, CREATE_TAG_FLAG).unwrap());
        assert!(feature_flag_enabled(&ri, DELETE_TAG_FLAG).unwrap());
        assert!(feature_flag_enabled(&ri, MOVE_NODE_FLAG).unwrap());

        let ri = ri
            .update_feature_flag(
                SpecVersionBin::current(),
                CREATE_TAG_FLAG,
                None,
                "foo",
                100,
            )
            .unwrap();
        assert!(feature_flag_enabled(&ri, CREATE_TAG_FLAG).unwrap());
        assert!(feature_flag_enabled(&ri, DELETE_TAG_FLAG).unwrap());
        assert!(feature_flag_enabled(&ri, MOVE_NODE_FLAG).unwrap());

        let ri = ri
            .update_feature_flag(
                SpecVersionBin::current(),
                CREATE_TAG_FLAG,
                Some(true),
                "foo",
                100,
            )
            .unwrap();
        assert!(feature_flag_enabled(&ri, CREATE_TAG_FLAG).unwrap());
        assert!(feature_flag_enabled(&ri, DELETE_TAG_FLAG).unwrap());
        assert!(feature_flag_enabled(&ri, MOVE_NODE_FLAG).unwrap());
    }

    #[tokio::test]
    async fn set_and_unset_flags_on_repo() {
        let storage: Arc<dyn Storage + Send + Sync> =
            new_in_memory_storage().await.unwrap();

        let repo =
            Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
                .await
                .unwrap();

        let all = repo.feature_flags().await.unwrap().collect::<Vec<_>>();
        assert_eq!(
            all.iter().find(|f| f.name == "move_node").unwrap().id,
            MOVE_NODE_FLAG
        );
        assert_eq!(
            all.iter().find(|f| f.name == "create_tag").unwrap().id,
            CREATE_TAG_FLAG
        );
        assert_eq!(
            all.iter().find(|f| f.name == "delete_tag").unwrap().id,
            DELETE_TAG_FLAG
        );
        assert_eq!(all, repo.enabled_feature_flags().await.unwrap().collect::<Vec<_>>());
        assert!(repo.disabled_feature_flags().await.unwrap().next().is_none());

        let mut updates = vec![UpdateType::RepoInitializedUpdate];

        // disable create tag explicitly
        repo.set_feature_flag("create_tag", Some(false)).await.unwrap();
        updates.push(UpdateType::FeatureFlagChanged {
            id: CREATE_TAG_FLAG,
            new_value: Some(false),
        });

        assert_eq!(
            repo.disabled_feature_flags().await.unwrap().next().unwrap().name,
            "create_tag"
        );
        assert_eq!(
            repo.enabled_feature_flags().await.unwrap().count(),
            FEATURE_FLAGS.len() - 1
        );
        let all = repo.feature_flags().await.unwrap().collect::<Vec<_>>();
        assert!(!all.iter().find(|f| f.name == "create_tag").unwrap().enabled());

        // enable delete_tag explicitly
        repo.set_feature_flag("delete_tag", Some(true)).await.unwrap();
        updates.push(UpdateType::FeatureFlagChanged {
            id: DELETE_TAG_FLAG,
            new_value: Some(true),
        });

        let all = repo.feature_flags().await.unwrap().collect::<Vec<_>>();
        assert!(all.iter().find(|f| f.name == "delete_tag").unwrap().enabled());
        // create tag is still disabled
        assert_eq!(
            repo.enabled_feature_flags().await.unwrap().count(),
            FEATURE_FLAGS.len() - 1
        );

        // set create_tag to default
        repo.set_feature_flag("create_tag", None).await.unwrap();
        updates.push(UpdateType::FeatureFlagChanged {
            id: CREATE_TAG_FLAG,
            new_value: None,
        });

        assert!(repo.disabled_feature_flags().await.unwrap().next().is_none());
        let all = repo.feature_flags().await.unwrap().collect::<Vec<_>>();
        assert!(all.iter().find(|f| f.name == "create_tag").unwrap().enabled());

        // check ops log
        let ops_log: Vec<_> = repo
            .ops_log()
            .await
            .unwrap()
            .0
            .map_ok(|(_, update, _)| update)
            .try_collect()
            .await
            .unwrap();

        updates.reverse();
        assert_eq!(ops_log, updates);
    }

    #[tokio::test]
    async fn try_tag_ops_without_feature_flag() {
        let storage: Arc<dyn Storage + Send + Sync> =
            new_in_memory_storage().await.unwrap();

        let repo =
            Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
                .await
                .unwrap();

        repo.create_tag("exists", &Snapshot::INITIAL_SNAPSHOT_ID).await.unwrap();

        repo.set_feature_flag("create_tag", Some(false)).await.unwrap();
        repo.set_feature_flag("delete_tag", Some(false)).await.unwrap();
        assert!(matches!(
            repo.create_tag("foo", &Snapshot::INITIAL_SNAPSHOT_ID).await,
            Err(RepositoryError {
                kind: RepositoryErrorKind::FormatError(
                    IcechunkFormatErrorKind::FeatureFlagDisabled {
                        feature_description,
                        feature_flag
                    },
                ),
                ..
            }) if feature_flag == "create_tag" && feature_description == "tag creation"
        ));
        assert!(matches!(
            repo.delete_tag("exists").await,
            Err(RepositoryError {
                kind: RepositoryErrorKind::FormatError(
                    IcechunkFormatErrorKind::FeatureFlagDisabled {
                        feature_description,
                        feature_flag
                    },
                ),
                ..
            }) if feature_flag == "delete_tag" && feature_description == "tag delete"
        ));
    }

    #[tokio::test]
    async fn try_rearrange_session_without_feature_flag() {
        let storage: Arc<dyn Storage + Send + Sync> =
            new_in_memory_storage().await.unwrap();

        let repo =
            Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
                .await
                .unwrap();

        // rearrange session works by default
        let _session = repo.rearrange_session("main").await.unwrap();

        // disable move_node
        repo.set_feature_flag("move_node", Some(false)).await.unwrap();

        assert!(matches!(
            repo.rearrange_session("main").await,
            Err(RepositoryError {
                kind: RepositoryErrorKind::FormatError(
                    IcechunkFormatErrorKind::FeatureFlagDisabled {
                        feature_description,
                        feature_flag
                    },
                ),
                ..
            }) if feature_flag == "move_node" && feature_description == "create rearrange session"
        ));

        // re-enable and confirm it works again
        repo.set_feature_flag("move_node", None).await.unwrap();
        let _session = repo.rearrange_session("main").await.unwrap();
    }

    #[tokio::test]
    async fn try_commit_rearrange_session_after_flag_disabled() {
        let storage: Arc<dyn Storage + Send + Sync> =
            new_in_memory_storage().await.unwrap();

        let repo =
            Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
                .await
                .unwrap();

        // create a group so we have something to move
        let mut session = repo.writable_session("main").await.unwrap();
        session.add_group(Path::root(), Bytes::copy_from_slice(b"")).await.unwrap();
        session
            .add_group("/source".try_into().unwrap(), Bytes::copy_from_slice(b""))
            .await
            .unwrap();
        session.commit("add group").max_concurrent_nodes(8).execute().await.unwrap();

        // create a rearrange session while the flag is enabled
        let mut session = repo.rearrange_session("main").await.unwrap();
        session
            .move_node("/source".try_into().unwrap(), "/dest".try_into().unwrap())
            .await
            .unwrap();

        // disable move_node after the session was created
        repo.set_feature_flag("move_node", Some(false)).await.unwrap();

        // commit should fail
        assert!(matches!(
            session.commit("should fail").max_concurrent_nodes(8).execute().await,
            Err(SessionError {
                kind: SessionErrorKind::RepositoryError(
                    RepositoryErrorKind::FormatError(
                        IcechunkFormatErrorKind::FeatureFlagDisabled {
                            feature_description,
                            feature_flag,
                        },
                    ),
                ),
                ..
            }) if feature_flag == "move_node" && feature_description == "commit rearrange session"
        ));
    }

    #[tokio::test]
    async fn try_flush_rearrange_session_after_flag_disabled() {
        let storage: Arc<dyn Storage + Send + Sync> =
            new_in_memory_storage().await.unwrap();

        let repo =
            Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
                .await
                .unwrap();

        // create a group so we have something to move
        let mut session = repo.writable_session("main").await.unwrap();
        session.add_group(Path::root(), Bytes::copy_from_slice(b"")).await.unwrap();
        session
            .add_group("/source".try_into().unwrap(), Bytes::copy_from_slice(b""))
            .await
            .unwrap();
        session.commit("add group").max_concurrent_nodes(8).execute().await.unwrap();

        // create a rearrange session while the flag is enabled
        let mut session = repo.rearrange_session("main").await.unwrap();
        session
            .move_node("/source".try_into().unwrap(), "/dest".try_into().unwrap())
            .await
            .unwrap();

        // disable move_node after the session was created
        repo.set_feature_flag("move_node", Some(false)).await.unwrap();

        // flush should fail
        assert!(matches!(
            session.commit("should fail").max_concurrent_nodes(8).anonymous().execute().await,
            Err(SessionError {
                kind: SessionErrorKind::RepositoryError(
                    RepositoryErrorKind::FormatError(
                        IcechunkFormatErrorKind::FeatureFlagDisabled {
                            feature_description,
                            feature_flag,
                        },
                    ),
                ),
                ..
            }) if feature_flag == "move_node" && feature_description == "flush rearrange session"
        ));
    }

    #[tokio::test]
    async fn try_fork_session_with_changes_after_flag_disabled() {
        let repo = new_repo().await;

        let mut session = repo.writable_session("main").await.unwrap();
        session.add_group(Path::root(), Bytes::copy_from_slice(b"")).await.unwrap();

        // forking a session with changes flushes an unregistered snapshot
        repo.set_feature_flag("create_new_nodes", Some(false)).await.unwrap();
        assert_flag_disabled_session(
            session.fork().await,
            "create_new_nodes",
            "flush new nodes",
        );

        repo.set_feature_flag("create_new_nodes", None).await.unwrap();
        session.fork().await.unwrap();
    }

    #[tokio::test]
    async fn try_commit_without_commit_flag() {
        let repo = new_repo().await;

        let mut session = repo.writable_session("main").await.unwrap();
        session.add_group(Path::root(), Bytes::copy_from_slice(b"")).await.unwrap();

        repo.set_feature_flag("commit", Some(false)).await.unwrap();
        assert_flag_disabled_session(
            session.commit("blocked").execute().await,
            "commit",
            "commit",
        );

        repo.set_feature_flag("commit", None).await.unwrap();
        session.commit("allowed").execute().await.unwrap();
    }

    #[tokio::test]
    async fn try_flush_without_commit_flag() {
        let repo = new_repo().await;

        let mut session = repo.writable_session("main").await.unwrap();
        session.add_group(Path::root(), Bytes::copy_from_slice(b"")).await.unwrap();

        repo.set_feature_flag("commit", Some(false)).await.unwrap();
        assert_flag_disabled_session(
            session.commit("blocked").anonymous().execute().await,
            "commit",
            "flush",
        );

        repo.set_feature_flag("commit", None).await.unwrap();
        session.commit("allowed").anonymous().execute().await.unwrap();
    }

    #[test]
    fn commit_required_flags_by_change_set() {
        use crate::format::NodeId;

        let ids = |flags: Vec<(u16, &'static str)>| {
            flags.into_iter().map(|(id, _)| id).collect::<Vec<_>>()
        };

        // empty edit change set: only the commit flag
        let empty = ChangeSet::for_edits();
        assert_eq!(
            ids(commit_required_flags(&empty, CommitMethod::NewCommit, false, false)),
            vec![COMMIT_FLAG]
        );
        // amend, rewrite and default metadata add their flags in order
        assert_eq!(
            ids(commit_required_flags(&empty, CommitMethod::Amend, true, true)),
            vec![
                COMMIT_FLAG,
                AMEND_FLAG,
                REWRITE_MANIFESTS_FLAG,
                SET_DEFAULT_COMMIT_METADATA_FLAG
            ]
        );

        // new array
        let mut cs = ChangeSet::for_edits();
        cs.add_array(
            "/a".try_into().unwrap(),
            NodeId::random(),
            crate::change_set::ArrayData {
                shape: ArrayShape::new(vec![(4, 4)]).unwrap(),
                dimension_names: None,
                user_data: Bytes::new(),
            },
        )
        .unwrap();
        assert_eq!(
            ids(commit_required_flags(&cs, CommitMethod::NewCommit, false, false)),
            vec![COMMIT_FLAG, CREATE_NEW_NODES_FLAG]
        );

        // deleted group
        let mut cs = ChangeSet::for_edits();
        cs.delete_group("/g".try_into().unwrap(), &NodeId::random()).unwrap();
        assert_eq!(
            ids(commit_required_flags(&cs, CommitMethod::NewCommit, false, false)),
            vec![COMMIT_FLAG, DELETE_NODES_FLAG]
        );

        // chunk change
        let mut cs = ChangeSet::for_edits();
        cs.set_chunk_ref(
            NodeId::random(),
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline(Bytes::from_static(b"1234"))),
        )
        .unwrap();
        assert_eq!(
            ids(commit_required_flags(&cs, CommitMethod::NewCommit, false, false)),
            vec![COMMIT_FLAG, UPDATE_CHUNKS_FLAG]
        );

        // updated array and updated group
        let mut cs = ChangeSet::for_edits();
        cs.update_array(
            &NodeId::random(),
            &"/a".try_into().unwrap(),
            crate::change_set::ArrayData {
                shape: ArrayShape::new(vec![(4, 4)]).unwrap(),
                dimension_names: None,
                user_data: Bytes::new(),
            },
        )
        .unwrap();
        cs.update_group(&NodeId::random(), &"/g".try_into().unwrap(), Bytes::new())
            .unwrap();
        assert_eq!(
            ids(commit_required_flags(&cs, CommitMethod::NewCommit, false, false)),
            vec![COMMIT_FLAG, UPDATE_ARRAY_METADATA_FLAG, UPDATE_GROUP_METADATA_FLAG]
        );

        // rearrange change set with one move
        let mut cs = ChangeSet::for_rearranging();
        let id = NodeId::random();
        cs.move_node(
            "/from".try_into().unwrap(),
            "/to".try_into().unwrap(),
            std::iter::empty(),
            &id,
            NodeType::Group,
        )
        .unwrap();
        assert_eq!(
            ids(commit_required_flags(&cs, CommitMethod::NewCommit, false, false)),
            vec![COMMIT_FLAG, MOVE_NODE_FLAG]
        );
    }

    #[tokio::test]
    async fn try_amend_without_feature_flag() {
        let repo = new_repo().await;
        commit_root_and_array(&repo).await;

        let mut session = repo.writable_session("main").await.unwrap();
        session
            .add_group("/g1".try_into().unwrap(), Bytes::copy_from_slice(b""))
            .await
            .unwrap();
        session.commit("amend ok").amend().execute().await.unwrap();

        repo.set_feature_flag("amend", Some(false)).await.unwrap();
        let mut session = repo.writable_session("main").await.unwrap();
        session
            .add_group("/g2".try_into().unwrap(), Bytes::copy_from_slice(b""))
            .await
            .unwrap();
        assert_flag_disabled_session(
            session.commit("blocked").amend().execute().await,
            "amend",
            "commit amend",
        );

        repo.set_feature_flag("amend", None).await.unwrap();
        session.commit("amend ok again").amend().execute().await.unwrap();
    }

    #[tokio::test]
    async fn try_rewrite_manifests_without_feature_flag() {
        let repo = new_repo().await;
        commit_root_and_array(&repo).await;

        let mut session = repo.writable_session("main").await.unwrap();
        session.commit("rewrite ok").rewrite_manifests().execute().await.unwrap();

        repo.set_feature_flag("rewrite_manifests", Some(false)).await.unwrap();
        let mut session = repo.writable_session("main").await.unwrap();
        assert_flag_disabled_session(
            session.commit("blocked").rewrite_manifests().execute().await,
            "rewrite_manifests",
            "commit manifest rewrite",
        );

        repo.set_feature_flag("rewrite_manifests", None).await.unwrap();
        session.commit("rewrite ok again").rewrite_manifests().execute().await.unwrap();
    }

    #[tokio::test]
    async fn try_default_commit_metadata_without_feature_flag() {
        let mut repo = new_repo().await;
        repo.set_default_commit_metadata(SnapshotProperties::from([(
            "author".to_string(),
            serde_json::Value::from("test"),
        )]));

        let mut session = repo.writable_session("main").await.unwrap();
        session.add_group(Path::root(), Bytes::copy_from_slice(b"")).await.unwrap();

        repo.set_feature_flag("set_default_commit_metadata", Some(false)).await.unwrap();
        assert_flag_disabled_session(
            session.commit("blocked").execute().await,
            "set_default_commit_metadata",
            "commit default commit metadata",
        );

        repo.set_feature_flag("set_default_commit_metadata", None).await.unwrap();
        session.commit("allowed").execute().await.unwrap();
    }

    #[tokio::test]
    async fn try_create_new_nodes_without_feature_flag() {
        let repo = new_repo().await;

        let mut session = repo.writable_session("main").await.unwrap();
        session.add_group(Path::root(), Bytes::copy_from_slice(b"")).await.unwrap();

        repo.set_feature_flag("create_new_nodes", Some(false)).await.unwrap();
        assert_flag_disabled_session(
            session.commit("blocked").execute().await,
            "create_new_nodes",
            "commit new nodes",
        );

        repo.set_feature_flag("create_new_nodes", None).await.unwrap();
        session.commit("allowed").execute().await.unwrap();
    }

    #[tokio::test]
    async fn try_delete_nodes_without_feature_flag() {
        let repo = new_repo().await;
        commit_root_and_array(&repo).await;

        let mut session = repo.writable_session("main").await.unwrap();
        session.delete_array("/array".try_into().unwrap()).await.unwrap();

        repo.set_feature_flag("delete_nodes", Some(false)).await.unwrap();
        assert_flag_disabled_session(
            session.commit("blocked").execute().await,
            "delete_nodes",
            "commit node delete",
        );

        repo.set_feature_flag("delete_nodes", None).await.unwrap();
        session.commit("allowed").execute().await.unwrap();
    }

    #[tokio::test]
    async fn try_update_chunks_without_feature_flag() {
        let repo = new_repo().await;
        commit_root_and_array(&repo).await;

        let mut session = repo.writable_session("main").await.unwrap();
        session
            .set_chunk_ref(
                "/array".try_into().unwrap(),
                ChunkIndices(vec![0]),
                Some(ChunkPayload::Inline(Bytes::from_static(b"1234"))),
            )
            .await
            .unwrap();

        repo.set_feature_flag("update_chunks", Some(false)).await.unwrap();
        assert_flag_disabled_session(
            session.commit("blocked").execute().await,
            "update_chunks",
            "commit chunk update",
        );

        repo.set_feature_flag("update_chunks", None).await.unwrap();
        session.commit("allowed").execute().await.unwrap();
    }

    #[tokio::test]
    async fn try_update_array_metadata_without_feature_flag() {
        let repo = new_repo().await;
        commit_root_and_array(&repo).await;

        let mut session = repo.writable_session("main").await.unwrap();
        session
            .update_array(
                &"/array".try_into().unwrap(),
                ArrayShape::new(vec![(8, 4)]).unwrap(),
                Some(vec!["t".into()]),
                Bytes::from_static(br#"{"this":"array2"}"#),
            )
            .await
            .unwrap();

        repo.set_feature_flag("update_array_metadata", Some(false)).await.unwrap();
        assert_flag_disabled_session(
            session.commit("blocked").execute().await,
            "update_array_metadata",
            "commit array metadata update",
        );

        repo.set_feature_flag("update_array_metadata", None).await.unwrap();
        session.commit("allowed").execute().await.unwrap();
    }

    #[tokio::test]
    async fn try_update_group_metadata_without_feature_flag() {
        let repo = new_repo().await;
        commit_root_and_array(&repo).await;

        let mut session = repo.writable_session("main").await.unwrap();
        session
            .update_group(&Path::root(), Bytes::from_static(br#"{"attr":1}"#))
            .await
            .unwrap();

        repo.set_feature_flag("update_group_metadata", Some(false)).await.unwrap();
        assert_flag_disabled_session(
            session.commit("blocked").execute().await,
            "update_group_metadata",
            "commit group metadata update",
        );

        repo.set_feature_flag("update_group_metadata", None).await.unwrap();
        session.commit("allowed").execute().await.unwrap();
    }

    #[tokio::test]
    async fn mixed_change_set_reports_first_disabled_flag() {
        let repo = new_repo().await;
        commit_root_and_array(&repo).await;

        // new array plus a chunk write: create_new_nodes comes before
        // update_chunks in the required flag list
        let mut session = repo.writable_session("main").await.unwrap();
        session
            .add_array(
                "/array2".try_into().unwrap(),
                ArrayShape::new(vec![(4, 4)]).unwrap(),
                Some(vec!["t".into()]),
                Bytes::from_static(br#"{"this":"array2"}"#),
            )
            .await
            .unwrap();
        session
            .set_chunk_ref(
                "/array2".try_into().unwrap(),
                ChunkIndices(vec![0]),
                Some(ChunkPayload::Inline(Bytes::from_static(b"1234"))),
            )
            .await
            .unwrap();

        repo.set_feature_flag("create_new_nodes", Some(false)).await.unwrap();
        repo.set_feature_flag("update_chunks", Some(false)).await.unwrap();
        assert_flag_disabled_session(
            session.commit("blocked").execute().await,
            "create_new_nodes",
            "commit new nodes",
        );

        // with create_new_nodes back on, the next flag in the list is reported
        repo.set_feature_flag("create_new_nodes", None).await.unwrap();
        assert_flag_disabled_session(
            session.commit("blocked").execute().await,
            "update_chunks",
            "commit chunk update",
        );
    }

    #[tokio::test]
    async fn try_create_sessions_without_commit_flag() {
        let repo = new_repo().await;

        let _ = repo.writable_session("main").await.unwrap();
        let _ = repo.rearrange_session("main").await.unwrap();

        repo.set_feature_flag("commit", Some(false)).await.unwrap();
        assert_flag_disabled_repo(
            repo.writable_session("main").await,
            "commit",
            "create writable session",
        );
        assert_flag_disabled_repo(
            repo.rearrange_session("main").await,
            "commit",
            "create rearrange session",
        );

        repo.set_feature_flag("commit", None).await.unwrap();
        let _ = repo.writable_session("main").await.unwrap();
        let _ = repo.rearrange_session("main").await.unwrap();
    }

    #[tokio::test]
    async fn try_rebase_without_feature_flag() {
        use crate::conflicts::detector::ConflictDetector;

        let repo = new_repo().await;
        commit_root_and_array(&repo).await;

        // session B starts behind the commit session A makes
        let mut session_b = repo.writable_session("main").await.unwrap();
        session_b
            .add_group("/b".try_into().unwrap(), Bytes::copy_from_slice(b""))
            .await
            .unwrap();
        let mut session_a = repo.writable_session("main").await.unwrap();
        session_a
            .add_group("/a".try_into().unwrap(), Bytes::copy_from_slice(b""))
            .await
            .unwrap();
        session_a.commit("a").execute().await.unwrap();

        repo.set_feature_flag("rebase", Some(false)).await.unwrap();
        assert_flag_disabled_session(
            session_b.rebase(&ConflictDetector).await,
            "rebase",
            "rebase session",
        );

        repo.set_feature_flag("rebase", None).await.unwrap();
        session_b.rebase(&ConflictDetector).await.unwrap();
        session_b.commit("b").execute().await.unwrap();
    }

    #[tokio::test]
    async fn try_branch_ops_without_feature_flags() {
        let repo = new_repo().await;
        let snap = commit_root_and_array(&repo).await;

        repo.create_branch("exists", &Snapshot::INITIAL_SNAPSHOT_ID).await.unwrap();
        repo.reset_branch("exists", &snap, None).await.unwrap();

        repo.set_feature_flag("create_branch", Some(false)).await.unwrap();
        repo.set_feature_flag("delete_branch", Some(false)).await.unwrap();
        repo.set_feature_flag("reset_branch", Some(false)).await.unwrap();

        assert_flag_disabled_repo(
            repo.create_branch("blocked", &Snapshot::INITIAL_SNAPSHOT_ID).await,
            "create_branch",
            "branch creation",
        );
        assert_flag_disabled_repo(
            repo.reset_branch("exists", &Snapshot::INITIAL_SNAPSHOT_ID, None).await,
            "reset_branch",
            "branch reset",
        );
        assert_flag_disabled_repo(
            repo.delete_branch("exists").await,
            "delete_branch",
            "branch delete",
        );

        repo.set_feature_flag("create_branch", None).await.unwrap();
        repo.set_feature_flag("delete_branch", None).await.unwrap();
        repo.set_feature_flag("reset_branch", None).await.unwrap();

        repo.create_branch("allowed", &Snapshot::INITIAL_SNAPSHOT_ID).await.unwrap();
        repo.reset_branch("exists", &Snapshot::INITIAL_SNAPSHOT_ID, None).await.unwrap();
        repo.delete_branch("exists").await.unwrap();
    }
}
