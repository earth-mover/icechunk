use std::{
    collections::{HashMap, HashSet},
    sync::LazyLock,
};

use crate::format::{
    IcechunkFormatError, IcechunkFormatErrorKind, IcechunkResult, repo_info::RepoInfo,
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
// IDs 1-2 are reserved for future commit/amend flags.
pub const MOVE_NODE_FLAG: u16 = 3;
pub const CREATE_TAG_FLAG: u16 = 4;
pub const DELETE_TAG_FLAG: u16 = 5;

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
        Err(IcechunkFormatError::from(IcechunkFormatErrorKind::FeatureFlagDisabled {
            feature_description: feature_description.to_string(),
            feature_flag: name.to_string(),
        }))
    }
}

pub fn find_feature_flag_id(flag: &str) -> IcechunkResult<u16> {
    FEATURE_FLAGS.get(flag).map(|(id, _)| *id).ok_or_else(|| {
        IcechunkFormatError::from(IcechunkFormatErrorKind::InvalidFeatureFlagName {
            name: flag.to_string(),
        })
    })
}

fn find_flag_by_id(flag_id: u16) -> IcechunkResult<(&'static str, bool)> {
    FEATURE_FLAGS
        .iter()
        .find(|(_, (id, _))| *id == flag_id)
        .map(|(name, (_, default))| (*name, *default))
        .ok_or_else(|| {
            IcechunkFormatError::from(IcechunkFormatErrorKind::InvalidFeatureFlagId {
                id: flag_id,
            })
        })
}

pub(crate) static FEATURE_FLAGS: LazyLock<HashMap<&str, (u16, bool)>> =
    LazyLock::new(|| {
        let res = HashMap::from([
            // (name, (id, default_enabled))
            ("move_node", (MOVE_NODE_FLAG, true)),
            ("create_tag", (CREATE_TAG_FLAG, true)),
            ("delete_tag", (DELETE_TAG_FLAG, true)),
        ]);
        //  check we didn't duplicate ids
        debug_assert_eq!(
            res.values().map(|(id, _)| id).collect::<HashSet<_>>().len(),
            res.len()
        );
        res
    });

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {

    use std::sync::Arc;

    use bytes::Bytes;
    use futures::TryStreamExt as _;

    use crate::{
        Repository, Storage,
        format::{
            format_constants::SpecVersionBin, repo_info::UpdateType, snapshot::Snapshot,
        },
        new_in_memory_storage,
        repository::{RepositoryError, RepositoryErrorKind},
        session::{SessionError, SessionErrorKind},
    };

    use super::*;

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
            (&initial).try_into().unwrap(),
            100,
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
        session
            .add_group("/source".try_into().unwrap(), Bytes::copy_from_slice(b""))
            .await
            .unwrap();
        session.commit("add group", None).await.unwrap();

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
            session.commit("should fail", None).await,
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
        session
            .add_group("/source".try_into().unwrap(), Bytes::copy_from_slice(b""))
            .await
            .unwrap();
        session.commit("add group", None).await.unwrap();

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
            session.flush("should fail", None).await,
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
}
