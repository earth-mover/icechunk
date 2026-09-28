//! Opening and creating repositories.
//!
//! [`Repository::open`], [`Repository::create`] and [`Repository::open_or_create`]
//! return a [`RepositoryBuilder`].
//!
//! The builder is generic over a [`Mode`] marker so options that only apply to
//! repository creation are rejected at compile time when opening an
//! existing repository.

use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use tokio::{join, try_join};
use tracing::{Instrument as _, debug, instrument, trace};

use crate::{
    Storage,
    asset_manager::AssetManager,
    change_set::{ChangeSet, transaction_log_from_change_set},
    config::{Credentials, DEFAULT_MAX_CONCURRENT_REQUESTS, RepositoryConfig},
    format::{
        format_constants::SpecVersionBin,
        repo_info::RepoInfo,
        snapshot::{Snapshot, SnapshotInfo},
    },
    private,
    refs::{self, Ref},
    storage::{self, Attribution, AttributionLabels, StorageContext},
};
use icechunk_types::{ICResultExt as _, error::ICResultCtxExt as _};

use super::{
    DetectedSpecVersion, Repository, RepositoryError, RepositoryErrorKind,
    RepositoryResult, raise_if_cant_write,
};

/// Marker for [`Repository::open`].
#[derive(Debug, Clone, Copy)]
pub struct Open;
/// Marker for [`Repository::create`].
#[derive(Debug, Clone, Copy)]
pub struct Create;
/// Marker for [`Repository::open_or_create`].
#[derive(Debug, Clone, Copy)]
pub struct OpenOrCreate;

/// What a [`RepositoryBuilder`] does on [`execute`](RepositoryBuilder::execute).
pub trait Mode: private::Sealed + Send + Sync + 'static {}

/// A [`Mode`] that may create a repository, enabling the creation-only options.
///
/// [`RepositoryBuilder::spec_version`] and [`RepositoryBuilder::check_clean_root`]
/// are only available in these modes, so this does not compile:
///
/// ```compile_fail
/// # use std::sync::Arc;
/// # use icechunk::{Repository, format::format_constants::SpecVersionBin};
/// # async fn f(storage: Arc<dyn icechunk::Storage + Send + Sync>) {
/// Repository::open(storage).spec_version(SpecVersionBin::V2).execute().await;
/// # }
/// ```
pub trait CreateMode: Mode {}

impl private::Sealed for Open {}
impl private::Sealed for Create {}
impl private::Sealed for OpenOrCreate {}
impl Mode for Open {}
impl Mode for Create {}
impl Mode for OpenOrCreate {}
impl CreateMode for Create {}
impl CreateMode for OpenOrCreate {}

/// Options for opening or creating a [`Repository`].
///
/// See the [module docs](self) for how the mode parameter works.
pub struct RepositoryBuilder<M: Mode> {
    storage: Arc<dyn Storage + Send + Sync>,
    config: Option<RepositoryConfig>,
    authorize_virtual_chunk_access: HashMap<String, Option<Credentials>>,
    attribution: Option<Attribution>,
    spec_version: Option<SpecVersionBin>,
    check_clean_root: bool,
    _mode: PhantomData<M>,
}

impl<M: Mode> std::fmt::Debug for RepositoryBuilder<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RepositoryBuilder")
            .field("mode", &std::any::type_name::<M>())
            .field("config", &self.config)
            .field("authorize_virtual_chunk_access", &self.authorize_virtual_chunk_access)
            .field("attribution", &self.attribution)
            .field("spec_version", &self.spec_version)
            .field("check_clean_root", &self.check_clean_root)
            .finish_non_exhaustive()
    }
}

impl<M: Mode> RepositoryBuilder<M> {
    fn new(storage: Arc<dyn Storage + Send + Sync>) -> Self {
        Self {
            storage,
            config: None,
            authorize_virtual_chunk_access: HashMap::new(),
            attribution: None,
            spec_version: None,
            check_clean_root: true,
            _mode: PhantomData,
        }
    }

    /// Configuration layered on top of the persisted repository config (if any)
    /// and the storage backend defaults.
    pub fn config(mut self, config: RepositoryConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Credentials for the virtual chunk containers this repository may read from.
    ///
    /// Keys are container URL prefixes. Containers not listed here cannot be
    /// read even if the repository config declares them.
    pub fn authorize_virtual_chunk_access(
        mut self,
        credentials: HashMap<String, Option<Credentials>>,
    ) -> Self {
        self.authorize_virtual_chunk_access = credentials;
        self
    }

    /// Attribution for the object store requests this repository makes.
    pub fn attribution(mut self, attribution: Attribution) -> Self {
        self.attribution = Some(attribution);
        self
    }
}

impl<M: CreateMode> RepositoryBuilder<M> {
    /// Format spec version for a newly created repository.
    ///
    /// Defaults to the current version. With [`Repository::open_or_create`] it has
    /// no effect if the repository already exists.
    pub fn spec_version(mut self, spec_version: SpecVersionBin) -> Self {
        self.spec_version = Some(spec_version);
        self
    }

    /// Whether creation fails if the storage location already has objects in it.
    ///
    /// Defaults to `true`. With [`Repository::open_or_create`] it has no effect if
    /// the repository already exists.
    pub fn check_clean_root(mut self, check: bool) -> Self {
        self.check_clean_root = check;
        self
    }
}

impl RepositoryBuilder<Open> {
    /// Open the existing repository.
    ///
    /// Fails with [`RepositoryErrorKind::RepositoryDoesntExist`] if there is no
    /// repository at the storage location.
    pub async fn execute(self) -> RepositoryResult<Repository> {
        open(
            self.config,
            self.storage,
            self.authorize_virtual_chunk_access,
            self.attribution,
        )
        .await
    }
}

impl RepositoryBuilder<Create> {
    /// Create a new repository.
    pub async fn execute(self) -> RepositoryResult<Repository> {
        create(
            self.config,
            self.storage,
            self.authorize_virtual_chunk_access,
            self.spec_version,
            self.check_clean_root,
            self.attribution,
        )
        .await
    }
}

impl RepositoryBuilder<OpenOrCreate> {
    /// Open the repository if it exists, create it otherwise.
    pub async fn execute(self) -> RepositoryResult<Repository> {
        let storage_defaults = self.storage.default_settings().await.inject()?;
        let settings = match self.config.as_ref().and_then(|c| c.storage().cloned()) {
            Some(user_storage) => storage_defaults.merge(user_storage),
            None => storage_defaults,
        };
        if Repository::fetch_spec_version_labelled(
            Arc::clone(&self.storage),
            Some(settings),
            self.attribution.clone().unwrap_or_default(),
        )
        .await?
        .is_some()
        {
            open(
                self.config,
                self.storage,
                self.authorize_virtual_chunk_access,
                self.attribution,
            )
            .await
        } else {
            create(
                self.config,
                self.storage,
                self.authorize_virtual_chunk_access,
                self.spec_version,
                self.check_clean_root,
                self.attribution,
            )
            .await
        }
    }
}

impl Repository {
    /// Open an existing repository.
    ///
    /// Returns a builder; call [`RepositoryBuilder::execute`] to perform the open.
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use icechunk::{Repository, RepositoryConfig};
    /// # async fn f(storage: Arc<dyn icechunk::Storage + Send + Sync>) -> Result<(), Box<dyn std::error::Error>> {
    /// let repo = Repository::open(storage)
    ///     .config(RepositoryConfig::default())
    ///     .execute()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn open(storage: Arc<dyn Storage + Send + Sync>) -> RepositoryBuilder<Open> {
        RepositoryBuilder::new(storage)
    }

    /// Create a new repository.
    ///
    /// Returns a builder; call [`RepositoryBuilder::execute`] to perform the creation.
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use icechunk::{Repository, format::format_constants::SpecVersionBin};
    /// # async fn f(storage: Arc<dyn icechunk::Storage + Send + Sync>) -> Result<(), Box<dyn std::error::Error>> {
    /// let repo = Repository::create(storage)
    ///     .spec_version(SpecVersionBin::V2)
    ///     .check_clean_root(false)
    ///     .execute()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn create(storage: Arc<dyn Storage + Send + Sync>) -> RepositoryBuilder<Create> {
        RepositoryBuilder::new(storage)
    }

    /// Open the repository if it exists, create it otherwise.
    ///
    /// Returns a builder; call [`RepositoryBuilder::execute`] to perform the operation.
    /// The creation-only options apply only if the repository has to be created.
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use icechunk::{Repository, RepositoryConfig, format::format_constants::SpecVersionBin};
    /// # async fn f(storage: Arc<dyn icechunk::Storage + Send + Sync>) -> Result<(), Box<dyn std::error::Error>> {
    /// let repo = Repository::open_or_create(storage)
    ///     .config(RepositoryConfig::default())
    ///     .spec_version(SpecVersionBin::V2)
    ///     .execute()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn open_or_create(
        storage: Arc<dyn Storage + Send + Sync>,
    ) -> RepositoryBuilder<OpenOrCreate> {
        RepositoryBuilder::new(storage)
    }
}

#[instrument(skip_all)]
async fn create(
    config: Option<RepositoryConfig>,
    storage: Arc<dyn Storage + Send + Sync>,
    authorize_virtual_chunk_access: HashMap<String, Option<Credentials>>,
    spec_version: Option<SpecVersionBin>,
    check_clean_root: bool,
    attribution: Option<Attribution>,
) -> RepositoryResult<Repository> {
    debug!("Creating Repository");
    raise_if_cant_write(storage.as_ref(), "Cannot create repository").await?;
    if storage.can_create_repository().await.inject()?
        == storage::RepositoryCreation::RefusedEmptyPrefix
    {
        return Err(RepositoryError::capture(RepositoryErrorKind::EmptyPrefixCreation));
    }
    storage.create_location_if_needed().await.inject()?;

    let has_overriden_config = match config {
        Some(ref config) => config != &RepositoryConfig::default(),
        None => false,
    };
    // Merge two layers of config (In order of preference):
    //   - User-provided config (passed to create())
    //   - Backend storage defaults (e.g. S3 retry/concurrency settings)
    let storage_defaults = storage.default_settings().await.inject()?;
    let config = config.unwrap_or_default();
    let storage_settings = match config.storage.clone() {
        Some(user_storage) => storage_defaults.merge(user_storage),
        None => storage_defaults,
    };
    let config = RepositoryConfig { storage: Some(storage_settings.clone()), ..config };
    let attribution = attribution.unwrap_or_default();
    let labels = AttributionLabels::from(&attribution);

    let spec_version = spec_version.unwrap_or_default();

    let asset_manager = Arc::new(
        AssetManager::new_with_config(
            Arc::clone(&storage),
            storage_settings.clone(),
            spec_version,
            config.caching(),
            config.compression().level(),
            config.max_concurrent_requests(),
            config.max_concurrent_decodes(),
        )
        .with_attribution(attribution.clone()),
    );

    if check_clean_root
        && !storage
            .root_is_clean(&StorageContext::without_node(&storage_settings, &labels))
            .await
            .inject()?
    {
        return Err(RepositoryError::capture(
            RepositoryErrorKind::ParentDirectoryNotClean,
        ));
    };

    let asset_manager_c = Arc::clone(&asset_manager);
    let storage_c = Arc::clone(&storage);
    let settings_ref = &storage_settings;
    let labels_ref = &labels;
    let num_updates = config.num_updates_per_repo_info_file();
    let config_ref = &config;
    let create_repo_info = async move {
        // On create we need to create the default branch
        let new_snapshot = Arc::new(Snapshot::initial(spec_version).inject()?);
        let write_snap = asset_manager_c.write_snapshot(Arc::clone(&new_snapshot));

        if spec_version >= SpecVersionBin::V2 {
            let empty_tx_log = transaction_log_from_change_set(
                &Snapshot::INITIAL_SNAPSHOT_ID,
                &ChangeSet::for_edits(),
            );
            let snap_info =
                SnapshotInfo::from_snapshot_file(new_snapshot.as_ref()).inject()?;
            let config_to_store =
                if has_overriden_config { Some(config_ref) } else { None };
            let repo_info = Arc::new(RepoInfo::initial(
                spec_version,
                snap_info,
                num_updates,
                config_to_store,
                None,
            ));

            // Write snapshot and transaction log concurrently first
            let write_tx = asset_manager_c.write_transaction_log(
                Snapshot::INITIAL_SNAPSHOT_ID,
                Arc::new(empty_tx_log),
            );
            try_join!(write_snap, write_tx)?;

            // Only write the repo info after both succeed, since the repo
            // object is the entry point that makes the repository valid.
            // Writing it last ensures we never create a repo pointing to
            // missing snapshot/tx data.
            asset_manager_c.create_repo_info(Arc::clone(&repo_info)).await?;
        } else {
            write_snap.await?;
            refs::update_branch(
                storage_c.as_ref(),
                &StorageContext::without_node(settings_ref, labels_ref),
                Ref::DEFAULT_BRANCH,
                new_snapshot.id().clone(),
                None,
            )
            .await
            .inject()?;
        }

        Ok::<_, RepositoryError>(())
    }
    .in_current_span();

    let config_version = if spec_version >= SpecVersionBin::V2 {
        // V2+ repos: config is already embedded in repo info, no config.yaml needed
        create_repo_info.await?;
        storage::VersionInfo::for_creation()
    } else {
        // V1 repos: write config.yaml separately
        let storage_c = Arc::clone(&storage);
        let config_c = config.clone();
        let attribution_c = attribution.clone();
        let update_config = async move {
            if has_overriden_config {
                let version = Repository::store_config(
                    storage_c,
                    &config_c,
                    &storage::VersionInfo::for_creation(),
                    attribution_c,
                )
                .await?;
                Ok::<_, RepositoryError>(version)
            } else {
                Ok(storage::VersionInfo::for_creation())
            }
        }
        .in_current_span();

        // Note that for V1 repos we don't actually create a repo info file despite the name here.
        // We are (writing the snap; then updating the branch pointer) & writing config.yaml concurrently.
        let (_, config_version) = try_join!(create_repo_info, update_config)?;
        config_version
    };

    debug_assert!(
        Repository::fetch_spec_version_labelled(
            Arc::clone(&storage),
            None,
            attribution.clone()
        )
        .await
        .is_ok_and(|v| v.is_some())
    );
    Repository::new(
        spec_version,
        config,
        config_version,
        storage,
        storage_settings,
        asset_manager,
        authorize_virtual_chunk_access,
    )
}

#[instrument(skip_all)]
async fn open(
    config: Option<RepositoryConfig>,
    storage: Arc<dyn Storage + Send + Sync>,
    authorize_virtual_chunk_access: HashMap<String, Option<Credentials>>,
    attribution: Option<Attribution>,
) -> RepositoryResult<Repository> {
    debug!("Opening Repository");

    // Merge user-provided storage settings with backend defaults upfront so
    // that every code path initializes the shared storage client with the
    // same settings. The S3 client is lazily built via `OnceCell`, so the
    // first caller locks in the config permanently.
    let storage_defaults = storage.default_settings().await.inject()?;
    let settings = match config.as_ref().and_then(|c| c.storage().cloned()) {
        Some(user_storage) => storage_defaults.merge(user_storage),
        None => storage_defaults,
    };
    let attribution = attribution.unwrap_or_default();

    // Launch spec version detection and an optimistic config.yaml fetch concurrently.
    // For IC1 repos this avoids a sequential round-trip; for V2+ repos the config.yaml
    // result is ignored (config lives in the repo info object instead).
    // Note: for V2+ repos, fetch_spec_version already fetches the RepoInfo
    // internally, so we reuse it to avoid a redundant round-trip.
    let temp_am = AssetManager::new_no_cache(
        Arc::clone(&storage),
        settings.clone(),
        SpecVersionBin::current(),
        1,
        DEFAULT_MAX_CONCURRENT_REQUESTS,
    )
    .with_attribution(attribution.clone());

    let storage_c = Arc::clone(&storage);
    let settings_c = settings.clone();
    let fetch_version = tokio::spawn(Repository::fetch_spec_version_labelled(
        storage_c,
        Some(settings_c),
        attribution.clone(),
    ));
    let fetch_config_yaml = temp_am.fetch_config();

    // Use join! (not try_join!) so that a config.yaml error doesn't fail the
    // open for V2+ repos that never had a config.yaml file.
    let (spec_version_result, config_yaml_result) =
        join!(fetch_version, fetch_config_yaml);

    let detected = match spec_version_result.capture()?? {
        Some(v) => Ok(v),
        None => Err(RepositoryError::capture(RepositoryErrorKind::RepositoryDoesntExist)),
    }?;
    let spec_version = detected.spec_version();
    trace!(%spec_version, "Repository version found");

    let (persisted_config, config_version) = match detected {
        DetectedSpecVersion::V2Plus { repo_info, .. } => {
            (repo_info.config().inject()?, storage::VersionInfo::for_creation())
        }
        DetectedSpecVersion::V1 => {
            // V1 repos: use the config.yaml result we already fetched
            match config_yaml_result? {
                Some((c, v)) => (Some(c), v),
                None => (None, storage::VersionInfo::for_creation()),
            }
        }
    };

    // Merge three layers of config (In order of preference):
    //   - User-provided config (passed to open())
    //   - Persisted repo config (saved alongside the data, if any)
    //   - Backend storage defaults (already merged with user settings above)
    let repo_config = match persisted_config {
        Some(c) => RepositoryConfig::default().merge(c),
        None => RepositoryConfig::default(),
    };
    // merge user config on top of persisted config and library defaults
    let merged_config = config.map(|c| repo_config.merge(c)).unwrap_or(repo_config);

    // Re-merge in case persisted config introduced additional storage
    // settings. Note: the S3 client is already initialized with `settings`
    // from above, so only Icechunk-level settings (concurrency, etc.) can
    // change here.
    let storage_settings = match merged_config.storage.clone() {
        Some(s) => settings.merge(s),
        None => settings,
    };
    // combine merged config settings + merged storage settings
    let final_config =
        RepositoryConfig { storage: Some(storage_settings.clone()), ..merged_config };

    let asset_manager = Arc::new(
        AssetManager::new_with_config(
            Arc::clone(&storage),
            storage_settings.clone(),
            spec_version,
            final_config.caching(),
            final_config.compression().level(),
            final_config.max_concurrent_requests(),
            final_config.max_concurrent_decodes(),
        )
        .with_attribution(attribution),
    );

    Repository::new(
        spec_version,
        final_config,
        config_version,
        storage,
        storage_settings,
        asset_manager,
        authorize_virtual_chunk_access,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::new_in_memory_storage;

    #[tokio::test]
    async fn builder_defaults() -> Result<(), Box<dyn std::error::Error>> {
        let storage = new_in_memory_storage().await?;
        let builder = Repository::create(Arc::clone(&storage));
        assert!(builder.config.is_none());
        assert!(builder.authorize_virtual_chunk_access.is_empty());
        assert!(builder.attribution.is_none());
        assert!(builder.spec_version.is_none());
        assert!(builder.check_clean_root);
        Ok(())
    }

    #[tokio::test]
    async fn open_or_create_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let storage = new_in_memory_storage().await?;
        assert!(!Repository::exists(Arc::clone(&storage), None).await?);

        let created = Repository::open_or_create(Arc::clone(&storage))
            .spec_version(SpecVersionBin::current())
            .execute()
            .await?;
        assert!(Repository::exists(Arc::clone(&storage), None).await?);

        let opened = Repository::open(Arc::clone(&storage)).execute().await?;
        assert_eq!(created.spec_version(), opened.spec_version());

        let reopened = Repository::open_or_create(storage).execute().await?;
        assert_eq!(created.spec_version(), reopened.spec_version());
        Ok(())
    }
}
