//! Native S3 client implementation of [`Storage`](icechunk_storage::Storage).

// Re-export AWS SDK types needed by consumers (e.g., icechunk's virtual_chunks)
pub use aws_sdk_s3;

use std::{
    collections::HashMap, fmt, future::ready, ops::Range, pin::Pin, sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use aws_config::{
    AppName, BehaviorVersion, meta::region::RegionProviderChain, retry::RetryConfig,
    timeout::TimeoutConfig,
};
use aws_credential_types::provider::error::CredentialsError;
use aws_sdk_s3::{
    Client,
    config::{
        Builder, ConfigBag, IdentityCache, Intercept, ProvideCredentials, Region,
        RuntimeComponents, StalledStreamProtectionConfig,
        interceptors::{
            BeforeDeserializationInterceptorContextMut,
            BeforeTransmitInterceptorContextMut,
        },
    },
    error::{BoxError, SdkError},
    operation::{copy_object::CopyObjectError, put_object::PutObjectError},
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart, Delete, Object, ObjectIdentifier},
};
use aws_smithy_runtime::client::retries::classifiers::HttpStatusCodeClassifier;
use aws_smithy_types_convert::{
    date_time::DateTimeExt as _, stream::PaginationStreamExt as _,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{
    Stream, StreamExt as _, TryStreamExt as _,
    stream::{self, BoxStream, FuturesOrdered},
};
pub use icechunk_storage::s3_config::{
    S3ChecksumAlgorithm, S3Credentials, S3CredentialsFetcher, S3Options,
    S3StaticCredentials,
};
use icechunk_storage::{
    DeleteObjectsResult, GetModifiedResult, ListInfo, RepositoryCreation, Settings,
    Storage, StorageError, StorageErrorKind, StorageInfo, StorageResult, VersionInfo,
    VersionedUpdateResult, obj_not_found_res, obj_store_error, obj_store_error_res,
    other_error, sealed, split_in_multiple_equal_requests, strip_quotes,
};
use icechunk_types::ICResultExt as _;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;
use tokio_util::io::StreamReader;
use tracing::{error, instrument, trace, warn};
use typed_path::Utf8UnixPath;
use uuid::Uuid;

/// How object keys are laid out inside the bucket for a given repository.
///
/// Native-S3 repositories written before the fix for
/// <https://github.com/earth-mover/icechunk/issues/2239> stored every object
/// under a leading slash when the prefix was empty (`"/chunks/..."`, etc.).
/// We keep being able to read and write those repositories via [`KeyLayout::LegacyRoot`],
/// while all new repositories use the clean [`KeyLayout::Standard`] layout.
///
/// A repository has exactly one layout, decided once and never mixed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyLayout {
    /// `prefix == ""` -> `"chunks/x"`; `prefix == "p"` -> `"p/chunks/x"`.
    Standard,
    /// Only used with an empty prefix: reproduces the historical `"/chunks/x"`.
    LegacyRoot,
}

/// Well-known object names probed by [`S3Storage::probe_layout`] to auto-detect
/// the key layout of a pre-existing repository. These are fixed keys that exist
/// in every repository.
///
/// It's awful to have to have this tacit dependency here, this is icechunk-format stuff.
/// But... reality hits you hard. I don't want an explicit dependency between the crates
/// but there are some tests in `icechunk/src/refs.rs` that verify these strings are "right".
pub const DEFAULT_LAYOUT_ANCHORS: &[&str] = &["repo", "refs/branch.main/ref.json"];

/// Serializes the resolved [`KeyLayout`] as `Option<KeyLayout>`.
mod layout_cell_serde {
    use super::{KeyLayout, OnceCell};
    use serde::{Deserialize as _, Deserializer, Serialize as _, Serializer};

    pub(super) fn serialize<S: Serializer>(
        cell: &OnceCell<KeyLayout>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        cell.get().copied().serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<OnceCell<KeyLayout>, D::Error> {
        let resolved = Option::<KeyLayout>::deserialize(deserializer)?;
        Ok(OnceCell::new_with(resolved))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct S3Storage {
    // config and credentials are stored so we are able to serialize and deserialize the struct
    config: S3Options,
    credentials: S3Credentials,
    bucket: String,
    prefix: String,
    can_write: bool,
    extra_read_headers: Vec<(String, String)>,
    extra_write_headers: Vec<(String, String)>,
    #[serde(default, with = "layout_cell_serde")]
    key_layout: OnceCell<KeyLayout>,
    /// Test/internal escape hatch permitting repository creation at an empty
    /// prefix.
    #[serde(skip)]
    allow_empty_prefix_creation: bool,
    #[serde(skip)]
    /// We need to use `OnceCell` to allow async initialization, because serde
    /// does not support async function calls from deserialization. This gives
    /// us a way to lazily initialize the client.
    client: OnceCell<Arc<Client>>,
}

impl fmt::Display for S3Storage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "S3Storage(bucket={}, prefix={}, config={})",
            self.bucket, self.prefix, self.config,
        )
    }
}
#[derive(Debug)]
struct ExtraHeadersInterceptor {
    extra_read_headers: Vec<(String, String)>,
    extra_write_headers: Vec<(String, String)>,
}

impl Intercept for ExtraHeadersInterceptor {
    fn name(&self) -> &'static str {
        "ExtraHeaders"
    }

    fn modify_before_retry_loop(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let request = context.request_mut();
        let headers = match request.method() {
            "GET" | "HEAD" | "OPTIONS" | "TRACE" => &self.extra_read_headers,
            _ => &self.extra_write_headers,
        };
        for (k, v) in headers.iter() {
            request.headers_mut().try_insert(k.clone(), v.clone()).map_err(
                |e| -> BoxError {
                    format!("invalid extra HTTP header {k:?}: {e}").into()
                },
            )?;
        }
        Ok(())
    }
}

/// Strips `x-amz-checksum-*` headers from HTTP 304 (Not Modified) responses.
///
/// R2 includes checksum headers (e.g. crc32, crc64nvme) on 304 responses, but
/// because 304 responses have no body the AWS SDK's checksum validation fails
/// with a mismatch, triggering transient-error retries with exponential backoff.
#[derive(Debug)]
struct StripChecksumOn304Interceptor;

impl Intercept for StripChecksumOn304Interceptor {
    fn name(&self) -> &'static str {
        "StripChecksumOn304"
    }

    fn modify_before_deserialization(
        &self,
        context: &mut BeforeDeserializationInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let response = context.response_mut();
        if response.status().as_u16() == 304 {
            let to_remove: Vec<_> = response
                .headers()
                .iter()
                .filter(|(name, _)| name.starts_with("x-amz-checksum-"))
                .map(|(name, _)| name.to_owned())
                .collect();
            for name in to_remove {
                response.headers_mut().remove(&name);
            }
        }
        Ok(())
    }
}

#[instrument(skip(credentials))]
pub async fn mk_client(
    config: &S3Options,
    credentials: S3Credentials,
    extra_read_headers: Vec<(String, String)>,
    extra_write_headers: Vec<(String, String)>,
    settings: &Settings,
) -> Client {
    let region = config
        .region
        .as_ref()
        .map(|r| RegionProviderChain::first_try(Some(Region::new(r.clone()))))
        .unwrap_or_else(RegionProviderChain::default_provider);

    let endpoint = config.endpoint_url.clone();
    let region = if endpoint.is_some() {
        // GH793, the S3 SDK requires a region even though it may not make sense
        // for S3-compatible object stores like Tigris or Ceph.
        // So we set a fake region, using the `endpoint_url` as a sign that
        // we are not talking to real S3
        region.or_else(Region::new("region-was-not-set"))
    } else {
        region
    };

    #[expect(clippy::unwrap_used)]
    let app_name = AppName::new(icechunk_types::user_agent()).unwrap();
    let mut aws_config = aws_config::defaults(BehaviorVersion::v2026_01_12())
        .region(region)
        .app_name(app_name);

    if let Some(endpoint) = endpoint {
        aws_config = aws_config.endpoint_url(endpoint);
    }

    let stalled_stream = if config.network_stream_timeout_seconds == Some(0) {
        StalledStreamProtectionConfig::disabled()
    } else {
        StalledStreamProtectionConfig::enabled()
            .grace_period(Duration::from_secs(
                config.network_stream_timeout_seconds.unwrap_or(10) as u64,
            ))
            .build()
    };
    aws_config = aws_config.stalled_stream_protection(stalled_stream);

    match credentials {
        S3Credentials::FromEnv => {}
        S3Credentials::Anonymous => aws_config = aws_config.no_credentials(),
        S3Credentials::Static(credentials) => {
            aws_config =
                aws_config.credentials_provider(aws_credential_types::Credentials::new(
                    credentials.access_key_id,
                    credentials.secret_access_key,
                    credentials.session_token,
                    credentials.expires_after.map(|e| e.into()),
                    "user",
                ));
        }
        S3Credentials::Refreshable(fetcher) => {
            aws_config =
                aws_config.credentials_provider(ProvideRefreshableCredentials(fetcher));
        }
    }

    let retry_config = RetryConfig::standard()
        .with_max_attempts(settings.retries().max_tries().get() as u32)
        .with_initial_backoff(Duration::from_millis(
            settings.retries().initial_backoff_ms() as u64,
        ))
        .with_max_backoff(Duration::from_millis(
            settings.retries().max_backoff_ms() as u64
        ));

    if let Some(timeouts) = settings.timeouts() {
        let mut timeout_builder = TimeoutConfig::builder();
        if let Some(ms) = timeouts.connect_timeout_ms {
            timeout_builder =
                timeout_builder.connect_timeout(Duration::from_millis(ms as u64));
        }
        if let Some(ms) = timeouts.read_timeout_ms {
            timeout_builder =
                timeout_builder.read_timeout(Duration::from_millis(ms as u64));
        }
        if let Some(ms) = timeouts.operation_timeout_ms {
            timeout_builder =
                timeout_builder.operation_timeout(Duration::from_millis(ms as u64));
        }
        if let Some(ms) = timeouts.operation_attempt_timeout_ms {
            timeout_builder = timeout_builder
                .operation_attempt_timeout(Duration::from_millis(ms as u64));
        }
        aws_config = aws_config.timeout_config(timeout_builder.build());
    }

    let mut s3_builder = Builder::from(&aws_config.load().await)
        .force_path_style(config.force_path_style)
        .retry_config(retry_config);

    // credentials may take a while to refresh, defaults are too strict
    let id_cache = IdentityCache::lazy()
        .load_timeout(Duration::from_secs(120))
        .buffer_time(Duration::from_secs(120))
        .build();

    s3_builder = s3_builder.identity_cache(id_cache);

    // Add retry classifier for HTTP 408 (Request Timeout) and 429 (Too Many Requests).
    // The default HttpStatusCodeClassifier only retries on 500, 502, 503, 504
    // Note R2 sends 429 for "slowdown" while S3 sends 503.
    //   - R2: https://developers.cloudflare.com/r2/api/error-codes/
    //   - S3: https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
    // Tigris can occasionally respond with 499: "Client Closed Request"
    static RETRY_CODES: &[u16] = &[408, 429, 499];
    // This confusingly named `retry_classifier` method ends up calling
    // `push_retry_classifier` after wrapping our custom classifier in `SharedRetryClassifier`.
    // Ultimately, this is a push on to a `Vec<SharedRetryClassifier>`, and is thus additive
    // to the existing default retry configuration.
    // https://github.com/smithy-lang/smithy-rs/blob/cfcc39cf4b5bea665bba684b64bfca2b89e4bc73/rust-runtime/aws-smithy-runtime-api/src/client/runtime_components.rs#L755
    // https://github.com/smithy-lang/smithy-rs/blob/cfcc39cf4b5bea665bba684b64bfca2b89e4bc73/rust-runtime/aws-smithy-runtime-api/src/client/runtime_components.rs#L370
    s3_builder = s3_builder
        .retry_classifier(HttpStatusCodeClassifier::new_from_codes(RETRY_CODES));

    if !extra_read_headers.is_empty() || !extra_write_headers.is_empty() {
        s3_builder = s3_builder.interceptor(ExtraHeadersInterceptor {
            extra_read_headers,
            extra_write_headers,
        });
    }

    // R2 and Scaleway include x-amz-checksum-* headers on 304 Not Modified
    // responses, but because 304 has no body the SDK's checksum validation
    // fails and triggers expensive transient-error retries.
    if config.endpoint_url.as_ref().is_some_and(|url| {
        url.contains(".r2.cloudflarestorage.com") || url.contains(".scw.cloud")
    }) {
        s3_builder = s3_builder.interceptor(StripChecksumOn304Interceptor);
    }

    let config = s3_builder.build();

    Client::from_conf(config)
}

fn to_sdk_checksum(a: S3ChecksumAlgorithm) -> aws_sdk_s3::types::ChecksumAlgorithm {
    use aws_sdk_s3::types::ChecksumAlgorithm as Sdk;
    match a {
        S3ChecksumAlgorithm::Crc32 => Sdk::Crc32,
        S3ChecksumAlgorithm::Crc32c => Sdk::Crc32C,
        S3ChecksumAlgorithm::Crc64Nvme => Sdk::Crc64Nvme,
        S3ChecksumAlgorithm::Sha1 => Sdk::Sha1,
        S3ChecksumAlgorithm::Sha256 => Sdk::Sha256,
    }
}

fn stream2stream(
    s: ByteStream,
) -> Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> {
    let res = stream::try_unfold(s, move |mut stream| async move {
        let next = stream.try_next().await?;
        Ok(next.map(|bytes| (bytes, stream)))
    });
    Box::pin(res)
}

/// Per-PUT token marking conditional writes (sent as
/// `x-amz-meta-icechunk_write_id`). Underscores so the key is portable to
/// Azure (C# identifier rules). Keep in sync with `icechunk-arrow-object-store`.
const WRITE_ID_METADATA_KEY: &str = "icechunk_write_id";

/// `Uuid::new_v4` in production; tests force a value via [`test_util`].
fn next_write_id() -> String {
    #[cfg(feature = "test-util")]
    if let Some(forced) = test_util::take_forced_write_id() {
        return forced;
    }
    Uuid::new_v4().to_string()
}

/// Test-only write-id injection. Thread-local, consumed once.
#[cfg(feature = "test-util")]
pub mod test_util {
    use std::cell::RefCell;

    pub const WRITE_ID_METADATA_KEY: &str = super::WRITE_ID_METADATA_KEY;

    thread_local! {
        static FORCED_WRITE_ID: RefCell<Option<String>> = const { RefCell::new(None) };
    }

    /// Make the next conditional PUT on this thread stamp `id`.
    pub fn force_next_write_id(id: impl Into<String>) {
        FORCED_WRITE_ID.with(|c| *c.borrow_mut() = Some(id.into()));
    }

    pub(crate) fn take_forced_write_id() -> Option<String> {
        FORCED_WRITE_ID.with(|c| c.borrow_mut().take())
    }
}

static CONDITIONAL_WITHOUT_METADATA_WARNED: std::sync::OnceLock<()> =
    std::sync::OnceLock::new();

fn warn_conditional_without_metadata_once() {
    CONDITIONAL_WITHOUT_METADATA_WARNED.get_or_init(|| {
        warn!(
            "conditional PUT is enabled but `unsafe_use_metadata` is \
             disabled — lost-response recovery for conditional writes \
             requires user metadata to stamp write-ids; without it, \
             transient PUT failures may surface as spurious conflicts \
             even when the write actually landed. See \
             icechunk_storage::Settings::unsafe_use_metadata."
        );
    });
}

/// Conditional header a PUT carries; shared by single-PUT and multipart.
enum Conditional<'a> {
    IfNoneMatch,
    IfMatch(&'a str),
}

fn conditional_for<'a>(
    previous_version: Option<&'a VersionInfo>,
    settings: &Settings,
) -> Option<Conditional<'a>> {
    let pv = previous_version?;
    match (
        pv.etag(),
        settings.unsafe_use_conditional_create(),
        settings.unsafe_use_conditional_update(),
    ) {
        (None, true, _) => Some(Conditional::IfNoneMatch),
        (Some(etag), _, true) => Some(Conditional::IfMatch(etag)),
        (_, _, _) => None,
    }
}

impl S3Storage {
    /// Build an [`S3Storage`].
    ///
    /// `extra_read_headers`/`extra_write_headers` are extra HTTP headers injected
    /// by an SDK interceptor and split by HTTP method: `GET`/`HEAD`/`OPTIONS`/`TRACE`
    /// carry the read headers, everything else the write headers.
    ///
    /// `legacy_rooted_keys` declares the bucket's key layout:
    /// - `None` — unknown: the layout is auto-detected by probing storage on first
    ///   use. The right choice when opening a repository whose layout you don't know.
    /// - `Some(true)` — force the legacy leading-slash layout used before the fix
    ///   for <https://github.com/earth-mover/icechunk/issues/2239>. Only valid with
    ///   an empty prefix; errors otherwise.
    /// - `Some(false)` — force the standard layout, skipping the probe.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        config: S3Options,
        bucket: String,
        prefix: Option<String>,
        credentials: S3Credentials,
        can_write: bool,
        extra_read_headers: Vec<(String, String)>,
        extra_write_headers: Vec<(String, String)>,
        legacy_rooted_keys: Option<bool>,
    ) -> Result<S3Storage, StorageError> {
        let client = OnceCell::new();
        let prefix = prefix.unwrap_or_default();
        let prefix = prefix.strip_suffix("/").unwrap_or(prefix.as_str()).to_string();
        // A known layout pre-seeds the cell so `probe_layout` is never reached;
        // `None` leaves it empty to be detected lazily on first use.
        let key_layout = match legacy_rooted_keys {
            Some(true) => {
                if !prefix.is_empty() {
                    return Err(other_error(
                        "legacy_rooted_keys is only valid with an empty prefix",
                    ));
                }
                OnceCell::new_with(Some(KeyLayout::LegacyRoot))
            }
            Some(false) => OnceCell::new_with(Some(KeyLayout::Standard)),
            None => OnceCell::new(),
        };
        Ok(S3Storage {
            client,
            config,
            bucket,
            prefix,
            credentials,
            can_write,
            extra_read_headers,
            extra_write_headers,
            key_layout,
            allow_empty_prefix_creation: false,
        })
    }

    /// Test/internal escape hatch: permit creating a new repository at an empty
    /// prefix (the bucket root), which [`Storage::can_create_repository`] would
    /// otherwise refuse.
    pub fn unsafe_allow_empty_prefix_creation(mut self) -> Self {
        self.allow_empty_prefix_creation = true;
        self
    }

    /// Get the client, initializing it if it hasn't been initialized yet. This is necessary because the
    /// client is not serializeable and must be initialized after deserialization. Under normal construction
    /// the original client is returned immediately.
    #[instrument(skip_all)]
    pub async fn get_client(&self, settings: &Settings) -> &Arc<Client> {
        self.client
            .get_or_init(|| async {
                Arc::new(
                    mk_client(
                        &self.config,
                        self.credentials.clone(),
                        self.extra_read_headers.clone(),
                        self.extra_write_headers.clone(),
                        settings,
                    )
                    .await,
                )
            })
            .await
    }

    /// Build the object key for a repository-relative path under the given layout.
    fn key_for(&self, layout: KeyLayout, relpath: &str) -> String {
        match layout {
            // Only reachable with an empty prefix (enforced at construction and
            // by `probe_layout`), so the prefix is intentionally ignored here.
            KeyLayout::LegacyRoot => format!("/{relpath}"),
            KeyLayout::Standard if self.prefix.is_empty() => relpath.to_string(),
            KeyLayout::Standard => format!("{}/{}", self.prefix, relpath),
        }
    }

    /// Key prefix to list. Strips one leading `/` from the relpath so it
    /// doesn't double against the join (S3 matches `//` literally).
    /// An interior `//` is left intact.
    fn list_prefix(&self, layout: KeyLayout, relpath: &str) -> String {
        self.key_for(layout, relpath.strip_prefix('/').unwrap_or(relpath))
    }

    /// Resolve this repository's [`KeyLayout`], probing storage at most once.
    async fn layout(&self, settings: &Settings) -> StorageResult<KeyLayout> {
        self.key_layout.get_or_try_init(|| self.probe_layout(settings)).await.copied()
    }

    /// Detect the key layout of the repository.
    ///
    /// Only ever issues requests for empty-prefix repositories — a non-empty
    /// prefix can never produce a leading slash, so the layout is unambiguously
    /// [`KeyLayout::Standard`]. The probe HEADs a few fixed anchor files under
    /// both layouts.
    async fn probe_layout(&self, settings: &Settings) -> StorageResult<KeyLayout> {
        if !self.prefix.is_empty() {
            // the legacy bug only triggered on empty prefixes
            return Ok(KeyLayout::Standard);
        }
        // For each anchor, HEAD the clean and rooted keys and compare `ETag`s
        // rather than mere existence. Some S3-compatible stores (e.g. MinIO) strip
        // a leading slash, so HEAD("/repo") returns the *same* object as
        // HEAD("repo"); that is key normalization, not a mixed repository, and must
        // resolve to the clean layout.
        let probes = DEFAULT_LAYOUT_ANCHORS.iter().map(|anchor| {
            let clean_key = self.key_for(KeyLayout::Standard, anchor);
            let rooted_key = self.key_for(KeyLayout::LegacyRoot, anchor);
            async move {
                let (clean, rooted) = futures::future::try_join(
                    self.head_etag(settings, &clean_key),
                    self.head_etag(settings, &rooted_key),
                )
                .await?;
                Ok::<_, StorageError>(AnchorProbe { clean, rooted })
            }
        });
        let anchors = futures::future::try_join_all(probes).await?;
        layout_from_anchor_etags(&anchors, &self.bucket)
    }

    /// HEAD a single key, returning its `ETag` if the object exists, or `None` if
    /// absent.
    ///
    /// Treats both 404 and 400 as "absent" because some S3-compatible stores
    /// (rustfs, some `MinIO` versions) reject leading-slash keys with 400 rather
    /// than 404. The `ETag` lets [`Self::probe_layout`] tell a genuinely-rooted
    /// object apart from a normalizing store that maps `"/x"` to the same object
    /// as `"x"`.
    async fn head_etag(
        &self,
        settings: &Settings,
        key: &str,
    ) -> StorageResult<Option<String>> {
        let mut req = self
            .get_client(settings)
            .await
            .head_object()
            .bucket(self.bucket.clone())
            .key(key);
        if self.config.requester_pays {
            req = req.request_payer(aws_sdk_s3::types::RequestPayer::Requester);
        }
        match req.send().await {
            Ok(out) => Ok(Some(out.e_tag().unwrap_or_default().to_string())),
            Err(sdk_err) => {
                let absent = sdk_err.as_service_error().is_some_and(|e| e.is_not_found())
                    || sdk_err
                        .raw_response()
                        .is_some_and(|r| matches!(r.status().as_u16(), 404 | 400));
                if absent { Ok(None) } else { obj_store_error_res(sdk_err) }
            }
        }
    }

    async fn put_object_single<
        I: IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    >(
        &self,
        settings: &Settings,
        key: &str,
        bytes: Bytes,
        content_type: Option<impl Into<String>>,
        metadata: I,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<VersionedUpdateResult> {
        let mut req = self
            .get_client(settings)
            .await
            .put_object()
            .bucket(self.bucket.clone())
            .key(key)
            .body(bytes.into());

        if let Some(algo) = self.config.checksum_algorithm {
            req = req.checksum_algorithm(to_sdk_checksum(algo));
        }

        if settings.unsafe_use_metadata() {
            if let Some(ct) = content_type {
                req = req.content_type(ct);
            };

            for (k, v) in metadata {
                req = req.metadata(k, v);
            }
        }

        if let Some(klass) = settings.storage_class() {
            let klass = klass.as_str().into();
            req = req.storage_class(klass);
        }

        let conditional_applied = match conditional_for(previous_version, settings) {
            Some(Conditional::IfNoneMatch) => {
                req = req.if_none_match("*");
                true
            }
            Some(Conditional::IfMatch(etag)) => {
                req = req.if_match(strip_quotes(etag));
                true
            }
            None => false,
        };

        let write_id = if conditional_applied && settings.unsafe_use_metadata() {
            let id = next_write_id();
            req = req.metadata(WRITE_ID_METADATA_KEY, &id);
            Some(id)
        } else {
            if conditional_applied {
                warn_conditional_without_metadata_once();
            }
            None
        };

        match req.send().await {
            Ok(out) => {
                let new_etag = out
                    .e_tag()
                    .ok_or(other_error("Object should have an etag".to_string()))?
                    .to_string();
                let new_version = VersionInfo::from_etag_only(new_etag);
                Ok(VersionedUpdateResult::Updated { new_version })
            }
            // minio returns this
            Err(SdkError::ServiceError(err)) => {
                let code = err.err().meta().code().unwrap_or_default();
                if code == "PreconditionFailed"
                    || code == "ConditionalRequestConflict"
                    // ConcurrentModification sent by Ceph Object Gateway
                    || code == "ConcurrentModification"
                {
                    let outcome = self
                        .read_back_after_conditional_failure(
                            settings,
                            key,
                            write_id.as_deref(),
                        )
                        .await?;
                    map_precondition_outcome(outcome, key)
                } else {
                    obj_store_error_res(SdkError::<PutObjectError>::ServiceError(err))
                }
            }
            // S3 API documents this
            Err(SdkError::ResponseError(err)) => {
                let status = err.raw().status().as_u16();
                // see https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#API_PutObject_RequestSyntax
                if status == 409 || status == 412 {
                    let outcome = self
                        .read_back_after_conditional_failure(
                            settings,
                            key,
                            write_id.as_deref(),
                        )
                        .await?;
                    map_precondition_outcome(outcome, key)
                } else {
                    obj_store_error_res(SdkError::<PutObjectError>::ResponseError(err))
                }
            }
            Err(err) => obj_store_error_res(err),
        }
    }

    async fn put_object_multipart<
        I: IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    >(
        &self,
        settings: &Settings,
        key: &str,
        bytes: &Bytes,
        content_type: Option<impl Into<String>>,
        metadata: I,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<VersionedUpdateResult> {
        let mut multi = self
            .get_client(settings)
            .await
            .create_multipart_upload()
            // We would like this, but it fails in MinIO
            //.checksum_type(aws_sdk_s3::types::ChecksumType::FullObject)
            //.checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Crc64Nvme)
            .bucket(self.bucket.clone())
            .key(key);

        // Write-id rides as metadata on `create_multipart_upload`, so decide
        // here whether the later `complete` will carry a condition.
        let will_apply_condition = conditional_for(previous_version, settings).is_some();
        let write_id = if will_apply_condition && settings.unsafe_use_metadata() {
            Some(next_write_id())
        } else {
            if will_apply_condition {
                warn_conditional_without_metadata_once();
            }
            None
        };

        if settings.unsafe_use_metadata() {
            if let Some(ct) = content_type {
                multi = multi.content_type(ct);
            };
            for (k, v) in metadata {
                multi = multi.metadata(k, v);
            }
            if let Some(id) = write_id.as_deref() {
                multi = multi.metadata(WRITE_ID_METADATA_KEY, id);
            }
        }

        if let Some(klass) = settings.storage_class() {
            let klass = klass.as_str().into();
            multi = multi.storage_class(klass);
        }

        let create_res = multi.send().await.capture_box()?;
        let upload_id = create_res.upload_id().ok_or(other_error(
            "No upload_id in create multipart upload result".to_string(),
        ))?;

        // We need to ensure all requests are the same size except for the last one, which can be
        // smaller. This is a requirement for R2 compatibility
        let parts = split_in_multiple_equal_requests(
            &(0..bytes.len() as u64),
            settings.concurrency().ideal_concurrent_request_size().get(),
            settings.concurrency().max_concurrent_requests_for_object().get(),
        )
        .collect::<Vec<_>>();

        let results = parts
            .into_iter()
            .enumerate()
            .map(|(part_idx, range)| async move {
                let body = bytes.slice(range.start as usize..range.end as usize).into();
                let idx = part_idx as i32 + 1;
                let mut req = self
                    .get_client(settings)
                    .await
                    .upload_part()
                    .upload_id(upload_id)
                    .bucket(self.bucket.clone())
                    .key(key)
                    .part_number(idx)
                    .body(body);

                if let Some(algo) = self.config.checksum_algorithm {
                    req = req.checksum_algorithm(to_sdk_checksum(algo));
                }

                req.send().await.map(|res| (idx, res))
            })
            .collect::<FuturesOrdered<_>>();

        let completed_parts = results
            .map_ok(|(idx, res)| {
                let etag = res.e_tag().unwrap_or("");
                CompletedPart::builder()
                    .e_tag(strip_quotes(etag))
                    .part_number(idx)
                    .build()
            })
            .try_collect::<Vec<_>>()
            .await
            .capture_box()?;

        let completed_parts =
            CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build();

        let mut req = self
            .get_client(settings)
            .await
            .complete_multipart_upload()
            .bucket(self.bucket.clone())
            .key(key)
            .upload_id(upload_id)
            //.checksum_type(aws_sdk_s3::types::ChecksumType::FullObject)
            .multipart_upload(completed_parts);

        match conditional_for(previous_version, settings) {
            Some(Conditional::IfNoneMatch) => req = req.if_none_match("*"),
            Some(Conditional::IfMatch(etag)) => req = req.if_match(strip_quotes(etag)),
            None => {}
        }

        match req.send().await {
            Ok(out) => {
                let new_etag = out
                    .e_tag()
                    .ok_or(other_error("Object should have an etag".to_string()))?
                    .to_string();
                let new_version = VersionInfo::from_etag_only(new_etag);
                Ok(VersionedUpdateResult::Updated { new_version })
            }
            Err(SdkError::ServiceError(err)) => {
                let code = err.err().meta().code().unwrap_or_default();
                // `ConcurrentModification` is Ceph. `NoSuchUpload` = SDK-retried
                // complete after a lost response, or a failed upload; readback
                // tells them apart.
                let is_precondition = code == "PreconditionFailed"
                    || code == "ConditionalRequestConflict"
                    || code == "ConcurrentModification";
                let is_lost_upload = code == "NoSuchUpload";
                if is_precondition || is_lost_upload {
                    let outcome = self
                        .read_back_after_conditional_failure(
                            settings,
                            key,
                            write_id.as_deref(),
                        )
                        .await?;
                    if is_precondition {
                        map_precondition_outcome(outcome, key)
                    } else {
                        map_lost_upload_outcome(outcome, key, SdkError::ServiceError(err))
                    }
                } else {
                    obj_store_error_res(SdkError::ServiceError(err))
                }
            }
            Err(SdkError::ResponseError(err)) => {
                let status = err.raw().status().as_u16();
                if status == 409 || status == 412 {
                    let outcome = self
                        .read_back_after_conditional_failure(
                            settings,
                            key,
                            write_id.as_deref(),
                        )
                        .await?;
                    map_precondition_outcome(outcome, key)
                } else if status == 404 {
                    // NoSuchUpload surfaced as a raw HTTP status.
                    let outcome = self
                        .read_back_after_conditional_failure(
                            settings,
                            key,
                            write_id.as_deref(),
                        )
                        .await?;
                    map_lost_upload_outcome(
                        outcome,
                        key,
                        SdkError::<PutObjectError>::ResponseError(err),
                    )
                } else {
                    obj_store_error_res(SdkError::<PutObjectError>::ResponseError(err))
                }
            }
            Err(err) => obj_store_error_res(err),
        }
    }

    /// HEAD `key` and classify it against our stamped write-id. `Err` only on
    /// a transient HEAD failure — an inconclusive read-back must never fake a
    /// conflict.
    async fn read_back_after_conditional_failure(
        &self,
        settings: &Settings,
        key: &str,
        write_id: Option<&str>,
    ) -> StorageResult<ReadbackOutcome> {
        if write_id.is_none() {
            return Ok(ReadbackOutcome::NotStamped);
        }
        let mut head = self
            .get_client(settings)
            .await
            .head_object()
            .bucket(self.bucket.clone())
            .key(key);
        if self.config.requester_pays {
            head = head.request_payer(aws_sdk_s3::types::RequestPayer::Requester);
        }
        let facts = match head.send().await {
            Ok(out) => HeadFacts::Found {
                stored_write_id: out
                    .metadata()
                    .and_then(|m| m.get(WRITE_ID_METADATA_KEY))
                    .cloned(),
                etag: out.e_tag().map(str::to_string),
            },
            Err(sdk_err)
                if sdk_err.as_service_error().is_some_and(|e| e.is_not_found())
                    || sdk_err
                        .raw_response()
                        .is_some_and(|r| r.status().as_u16() == 404) =>
            {
                HeadFacts::Absent
            }
            Err(sdk_err) => {
                warn!(key, error = %sdk_err, "readback HEAD failed; propagating original error");
                return obj_store_error_res(sdk_err);
            }
        };
        Ok(classify_readback(write_id, &facts))
    }
}

/// Object facts read back after a conditional PUT reported a conflict.
#[derive(Debug)]
enum HeadFacts {
    Found { stored_write_id: Option<String>, etag: Option<String> },
    Absent,
}

/// Only `OurWrite` is universally success; the rest are mapped per-caller.
#[derive(Debug, PartialEq, Eq)]
enum ReadbackOutcome {
    OurWrite { etag: String },
    NotOurWrite,
    NotStamped,
    Absent,
    MissingEtag,
}

/// Pure decision (unit-tested): does the read-back prove our own write landed?
fn classify_readback(our_write_id: Option<&str>, facts: &HeadFacts) -> ReadbackOutcome {
    let Some(our) = our_write_id else { return ReadbackOutcome::NotStamped };
    match facts {
        HeadFacts::Absent => ReadbackOutcome::Absent,
        HeadFacts::Found { stored_write_id, etag } => {
            if stored_write_id.as_deref() != Some(our) {
                ReadbackOutcome::NotOurWrite
            } else {
                match etag {
                    Some(etag) => ReadbackOutcome::OurWrite { etag: etag.clone() },
                    None => ReadbackOutcome::MissingEtag,
                }
            }
        }
    }
}

/// 412/409 caller: an absent object is a genuine lost race.
fn map_precondition_outcome(
    outcome: ReadbackOutcome,
    key: &str,
) -> StorageResult<VersionedUpdateResult> {
    match outcome {
        ReadbackOutcome::OurWrite { etag } => {
            warn!(
                key,
                "precondition failed but our write-id is stored; retried PUT, success"
            );
            Ok(VersionedUpdateResult::Updated {
                new_version: VersionInfo::from_etag_only(etag),
            })
        }
        ReadbackOutcome::NotOurWrite
        | ReadbackOutcome::NotStamped
        | ReadbackOutcome::Absent => Ok(VersionedUpdateResult::NotOnLatestVersion),
        ReadbackOutcome::MissingEtag => {
            Err(other_error("Object should have an etag".to_string()))
        }
    }
}

/// `NoSuchUpload`/404 caller: only our own landed write rescues it; absence
/// means the upload failed, so everything else propagates the error.
fn map_lost_upload_outcome<E>(
    outcome: ReadbackOutcome,
    key: &str,
    original: SdkError<E>,
) -> StorageResult<VersionedUpdateResult>
where
    E: std::error::Error + Send + Sync + 'static,
{
    match outcome {
        ReadbackOutcome::OurWrite { etag } => {
            warn!(
                key,
                "multipart completion response lost; our write-id is stored, success"
            );
            Ok(VersionedUpdateResult::Updated {
                new_version: VersionInfo::from_etag_only(etag),
            })
        }
        _ => obj_store_error_res(original),
    }
}

pub fn range_to_header(range: &Range<u64>) -> String {
    format!("bytes={}-{}", range.start, range.end - 1)
}

impl sealed::Sealed for S3Storage {}

#[async_trait]
#[typetag::serde]
impl Storage for S3Storage {
    fn storage_info(&self) -> StorageInfo {
        let mut fields = vec![("bucket", self.bucket.clone())];
        if !self.prefix.is_empty() {
            fields.push(("prefix", self.prefix.clone()));
        }
        fields.extend(self.config.info_fields());
        StorageInfo { backend_type: "S3 (native)", fields }
    }

    async fn can_write(&self) -> StorageResult<bool> {
        Ok(self.can_write)
    }

    async fn can_create_repository(&self) -> StorageResult<RepositoryCreation> {
        if self.prefix.is_empty() && !self.allow_empty_prefix_creation {
            Ok(RepositoryCreation::RefusedEmptyPrefix)
        } else {
            Ok(RepositoryCreation::Allowed)
        }
    }

    async fn put_object(
        &self,
        settings: &Settings,
        path: &str,
        bytes: Bytes,
        content_type: Option<&str>,
        metadata: Vec<(String, String)>,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<VersionedUpdateResult> {
        let layout = self.layout(settings).await?;
        let path = self.key_for(layout, path);
        if bytes.len() >= settings.minimum_size_for_multipart_upload() as usize {
            self.put_object_multipart(
                settings,
                path.as_str(),
                &bytes,
                content_type,
                metadata,
                previous_version,
            )
            .await
        } else {
            self.put_object_single(
                settings,
                path.as_str(),
                bytes,
                content_type,
                metadata,
                previous_version,
            )
            .await
        }
    }

    async fn copy_object(
        &self,
        settings: &Settings,
        from: &str,
        to: &str,
        content_type: Option<&str>,
        version: &VersionInfo,
    ) -> StorageResult<VersionedUpdateResult> {
        let layout = self.layout(settings).await?;
        let from = format!("{}/{}", self.bucket, self.key_for(layout, from));
        let to = self.key_for(layout, to);
        let mut req = self
            .get_client(settings)
            .await
            .copy_object()
            .bucket(self.bucket.clone())
            .key(to)
            .copy_source(from);
        if settings.unsafe_use_conditional_update()
            && let Some(etag) = version.etag()
        {
            req = req.copy_source_if_match(strip_quotes(etag));
        }
        if let Some(klass) = settings.storage_class() {
            let klass = klass.as_str().into();
            req = req.storage_class(klass);
        }
        if let Some(ct) = content_type {
            req = req.content_type(ct);
        }
        if self.config.requester_pays {
            req = req.request_payer(aws_sdk_s3::types::RequestPayer::Requester);
        }
        match req.send().await {
            Ok(_) => Ok(VersionedUpdateResult::Updated { new_version: version.clone() }),
            Err(SdkError::ServiceError(err)) => {
                let code = err.err().meta().code().unwrap_or_default();
                if code == "PreconditionFailed"
                    || code == "ConditionalRequestConflict"
                    // ConcurrentModification sent by Ceph Object Gateway
                    || code == "ConcurrentModification"
                {
                    Ok(VersionedUpdateResult::NotOnLatestVersion)
                } else {
                    obj_store_error_res(SdkError::<CopyObjectError>::ServiceError(err))
                }
            }
            // S3 API documents this
            Err(SdkError::ResponseError(err)) => {
                let status = err.raw().status().as_u16();
                // see https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#API_PutObject_RequestSyntax
                if status == 409 || status == 412 {
                    Ok(VersionedUpdateResult::NotOnLatestVersion)
                } else {
                    obj_store_error_res(SdkError::<PutObjectError>::ResponseError(err))
                }
            }
            Err(sdk_err) => match sdk_err.as_service_error() {
                Some(_)
                    if sdk_err
                        .raw_response()
                        .is_some_and(|x| x.status().as_u16() == 404) =>
                {
                    // needed for Cloudflare R2 public bucket URLs
                    // if object doesn't exist we get a 404 that isn't parsed by the AWS SDK
                    // into anything useful. So we need to parse the raw response, and match
                    // the status code.
                    obj_not_found_res()
                }
                _ => obj_store_error_res(sdk_err),
            },
        }
    }

    #[instrument(skip(self, settings))]
    async fn list_objects<'a>(
        &'a self,
        settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        let layout = self.layout(settings).await?;
        let prefix = self.list_prefix(layout, prefix);
        let mut req = self
            .get_client(settings)
            .await
            .list_objects_v2()
            .bucket(self.bucket.clone())
            .prefix(prefix.clone());

        if self.config.requester_pays {
            req = req.request_payer(aws_sdk_s3::types::RequestPayer::Requester);
        }

        let stream = req
            .into_paginator()
            .send()
            .into_stream_03x()
            .map_err(obj_store_error)
            .try_filter_map(|page| {
                let contents = page.contents.map(|cont| stream::iter(cont).map(Ok));
                ready(Ok(contents))
            })
            .try_flatten()
            .and_then(move |object| {
                let prefix = prefix.clone();
                ready(object_to_list_info(prefix.as_str(), &object))
            });
        Ok(stream.boxed())
    }

    #[instrument(skip(self, batch))]
    async fn delete_batch(
        &self,
        settings: &Settings,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        fn join_prefix_id(prefix: &str, id: &str) -> String {
            if prefix.is_empty() {
                id.to_string()
            } else {
                format!("{}/{}", prefix.trim_end_matches('/'), id)
            }
        }

        let layout = self.layout(settings).await?;
        let mut sizes = HashMap::new();
        let mut ids = Vec::new();
        for (id, size) in batch.into_iter() {
            let key = self.key_for(layout, &join_prefix_id(prefix, &id));
            if let Ok(ident) = ObjectIdentifier::builder().key(key.clone()).build() {
                ids.push(ident);
                sizes.insert(key, size);
            }
        }

        let delete = Delete::builder()
            .set_objects(Some(ids))
            .build()
            .map_err(|e| other_error(e.to_string()))?;

        let mut req = self
            .get_client(settings)
            .await
            .delete_objects()
            .bucket(self.bucket.clone())
            .delete(delete);

        if let Some(algo) = self.config.checksum_algorithm {
            req = req.checksum_algorithm(to_sdk_checksum(algo));
        }

        if self.config.requester_pays {
            req = req.request_payer(aws_sdk_s3::types::RequestPayer::Requester);
        }

        let res = req.send().await.capture_box()?;

        if let Some(err) = res.errors.as_ref().and_then(|e| e.first()) {
            tracing::error!(
                error = ?err,
                "Errors deleting objects",
            );
        }

        let mut result = DeleteObjectsResult::default();
        for deleted in res.deleted() {
            if let Some(key) = deleted.key() {
                let size = sizes.get(key).unwrap_or(&0);
                result.deleted_bytes += *size;
                result.deleted_objects += 1;
            } else {
                tracing::error!("Deleted object without key");
            }
        }
        Ok(result)
    }

    #[instrument(skip(self, settings))]
    async fn get_object_last_modified(
        &self,
        path: &str,
        settings: &Settings,
    ) -> StorageResult<DateTime<Utc>> {
        let layout = self.layout(settings).await?;
        let key = self.key_for(layout, path);
        let mut req = self
            .get_client(settings)
            .await
            .head_object()
            .bucket(self.bucket.clone())
            .key(key);

        if self.config.requester_pays {
            req = req.request_payer(aws_sdk_s3::types::RequestPayer::Requester);
        }

        let res = req.send().await.capture_box()?;

        let res = res
            .last_modified
            .ok_or(other_error("Object has no last_modified field".to_string()))?;
        let res = res
            .to_chrono_utc()
            .map_err(|_| other_error("Invalid metadata timestamp".to_string()))?;

        Ok(res)
    }

    #[instrument(skip(self, settings))]
    async fn get_object_conditional(
        &self,
        settings: &Settings,
        path: &str,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
        match self
            .get_object_range_conditional(settings, path, None, previous_version)
            .await
        {
            Ok(Some((stream, new_version))) => {
                let reader = StreamReader::new(stream.map_err(std::io::Error::other));
                Ok(GetModifiedResult::Modified { data: Box::pin(reader), new_version })
            }
            Ok(None) => Ok(GetModifiedResult::OnLatestVersion),
            Err(e) => Err(e),
        }
    }

    async fn get_object_range(
        &self,
        settings: &Settings,
        path: &str,
        range: Option<&Range<u64>>,
    ) -> StorageResult<(
        Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>,
        VersionInfo,
    )> {
        self.get_object_range_conditional(settings, path, range, None).await.map(|v| {
            // If we got a result, then we can unwrap safely here:
            // Errors would be in the other branch, and None is only expected
            // if previous_version was passed in function call, but we set it to None
            #[expect(clippy::expect_used)]
            v.expect("Logic bug in get_object_range_conditional, should not get None")
        })
    }
}

impl S3Storage {
    async fn get_object_range_conditional(
        &self,
        settings: &Settings,
        path: &str,
        range: Option<&Range<u64>>,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<
        Option<(
            Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>,
            VersionInfo,
        )>,
    > {
        let layout = self.layout(settings).await?;
        let client = self.get_client(settings).await;
        let bucket = self.bucket.clone();
        let key = self.key_for(layout, path);

        let mut req = client.get_object().bucket(bucket).key(key);

        if let Some(range) = range {
            req = req.range(range_to_header(range));
        }

        if self.config.requester_pays {
            req = req.request_payer(aws_sdk_s3::types::RequestPayer::Requester);
        }

        if let Some(previous_version) = previous_version.as_ref()
            && let Some(etag) = previous_version.etag()
        {
            req = req.if_none_match(strip_quotes(etag));
        };

        match req.send().await {
            Ok(output) => match output.e_tag {
                Some(etag) => {
                    let stream = stream2stream(output.body)
                        .map_err(|e| StorageError::capture(e.into()));
                    Ok(Some((Box::pin(stream), VersionInfo::from_etag_only(etag))))
                }
                None => Err(other_error("Object should have an etag".to_string())),
            },
            Err(sdk_err) => {
                match sdk_err.as_service_error() {
                    Some(e) if e.is_no_such_key() => {
                        obj_not_found_res()
                    }
                    Some(_)
                        if sdk_err
                            .raw_response()
                            .is_some_and(|x| x.status().as_u16() == 404) =>
                    {
                        // needed for Cloudflare R2 public bucket URLs
                        // if object doesn't exist we get a 404 that isn't parsed by the AWS SDK
                        // into anything useful. So we need to parse the raw response, and match
                        // the status code.
                        obj_not_found_res()
                    }
                    Some(_)
                        // aws_sdk_s3 doesn't return an error when
                        // status 304 (Not Modified) happens, so
                        // check the http status code here and
                        // return None to make it easy to catch
                        // downstream
                        if sdk_err
                            .raw_response()
                            .is_some_and(|x| x.status().as_u16() == 304) =>
                    {
                        trace!("Received 304 (Not Modified). Treating requested object as up-to-date.");
                        Ok(None)
                    }
                    _ => obj_store_error_res(sdk_err),
                }
            }
        }
    }
}

#[derive(Debug)]
struct AnchorProbe<T> {
    /// Modern layout using non-rooted keys
    clean: T,
    /// Legacy buggy layout using /chunks style keys on empty prefixes
    rooted: T,
}

/// Resolve the layout from per-anchor HEAD results.
///
/// For each anchor, comparing `ETag`s (not mere existence) distinguishes a store
/// that normalizes `"/x"` to `"x"` (same object under both keys → clean layout)
/// from one that genuinely holds distinct objects at both keys (→ mixed).
fn layout_from_anchor_etags(
    anchors: &[AnchorProbe<Option<String>>],
    bucket: &str,
) -> StorageResult<KeyLayout> {
    let seen = anchors.iter().fold(
        AnchorProbe { clean: false, rooted: false },
        |AnchorProbe { clean: clean_seen, rooted: rooted_seen },
         AnchorProbe { clean, rooted }| {
            let (clean_vote, rooted_vote) = match (clean, rooted) {
                // Same physical object under both keys: the store normalized away
                // the leading slash. Clean layout, not a mixed repository.
                (Some(c), Some(r)) if c == r => (true, false),
                // Genuinely distinct objects at both keys: ambiguous; flag as mixed.
                (Some(_), Some(_)) => (true, true),
                (Some(_), None) => (true, false),
                (None, Some(_)) => (false, true),
                (None, None) => (false, false),
            };
            AnchorProbe {
                clean: clean_seen || clean_vote,
                rooted: rooted_seen || rooted_vote,
            }
        },
    );

    decide_layout(&seen, bucket)
}

/// `bucket` is only used to build the mixed-layout error.
fn decide_layout(seen: &AnchorProbe<bool>, bucket: &str) -> StorageResult<KeyLayout> {
    match seen {
        // Both layouts present: ambiguous and unsafe
        AnchorProbe { clean: true, rooted: true } => Err(other_error(format!(
            "repository in bucket {bucket} has objects under both the standard and the \
             legacy leading-slash key layouts; this is ambiguous and unsafe. \
             See https://github.com/earth-mover/icechunk/issues/2239"
        ))),
        AnchorProbe { clean: false, rooted: true } => Ok(KeyLayout::LegacyRoot),
        // (true, false) => existing clean repo;
        // (false, false) => empty/new repository. Both use the clean layout.
        _ => Ok(KeyLayout::Standard),
    }
}

fn object_to_list_info(prefix: &str, object: &Object) -> StorageResult<ListInfo<String>> {
    let inner = || {
        let key = object.key()?;
        let last_modified = object.last_modified()?;
        let created_at = last_modified.to_chrono_utc().ok()?;
        let prefix = Utf8UnixPath::new(prefix);
        let id = Utf8UnixPath::new(key).strip_prefix(prefix).ok()?.to_string();
        let size_bytes = object.size.unwrap_or(0) as u64;
        Some(ListInfo { id, created_at, size_bytes })
    };
    inner()
        .ok_or_else(|| StorageError::capture(StorageErrorKind::BadPrefix(prefix.into())))
}

#[derive(Debug)]
struct ProvideRefreshableCredentials(Arc<dyn S3CredentialsFetcher>);

impl ProvideCredentials for ProvideRefreshableCredentials {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::new(self.provide())
    }
}

impl ProvideRefreshableCredentials {
    async fn provide(
        &self,
    ) -> Result<aws_credential_types::Credentials, CredentialsError> {
        let creds = self
            .0
            .get()
            .await
            .inspect_err(|err| error!(error = err, "Cannot load credentials"))
            .map_err(CredentialsError::not_loaded)?;
        let creds = aws_credential_types::Credentials::new(
            creds.access_key_id,
            creds.secret_access_key,
            creds.session_token,
            creds.expires_after.map(|e| e.into()),
            "user",
        );
        Ok(creds)
    }
}

// Factory functions

/// Build storage for an S3 (or S3-compatible, non-Tigris) bucket.
///
/// `extra_read_headers`/`extra_write_headers` are extra HTTP headers attached to
/// read/write requests respectively.
///
/// For `legacy_rooted_keys`, see [`S3Storage::new`]: `None` auto-detects the key
/// layout (the usual choice), `Some(true)` forces the legacy leading-slash layout,
/// and `Some(false)` forces the standard layout.
pub fn new_s3_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
    extra_read_headers: Vec<(String, String)>,
    extra_write_headers: Vec<(String, String)>,
    legacy_rooted_keys: Option<bool>,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    Ok(Arc::new(s3_storage(
        config,
        bucket,
        prefix,
        credentials,
        extra_read_headers,
        extra_write_headers,
        legacy_rooted_keys,
    )?))
}

/// Build storage for an S3 (or S3-compatible, non-Tigris) bucket.
///
/// `extra_read_headers`/`extra_write_headers` are extra HTTP headers attached to
/// read/write requests respectively.
///
/// For `legacy_rooted_keys`, see [`S3Storage::new`]: `None` auto-detects the key
/// layout (the usual choice), `Some(true)` forces the legacy leading-slash layout,
/// and `Some(false)` forces the standard layout.
pub fn s3_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
    extra_read_headers: Vec<(String, String)>,
    extra_write_headers: Vec<(String, String)>,
    legacy_rooted_keys: Option<bool>,
) -> StorageResult<S3Storage> {
    if let Some(endpoint) = &config.endpoint_url
        && (endpoint.contains("fly.storage.tigris.dev")
            || endpoint.contains("t3.storage.dev"))
    {
        return Err(other_error(
            "Tigris Storage is not S3 compatible, use the Tigris specific constructor instead"
                .to_string(),
        ));
    }

    S3Storage::new(
        config,
        bucket,
        prefix,
        credentials.unwrap_or(S3Credentials::FromEnv),
        true,
        extra_read_headers,
        extra_write_headers,
        legacy_rooted_keys,
    )
}

/// Build storage for a Cloudflare R2 bucket.
///
/// `extra_read_headers`/`extra_write_headers` are extra HTTP headers attached to
/// read/write requests respectively.
///
/// For `legacy_rooted_keys`, see [`S3Storage::new`]: `None` auto-detects the key
/// layout (the usual choice), `Some(true)` forces the legacy leading-slash layout,
/// and `Some(false)` forces the standard layout.
#[expect(clippy::too_many_arguments)]
pub fn new_r2_storage(
    config: S3Options,
    bucket: Option<String>,
    prefix: Option<String>,
    account_id: Option<String>,
    credentials: Option<S3Credentials>,
    extra_read_headers: Vec<(String, String)>,
    extra_write_headers: Vec<(String, String)>,
    legacy_rooted_keys: Option<bool>,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    Ok(Arc::new(r2_storage(
        config,
        bucket,
        prefix,
        account_id,
        credentials,
        extra_read_headers,
        extra_write_headers,
        legacy_rooted_keys,
    )?))
}

/// Build storage for a Cloudflare R2 bucket.
///
/// `extra_read_headers`/`extra_write_headers` are extra HTTP headers attached to
/// read/write requests respectively.
///
/// For `legacy_rooted_keys`, see [`S3Storage::new`]: `None` auto-detects the key
/// layout (the usual choice), `Some(true)` forces the legacy leading-slash layout,
/// and `Some(false)` forces the standard layout.
#[expect(clippy::too_many_arguments)]
pub fn r2_storage(
    config: S3Options,
    bucket: Option<String>,
    prefix: Option<String>,
    account_id: Option<String>,
    credentials: Option<S3Credentials>,
    extra_read_headers: Vec<(String, String)>,
    extra_write_headers: Vec<(String, String)>,
    legacy_rooted_keys: Option<bool>,
) -> StorageResult<S3Storage> {
    let (bucket, prefix) = match (bucket, prefix) {
        (Some(bucket), Some(prefix)) => (bucket, Some(prefix)),
        (None, Some(prefix)) => match prefix.split_once("/") {
            Some((bucket, prefix)) => (bucket.to_string(), Some(prefix.to_string())),
            None => (prefix, None),
        },
        (Some(bucket), None) => (bucket, None),
        (None, None) => {
            return Err(StorageErrorKind::R2ConfigurationError(
                "Either bucket or prefix must be provided.".to_string(),
            ))
            .capture();
        }
    };

    if config.endpoint_url.is_none() && account_id.is_none() {
        return Err(StorageErrorKind::R2ConfigurationError(
            "Either endpoint_url or account_id must be provided.".to_string(),
        ))
        .capture();
    }

    let mut config = config;
    if config.region.is_none() {
        config.region = Some("auto".to_string());
    }
    if config.endpoint_url.is_none() {
        config.endpoint_url =
            account_id.map(|x| format!("https://{x}.r2.cloudflarestorage.com"));
    }
    config.force_path_style = true;
    S3Storage::new(
        config,
        bucket,
        prefix,
        credentials.unwrap_or(S3Credentials::FromEnv),
        true,
        extra_read_headers,
        extra_write_headers,
        legacy_rooted_keys,
    )
}

/// Build storage for a Tigris bucket.
///
/// `extra_read_headers`/`extra_write_headers` are extra HTTP headers attached to
/// read/write requests. The required `X-Tigris-*` consistency headers take
/// precedence on a name conflict.
///
/// For `legacy_rooted_keys`, see [`S3Storage::new`]: `None` auto-detects the key
/// layout (the usual choice), `Some(true)` forces the legacy leading-slash layout,
/// and `Some(false)` forces the standard layout.
#[expect(clippy::too_many_arguments)]
pub fn new_tigris_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
    use_weak_consistency: bool,
    extra_read_headers: Vec<(String, String)>,
    extra_write_headers: Vec<(String, String)>,
    legacy_rooted_keys: Option<bool>,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    Ok(Arc::new(tigris_storage(
        config,
        bucket,
        prefix,
        credentials,
        use_weak_consistency,
        extra_read_headers,
        extra_write_headers,
        legacy_rooted_keys,
    )?))
}

/// Merge user-supplied headers with headers Icechunk must set itself. On a
/// case-insensitive name conflict the required header wins (user entries with a
/// colliding name are dropped), since the required headers carry correctness
/// guarantees (e.g. Tigris consistency).
fn merge_required_headers(
    user: Vec<(String, String)>,
    required: Vec<(String, String)>,
) -> Vec<(String, String)> {
    let mut merged: Vec<(String, String)> = user
        .into_iter()
        .filter(|(k, _)| !required.iter().any(|(rk, _)| rk.eq_ignore_ascii_case(k)))
        .collect();
    merged.extend(required);
    merged
}

/// Build storage for a Tigris bucket.
///
/// `extra_read_headers`/`extra_write_headers` are extra HTTP headers attached to
/// read/write requests. The required `X-Tigris-*` consistency headers take
/// precedence on a name conflict.
///
/// For `legacy_rooted_keys`, see [`S3Storage::new`]: `None` auto-detects the key
/// layout (the usual choice), `Some(true)` forces the legacy leading-slash layout,
/// and `Some(false)` forces the standard layout.
#[expect(clippy::too_many_arguments)]
pub fn tigris_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
    use_weak_consistency: bool,
    extra_read_headers: Vec<(String, String)>,
    extra_write_headers: Vec<(String, String)>,
    legacy_rooted_keys: Option<bool>,
) -> StorageResult<S3Storage> {
    let mut config = config;
    if config.endpoint_url.is_none() {
        config.endpoint_url = Some("https://t3.storage.dev".to_string());
    }
    let mut tigris_write_headers = Vec::with_capacity(2);
    let mut tigris_read_headers = Vec::with_capacity(3);

    if !use_weak_consistency {
        // TODO: Tigris will need more than this to offer good eventually consistent behavior
        // For example: we should use no-cache for branches and config file
        if let Some(region) = config.region.as_ref() {
            tigris_write_headers.push(("X-Tigris-Regions".to_string(), region.clone()));
            tigris_write_headers
                .push(("X-Tigris-Consistent".to_string(), "true".to_string()));

            tigris_read_headers.push(("X-Tigris-Regions".to_string(), region.clone()));
            tigris_read_headers
                .push(("Cache-Control".to_string(), "no-cache".to_string()));
            tigris_read_headers
                .push(("X-Tigris-Consistent".to_string(), "true".to_string()));
        } else {
            return Err(other_error("Tigris storage requires a region to provide full consistency. Either set the region for the bucket or use the read-only, eventually consistent storage by passing `use_weak_consistency=True` (experts only)".to_string()));
        }
    }
    S3Storage::new(
        config,
        bucket,
        prefix,
        credentials.unwrap_or(S3Credentials::FromEnv),
        !use_weak_consistency, // notice eventually consistent storage can't do writes
        merge_required_headers(extra_read_headers, tigris_read_headers),
        merge_required_headers(extra_write_headers, tigris_write_headers),
        legacy_rooted_keys,
    )
}

#[cfg(test)]
mod tests {
    use icechunk_macros::tokio_test;

    use super::*;

    // Load-bearing: a write-id match returns the read-back object's own etag,
    // not a clone of the previous version.
    #[test]
    fn classify_readback_match_returns_readback_etag() {
        let facts = HeadFacts::Found {
            stored_write_id: Some("W".to_string()),
            etag: Some("E1".to_string()),
        };
        assert_eq!(
            classify_readback(Some("W"), &facts),
            ReadbackOutcome::OurWrite { etag: "E1".to_string() }
        );
    }

    #[test]
    fn classify_readback_branches() {
        let other = HeadFacts::Found {
            stored_write_id: Some("OTHER".to_string()),
            etag: Some("E1".to_string()),
        };
        assert_eq!(classify_readback(Some("W"), &other), ReadbackOutcome::NotOurWrite);

        let unstamped =
            HeadFacts::Found { stored_write_id: None, etag: Some("E1".to_string()) };
        assert_eq!(
            classify_readback(Some("W"), &unstamped),
            ReadbackOutcome::NotOurWrite
        );

        let ours_no_etag =
            HeadFacts::Found { stored_write_id: Some("W".to_string()), etag: None };
        assert_eq!(
            classify_readback(Some("W"), &ours_no_etag),
            ReadbackOutcome::MissingEtag
        );

        assert_eq!(
            classify_readback(Some("W"), &HeadFacts::Absent),
            ReadbackOutcome::Absent
        );

        // We never stamped one: can't disambiguate, regardless of facts.
        let matching = HeadFacts::Found {
            stored_write_id: Some("W".to_string()),
            etag: Some("E1".to_string()),
        };
        assert_eq!(classify_readback(None, &matching), ReadbackOutcome::NotStamped);
    }

    #[tokio_test]
    async fn test_serialize_s3_storage() {
        let config = S3Options::default()
            .with_region("us-west-2")
            .with_endpoint_url("http://localhost:4200")
            .with_allow_http(true);
        let credentials = S3Credentials::Static(S3StaticCredentials {
            access_key_id: "access_key_id".to_string(),
            secret_access_key: "secret_access_key".to_string(),
            session_token: Some("session_token".to_string()),
            expires_after: None,
        });
        let storage = S3Storage::new(
            config,
            "bucket".to_string(),
            Some("prefix".to_string()),
            credentials,
            true,
            Vec::new(),
            Vec::new(),
            None,
        )
        .unwrap();

        let serialized = serde_json::to_string(&storage).unwrap();

        assert_eq!(
            serialized,
            r#"{"config":{"region":"us-west-2","endpoint_url":"http://localhost:4200","anonymous":false,"allow_http":true,"force_path_style":false,"network_stream_timeout_seconds":null,"requester_pays":false,"checksum_algorithm":null},"credentials":{"s3_credential_type":"static","access_key_id":"access_key_id","secret_access_key":"secret_access_key","session_token":"session_token","expires_after":null},"bucket":"bucket","prefix":"prefix","can_write":true,"extra_read_headers":[],"extra_write_headers":[],"key_layout":null}"#
        );

        let deserialized: S3Storage = serde_json::from_str(&serialized).unwrap();
        assert_eq!(storage.config, deserialized.config);
    }

    /// Extra headers are serialized on the struct (so they survive the pickle /
    /// distributed path), and round-trip intact.
    #[tokio_test]
    async fn test_serialize_s3_storage_with_headers() {
        let read_headers = vec![("x-amz-meta-reader".to_string(), "r".to_string())];
        let write_headers = vec![
            ("x-amz-acl".to_string(), "bucket-owner-full-control".to_string()),
            ("x-amz-meta-writer".to_string(), "w".to_string()),
        ];
        let storage = S3Storage::new(
            S3Options::default(),
            "bucket".to_string(),
            Some("prefix".to_string()),
            S3Credentials::FromEnv,
            true,
            read_headers.clone(),
            write_headers.clone(),
            None,
        )
        .unwrap();

        let serialized = serde_json::to_string(&storage).unwrap();
        assert!(serialized.contains("x-amz-acl"), "got: {serialized}");

        let deserialized: S3Storage = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.extra_read_headers, read_headers);
        assert_eq!(deserialized.extra_write_headers, write_headers);
    }

    /// Tigris consistency headers win over user-supplied headers on a
    /// case-insensitive name conflict; non-conflicting user headers survive.
    #[test]
    fn test_tigris_user_headers_merge() {
        let storage = tigris_storage(
            S3Options::default().with_region("iad"),
            "bucket".to_string(),
            Some("prefix".to_string()),
            Some(S3Credentials::FromEnv),
            false,
            vec![("x-amz-meta-reader".to_string(), "r".to_string())],
            vec![
                ("x-amz-acl".to_string(), "bucket-owner-full-control".to_string()),
                // collides (case-insensitively) with the required Tigris header
                ("x-tigris-regions".to_string(), "hijacked".to_string()),
            ],
            None,
        )
        .unwrap();

        // user read header preserved alongside the injected Tigris read headers
        assert!(
            storage
                .extra_read_headers
                .contains(&("x-amz-meta-reader".to_string(), "r".to_string()))
        );
        assert!(
            storage
                .extra_read_headers
                .iter()
                .any(|(k, v)| k == "X-Tigris-Regions" && v == "iad")
        );

        // user x-amz-acl preserved; the colliding x-tigris-regions override dropped
        assert!(storage.extra_write_headers.contains(&(
            "x-amz-acl".to_string(),
            "bucket-owner-full-control".to_string()
        )));
        assert!(!storage.extra_write_headers.iter().any(|(_, v)| v == "hijacked"));
        assert!(
            storage
                .extra_write_headers
                .iter()
                .any(|(k, v)| k == "X-Tigris-Regions" && v == "iad")
        );
    }

    fn storage_with_prefix(prefix: Option<&str>, legacy_rooted_keys: bool) -> S3Storage {
        S3Storage::new(
            S3Options::default(),
            "bucket".to_string(),
            prefix.map(str::to_string),
            S3Credentials::FromEnv,
            true,
            Vec::new(),
            Vec::new(),
            // map the helper's bool: force legacy when set, else auto-detect
            legacy_rooted_keys.then_some(true),
        )
        .unwrap()
    }

    /// empty prefix + Standard layout never produces a
    /// leading slash, for every kind of object.
    #[test]
    fn test_key_for_standard_empty_prefix() {
        let s = storage_with_prefix(Some(""), false);
        for relpath in
            ["chunks/abc123", "repo", "refs/branch.main/ref.json", "config.yaml"]
        {
            let key = s.key_for(KeyLayout::Standard, relpath);
            assert_eq!(key, relpath);
            assert!(!key.starts_with('/'), "key {key:?} must not start with a slash");
        }
    }

    #[test]
    fn test_key_for_standard_nonempty_prefix() {
        let s = storage_with_prefix(Some("foo"), false);
        assert_eq!(s.key_for(KeyLayout::Standard, "chunks/abc123"), "foo/chunks/abc123");
        // A trailing slash on the prefix is normalized away at construction.
        let s = storage_with_prefix(Some("foo/"), false);
        assert_eq!(s.key_for(KeyLayout::Standard, "chunks/abc123"), "foo/chunks/abc123");
    }

    /// `None` and `""` are the two ways #2239 was triggered; they must produce an
    /// identical empty prefix (they converge at `unwrap_or_default` in `new`).
    #[test]
    fn test_none_and_empty_prefix_are_equivalent() {
        let from_none = storage_with_prefix(None, false);
        let from_empty = storage_with_prefix(Some(""), false);
        for relpath in ["chunks/abc123", "repo"] {
            assert_eq!(
                from_none.key_for(KeyLayout::Standard, relpath),
                from_empty.key_for(KeyLayout::Standard, relpath),
            );
        }
    }

    /// A leading slash on the relpath is collapsed at the join, but an interior
    /// `//` is preserved
    #[test]
    fn test_list_prefix() {
        // leading slash stripped for each layout
        let s = storage_with_prefix(Some("foo"), false);
        assert_eq!(s.list_prefix(KeyLayout::Standard, "/chunks"), "foo/chunks");
        let s = storage_with_prefix(Some(""), false);
        assert_eq!(s.list_prefix(KeyLayout::Standard, "/chunks"), "chunks");
        let s = storage_with_prefix(Some(""), true);
        assert_eq!(s.list_prefix(KeyLayout::LegacyRoot, "/chunks"), "/chunks");

        // interior `//` is preserved, matching `key_for`
        let s = storage_with_prefix(Some("a//b"), false);
        assert_eq!(s.list_prefix(KeyLayout::Standard, "chunks/x"), "a//b/chunks/x");
        assert_eq!(
            s.list_prefix(KeyLayout::Standard, "chunks/x"),
            s.key_for(KeyLayout::Standard, "chunks/x"),
        );
    }

    /// `LegacyRoot` reproduces the exact pre-#2239 buggy layout, so we can keep
    /// reading repositories written by old clients.
    #[test]
    fn test_key_for_legacy_root_reproduces_bug() {
        let s = storage_with_prefix(Some(""), true);
        assert_eq!(s.key_for(KeyLayout::LegacyRoot, "chunks/abc123"), "/chunks/abc123");
        assert_eq!(s.key_for(KeyLayout::LegacyRoot, "repo"), "/repo");
    }

    #[test]
    fn test_legacy_rooted_keys_rejected_with_nonempty_prefix() {
        let err = S3Storage::new(
            S3Options::default(),
            "bucket".to_string(),
            Some("foo".to_string()),
            S3Credentials::FromEnv,
            true,
            Vec::new(),
            Vec::new(),
            Some(true),
        )
        .unwrap_err();
        assert!(err.to_string().contains("empty prefix"), "got: {err}");
    }

    #[test]
    fn test_decide_layout_four_way() {
        let seen = |clean, rooted| AnchorProbe { clean, rooted };
        // empty/new repo -> clean
        assert_eq!(decide_layout(&seen(false, false), "b").unwrap(), KeyLayout::Standard);
        // clean repo -> clean
        assert_eq!(decide_layout(&seen(true, false), "b").unwrap(), KeyLayout::Standard);
        // legacy rooted repo -> legacy
        assert_eq!(
            decide_layout(&seen(false, true), "b").unwrap(),
            KeyLayout::LegacyRoot
        );
        // mixed -> hard error
        let err = decide_layout(&seen(true, true), "mybucket").unwrap_err();
        assert!(err.to_string().contains("both"), "got: {err}");
        assert!(err.to_string().contains("mybucket"), "got: {err}");
    }

    #[test]
    fn test_layout_from_anchor_etags() {
        let et = |s: &str| Some(s.to_string());
        let probe =
            |clean: Option<String>, rooted: Option<String>| AnchorProbe { clean, rooted };
        let layout = |anchors: &[AnchorProbe<Option<String>>]| {
            layout_from_anchor_etags(anchors, "b")
        };

        // empty/new repo: nothing found -> clean
        assert_eq!(layout(&[probe(None, None)]).unwrap(), KeyLayout::Standard);
        // clean repo (clean key found, rooted key absent: AWS 404 or rustfs 400)
        assert_eq!(layout(&[probe(et("E"), None)]).unwrap(), KeyLayout::Standard);
        // genuine legacy rooted repo: only the rooted key exists
        assert_eq!(layout(&[probe(None, et("E"))]).unwrap(), KeyLayout::LegacyRoot);
        // normalizing store (MinIO): "/x" and "x" are the SAME object (equal ETag)
        // -> clean, NOT a spurious mixed-layout error (regression for #2239 probe).
        assert_eq!(layout(&[probe(et("E"), et("E"))]).unwrap(), KeyLayout::Standard);
        // genuinely distinct objects at both keys -> mixed, hard error
        assert!(layout(&[probe(et("CLEAN"), et("ROOTED"))]).is_err());
        // multi-anchor: V2 repo on a normalizing store (repo found+normalized,
        // refs anchor absent) -> clean
        assert_eq!(
            layout(&[probe(et("E"), et("E")), probe(None, None)]).unwrap(),
            KeyLayout::Standard
        );
    }

    /// Forcing the layout pre-seeds the cell and survives a serialize round-trip,
    /// so a deserialized (forked/pickled) storage inherits it without probing.
    #[test]
    fn test_forced_layout_serializes_and_is_inherited() {
        let s = storage_with_prefix(Some(""), true);
        assert_eq!(s.key_layout.get().copied(), Some(KeyLayout::LegacyRoot));
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains(r#""key_layout":"LegacyRoot""#), "got: {json}");
        let restored: S3Storage = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.key_layout.get().copied(), Some(KeyLayout::LegacyRoot));
    }
}
