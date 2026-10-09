pub(crate) mod capture;

use std::{env, sync::Arc};

use chrono::{DateTime, TimeDelta, Utc};
use futures::TryStreamExt as _;
use icechunk::{
    Repository, Storage,
    config::{S3Credentials, S3Options, S3StaticCredentials},
    format::SnapshotId,
    storage::{Flavor, ListInfo, S3Storage, S3StorageBuilder, Settings, mk_client},
};

pub(crate) enum Permission {
    #[expect(dead_code)]
    ReadOnly, // GetObject
    Modify, // {Get,Put,Delete}Object, ListBucket
}

impl Permission {
    pub(crate) fn keys(&self) -> (&str, &str) {
        match self {
            Permission::Modify => ("modify", "modifydata"),
            Permission::ReadOnly => ("readonly", "basicuser"),
        }
    }
}

pub(crate) fn make_minio_integration_storage(
    prefix: String,
    permission: &Permission,
) -> Result<Arc<dyn Storage + Send + Sync>, Box<dyn std::error::Error>> {
    let (access_key_id, secret_access_key) = permission.keys();

    let storage = S3Storage::s3(
        S3Options::default()
            .with_region("us-east-1")
            .with_endpoint_url("http://localhost:4200")
            .with_allow_http(true)
            .with_force_path_style(true),
        "testbucket".to_string(),
    )
    .prefix(prefix)
    .credentials(S3Credentials::Static(S3StaticCredentials {
        access_key_id: access_key_id.into(),
        secret_access_key: secret_access_key.into(),
        session_token: None,
        expires_after: None,
    }))
    .execute()?;
    Ok(Arc::new(storage))
}

pub(crate) fn make_tigris_integration_storage(
    prefix: String,
) -> Result<Arc<dyn Storage + Send + Sync>, Box<dyn std::error::Error>> {
    let credentials = S3Credentials::Static(S3StaticCredentials {
        access_key_id: env::var("TIGRIS_ACCESS_KEY_ID")?,
        secret_access_key: env::var("TIGRIS_SECRET_ACCESS_KEY")?,
        session_token: None,
        expires_after: None,
    });
    let bucket = env::var("TIGRIS_BUCKET")?;
    let region = env::var("TIGRIS_REGION")?;

    let storage = S3Storage::tigris(S3Options::default().with_region(region), bucket)
        .prefix(prefix)
        .weak_consistency(false)
        .credentials(credentials)
        .execute()?;
    Ok(Arc::new(storage))
}

pub(crate) fn make_r2_integration_storage(
    prefix: String,
) -> Result<Arc<dyn Storage + Send + Sync>, Box<dyn std::error::Error>> {
    let credentials = S3Credentials::Static(S3StaticCredentials {
        access_key_id: env::var("R2_ACCESS_KEY_ID")?,
        secret_access_key: env::var("R2_SECRET_ACCESS_KEY")?,
        session_token: None,
        expires_after: None,
    });
    let bucket = env::var("R2_BUCKET")?;

    let storage = S3Storage::r2(S3Options::default())
        .bucket(bucket)
        .prefix(prefix)
        .account_id(env::var("R2_ACCOUNT_ID")?)
        .credentials(credentials)
        .execute()?;
    Ok(Arc::new(storage))
}

pub(crate) fn make_hf_integration_storage(
    prefix: String,
) -> Result<Arc<dyn Storage + Send + Sync>, Box<dyn std::error::Error>> {
    let credentials = S3Credentials::Static(S3StaticCredentials {
        access_key_id: env::var("HF_ACCESS_KEY_ID")?,
        secret_access_key: env::var("HF_SECRET_ACCESS_KEY")?,
        session_token: None,
        expires_after: None,
    });
    let bucket = env::var("HF_BUCKET")?;

    let storage = S3Storage::hf(S3Options::default(), bucket, &env::var("HF_NAMESPACE")?)
        .prefix(prefix)
        .credentials(credentials)
        .execute()?;
    Ok(Arc::new(storage))
}

pub(crate) fn get_aws_integration_bucket() -> Result<String, Box<dyn std::error::Error>> {
    Ok(env::var("AWS_BUCKET")?)
}

pub(crate) fn get_aws_integration_region() -> Result<String, Box<dyn std::error::Error>> {
    Ok(env::var("AWS_REGION")?)
}

pub(crate) fn get_aws_integration_credentials()
-> Result<S3Credentials, Box<dyn std::error::Error>> {
    let credentials = S3Credentials::Static(S3StaticCredentials {
        access_key_id: env::var("AWS_ACCESS_KEY_ID")?,
        secret_access_key: env::var("AWS_SECRET_ACCESS_KEY")?,
        session_token: None,
        expires_after: None,
    });
    Ok(credentials)
}

pub(crate) fn get_aws_integration_options()
-> Result<S3Options, Box<dyn std::error::Error>> {
    let res = S3Options::default().with_region(get_aws_integration_region()?);
    Ok(res)
}

pub(crate) fn make_aws_integration_storage(
    prefix: String,
) -> Result<Arc<dyn Storage + Send + Sync>, Box<dyn std::error::Error>> {
    let storage =
        S3Storage::s3(get_aws_integration_options()?, get_aws_integration_bucket()?)
            .prefix(prefix)
            .credentials(get_aws_integration_credentials()?)
            .execute()?;
    Ok(Arc::new(storage))
}

/// A real (non-local) object store configured from environment variables, used by
/// the `#[ignore]` integration tests that run in the nightly / `workflow_dispatch`
/// CI job. `options`/`credentials` are the *resolved* values (endpoint, region,
/// path-style already filled in) so they drive both the icechunk storage and a raw
/// client for cleanup.
pub(crate) struct RealStore {
    options: S3Options,
    credentials: S3Credentials,
    bucket: String,
    kind: RealStoreKind,
}

pub(crate) enum RealStoreKind {
    Aws,
    R2,
    Tigris,
}

impl RealStore {
    /// Native-S3 storage at the bucket root (empty prefix), optionally forcing the
    /// legacy leading-slash layout. Goes through each store's real constructor.
    ///
    /// Empty-prefix creation is normally refused; these tests deliberately need it,
    /// so the escape hatch is applied before erasing the concrete storage type.
    pub(crate) fn rooted_storage(
        &self,
        legacy_rooted_keys: bool,
    ) -> Result<Arc<dyn Storage + Send + Sync>, Box<dyn std::error::Error>> {
        let storage = match self.kind {
            RealStoreKind::Aws => self
                .rooted(
                    S3Storage::s3(self.options.clone(), self.bucket.clone()),
                    legacy_rooted_keys,
                )
                .execute()?,
            // endpoint already resolved into options
            RealStoreKind::R2 => self
                .rooted(
                    S3Storage::r2(self.options.clone()).bucket(self.bucket.clone()),
                    legacy_rooted_keys,
                )
                .execute()?,
            RealStoreKind::Tigris => self
                .rooted(
                    S3Storage::tigris(self.options.clone(), self.bucket.clone())
                        .weak_consistency(false),
                    legacy_rooted_keys,
                )
                .execute()?,
        };
        Ok(Arc::new(storage.unsafe_allow_empty_prefix_creation()))
    }

    /// Empty prefix and the store credentials; `legacy_rooted_keys` forces the legacy layout.
    fn rooted<F: Flavor>(
        &self,
        builder: S3StorageBuilder<F>,
        legacy_rooted_keys: bool,
    ) -> S3StorageBuilder<F> {
        let builder = builder.prefix(String::new()).credentials(self.credentials.clone());
        if legacy_rooted_keys { builder.legacy_rooted_keys(true) } else { builder }
    }

    pub(crate) fn options(&self) -> &S3Options {
        &self.options
    }

    pub(crate) fn credentials(&self) -> &S3Credentials {
        &self.credentials
    }

    pub(crate) fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Native-S3 storage at `prefix` carrying the given write headers, through
    /// each store's real constructor. Used by the header round-trip tests.
    pub(crate) fn write_header_storage(
        &self,
        prefix: String,
        write_headers: Vec<(String, String)>,
    ) -> Result<Arc<dyn Storage + Send + Sync>, Box<dyn std::error::Error>> {
        let creds = self.credentials.clone();
        let storage = match self.kind {
            RealStoreKind::Aws => {
                S3Storage::s3(self.options.clone(), self.bucket.clone())
                    .prefix(prefix)
                    .credentials(creds)
                    .write_headers(write_headers)
                    .execute()?
            }
            // endpoint already resolved into options
            RealStoreKind::R2 => S3Storage::r2(self.options.clone())
                .bucket(self.bucket.clone())
                .prefix(prefix)
                .credentials(creds)
                .write_headers(write_headers)
                .execute()?,
            RealStoreKind::Tigris => {
                S3Storage::tigris(self.options.clone(), self.bucket.clone())
                    .prefix(prefix)
                    .weak_consistency(false)
                    .credentials(creds)
                    .write_headers(write_headers)
                    .execute()?
            }
        };
        Ok(Arc::new(storage))
    }

    /// Delete every object whose key starts with `/`. A legacy-rooted repository
    /// lives entirely under `/...`, so this makes the shared bucket reusable across
    /// runs without disturbing other tests' (non-slash-prefixed) objects.
    pub(crate) async fn cleanup_rooted_keys(
        &self,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client =
            mk_client(&self.options, self.credentials.clone(), &Settings::default())
                .await;
        let resp =
            client.list_objects_v2().bucket(&self.bucket).prefix("/").send().await?;
        for obj in resp.contents() {
            if let Some(key) = obj.key() {
                client.delete_object().bucket(&self.bucket).key(key).send().await?;
            }
        }
        Ok(())
    }
}

/// `None` when the AWS env vars are not set.
pub(crate) fn aws_real_store() -> Option<RealStore> {
    let bucket = env::var("AWS_BUCKET").ok().filter(|s| !s.is_empty())?;
    Some(RealStore {
        options: S3Options::default().with_region(env::var("AWS_REGION").ok()?),
        credentials: S3Credentials::Static(S3StaticCredentials {
            access_key_id: env::var("AWS_ACCESS_KEY_ID").ok()?,
            secret_access_key: env::var("AWS_SECRET_ACCESS_KEY").ok()?,
            session_token: None,
            expires_after: None,
        }),
        bucket,
        kind: RealStoreKind::Aws,
    })
}

/// `None` when the R2 env vars are not set.
pub(crate) fn r2_real_store() -> Option<RealStore> {
    let bucket = env::var("R2_BUCKET").ok().filter(|s| !s.is_empty())?;
    let account_id = env::var("R2_ACCOUNT_ID").ok()?;
    Some(RealStore {
        options: S3Options::default()
            .with_region("auto")
            .with_endpoint_url(format!("https://{account_id}.r2.cloudflarestorage.com"))
            .with_force_path_style(true),
        credentials: S3Credentials::Static(S3StaticCredentials {
            access_key_id: env::var("R2_ACCESS_KEY_ID").ok()?,
            secret_access_key: env::var("R2_SECRET_ACCESS_KEY").ok()?,
            session_token: None,
            expires_after: None,
        }),
        bucket,
        kind: RealStoreKind::R2,
    })
}

/// `None` when the Tigris env vars are not set.
pub(crate) fn tigris_real_store() -> Option<RealStore> {
    let bucket = env::var("TIGRIS_BUCKET").ok().filter(|s| !s.is_empty())?;
    Some(RealStore {
        options: S3Options::default()
            .with_region(env::var("TIGRIS_REGION").ok()?)
            .with_endpoint_url("https://t3.storage.dev"),
        credentials: S3Credentials::Static(S3StaticCredentials {
            access_key_id: env::var("TIGRIS_ACCESS_KEY_ID").ok()?,
            secret_access_key: env::var("TIGRIS_SECRET_ACCESS_KEY").ok()?,
            session_token: None,
            expires_after: None,
        }),
        bucket,
        kind: RealStoreKind::Tigris,
    })
}

pub(crate) fn get_random_prefix(base: &str) -> String {
    let suffix: u64 = rand::random();
    format!("{}_{}_{}", base, Utc::now().timestamp_micros(), suffix)
}

/// Cutoff past every listed snapshot, from the store clock.
/// The extra second covers whole-second listings.
pub(crate) async fn cutoff_after_all_listed(
    repo: &Repository,
) -> Result<DateTime<Utc>, Box<dyn std::error::Error>> {
    let listed = listed_snapshots(repo).await?;
    let newest = listed.iter().map(|s| s.created_at).max().expect("snapshots listed");
    Ok(newest + TimeDelta::seconds(1))
}

pub(crate) async fn listed_snapshots(
    repo: &Repository,
) -> Result<Vec<ListInfo<SnapshotId>>, Box<dyn std::error::Error>> {
    Ok(repo.asset_manager().list_snapshots().await?.try_collect().await?)
}
