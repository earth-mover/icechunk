use std::{env, sync::Arc};

use chrono::Utc;
use icechunk::{
    Storage,
    config::{S3Credentials, S3Options, S3StaticCredentials},
    new_s3_storage,
    storage::{Settings, mk_client, new_r2_storage, new_tigris_storage},
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

    let storage: Arc<dyn Storage + Send + Sync> = new_s3_storage(
        S3Options::default()
            .with_region("us-east-1")
            .with_endpoint_url("http://localhost:4200")
            .with_allow_http(true)
            .with_force_path_style(true),
        "testbucket".to_string(),
        Some(prefix),
        Some(S3Credentials::Static(S3StaticCredentials {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token: None,
            expires_after: None,
        })),
        false,
    )?;
    Ok(storage)
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

    let storage: Arc<dyn Storage + Send + Sync> = new_tigris_storage(
        S3Options::default().with_region(region),
        bucket,
        Some(prefix),
        Some(credentials),
        false,
        false,
    )?;
    Ok(storage)
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

    let storage: Arc<dyn Storage + Send + Sync> = new_r2_storage(
        S3Options::default(),
        Some(bucket),
        Some(prefix),
        Some(env::var("R2_ACCOUNT_ID")?),
        Some(credentials),
        false,
    )?;
    Ok(storage)
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
    let storage: Arc<dyn Storage + Send + Sync> = new_s3_storage(
        get_aws_integration_options()?,
        get_aws_integration_bucket()?,
        Some(prefix),
        Some(get_aws_integration_credentials()?),
        false,
    )?;
    Ok(storage)
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
    pub(crate) fn rooted_storage(
        &self,
        legacy_rooted_keys: bool,
    ) -> Result<Arc<dyn Storage + Send + Sync>, Box<dyn std::error::Error>> {
        let prefix = Some(String::new());
        let creds = Some(self.credentials.clone());
        let storage: Arc<dyn Storage + Send + Sync> = match self.kind {
            RealStoreKind::Aws => new_s3_storage(
                self.options.clone(),
                self.bucket.clone(),
                prefix,
                creds,
                legacy_rooted_keys,
            )?,
            RealStoreKind::R2 => new_r2_storage(
                self.options.clone(),
                Some(self.bucket.clone()),
                prefix,
                None, // endpoint already resolved into options
                creds,
                legacy_rooted_keys,
            )?,
            RealStoreKind::Tigris => new_tigris_storage(
                self.options.clone(),
                self.bucket.clone(),
                prefix,
                creds,
                false,
                legacy_rooted_keys,
            )?,
        };
        Ok(storage)
    }

    /// Delete every object whose key starts with `/`. A legacy-rooted repository
    /// lives entirely under `/...`, so this makes the shared bucket reusable across
    /// runs without disturbing other tests' (non-slash-prefixed) objects.
    pub(crate) async fn cleanup_rooted_keys(
        &self,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = mk_client(
            &self.options,
            self.credentials.clone(),
            vec![],
            vec![],
            &Settings::default(),
        )
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

/// `None` when the AWS env vars are not set (so the caller can skip).
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
