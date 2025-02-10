
// Requirements
// Should be able to get the options from
// - env
// - config file
// - command line

// Usecase: Just create a repo in s3://bucket/prefix, get env vars + defaults
//
// Usecase: Want to precisely control the credentials

// Question: Re-use S3Credentials?

// Requirements: Repository aliases

// Details that are needed
// def s3_storage(
//     *,
//     bucket: str,
//     prefix: str | None,
//     region: str | None = None,
//     endpoint_url: str | None = None,
//     allow_http: bool = False,
//     access_key_id: str | None = None,
//     secret_access_key: str | None = None,
//     session_token: str | None = None,
//     expires_after: datetime | None = None,
//     anonymous: bool | None = None,
//     from_env: bool | None = None,
//     get_credentials: Callable[[], S3StaticCredentials] | None = None,
// ) -> Storage:

// NEED REPO CONFIG, TOO (for creation)

use std::collections::HashMap;

use crate::config::{RepositoryConfig, ObjectStoreConfig, Credentials}

pub struct RepoLocation {
    bucket: String,
    prefix: String,
}

pub struct RepositoryDefinition {
    location: RepoLocation,
    object_store_config: ObjectStoreConfig,
    credentials: Credentials,
    config: RepositoryConfig,
}

struct RepositoryAlias(String);

pub struct Repositories {
    repos: HashMap<RepositoryAlias, RepositoryDefinition>,
}