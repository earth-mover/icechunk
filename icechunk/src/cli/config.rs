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