use chrono::{DateTime, Utc};
use std::{collections::HashMap, str::FromStr};

use serde::{Deserialize, Serialize};

use serde_yaml_ng;

use crate::config::{ObjectStoreConfig, RepositoryConfig, S3StaticCredentials};

// Redefine to remove the Refreshable field
#[derive(Clone, Debug, Deserialize, Serialize, Default, PartialEq, Eq)]
pub enum S3Credentials {
    #[default]
    FromEnv,
    Anonymous,
    Static(S3StaticCredentials),
}

impl From<S3Credentials> for crate::config::S3Credentials {
    fn from(s3_credentials: S3Credentials) -> Self {
        match s3_credentials {
            S3Credentials::Anonymous => crate::config::S3Credentials::Anonymous,
            S3Credentials::FromEnv => crate::config::S3Credentials::FromEnv,
            S3Credentials::Static(credentials) => {
                crate::config::S3Credentials::Static(credentials)
            }
        }
    }
}

// TODO (Daniel): Add the rest of the fields
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum Credentials {
    None,
    S3(S3Credentials),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepoLocation {
    pub bucket: String,
    pub prefix: String,
}

// TODO (Daniel): Is there a way to restrict valid combinations (e.g. S3 with S3Credentials)?
// TODO (Daniel): Have top-level object store type for easy matching?
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepositoryDefinition {
    // LocalFileSystem sets its root path in the object store config
    pub location: Option<RepoLocation>,
    pub object_store_config: ObjectStoreConfig,
    pub credentials: Credentials,
    pub config: RepositoryConfig,
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct RepositoryAlias(pub String);

impl FromStr for RepositoryAlias {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(RepositoryAlias(s.to_string()))
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Repositories {
    pub repos: HashMap<RepositoryAlias, RepositoryDefinition>,
}

#[cfg(test)]
mod tests {
    use crate::config::S3Options;

    use super::*;

    #[test]
    fn test_serialization() {
        let location = Some(RepoLocation {
            bucket: "my-bucket".to_string(),
            prefix: "my-prefix".to_string(),
        });
        let object_store_config = ObjectStoreConfig::S3(S3Options {
            region: Some("us-west-2".to_string()),
            endpoint_url: None,
            anonymous: false,
            allow_http: false,
        });
        let credentials = Credentials::S3(S3Credentials::FromEnv);
        let repo_config = RepositoryConfig::default();

        let repo_def = RepositoryDefinition {
            location,
            object_store_config,
            credentials,
            config: repo_config,
        };

        let mut repos = Repositories { repos: HashMap::new() };

        let alias = RepositoryAlias("my-repo".to_string());
        repos.repos.insert(alias.clone(), repo_def);

        // Assert: serde round-trip
        let serialized = serde_yaml_ng::to_string(&repos).unwrap();
        let deserialized: Repositories = serde_yaml_ng::from_str(&serialized).unwrap();
        assert_eq!(deserialized, repos);

        // Assert: file round-trip
        let path = "test.yaml";
        let file = std::fs::File::create(path).unwrap();
        serde_yaml_ng::to_writer(file, &repos).unwrap();
        let file = std::fs::File::open(path).unwrap();
        let deserialized: Repositories = serde_yaml_ng::from_reader(file).unwrap();
        assert_eq!(deserialized, repos);
    }
}
