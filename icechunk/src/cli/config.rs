use std::{collections::HashMap, path::PathBuf, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::config::{
    AzureCredentials, GcsCredentials, RepositoryConfig, S3Credentials, S3Options,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RepoLocation {
    pub bucket: String,
    pub prefix: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AzureRepoLocation {
    pub account: String,
    pub container: String,
    pub prefix: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum RepositoryDefinition {
    LocalFileSystem {
        path: PathBuf,
        config: RepositoryConfig,
    },
    S3 {
        location: RepoLocation,
        object_store_config: S3Options,
        credentials: S3Credentials,
        config: RepositoryConfig,
    },
    Tigris {
        location: RepoLocation,
        object_store_config: S3Options,
        credentials: S3Credentials,
        config: RepositoryConfig,
    },
    Azure {
        location: AzureRepoLocation,
        object_store_config: HashMap<String, String>,
        credentials: AzureCredentials,
        config: RepositoryConfig,
    },
    GCS {
        location: RepoLocation,
        object_store_config: HashMap<String, String>,
        credentials: GcsCredentials,
        config: RepositoryConfig,
    },
}

impl RepositoryDefinition {
    pub fn get_config(&self) -> &RepositoryConfig {
        match self {
            RepositoryDefinition::LocalFileSystem { config, .. }
            | RepositoryDefinition::S3 { config, .. }
            | RepositoryDefinition::Tigris { config, .. }
            | RepositoryDefinition::Azure { config, .. }
            | RepositoryDefinition::GCS { config, .. } => config,
        }
    }
}

#[derive(Debug, Hash, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct RepositoryAlias(pub String);

impl FromStr for RepositoryAlias {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(RepositoryAlias(s.to_string()))
    }
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct CliConfig {
    pub repos: HashMap<RepositoryAlias, RepositoryDefinition>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::env::temp_dir;
    use std::fs::File;

    use serde_yaml_ng::{from_reader, to_writer};

    use crate::config::S3Options;

    use super::*;

    #[icechunk_macros::test]
    fn test_serialization() {
        let location = RepoLocation {
            bucket: "my-bucket".to_string(),
            prefix: Some("my-prefix".to_string()),
        };
        let object_store_config = S3Options {
            region: Some("us-west-2".to_string()),
            endpoint_url: None,
            anonymous: false,
            allow_http: false,
            force_path_style: false,
        };
        let credentials = S3Credentials::FromEnv;
        let repo_config = RepositoryConfig::default();

        let repo_def = RepositoryDefinition::S3 {
            location,
            object_store_config,
            credentials,
            config: repo_config,
        };

        let mut _repos = CliConfig { repos: HashMap::new() };

        let alias = RepositoryAlias("my-repo".to_string());
        _repos.repos.insert(alias.clone(), repo_def);

        // Assert: serde round-trip
        let serialized = serde_yaml_ng::to_string(&_repos).unwrap();
        let deserialized: CliConfig = serde_yaml_ng::from_str(&serialized).unwrap();
        assert!(matches!(deserialized, _repos));

        // Assert: file round-trip
        let path = temp_dir().join("test_serialization.yaml");
        let file = File::create(&path).unwrap();
        to_writer(file, &_repos).unwrap();
        let file = File::open(path).unwrap();
        let deserialized: CliConfig = from_reader(file).unwrap();
        assert!(matches!(deserialized, _repos));
    }
}
