use std::collections::HashMap;

use serde_yaml_ng;

use crate::{
    cli::config::{
        Credentials, RepoLocation, Repositories, RepositoryAlias, RepositoryDefinition,
        S3Credentials,
    },
    config::S3Options,
    ObjectStoreConfig, RepositoryConfig,
};

// write a function for the repo below

pub fn create_s3_repo() -> RepositoryDefinition {
    let location = Some(RepoLocation {
        bucket: "testbucket".to_string(),
        prefix: "testfolder".to_string(),
    });
    let object_store_config = ObjectStoreConfig::S3(S3Options {
        region: Some("eu-west-1".to_string()),
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

    return repo_def;
}

pub fn create_local_repo() -> RepositoryDefinition {
    let location = None;
    let path = std::path::PathBuf::from("testfolder");

    let object_store_config = ObjectStoreConfig::LocalFileSystem(path);
    let credentials = Credentials::None;
    let repo_config = RepositoryConfig::default();

    let repo_def = RepositoryDefinition {
        location,
        object_store_config,
        credentials,
        config: repo_config,
    };

    return repo_def;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_repos() {
        let s3_repo = create_s3_repo();
        let local_repo = create_local_repo();

        let mut repos = Repositories { repos: HashMap::new() };

        repos.repos.insert(RepositoryAlias("s3repo".to_string()), s3_repo);
        repos.repos.insert(RepositoryAlias("localrepo".to_string()), local_repo);

        let path = "default.yaml";
        let file = std::fs::File::create(path).unwrap();
        serde_yaml_ng::to_writer(file, &repos).unwrap();
        let file = std::fs::File::open(path).unwrap();
        let deserialized: Repositories = serde_yaml_ng::from_reader(file).unwrap();
        assert_eq!(deserialized, repos);
    }
}
