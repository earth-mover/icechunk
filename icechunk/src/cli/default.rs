use crate::{
    cli::config::{RepoLocation, RepositoryDefinition, S3Credentials},
    config::S3Options,
    RepositoryConfig,
};

pub fn create_s3_repo() -> RepositoryDefinition {
    RepositoryDefinition::S3 {
        location: RepoLocation {
            bucket: "testbucket".to_string(),
            prefix: "testfolder".to_string(),
        },
        object_store_config: S3Options {
            region: Some("eu-west-1".to_string()),
            endpoint_url: None,
            anonymous: false,
            allow_http: false,
        },
        credentials: S3Credentials::FromEnv,
        config: RepositoryConfig::default(),
    }
}

pub fn create_local_repo() -> RepositoryDefinition {
    RepositoryDefinition::LocalFileSystem {
        path: std::path::PathBuf::from("testfolder"),
        config: RepositoryConfig::default(),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs::File};

    use serde_yaml_ng::{from_reader, to_writer};

    use crate::cli::config::{Repositories, RepositoryAlias};

    use super::*;

    #[test]
    fn test_generate_repos() {
        let s3_repo = create_s3_repo();
        let local_repo = create_local_repo();

        let mut repos = Repositories { repos: HashMap::new() };

        repos.repos.insert(RepositoryAlias("s3repo".to_string()), s3_repo);
        repos.repos.insert(RepositoryAlias("localrepo".to_string()), local_repo);

        let path = "default.yaml";
        let file = File::create(path).unwrap();
        to_writer(file, &repos).unwrap();
        let file = File::open(path).unwrap();
        let deserialized: Repositories = from_reader(file).unwrap();
        assert_eq!(deserialized, repos);
    }
}
