use std::{collections::HashMap, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::virtual_chunks::{
    mk_default_containers, ContainerName, VirtualChunkContainer,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct S3Options {
    pub region: Option<String>,
    pub endpoint_url: Option<String>,
    pub anonymous: bool,
    pub allow_http: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ObjectStoreConfig {
    InMemory,
    LocalFileSystem(PathBuf),
    S3Compatible(S3Options),
    S3(S3Options),
    Gcs(Option<Vec<(String, String)>>),
    Azure {
        // TODO:
    },
    Tigris {
        // TODO:
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepositoryConfig {
    /// Chunks smaller than this will be stored inline in the manifst
    pub inline_chunk_threshold_bytes: u16,
    /// Unsafely overwrite refs on write. This is not recommended, users should only use it at their
    /// own risk in object stores for which we don't support write-object-if-not-exists. There is
    /// the possibility of race conditions if this variable is set to true and there are concurrent
    /// commit attempts.
    pub unsafe_overwrite_refs: bool,

    /// Concurrency used by the get_partial_values operation to fetch different keys in parallel
    pub get_partial_values_concurrency: u16,

    pub virtual_chunk_containers: HashMap<ContainerName, VirtualChunkContainer>,
}

impl Default for RepositoryConfig {
    fn default() -> Self {
        Self {
            inline_chunk_threshold_bytes: 512,
            unsafe_overwrite_refs: false,
            get_partial_values_concurrency: 10,
            virtual_chunk_containers: mk_default_containers(),
        }
    }
}

impl RepositoryConfig {
    pub fn set_virtual_chunk_container(&mut self, cont: VirtualChunkContainer) {
        self.virtual_chunk_containers.insert(cont.name.clone(), cont);
    }

    pub fn virtual_chunk_containers(
        &self,
    ) -> impl Iterator<Item = &VirtualChunkContainer> {
        self.virtual_chunk_containers.values()
    }

    pub fn clear_virtual_chunk_containers(&mut self) {
        self.virtual_chunk_containers.clear();
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct S3StaticCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum GcsCredentials {
    ServiceAccount(PathBuf),
    ServiceAccountKey(String),
    ApplicationCredentials(PathBuf),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(tag = "type")]
pub enum S3Credentials {
    #[default]
    #[serde(rename = "from_env")]
    FromEnv,
    #[serde(rename = "none")]
    DontSign,
    #[serde(rename = "static")]
    Static(S3StaticCredentials),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum Credentials {
    S3(S3Credentials),
    #[serde(rename = "gcs")]
    Gcs(GcsCredentials),
}
