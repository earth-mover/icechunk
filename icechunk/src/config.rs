use serde::{Deserialize, Serialize};

use crate::virtual_chunks::{
    mk_default_containers, ContainerName, VirtualChunkContainer,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct S3CompatibleOptions {
    pub region: Option<String>,
    pub endpoint_url: Option<String>,
    pub anonymous: bool,
    pub allow_http: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ObjectStoreConfig {
    InMemory,
    LocalFileSystem,
    S3Compatible(S3CompatibleOptions),
    S3(S3CompatibleOptions),
    GCS {
        // TODO:
    },
    Azure {
        // TODO:
    },
    Tigris {
        // TODO:
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepositoryConfig {
    // Chunks smaller than this will be stored inline in the manifst
    pub inline_chunk_threshold_bytes: u16,
    // Unsafely overwrite refs on write. This is not recommended, users should only use it at their
    // own risk in object stores for which we don't support write-object-if-not-exists. There is
    // the possibility of race conditions if this variable is set to true and there are concurrent
    // commit attempts.
    pub unsafe_overwrite_refs: bool,

    pub virtual_chunk_containers: Vec<VirtualChunkContainer>,
}

impl Default for RepositoryConfig {
    fn default() -> Self {
        Self {
            inline_chunk_threshold_bytes: 512,
            unsafe_overwrite_refs: false,
            virtual_chunk_containers: mk_default_containers(),
        }
    }
}

impl RepositoryConfig {
    pub fn add_virtual_chunk_container(&mut self, cont: VirtualChunkContainer) {
        self.virtual_chunk_containers.push(cont);
    }

    pub fn virtual_chunk_containers(&self) -> &Vec<VirtualChunkContainer> {
        &self.virtual_chunk_containers
    }

    pub fn clear_virtual_chunk_containers(&mut self) {
        self.virtual_chunk_containers.clear();
    }

    pub fn update_virtual_chunk_container(
        &mut self,
        name: ContainerName,
    ) -> Option<&mut VirtualChunkContainer> {
        self.virtual_chunk_containers.iter_mut().find(|cont| cont.name == name)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct StaticCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(tag = "type")]
pub enum Credentials {
    #[default]
    #[serde(rename = "from_env")]
    FromEnv,
    #[serde(rename = "none")]
    None,
    #[serde(rename = "static")]
    Static(StaticCredentials),
}
