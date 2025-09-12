use std::{path::Path, sync::Arc};

use icechunk::{
    Repository, Storage, config::S3Credentials, new_local_filesystem_storage,
    new_s3_storage,
};
use icechunk_export::{ProgressBars, export};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let repo_path = Path::new("./icechunk-python/tests/data/test-repo-v2");
    let source_storage = new_local_filesystem_storage(repo_path).await?;
    // let source_storage = new_s3_storage(
    //     icechunk::config::S3Options {
    //         region: Some("us-east-1".to_string()),
    //         endpoint_url: None,
    //         anonymous: false,
    //         allow_http: false,
    //         force_path_style: false,
    //         network_stream_timeout_seconds: None,
    //     },
    //     "icechunk-public-data".to_string(),
    //     Some("v1/glad".to_string()),
    //     Some(S3Credentials::FromEnv),
    // )?;

    let source = Repository::open(None, source_storage, Default::default()).await?;
    let destination_path = Path::new("/tmp/test-export");
    let destination = new_local_filesystem_storage(destination_path).await?;
    export(
        &source,
        destination,
        &icechunk_export::VersionSelection::AllHistory,
        Arc::new(ProgressBars::new()),
        100,
    )
    .await?;
    println!("done");

    Ok(())
}
