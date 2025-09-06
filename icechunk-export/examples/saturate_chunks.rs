use std::path::Path;

use icechunk_export::test_saturation;

#[tokio::main]
async fn main() {
    //let path = Path::new("/tmp/chunk_ids.txt");
    let args: Vec<String> = std::env::args().collect();
    let path = Path::new(args[1].as_str());
    let max_concurrent = args[2].parse().unwrap();
    let copy_chunks = args[3].parse().unwrap();
    test_saturation(max_concurrent, copy_chunks, path).await.unwrap();
}
