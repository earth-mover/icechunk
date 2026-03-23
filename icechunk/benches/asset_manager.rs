use std::sync::Arc;

use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group};
use icechunk::asset_manager::AssetManager;
use icechunk::format::format_constants::SpecVersionBin;
use icechunk::format::manifest::{ChunkInfo, ChunkPayload, Manifest};
use icechunk::format::snapshot::{ArrayShape, NodeData, NodeSnapshot, Snapshot};
use icechunk::format::{ChunkIndices, IcechunkFormatError, ManifestId, NodeId, Path};
use icechunk::storage::{self, new_local_filesystem_storage};
use tempfile::TempDir;
use tokio::runtime::Runtime;

fn make_snapshot(num_nodes: usize) -> Arc<Snapshot> {
    let user_data = Bytes::from_static(br#"{"this":"node"}"#);
    let mut nodes: Vec<Result<NodeSnapshot, IcechunkFormatError>> =
        Vec::with_capacity(num_nodes);

    // Root group at "/"
    nodes.push(Ok(NodeSnapshot {
        id: NodeId::random(),
        path: Path::root(),
        user_data: user_data.clone(),
        node_data: NodeData::Group,
    }));

    // Remaining nodes are arrays under /group/array_NNNN
    for i in 0..num_nodes - 1 {
        let path: Path = format!("/group/array_{i:04}").try_into().unwrap();
        let shape = ArrayShape::new(vec![(100, 10)]).unwrap();
        nodes.push(Ok(NodeSnapshot {
            id: NodeId::random(),
            path,
            user_data: user_data.clone(),
            node_data: NodeData::Array {
                shape,
                dimension_names: None,
                manifests: vec![],
            },
        }));
    }

    Arc::new(
        Snapshot::from_iter(
            None,
            None,
            SpecVersionBin::current(),
            "bench snapshot",
            None,
            vec![],
            None,
            nodes,
        )
        .unwrap(),
    )
}

fn benchmark_write_new_snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_new_snapshot");

    let rt = Runtime::new().unwrap();

    let tmp_dir = TempDir::new().unwrap();
    let storage = rt.block_on(new_local_filesystem_storage(tmp_dir.path())).unwrap();
    let settings =
        storage::Settings { unsafe_use_metadata: Some(false), ..Default::default() };
    let asset_manager = AssetManager::new_no_cache(
        storage,
        settings,
        SpecVersionBin::current(),
        3, // compression level
        16,
    );

    for num_nodes in [1000, 100_000] {
        group.throughput(Throughput::Elements(num_nodes as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_nodes),
            &num_nodes,
            |b, &num_nodes| {
                b.iter_batched(
                    || make_snapshot(num_nodes),
                    |snapshot| {
                        rt.block_on(async {
                            asset_manager.write_snapshot(snapshot).await.unwrap();
                        });
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn make_manifest(num_chunks: u32, inline_size: usize) -> Arc<Manifest> {
    let node_id = NodeId::random();
    let manifest_id = ManifestId::random();
    let inline_data = Bytes::from(vec![0xABu8; inline_size]);
    let chunks: Vec<ChunkInfo> = (0..num_chunks)
        .map(|i| ChunkInfo {
            node: node_id.clone(),
            coord: ChunkIndices(vec![i]),
            payload: ChunkPayload::Inline(inline_data.clone()),
        })
        .collect();
    Arc::new(Manifest::from_sorted_vec(&manifest_id, chunks, None).unwrap().unwrap())
}

fn benchmark_write_new_manifest(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_new_manifest");
    group.sample_size(20).sampling_mode(criterion::SamplingMode::Flat);

    let rt = Runtime::new().unwrap();

    let num_chunks = 20_000u32;
    let inline_size = 1024; // 1KB per chunk → ~10MB total inline data

    let tmp_dir = TempDir::new().unwrap();
    let storage = rt.block_on(new_local_filesystem_storage(tmp_dir.path())).unwrap();
    let settings =
        storage::Settings { unsafe_use_metadata: Some(false), ..Default::default() };
    let asset_manager =
        AssetManager::new_no_cache(storage, settings, SpecVersionBin::current(), 3, 16);

    group.throughput(Throughput::Bytes(num_chunks as u64 * inline_size as u64));
    group.bench_function(BenchmarkId::new("inline", num_chunks), |b| {
        b.iter_batched(
            || make_manifest(num_chunks, inline_size),
            |manifest| {
                rt.block_on(async {
                    asset_manager.write_manifest(manifest).await.unwrap();
                });
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(
    asset_manager_benches,
    benchmark_write_new_snapshot,
    benchmark_write_new_manifest
);
