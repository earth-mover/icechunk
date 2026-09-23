//! Flatbuffer serialization for Icechunk metadata.
//!
//! How serializers work:
//!
//! - Main goal is to make sure newer version of Icechunk can read metadata files created using
//!   older versions. In this way, a repository can evolve during its life. As users upgrade their
//!   Icechunk versions they don't need to migrate their data.
//! - Of course we may choose to limit backwards compatibility after certain number of versions or
//!   a time limit.
//! - Performance is critical, so we cannot copy much data around during the process of
//!   serialization/deserialization
//! - For serialization:
//!     - We define a new `XSerializer` for each metadata file type `X`. Example: `SnapshotSerializer`.
//!     - This type implement [`serde::Serialize`]
//!     - This type holds only references to the same fields as `X`
//!     - This type implements `From<&X>` (notice by reference) Example:
//!       ```ignore
//!       impl<'a> From<&'a Snapshot> for SnapshotSerializer<'a> {
//!       ...
//!       }
//!       ```
//!     - Because the serializer only holds references it's essentially free to call
//!       `snapshot.into()` to get one.
//!     - Then this object is serialized using serde.
//! - For deserialization:
//!     - We define a new `XDeserializer` for each metadata file type `X`. Example: `SnapshotDeserializer`.
//!     - This type implement [`serde::Deserialize`]
//!     - This type holds the same fields as `X` by value
//!     - `X` implements `From<XDeserializer>` (notice by value). Example:
//!       ```ignore
//!        impl From<SnapshotDeserializer> for Snapshot {
//!        ...
//!        }
//!       ```
//!     - Because the deserializer can be destructed and `X` implements `From`,  it's essentially free to call
//!       obtain the original type `X`
//!     - Then this new type `XDeserializer` is deserialized using serde and converted with `into`.
//!
//! - `serializers.current.rs` holds all the serializers and deserializers for the current version
//!   of the spec
//! - `serializers.version_foo.rs` holds all the serializers and deserializers for version foo of
//!   the spec
//! - The `serializers` module root has functions `serialize_X` and `deserialize_X` that take a
//!   spec version number and use the right (de)-serializer to do the job.
use std::io::Write;

use icechunk_types::ICResultExt as _;

use crate::{
    IcechunkFormatError, IcechunkFormatErrorKind,
    format_constants::{FileTypeBin, SpecVersionBin},
    manifest::Manifest,
    repo_info::RepoInfo,
    snapshot::Snapshot,
    transaction_log::TransactionLog,
};

pub fn serialize_snapshot(
    snapshot: &Snapshot,
    version: SpecVersionBin,
    write: &mut impl Write,
) -> Result<(), std::io::Error> {
    match version {
        SpecVersionBin::V1 | SpecVersionBin::V2 => write.write_all(snapshot.bytes()),
    }
}

pub fn serialize_manifest(
    manifest: &Manifest,
    version: SpecVersionBin,
    write: &mut impl Write,
) -> Result<(), std::io::Error> {
    match version {
        SpecVersionBin::V1 | SpecVersionBin::V2 => write.write_all(manifest.bytes()),
    }
}

pub fn serialize_transaction_log(
    transaction_log: &TransactionLog,
    version: SpecVersionBin,
    write: &mut impl Write,
) -> Result<(), std::io::Error> {
    match version {
        SpecVersionBin::V1 | SpecVersionBin::V2 => {
            write.write_all(transaction_log.bytes())
        }
    }
}

pub fn serialize_repo_info(
    info: &RepoInfo,
    version: SpecVersionBin,
    write: &mut impl Write,
) -> Result<(), std::io::Error> {
    match version {
        SpecVersionBin::V2 => write.write_all(info.bytes()),
        SpecVersionBin::V1 => Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Trying to write to an old Icechunk format version. Aborting.",
        )),
    }
}

pub fn deserialize_snapshot(
    version: SpecVersionBin,
    buffer: Vec<u8>,
) -> Result<Snapshot, IcechunkFormatError> {
    match version {
        SpecVersionBin::V1 | SpecVersionBin::V2 => Snapshot::from_buffer(version, buffer),
    }
}

pub fn deserialize_manifest(
    version: SpecVersionBin,
    buffer: Vec<u8>,
) -> Result<Manifest, IcechunkFormatError> {
    match version {
        SpecVersionBin::V1 | SpecVersionBin::V2 => Manifest::from_buffer(buffer),
    }
}

pub fn deserialize_transaction_log(
    version: SpecVersionBin,
    buffer: Vec<u8>,
) -> Result<TransactionLog, IcechunkFormatError> {
    match version {
        SpecVersionBin::V1 | SpecVersionBin::V2 => TransactionLog::from_buffer(buffer),
    }
}

pub fn deserialize_repo_info(
    version: SpecVersionBin,
    buffer: Vec<u8>,
) -> Result<RepoInfo, IcechunkFormatError> {
    match version {
        SpecVersionBin::V2 => RepoInfo::from_buffer(buffer),
        SpecVersionBin::V1 => {
            Err(IcechunkFormatErrorKind::UnsupportedOperationForVersion {
                version: SpecVersionBin::V1 as u8,
            })
            .capture()
        }
    }
}

/// Files at or above this size should be verified before being written.
///
/// Verification costs about as much as the zstd pass a write already does, so we
/// only pay it for files large enough to be at risk of tripping a flatbuffers
/// limit on read. No buffer below this size can trip one:
///
/// - Tables: a table costs at least [`MIN_BYTES_PER_TABLE`] bytes of buffer, so
///   reaching the smallest `max_tables` we allow takes several times this size.
/// - Apparent size: the verifier's counter runs at most
///   [`MAX_APPARENT_SIZE_INFLATION`] times the buffer, because it charges every
///   table visit for a vtable the builder stored only once.
///
/// The tests in this module measure both bounds against every file type and
/// check them against the limits, so a schema change that erodes them fails
/// there rather than silently in a repository.
pub const VERIFY_THRESHOLD_BYTES: usize = 128 * 1024 * 1024;

/// Lower bound on the buffer bytes one flatbuffers table costs: a 4 byte soffset
/// plus its 4 byte slot in the enclosing vector. Vtables are deduplicated by the
/// builder, so they don't count towards the floor.
pub const MIN_BYTES_PER_TABLE: usize = 8;

/// Upper bound on how much larger the verifier's apparent-size counter runs than
/// the buffer it walks, over every element type we serialize.
pub const MAX_APPARENT_SIZE_INFLATION: usize = 2;

/// Check that `buffer` can be read back as `file_type`.
///
/// Applies exactly the checks the matching `from_buffer` applies on read, so a
/// buffer that passes here cannot fail to deserialize later. File types that are
/// not flatbuffers have nothing to check.
pub fn verify_buffer(
    file_type: FileTypeBin,
    buffer: &[u8],
) -> Result<(), IcechunkFormatError> {
    match file_type {
        FileTypeBin::Snapshot => Snapshot::verify_buffer(buffer),
        FileTypeBin::Manifest => Manifest::verify_buffer(buffer),
        FileTypeBin::TransactionLog => TransactionLog::verify_buffer(buffer),
        FileTypeBin::RepoInfo => RepoInfo::verify_buffer(buffer),
        FileTypeBin::Attributes | FileTypeBin::Chunk => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use flatbuffers::VerifierOptions;
    use icechunk_types::error::ICError;

    use super::*;
    use crate::{
        ChunkId, ChunkIndices, ManifestId, NodeId, SnapshotId,
        format_constants::FileTypeBin,
        manifest::{ChunkInfo, ChunkPayload, ChunkRef},
    };

    fn all_root_options() -> Vec<(&'static str, &'static VerifierOptions)> {
        vec![
            ("manifest", crate::manifest::root_options()),
            ("snapshot", crate::snapshot::root_options()),
            ("transaction log", crate::transaction_log::root_options()),
            ("repo info", crate::repo_info::root_options()),
        ]
    }

    fn native_payload(i: usize) -> ChunkPayload {
        ChunkPayload::Ref(ChunkRef {
            id: ChunkId::random(),
            offset: i as u64,
            length: 42,
        })
    }

    fn virtual_payload(i: usize) -> ChunkPayload {
        ChunkPayload::Virtual(crate::manifest::VirtualChunkRef {
            location: crate::manifest::VirtualChunkLocation::from_url(&format!(
                "s3://some-bucket/some/prefix/object-{i:012}"
            ))
            .expect("bad url"),
            offset: i as u64,
            length: 42,
            checksum: None,
        })
    }

    fn inline_payload(_i: usize) -> ChunkPayload {
        ChunkPayload::Inline(Bytes::from(vec![42u8; 64]))
    }

    fn many_chunks_manifest(n: usize, payload: fn(usize) -> ChunkPayload) -> Vec<u8> {
        let node = NodeId::random();
        let chunks = (0..n)
            .map(|i| ChunkInfo {
                node: node.clone(),
                coord: ChunkIndices(vec![i as u32, 0, 0]),
                payload: payload(i),
            })
            .collect();
        Manifest::from_sorted_vec(&ManifestId::random(), chunks, None)
            .expect("cannot build manifest")
            .expect("manifest is empty")
            .bytes()
            .to_vec()
    }

    fn many_nodes_snapshot(n: usize, arrays: bool) -> Vec<u8> {
        use crate::snapshot::{ArrayShape, NodeData, NodeSnapshot};
        let nodes: Vec<_> = (0..n)
            .map(|i| {
                let node_data = if arrays {
                    NodeData::Array {
                        shape: ArrayShape::new([(1000u64, 10u32); 3]).expect("bad shape"),
                        dimension_names: None,
                        manifests: vec![crate::manifest::ManifestRef {
                            object_id: ManifestId::random(),
                            extents: crate::manifest::ManifestExtents::new(
                                &[0, 0, 0],
                                &[10, 10, 10],
                            ),
                        }],
                    }
                } else {
                    NodeData::Group
                };
                Ok(NodeSnapshot {
                    id: NodeId::random(),
                    path: crate::Path::new(&format!("/node{i:012}")).expect("bad path"),
                    user_data: Bytes::from(vec![b'u'; 100]),
                    node_data,
                })
            })
            .collect();
        Snapshot::from_iter(
            None,
            None,
            SpecVersionBin::current(),
            "a message",
            None,
            vec![],
            None,
            nodes,
        )
        .expect("cannot build snapshot")
        .bytes()
        .to_vec()
    }

    fn many_coords_transaction_log(n: usize) -> Vec<u8> {
        let empty = Vec::<NodeId>::new();
        let chunks =
            vec![(NodeId::random(), (0..n).map(|i| ChunkIndices(vec![i as u32, 0, 0])))];
        TransactionLog::new_from_parts(
            &SnapshotId::random(),
            empty.clone().into_iter(),
            empty.clone().into_iter(),
            empty.clone().into_iter(),
            empty.clone().into_iter(),
            empty.clone().into_iter(),
            empty.into_iter(),
            chunks.into_iter(),
            std::iter::empty(),
        )
        .bytes()
        .to_vec()
    }

    fn many_snapshots_repo_info(n: usize) -> Vec<u8> {
        use crate::repo_info::{RepoAvailability, RepoStatus, UpdateInfo, UpdateType};
        let snapshots: Vec<_> = (0..n)
            .map(|i| crate::snapshot::SnapshotInfo {
                id: SnapshotId::random(),
                parent_id: None,
                flushed_at: chrono::Utc::now(),
                message: format!("commit number {i}"),
                metadata: Default::default(),
                pruned_ancestor_tx_logs: vec![],
            })
            .collect();
        let branch_target = snapshots[0].id.clone();
        let updates: Vec<crate::repo_info::UpdateTuple<'_>> = vec![];
        RepoInfo::new(
            SpecVersionBin::current(),
            [],
            [("main", branch_target)],
            [],
            snapshots,
            &Default::default(),
            UpdateInfo {
                update_type: UpdateType::RepoInitializedUpdate,
                update_time: chrono::Utc::now(),
                previous_updates: updates,
            },
            None,
            10,
            None,
            None,
            None::<std::iter::Empty<u16>>,
            None::<std::iter::Empty<u16>>,
            &RepoStatus {
                availability: RepoAvailability::Online,
                set_at: chrono::Utc::now(),
                limited_availability_reason: None,
            },
        )
        .expect("cannot build repo info")
        .bytes()
        .to_vec()
    }

    fn a_manifest() -> Manifest {
        Manifest::from_sorted_vec(
            &ManifestId::random(),
            vec![ChunkInfo {
                node: NodeId::random(),
                coord: ChunkIndices(vec![0, 0]),
                payload: ChunkPayload::Ref(ChunkRef {
                    id: ChunkId::random(),
                    offset: 0,
                    length: 42,
                }),
            }],
            None,
        )
        .expect("cannot build manifest")
        .expect("manifest is empty")
    }

    fn a_transaction_log() -> TransactionLog {
        let empty = Vec::<NodeId>::new();
        let no_chunks = Vec::<(NodeId, std::vec::IntoIter<ChunkIndices>)>::new();
        TransactionLog::new_from_parts(
            &SnapshotId::random(),
            empty.clone().into_iter(),
            empty.clone().into_iter(),
            empty.clone().into_iter(),
            empty.clone().into_iter(),
            empty.clone().into_iter(),
            empty.into_iter(),
            no_chunks.into_iter(),
            std::iter::empty(),
        )
    }

    fn a_repo_info() -> RepoInfo {
        RepoInfo::initial(
            SpecVersionBin::current(),
            crate::snapshot::SnapshotInfo {
                id: SnapshotId::random(),
                parent_id: None,
                flushed_at: chrono::Utc::now(),
                message: "initial".to_string(),
                metadata: Default::default(),
                pruned_ancestor_tx_logs: vec![],
            },
            10,
            None::<&()>,
            None,
        )
    }

    #[icechunk_macros::test]
    fn verify_buffer_accepts_buffers_we_serialize()
    -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = Snapshot::initial(SpecVersionBin::current())?;
        verify_buffer(FileTypeBin::Snapshot, snapshot.bytes())?;
        verify_buffer(FileTypeBin::Manifest, a_manifest().bytes())?;
        verify_buffer(FileTypeBin::TransactionLog, a_transaction_log().bytes())?;
        verify_buffer(FileTypeBin::RepoInfo, a_repo_info().bytes())?;
        Ok(())
    }

    #[icechunk_macros::test]
    fn verify_buffer_rejects_buffers_that_cannot_be_read_back() {
        // a valid flatbuffer of the wrong type, and plain garbage
        let snapshot_bytes = a_manifest().bytes().to_vec();
        for file_type in [
            FileTypeBin::Snapshot,
            FileTypeBin::Manifest,
            FileTypeBin::TransactionLog,
            FileTypeBin::RepoInfo,
        ] {
            for bad in [vec![0u8; 64], vec![0xffu8; 7], snapshot_bytes[..32].to_vec()] {
                assert!(
                    verify_buffer(file_type, bad.as_slice()).is_err(),
                    "{file_type:?} accepted a buffer it cannot read back"
                );
            }
        }
    }

    #[icechunk_macros::test]
    fn verify_buffer_ignores_file_types_that_are_not_flatbuffers() {
        // chunks are opaque bytes, there is nothing to verify
        let chunk = Bytes::from_static(b"not a flatbuffer");
        assert!(verify_buffer(FileTypeBin::Chunk, chunk.as_ref()).is_ok());
    }

    /// Runs a root type's verifier over a buffer with the given limits.
    type VerifyFn = Box<dyn Fn(&[u8], &VerifierOptions) -> bool>;

    /// A buffer to measure, named, with the verifier for its root type.
    type Sample = (&'static str, Vec<u8>, VerifyFn);

    /// What the verifier counted while walking `buffer`, found by searching for
    /// the smallest limit that still lets it through.
    struct Counters {
        apparent_size: usize,
        tables: usize,
        bytes: usize,
    }

    fn min_limit_that_passes(hi: usize, passes: impl Fn(usize) -> bool) -> usize {
        assert!(passes(hi), "no limit in range lets the buffer through");
        let (mut lo, mut hi) = (0usize, hi);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if passes(mid) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        lo
    }

    fn unlimited() -> VerifierOptions {
        VerifierOptions {
            max_depth: 1 << 20,
            max_tables: 1 << 40,
            max_apparent_size: 1 << 50,
            ignore_missing_null_terminator: true,
        }
    }

    fn with_apparent_size(limit: usize) -> VerifierOptions {
        VerifierOptions { max_apparent_size: limit, ..unlimited() }
    }

    fn with_tables(limit: usize) -> VerifierOptions {
        VerifierOptions { max_tables: limit, ..unlimited() }
    }

    fn count(verify: &VerifyFn, buffer: &[u8]) -> Counters {
        assert!(verify(buffer, &unlimited()), "buffer does not verify at all");
        Counters {
            apparent_size: min_limit_that_passes(1 << 40, |limit| {
                verify(buffer, &with_apparent_size(limit))
            }),
            tables: min_limit_that_passes(1 << 32, |limit| {
                verify(buffer, &with_tables(limit))
            }),
            bytes: buffer.len(),
        }
    }

    /// One buffer per element type we serialize, each with enough elements that
    /// per-element costs dominate the root table's fixed overhead.
    fn sample_buffers() -> Vec<Sample> {
        let n = 5_000;
        let manifest_verify = || {
            Box::new(|b: &[u8], o: &VerifierOptions| {
                flatbuffers::root_with_opts::<crate::flatbuffers::generated::Manifest<'_>>(
                    o, b,
                )
                .is_ok()
            })
        };
        vec![
            (
                "manifest native refs",
                many_chunks_manifest(n, native_payload),
                manifest_verify(),
            ),
            (
                "manifest virtual refs",
                many_chunks_manifest(n, virtual_payload),
                manifest_verify(),
            ),
            (
                "manifest inline refs",
                many_chunks_manifest(n, inline_payload),
                manifest_verify(),
            ),
            (
                "snapshot groups",
                many_nodes_snapshot(n, false),
                Box::new(|b: &[u8], o: &VerifierOptions| {
                    flatbuffers::root_with_opts::<
                        crate::flatbuffers::generated::Snapshot<'_>,
                    >(o, b)
                    .is_ok()
                }),
            ),
            (
                "snapshot arrays",
                many_nodes_snapshot(n, true),
                Box::new(|b: &[u8], o: &VerifierOptions| {
                    flatbuffers::root_with_opts::<
                        crate::flatbuffers::generated::Snapshot<'_>,
                    >(o, b)
                    .is_ok()
                }),
            ),
            (
                "transaction log chunk coords",
                many_coords_transaction_log(n),
                Box::new(|b: &[u8], o: &VerifierOptions| {
                    flatbuffers::root_with_opts::<
                        crate::flatbuffers::generated::TransactionLog<'_>,
                    >(o, b)
                    .is_ok()
                }),
            ),
            (
                "repo snapshot infos",
                many_snapshots_repo_info(n),
                Box::new(|b: &[u8], o: &VerifierOptions| {
                    flatbuffers::root_with_opts::<crate::flatbuffers::generated::Repo<'_>>(
                        o, b,
                    )
                    .is_ok()
                }),
            ),
        ]
    }

    #[icechunk_macros::test]
    fn apparent_size_stays_within_the_inflation_bound() {
        for (name, buffer, verify) in sample_buffers() {
            let counted = count(&verify, buffer.as_slice());
            assert!(
                counted.apparent_size <= MAX_APPARENT_SIZE_INFLATION * counted.bytes,
                "{name}: apparent size {} is more than {MAX_APPARENT_SIZE_INFLATION}x the {} bytes it walks",
                counted.apparent_size,
                counted.bytes,
            );
        }
    }

    #[icechunk_macros::test]
    fn tables_cost_at_least_the_assumed_minimum_bytes() {
        for (name, buffer, verify) in sample_buffers() {
            let counted = count(&verify, buffer.as_slice());
            assert!(
                counted.bytes >= MIN_BYTES_PER_TABLE * counted.tables,
                "{name}: {} tables in only {} bytes, under {MIN_BYTES_PER_TABLE} bytes each",
                counted.tables,
                counted.bytes,
            );
        }
    }

    #[icechunk_macros::test]
    fn verifier_limits_leave_room_below_the_write_threshold() {
        for (name, options) in all_root_options() {
            // a file under the write threshold is never verified before writing,
            // so it must not be able to trip either limit on read
            assert!(
                MAX_APPARENT_SIZE_INFLATION * VERIFY_THRESHOLD_BYTES
                    <= options.max_apparent_size,
                "{name}: a file just under the write threshold could exceed max_apparent_size",
            );
            assert!(
                VERIFY_THRESHOLD_BYTES / MIN_BYTES_PER_TABLE <= options.max_tables,
                "{name}: a file just under the write threshold could exceed max_tables",
            );
        }
    }

    #[icechunk_macros::test]
    fn every_file_we_can_build_can_be_read_back() {
        // the builder refuses to grow past FLATBUFFERS_MAX_BUFFER_SIZE, so a file
        // at that size is the largest we can produce: the verifier has to accept it
        for (name, options) in all_root_options() {
            assert!(
                MAX_APPARENT_SIZE_INFLATION
                    .saturating_mul(flatbuffers::FLATBUFFERS_MAX_BUFFER_SIZE)
                    <= options.max_apparent_size,
                "{name}: a file we can build could be rejected by max_apparent_size",
            );
        }
    }

    #[icechunk_macros::test]
    fn verify_buffer_error_names_the_file_type() {
        let err = verify_buffer(FileTypeBin::Manifest, &[0u8; 64])
            .expect_err("garbage must not verify");
        assert!(matches!(
            err,
            ICError { kind: IcechunkFormatErrorKind::InvalidFlatBuffer(_), .. }
        ));
    }
}
