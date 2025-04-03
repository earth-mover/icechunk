//! How seralizers work:
//!
//! - Main goal is to make sure newer version of Icechunk can read metadata files created using
//!   older versions. In this way, a repository can evolve during its life. As users upgrade their
//!   Icechunk versions they don't need to migrate their data.
//! - Of course we may choose to limit backwards compatibility after certain number of versions or
//!   a time limit.
//! - Performance is critical, so we cannot copy much data around during the process of
//!   serialization/deserialization
//! - For serialization:
//!     - We define a new `XSerializer` for each metadata file type `X`. Example: [`SnapshotSerializer`].
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
//!     - We define a new `XDeserializer` for each metadata file type `X`. Example: [`SnapshotDeserializer`].
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
use std::io::{Read, Write};

use super::{
    IcechunkFormatError, format_constants::SpecVersionBin, manifest::Manifest,
    snapshot::Snapshot, transaction_log::TransactionLog,
};

pub fn serialize_snapshot(
    snapshot: &Snapshot,
    version: SpecVersionBin,
    write: &mut impl Write,
) -> Result<(), std::io::Error> {
    match version {
        SpecVersionBin::V0dot1 => write.write_all(snapshot.bytes()),
    }
}

pub fn serialize_manifest(
    manifest: &Manifest,
    version: SpecVersionBin,
    write: &mut impl Write,
) -> Result<(), std::io::Error> {
    match version {
        SpecVersionBin::V0dot1 => write.write_all(manifest.bytes()),
    }
}

pub fn serialize_transaction_log(
    transaction_log: &TransactionLog,
    version: SpecVersionBin,
    write: &mut impl Write,
) -> Result<(), std::io::Error> {
    match version {
        SpecVersionBin::V0dot1 => write.write_all(transaction_log.bytes()),
    }
}

pub fn deserialize_snapshot(
    version: SpecVersionBin,
    mut read: Box<dyn Read>,
) -> Result<Snapshot, IcechunkFormatError> {
    match version {
        SpecVersionBin::V0dot1 => {
            // TODO: what's a good capacity?
            let mut buffer = Vec::with_capacity(8_192);
            read.read_to_end(&mut buffer)?;
            buffer.shrink_to_fit();
            Snapshot::from_buffer(buffer)
        }
    }
}

pub fn deserialize_manifest(
    version: SpecVersionBin,
    mut read: Box<dyn Read>,
) -> Result<Manifest, IcechunkFormatError> {
    match version {
        SpecVersionBin::V0dot1 => {
            // TODO: what's a good capacity?
            let mut buffer = Vec::with_capacity(1024 * 1024);
            read.read_to_end(&mut buffer)?;
            buffer.shrink_to_fit();
            Manifest::from_buffer(buffer)
        }
    }
}

pub fn deserialize_transaction_log(
    version: SpecVersionBin,
    mut read: Box<dyn Read>,
) -> Result<TransactionLog, IcechunkFormatError> {
    match version {
        SpecVersionBin::V0dot1 => {
            // TODO: what's a good capacity?
            let mut buffer = Vec::with_capacity(1024 * 1024);
            read.read_to_end(&mut buffer)?;
            buffer.shrink_to_fit();
            TransactionLog::from_buffer(buffer)
        }
    }
}
