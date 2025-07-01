---
title: Specification
---
# Icechunk Specification

**!!! Note:**
    The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119.html).

## Introduction

The Icechunk specification is a storage specification for [Zarr](https://zarr-specs.readthedocs.io/en/latest/specs.html) data.
Icechunk is inspired by Apache Iceberg and borrows many concepts and ideas from the [Iceberg Spec](https://iceberg.apache.org/spec/#version-2-row-level-deletes).

This specification describes a single Icechunk **repository**.
A repository is defined as a Zarr store containing one or more Arrays and Groups.
The most common scenario is for a repository to contain a single Zarr group with multiple arrays, each corresponding to different physical variables but sharing common spatiotemporal coordinates.
However, formally a repository can be any valid Zarr hierarchy, from a single Array to a deeply nested structure of Groups and Arrays.
Users of Icechunk should aim to scope their repository only to related arrays and groups that require consistent transactional updates.

Icechunk defines a series of interconnected metadata and data files that together comprise the format.
All the data and metadata for a repository are stored in a directory in object storage or file storage.

## Goals

The goals of the specification are as follows:

1. **Object storage** - the format is designed around the consistency features and performance characteristics available in modern cloud object storage. No external database or catalog is required.
1. **Serializable isolation** - Reads will be isolated from concurrent writes and always use a committed snapshot of a repository. Writes to repositories will be committed atomically and will not be partially visible. Readers will not acquire locks.
1. **Time travel** - Previous snapshots of a repository remain accessible after new ones have been written.
1. **Chunk sharding and references** - Chunk storage is decoupled from specific file names. Multiple chunks can be packed into a single object (sharding). Zarr-compatible chunks within other file formats (e.g. HDF5, NetCDF) can be referenced.
1. **Schema Evolution** - Arrays and Groups can be added and removed from the hierarchy with minimal overhead.

### Non Goals

1. **Low Latency** - Icechunk is designed to support analytical workloads for large repositories. We accept that the extra layers of metadata files and indirection will introduce additional cold-start latency compared to regular Zarr.
1. **No Catalog** - The spec does not extend beyond a single repository or provide a way to organize multiple repositories into a hierarchy.
1. **Access Controls** - Access control is the responsibility of the storage medium.
The spec is not designed to enable fine-grained access restrictions (e.g. only read specific arrays) within a single repository.

### Storage Operations

Icechunk requires that the storage system support the following operations:

- **In-place write** - Strong read-after-write and list-after-write consistency is expected. Files are not moved or altered once they are written.
- **Write-if-not-exists** - For creating new references.
- **Conditional update** - For the commit process to be safe and consistent, the storage system must be able to atomically update a file only if the current version is known to the writer.
- **Seekable reads** - Chunk file formats may require seek support (e.g. shards).
- **Deletes** - Delete files that are no longer used (via a garbage-collection operation).
- **Sorted List** - The storage system must allow the listing of directories / prefixes in lexicographical order.

These requirements are compatible with object stores, like S3, as well as with filesystems.

The storage system is not required to support random-access writes. Once written, most files are immutable until they are deleted. The exceptions to this rule are:

- the repository configuration file doesn't track history, updates are done atomically but in place,
- branch reference files are also atomically updated in place,
- snapshot files can be updated in place by the expiration process (and administrative operation).

## Specification

### Overview

Icechunk uses a series of linked metadata files to describe the state of the repository.

- The **Snapshot file** records all of the different arrays and groups in a specific snapshot of the repository, plus their metadata. Every new commit creates a new snapshot file. The snapshot file contains pointers to one or more chunk manifest files.
- **Chunk manifests** store references to individual chunks. A single manifest may store references for multiple arrays or a subset of all the references for a single array.
- **Chunk files** store the actual compressed chunk data, potentially containing data for multiple chunks in a single file.
- **Transaction log files**, an overview of the operations executed during a session, used for rebase and diffs.
- **Reference files**, also called refs, track the state of branches and tags, containing a lightweight pointer to a snapshot file. Transactions on a branch are committed by atomically updating the branch reference file.
- **Tag tombstones**, tags are immutable in Icechunk but can be deleted. When they are deleted a tombstone file is created so the
same tag name cannot be reused later.
- **Config file**, a yaml file with the default repository configuration.

When reading from object store, the client opens the latest branch or tag file to obtain a pointer to the relevant snapshot file.
The client then reads the snapshot file to determine the structure and hierarchy of the repository.
When fetching data from an array, the client first examines the chunk manifest file[s] for that array and finally fetches the chunks referenced therein.

When writing a new repository snapshot, the client first writes a new set of chunks and chunk manifests, and then generates a new snapshot file.
Finally, to commit the transaction, it updates the branch reference file using an atomic conditional update operation.
This operation may fail if a different client has already committed the next snapshot.
In this case, the client may attempt to resolve the conflicts and retry the commit.

```mermaid
flowchart TD
    subgraph metadata[Metadata]
    subgraph reference_files[Reference Files]
    old_branch[Main Branch File 001]
    branch[Main Branch File 002]
    end
    subgraph snapshots[Snapshots]
    snapshot1[Snapshot File 1]
    snapshot2[Snapshot File 2]
    end
    end
    subgraph manifests[Manifests]
    manifestA[Chunk Manifest A]
    manifestB[Chunk Manifest B]
    end
    end
    subgraph data
    chunk1[Chunk File 1]
    chunk2[Chunk File 2]
    chunk3[Chunk File 3]
    chunk4[Chunk File 4]
    end

    branch -- snapshot ID --> snapshot2
    snapshot1 --> manifestA
    snapshot2 -->manifestA
    snapshot2 -->manifestB
    manifestA --> chunk1
    manifestA --> chunk2
    manifestB --> chunk3
    manifestB --> chunk4

```

### File Layout

All data and metadata files are stored within a root directory (typically a prefix within an object store) using the following directory structure.

- `$ROOT` base URI (s3, gcs, local directory, etc.)
- `$ROOT/config.yaml` optional persistent default configuration for the repository
- `$ROOT/refs/` reference files
- `$ROOT/snapshots/` snapshot files
- `$ROOT/manifests/` chunk manifests
- `$ROOT/transactions/` transaction log files
- `$ROOT/chunks/` chunks

### File Formats

### Reference Files

Similar to Git, Icechunk supports the concept of _branches_ and _tags_.
These references point to a specific snapshot of the repository.

- **Branches** are _mutable_ references to a snapshot.
  Repositories may have one or more branches.
  The default branch name is `main`.
  Repositories must always have a `main` branch, which is used to detect the existence of a valid repository in a given path.
  After creation, branches may be updated to point to a different snapshot.
- **Tags** are _immutable_ references to a snapshot.
  A repository may contain zero or more tags.
  After creation, tags may never be updated, unlike in Git.

References are very important in the Icechunk design.
Creating or updating references is the point at which consistency and atomicity of Icechunk transactions is enforced.
Different client sessions may simultaneously create two inconsistent snapshots; however, only one session may successfully update a reference to point it to its snapshot.

References (both branches and tags) are stored as JSON files, the content is a JSON object with:

- keys: a single key `"snapshot"`,
- value: a string representation of the snapshot id, using [Base 32 Crockford](https://www.crockford.com/base32.html) encoding. The snapshot id is 12 byte random binary, so the encoded string has 20 characters.

Here is an example of a JSON file corresponding to a tag or branch:

```json
{"snapshot":"VY76P925PRY57WFEK410"}
```

#### Creating and Updating Branches

The process of creating and updating branches is designed to use the limited consistency guarantees offered by object storage to ensure transactional consistency.
When a client checks out a branch, it obtains a specific snapshot ID and uses this snapshot as the basis for any changes it creates during its session.
The client creates a new snapshot and then updates the branch reference to point to the new snapshot (a "commit").
However, when updating the branch reference, the client must detect whether a _different session_ has updated the branch reference in the interim, possibly retrying or failing the commit if so.
This is an "optimistic concurrency" strategy; the resolution mechanism can be expensive, but conflicts are expected to be infrequent.

All major object stores support a "conditional update" operation.
In other words, object stores can guard against the race condition which occurs when two sessions attempt to update the same file at the same time. Only one of those will succeed.

This mechanism is used by Icechunk on commits.
When a client checks out a branch, it keeps track of the "version" of the reference file for the branch.
When it tries to commit, it attempts to conditionally update this file in an atomic "all or nothing" operation.
If this succeeds, the commit is successful.
If this fails (because another client updated that file since the session started), the commit fails.
At this point, the client may choose to retry its commit (possibly re-reading the updated data) and then try the conditional update again.

Branch references are stored in the `refs/` directory within a subdirectory corresponding to the branch name prepended by the string `branch.`: `refs/branch.$BRANCH_NAME/ref.json`.
Branch names may not contain the `/` character.

Branch are deleted simply eliminating their ref file.

#### Tags

Tags are immutable. Their files follow the pattern `refs/tag.$TAG_NAME/ref.json`.

Tag names may not contain the `/` character.

When creating a new tag, the client attempts to create the tag file using a "create if not exists" operation.
If successful, the tag is created.
If not, that means another client has already created that tag.

Tags can also be deleted once created, but we cannot allow a delete followed by a creation, since that would
result in an observable mutation of the tag. To solve this issue, we don't allow recreating tags that were deleted.
When a tag is deleted, its reference file is not deleted, but a new tombstone file is created in the path:
`refs/tags.$TAG_NAME/ref.json.deleted`.

### Snapshot Files

The snapshot file fully describes the schema of the repository, including all arrays and groups.

The snapshot file is encoded using [flatbuffers](https://github.com/google/flatbuffers). The IDL for the
on-disk format can be found in [the repository file](https://github.com/earth-mover/icechunk/tree/main/icechunk/flatbuffers/snapshot.fbs)

The most important parts of a snapshot file are:

- An id, 12 random bytes also encoded in the file name.
- The id of its parent snapshot. All snapshots but the first one in the repository must have a parent.
- The commit time (`flushed_at`), message string, (`message`) and metadata map (`metadata`).
- A list of `NodeSnapshot`, one item for each group or array in the repository snapshot.
- A list of `ManifestFileInfo`

`NodeSnapshot` objects can also be found in the same flatbuffers file. They contain:

- A node id (8 random bytes).
- The node path within the repository hierarchy, for example `foo/bar/baz`.
- `user_data`, any metadata used to create the node, this will usually be the Zarr metadata.
- A `node_data` union, that can be either an `ArrayNodeData` or a `GroupNodeData`.

`GroupNodeData` is empty, so it works as a pure marker signaling that the node is a group.

`ArrayNodeData` is a richer datastructure that keeps:

- The array shape, both for the whole array and its chunks.
- The array dimension names
- A list of `ManifestRef`

A `ManifestRef` is a pointer to a manifest file. It includes an id, that is used to determine the file path,
and a range of coordinates contained in the manifest for each array dimension.

Finally, a `ManifestFileInfo` is also a pointer to a manifest file, but it includes information about all the chunks
held in the manifest.

### Chunk Manifest Files

A chunk manifest file stores chunk references.
Chunk references from multiple arrays can be stored in the same chunk manifest.
The chunks from a single array can also be spread across multiple manifests.

Manifest files are encoded using flatbuffers. The IDL for the
on-disk format can be found in [the repository file](https://github.com/earth-mover/icechunk/tree/main/icechunk/flatbuffers/manifest.fbs)

A manifest file has:

- An id (12 random bytes), that is also encoded in the file name.
- A list of `ArrayManifest` sorted by node id

Each `ArrayManifest` contains chunk references for a given array. It contains the `node_id`
of the array and a list of `ChunkRef` sorted by the chunk coordinate.

`ChunkRef` is a complex data structure because chunk references in Icechunk can have three different types:

- Native, pointing to a chunk object within the Icechunk repository.
- Inline, an optimization for very small chunks that can be embedded directly in the manifest. Mostly used for coordinate arrays.
- Virtual, pointing to a region of a file outside of the Icechunk repository, for example,
  a chunk that is inside a NetCDF file in object store

These three types of chunks references are encoded in the same flatbuffers table, using optional fields.

### Chunk Files

Chunk files contain the compressed binary chunks of a Zarr array.
Icechunk permits quite a bit of flexibility about how chunks are stored.
Chunk files can be:

- One chunk per chunk file (i.e. standard Zarr)
- Multiple contiguous chunks from the same array in a single chunk file (similar to Zarr V3 shards)
- Chunks from multiple different arrays in the same file
- Other file types (e.g. NetCDF, HDF5) which contain Zarr-compatible chunks

Applications may choose to arrange chunks within files in different ways to optimize I/O patterns.

### Transaction logs

Transaction logs keep track of the operations done in a commit. They are not used to read objects
from the repo, but they are useful for features such as commit diff and conflict resolution.

Transaction logs are an optimization, to provide fast conflict resolution and commit diff. They are
not absolutely required to implement the core Icechunk operations.

Transaction log files are encoded using flatbuffers. The IDL for the
on-disk format can be found in [the repository file](https://github.com/earth-mover/icechunk/tree/main/icechunk/flatbuffers/transaction_log.fbs)

The transaction log file maintains information about the id of modified objects:

- `new_groups`: list of node ids.
- `new_arrays`: list of node ids.
- `deleted_groups`: list of node ids.
- `deleted_arrays`: list of node ids.
- `updated_groups`: list of node ids.
- `updated_arrays`: list of node ids.
- `updated_chunks`: list of node ids and chunk indices.

## Algorithms

### Initialize New Repository

A new repository is initialized by creating a new empty snapshot file and then creating the reference for branch `main`.
The first snapshot has a well known id, that encodes to a file name: `1CECHNKREP0F1RSTCMT0`. All object ids are
encoded in paths using Crockford base 32.

If another client attempts to initialize a repository in the same location, only one can succeed.

### Read from Repository

#### From Snapshot ID

If the specific snapshot ID is known, a client can open it directly in read only mode.

1. Use the specified snapshot ID to fetch the snapshot file.
1. Inspect the snapshot to find the relevant manifest or manifests.
1. Fetch the relevant manifests and the desired chunks pointed by them.

#### From Branch

Usually, a client will want to read from the latest branch (e.g. `main`).

1. Resolve the object store prefix `refs/branch.$BRANCH_NAME/ref.json` to obtain the latest ref file.
1. Parse the branch file JSON contents to obtain the snapshot ID.
1. Use the snapshot ID to fetch the snapshot file.
1. Fetch the relevant manifests and the desired chunks pointed by them.

#### From Tag

1. Read the tag file found at `refs/tag.$TAG_NAME/ref.json` to obtain the snapshot ID.
1. Use the snapshot ID to fetch the snapshot file.
1. Fetch the relevant manifests and the desired chunks pointed by them.

### Write New Snapshot

1. Open a repository at a specific branch as described above, keeping track of the sequence number and branch name in the session context.
1. [optional] Write new chunk files.
1. [optional] Write new chunk manifests.
1. Write a new transaction log file summarizing all changes in the session.
1. Write a new snapshot file with the new repository hierarchy and manifest links.
1. Do conditional update to write the new value of the branch reference file
    1. If successful, the commit succeeded and the branch is updated.
    1. If unsuccessful, attempt to reconcile and retry the commit.

### Create New Tag

A tag can be created from any snapshot.

1. Open the repository at a specific snapshot.
1. Attempt to create the tag file.
   a. If successful, the tag was created.
   b. If unsuccessful, the tag already exists.
