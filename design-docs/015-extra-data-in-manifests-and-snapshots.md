# Extra data in manifests and snapshots

We have several candidates to add "extra information" to snapshots,
manifests and manifest refs. We currently don't have a natural place to put this
information. Examples are statistics, pre computed quantities, and others.

This could also give us more flexibility as a way to create new features without
having to change the format.

## Design

We add extra information to the different objects using opaque byte vectors
reserved for future extensibility. The encoding of these bytes is intentionally
left unspecified — consumers should not assume any particular format.

This document doesn't dictate what data can or should be added in the
extra fields.

### Manifest references

This is the hardest case to treat because:

* manifest refs are embedded in the Snapshot file
* they are currently encoded as a flatbuffers `struct`, which doesn't allow
addition of new fields:

```flatbuffers
struct ManifestFileInfo {
    id: ObjectId12;
    size_bytes: uint64;
    num_chunk_refs: uint32;
}
```

To fix this, we define:

```flatbuffers
table ManifestFileInfoV2 {
    id: ObjectId12;
    size_bytes: uint64;
    num_chunk_refs: uint32;

    // NEW FIELD
    extra: [uint8];
}
```

And add a field to the snapshot:

```flexbuffers
table Snapshot {

  // existing fields
  ...
  manifest_files: [ManifestFileInfo] (required);

  // NEW FIELD
  manifest_files_v2: [ManifestFileInfoV2];
}
```

IC2 manifests will use only `manifest_files_v2`.

### Manifest

We add to the flatbuffer format:

```flexbuffers
table Manifest {

  // existing fields
  id: ObjectId12 (required);
  arrays: [ArrayManifest] (required);

  // NEW FIELD
  extra: [uint8];
}
```

### Transaction log

We add to the flatbuffer format:

```flexbuffers
table TransactionLog {

  // existing fields
  id: ObjectId12 (required);
  new_groups: [ObjectId8] (required);
  new_arrays: [ObjectId8] (required);
  deleted_groups: [ObjectId8] (required);
  deleted_arrays: [ObjectId8] (required);
  updated_arrays: [ObjectId8] (required);
  updated_groups: [ObjectId8] (required);
  updated_chunks: [ArrayUpdatedChunks] (required);
  moved_nodes: [MoveOperation];

  // NEW FIELD
  extra: [uint8];
}
```

### Repo info

We add to the flatbuffer format:

```flexbuffers
table Repo {

  // existing fields
  spec_version: uint8;
  tags: [Ref] (required);
  branches: [Ref] (required);
  deleted_tags: [string] (required);
  snapshots: [SnapshotInfo] (required);
  status: RepoStatus (required);
  metadata: [MetadataItem];
  latest_updates: [Update] (required);
  repo_before_updates: string;
  config: [uint8] (flexbuffer);

  // NEW FIELD
  extra: [uint8];
}
```

### Snapshot

We add to the flatbuffer format:

```flexbuffers
table Snapshot {

  // existing fields
  id: ObjectId12 (required);
  parent_id: ObjectId12;
  nodes: [NodeSnapshot] (required);
  flushed_at: uint64;
  message: string (required);
  metadata: [MetadataItem] (required);
  manifest_files: [ManifestFileInfo] (required);

  // NEW FIELD
  extra: [uint8];

  // NEW FIELD (spec version 2)
  manifest_files_v2: [ManifestFileInfoV2];
}
```

### ArrayManifest (in manifest.fbs)

Per-array extensibility point in manifests (e.g., per-array statistics).

```flatbuffers
table ArrayManifest {

  // existing fields
  node_id: ObjectId8 (required);
  refs: [ChunkRef] (required);

  // NEW FIELD
  extra: [uint8];
}
```

### ChunkRef (in manifest.fbs)

Per-chunk extensibility point. There can be millions of ChunkRefs, but since
`ChunkRef` is a FlatBuffers `table`, absent optional fields have zero
per-instance overhead (only ~2 bytes in the shared vtable).

```flatbuffers
table ChunkRef {

  // existing fields
  index: [uint32] (required);
  inline: [uint8];
  offset: uint64 = 0;
  length: uint64 = 0;
  chunk_id: ObjectId12;
  location: string;
  checksum_etag: string;
  checksum_last_modified: uint32 = 0;
  compressed_location: [uint8];

  // NEW FIELD
  extra: [uint8];
}
```

### NodeSnapshot (in snapshot.fbs)

Per-node extensibility point in snapshots (e.g., computed node-level stats
like total size, chunk count).

```flatbuffers
table NodeSnapshot {

  // existing fields
  id: ObjectId8 (required);
  path: string (required);
  user_data: [uint8] (required);
  node_data: NodeData (required);

  // NEW FIELD
  extra: [uint8];
}
```

## Size overhead analysis

Since FlatBuffers tables only store offsets for fields that are actually
present, the cost of adding an optional `extra` field depends on usage:

| Scenario | Per-instance overhead |
|---|---|
| Field absent (not set) | 0 bytes (only ~2 bytes added to the shared vtable) |
| Empty byte vector | ~6–10 bytes |
| Small payload (e.g. 3 key-value pairs) | ~40–80 bytes depending on encoding |

This means structures like `ChunkRef`, where millions of instances may exist,
pay no per-instance cost when `extra` is not used.

## Other format changes

See [more format changes](./012-some-more-IC2-format-changes.md) in the
Icechunk 2.0 library.
