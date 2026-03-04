# Extra data in manifests and snapshots

We have several candidates to add "extra information" to snapshots,
manifests and manifest refs. We currently don't have a natural place to put this
information. Examples are statistics, pre computed quantities, and others.

This could also give us more flexibility as a way to create new features without
having to change the format.

## Design

We add extra information to the different objects using flexbuffers maps.
Icechunk 2 already uses flexbuffers for serialization of repo and snapshot
metadata.

This document doesn't dictate what data can or should be added in the
flexbuffer map.

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
    extra: [uint8] (flexbuffer);
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
  extra: [uint8] (flexbuffer);
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
  extra: [uint8] (flexbuffer);
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
  config: [uint8];

  // NEW FIELD
  extra: [uint8] (flexbuffer);
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
  extra: [uint8] (flexbuffer);
}
```

## Other format changes

See [more format changes](./012-some-more-IC2-format-changes.md) in the
Icechunk 2.0 library.
