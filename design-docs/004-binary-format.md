# Icechunk on disk binary format

This document defines (without final detail) the general binary structure of Icechunk on-disk format.

## Chunk files

We don't modify them. Whatever Zarr writes for the chunk we write to object store.

## Metadata files

Reference and configuration files are stored in pure JSON format. The format will be defined in the spec once finalized.

Binary files are used for snapshot, manifest, attributes and transaction log files.

All these files start with 12 magic bytes:

```hex
4943 45f0 9fa7 8a43 4855 4e4b
```

Which correspond to the string `ICE🧊CHUNK` in UTF-8.

The next 24 bytes, with indexes 12-35 (zero based) identify the Icechunk client that wrote the file.
Different implementations will use different strings in UTF-8 encoding, padding them with ASCII spaces on the right. The official Icechunk implementation uses "ic-" followed by the version:

* "ic-0.1.0-a.8"
* "ic-1.0.0"
* etc.

Byte 36 identifies Icechunk spec version used to write the file.

* 0x00 - Reserved for future use
* 0x01 - Spec version 1
* 0x02 - Spec version 2
* ...

If more than 255 version are needed, we will set this byte to 0 and modify the following bytes according to a new spec.

The next byte (index 37) identifies file type :

* 0x01 - Snapshot
* 0x02 - Manifest
* 0x03 - Attributes file
* 0x04 - Transaction log

Byte 38 identifies compression type:

* 0x00 - No compression
* 0x01 - Zstd

The following bytes are the compressed Msgpack serialization of the corresponding Rust datastructure. This part of the file will see a lot of change and improvements after Icechunk 1.0. Notice compression doesn't apply to the header, only to bytes after and including index 27.

In object stores that support it, the file information (spec version, compression, type, etc) is also stored as object store metadata. Icechunk writes this information but currently doesn't use it during reads. Details of the metadata format TBD in the spec.

### File structure

| Byte index    | Content               | Example |
| ------------- | --------------- |--------- |
| 0 - 11        | Magic bytes           | ICE🧊CHUNK              |
| 12 - 35       | Implementation id     | 'ic-1.0.0    '          |
| 36            | Spec version          | 0x01                    |
| 37            | File type             | 0x01                    |
| 38            | Compression           | 0x01                    |
| 39..end       | Msgpack serializanion |                         |
