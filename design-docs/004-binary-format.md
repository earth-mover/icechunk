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

The next 12 bytes, with indexes 12-23 (zero based) identify the Icechunk client that wrote the file.
Different implementations will use different strings in UTF-8 encoding, padding them with ASCII spaces on the right. The official Icechunk implementation uses "ic-" followed by the version:

* "ic-0.1.0-a.8"
* "ic-1.0.0"
* etc.

Byte 24 identifies Icechunk spec version used to write the file.

* 0x00 - Reserved for future use
* 0x01 - Spec version 1
* 0x02 - Spec version 2
* ...

The next byte (index 25) identifies file type :

* 0x01 - Snapshot
* 0x02 - Manifest
* 0x03 - Attributes file
* 0x04 - Transaction log

Byte 26 identifies compression type:

* 0x00 - No compression
* 0x01 - Zstd

The following bytes are the Msgpack serialization of the Rust datastructure. This part of the file will see a lot of change and improvements after Icechunk 1.0.

In object stores that supported, the file information (spec version, compression, type, etc) is also store as object store metadata. Icechunk writes this information but currently doesn't use it during reads. Details of the metadata format TBD.

### File structure

| Byte index    | Content               | Example |
| ------------- | --------------- |--------- |
| 0 - 11        | Magic bytes           | ICE🧊CHUNK              |
| 12 - 23       | Implementation id     | 'ic-1.0.0    '          |
| 24            | Spec version          | 0x01                    |
| 25            | File type             | 0x01                    |
| 26            | Compression           | 0x01                    |
| 27..end       | Msgpack serializanion |                         |
