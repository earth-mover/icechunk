# Evaluation of different serialization formats

We want to move away from msgpack serialization for Icechunk metadata files.

## Why

* Msgpack requires a expensive parsing process upfront. If the user only wants
to pull a few chunk refs from a manifest, they still need to parse the whole manifest.
* Msgpack deserializes to Rust datastructures. This is good for simplicity of code, but
probably not good for memory consumption (more pointers everywhere).
* Msgpack gives too many options on how to serialize things, there is no canonical way,
so it's not easy to predict how `serde` is going to serialize our detastructures, and
could even change from version to version.
* It's hard to explain in the spec what goes into the metadata files, we would need to go
into `rmp_serde` implementation, see what they do, and document that in the spec.

## Other options

There is a never ending menu. From a custom binary format, to Parquet, and everything else.
We focused mostly on no-copy formats, for some of the issues enumerated above. Also
there is a preference for formats that have a tight schema and can be documented with
some form of IDL.

## Performance evaluation

We evaluated performance of msgpack, flatbuffers and capnproto. Evaluation looks at:

* Manifest file size, for a big manifest with 1M native chunk refs.
* Speed of writing.
* Speed of reading.

We wrote an example program in `examples/multithreaded_get_chunk_refs.rs`.
This program writes a big repo to local file storage, it doesn't really write the chunks,
we are not interested in benchmarking that. It executes purely in Rust, not using the python interface.

It writes a manifest with 1M native chunk refs, using zstd compression level 3. The writes are done
from 1M concurrent async tasks.

It then executes 1M chunk ref reads (notice, the refs are read, not the chunks that are not there).
Reads are executed from 4 threads with 250k concurrent async tasks each.

Notice:

* We are comparing local file system on purpose, to not account for network times
* We are comparing pulling refs only, not chunks, which is a worst case. In the real
  world, read operations are dominated by the time taken to fetch the chunks.
* The evaluation was done in an early state of the code, where many parts were unsafe,
  but we have verified there are no huge differences.

### Results for writes

```sh
nix run nixpkgs#hyperfine -- \
  --prepare 'rm -rf /tmp/test-perf' \
  --warmup 1 \
  'cargo run --release --example multithreaded_get_chunk_refs -- --write /tmp/test-perf'
```

#### Flatbuffers

Compressed manifest size:  27_527_680 bytes

```
Time (mean ± σ):      5.698 s ±  0.163 s    [User: 4.764 s, System: 0.910 s]
Range (min … max):    5.562 s …  6.103 s    10 runs
```

#### Capnproto

Compressed manifest size:  26_630_927 bytes

```
Time (mean ± σ):      6.276 s ±  0.163 s    [User: 5.225 s, System: 1.017 s]
Range (min … max):    6.126 s …  6.630 s    10 runs
```

#### Msgpack

Compressed manifest size: 22_250_152 bytes

```
Time (mean ± σ):      6.224 s ±  0.155 s    [User: 5.488 s, System: 0.712 s]
Range (min … max):    6.033 s …  6.532 s    10 runs
```

### Results for reads

```sh
nix run nixpkgs#hyperfine -- \
  --warmup 1 \
  'cargo run --release --example multithreaded_get_chunk_refs -- --read /tmp/test-perf'
```

#### Flatbuffers

```
Time (mean ± σ):      3.676 s ±  0.257 s    [User: 7.385 s, System: 1.819 s]
Range (min … max):    3.171 s …  4.038 s    10 runs
```

#### Capnproto

```
Time (mean ± σ):      5.254 s ±  0.234 s    [User: 11.370 s, System: 1.962 s]
Range (min … max):    4.992 s …  5.799 s    10 runs
```

#### Msgpack

```
Time (mean ± σ):      3.310 s ±  0.606 s    [User: 5.975 s, System: 1.762 s]
Range (min … max):    2.392 s …  4.102 s    10 runs
```

## Conclusions

* Compressed manifest is 25% larger in flatbuffers than msgpack
* Flatbuffers is slightly faster for commits
* Flatbuffers is slightly slower for reads
* Timing differences are not significant for real world scenarios, where performance
is dominated by the time taken downloading or uploading chunks.
* Manifest fetch time differences could be somewhat significant for workloads where
latency to first byte is important. This is not the use case Icechunk optimizes for.

## Decision

We are going to use flatbuffers for our metadata on-disk format.
