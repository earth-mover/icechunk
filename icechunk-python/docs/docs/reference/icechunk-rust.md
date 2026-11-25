---
title: Rust
---

# Icechunk Rust

The Icechunk rust library is used internally by Icechunk Python.
It is currently not designed to be used in standalone form.

- [Icechunk Rust Documentation](https://docs.rs/icechunk/latest/icechunk/) at docs.rs

We welcome contributors interested in implementing more Rust functionality!

## WebAssembly (browsers)

`icechunk` builds for `wasm32-unknown-unknown` with `default-features = false`.
There it runs on the JavaScript event loop, with no Tokio runtime or threads:

- Async traits such as `Storage` do not require `Send` futures, so a backend can
  await JS promises (for example `fetch`). Implement them with
  `#[icechunk_storage::maybe_send_async_trait]` instead of `#[async_trait]`; it
  keeps `Send` futures on every other target.
- `icechunk::rt` runs tasks and timers on the JS event loop and blocking work
  (decompression, decoding) inline. Use a Web Worker to keep a page responsive
  with large repositories.
- `zstd-sys` compiles C code: build with `clang` and
  `AR_wasm32_unknown_unknown=llvm-ar`. Other `ar`s write archives the wasm linker
  cannot index, and zstd silently goes missing at link time.
- Garbage collection and expiration are not supported yet (they use Tokio timers).

The JavaScript package [`@earthmover/icechunk`](https://github.com/earth-mover/icechunk/tree/main/icechunk-js) is built this way
for browsers.
