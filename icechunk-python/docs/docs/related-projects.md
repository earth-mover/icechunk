---
title: Related Projects
---
# Related Projects

This page lists community and ecosystem projects that integrate with Icechunk.

!!! tip "Add your project"

    If you have a project that integrates with Icechunk, please [open a pull request](https://github.com/earth-mover/icechunk/pulls) to add it here!

## zarrs_icechunk

[zarrs_icechunk](https://github.com/zarrs/zarrs_icechunk) provides Icechunk store support for the [zarrs](https://zarrs.dev) Rust crate. This enables reading and writing Icechunk repositories using the zarrs library, an alternative Rust implementation of the Zarr specification.

## icechunk-js

[icechunk-js](https://github.com/EarthyScience/icechunk-js) is a read-only JavaScript/TypeScript reader for Icechunk repositories, designed for use with [zarrita.js](https://github.com/manzt/zarrita.js). This enables browser-based and Node.js applications to read data directly from Icechunk repositories.

!!! note

    This is a community-built alternative to the official [Icechunk JavaScript/TypeScript package](./icechunk-js.md), which provides full read-write support via NAPI and WASM bindings.

## Zarrs.jl

[Zarrs.jl](https://github.com/earth-mover/Zarrs.jl) brings Icechunk support to the Julia language via zarrs. This enables Julia users to read and write Icechunk repositories using the zarrs storage backend.
