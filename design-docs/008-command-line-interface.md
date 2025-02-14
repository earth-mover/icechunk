# Icechunk Command Line Interface

This document outlines the design of the Icechunk command line interface.

## Functionality

Here is a list of tasks a user might want to do with Icechunk:

- List repositories in the configuration
- List a history of a repo
- List branches in a repo
- List tags in a repo
- Print the zarr hierarchy
- Get repo statistics (e.g. `getsize`)
- Create a new repository
- Check configuration
- Diff between two commits
- Invoke administrative tasks (garbage collection, compaction, etc)

This is not an exhaustive list.

## Interface

General command structure

```bash
icechunk <object> <action> <args>
```

Examples

```bash
icechunk repo list

icechunk repo create <repo>
icechunk repo info <repo>
icechunk repo tree <repo>
icechunk repo delete <repo>

icechunk branch list <repo>
icechunk branch create <repo> <branch_name>
icechunk snapshot list <repo>
icechunk snapshot diff <repo> <snapshot_id_1> <snapshot_id_2>
icechunk ref list <repo>

icechunk config init   # init: interactive setup
icechunk config list 
icechunk config get <key>
icechunk config set <key> <value>

```

### Git-like interface

Alternative would be a more git-like structure (`git diff`, `git show`, ..).

The git interface is familiar, but

- The differences between git and Icechunk can be deceptive to new users
- The git interface is (arguably) not very user-friendly if you're not familiar with it
- This structure is more extensible
  - Example: Docker [adopting](https://www.docker.com/blog/whats-new-in-docker-1-13/) this structure over time (`docker ps` -> `docker container ls`)

## Configuration

Two guiding use-cases

- User just wants to `icechunk repo create s3://bucket/path`, get credentials from environment/aws config, and use default repo settings.
- User wants to manage multiple repositories stored in different locations, with different credentials and settings.

Following Icechunk's config module, there are four types of information needed to work with a repository:

- Location: `bucket`, `path`
- Credentials: `access_key_id`, `secret_access_key`, ..
- Options: `region`, `endpoint_url`, ..
- Repo configuration: `compression`, `caching`, `virtual_chunk_containers`, ..

There are three ways to provide this information, in the standard order of precedence:

1. Command line arguments
2. Environment variables
3. Configuration file


### Repositories configuration

The CLI repositories configuration file.

> Note: This configuration could also be used by the library.

A first draft of the structure:

```rust
use std::collections::HashMap;

use crate::config::{RepositoryConfig, ObjectStoreConfig, Credentials}

pub struct RepoLocation {
    bucket: String,
    prefix: String,
}

pub struct RepositoryDefinition {
    location: RepoLocation,
    object_store_config: ObjectStoreConfig,
    credentials: Credentials,
    config: RepositoryConfig,
}

pub struct RepositoryAlias(String);

pub struct Repositories {
    repos: HashMap<RepositoryAlias, RepositoryDefinition>,
}
```

## Python packaging

Following the [Python entrypoint](https://www.maturin.rs/bindings#both-binary-and-library) approach.

- cli implemented in `icechunk/src/cli/`
- cli exposed to Rust in `icechunk/src/bin/icechunk/`
- cli exposed to Python through an entrypoint function, exposed in `pyproject.toml`

```ini
[project.scripts]
icechunk = "icechunk._icechunk_python:cli_entrypoint"
```

The disadvantage is that Python users need to call Python to use the CLI, resulting in hundreds of milliseconds of latency.

The user can also install the Rust binary directly through `cargo install`.

## Implementation details

Implemented with

- [clap](https://crates.io/crates/clap) for the CLI
  - [clap_complete](https://crates.io/crates/clap_complete) for shell completion
- [anyhow](https://crates.io/crates/anyhow) for error handling
- [serde_yaml_ng](https://crates.io/crates/serde_yaml_ng) for configuration
- [dialoguer](https://crates.io/crates/dialoguer) for user input

## Optional features

- Structured output option (e.g. JSON)
- Short version of the command (e.g. `ic`)
- Support for tab completion
