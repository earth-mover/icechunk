# Compatibility Notes for Icechunk Libraries

The [version policy](./version-policy.md) promises that any 2.x library reads any spec 2.x repository, and either writes it safely or refuses to write it. Some releases of the official Icechunk libraries fall short of that promise in specific and non-critical cases, listed here by the releases they affect.

## Icechunk 2.0.x

- **Spec 2.1 expiration history.** The `pruned_ancestor_tx_logs` field added in [spec 2.1](./spec-v2-1.md) predates [writer features](./spec-v2-2.md#writer-features). An Icechunk 2.0.x library removes this field from every snapshot whenever it modifies a repository, and its garbage collection deletes the transaction logs the field lists. After that, `diff`, `rebase`, `amend` and `inspect` across expired commits are incomplete. Use Icechunk 2.1.0 or later for every library that modifies a repository that has been expired.

## Icechunk 2.0.0 to 2.2.0

- **Large repositories.** These releases cannot open a repository whose repository info holds more than about one million internal records (roughly 300,000 to 1,000,000 snapshots). Icechunk 2.2.1 and later open them.

## Icechunk 2.0 to 2.3

- **Writer features.** These releases don't know about [writer features](./spec-v2-2.md#writer-features). They can't open repositories in the [filesystem layout](./spec-filesystem-layout-v1.md), the only layout that uses one. Convert such a repository to the object store layout to use it with them.
- **Configuration.** These releases cannot open a repository whose configuration uses a setting value added later, and drop settings they do not recognize when they change the configuration. Icechunk 2.4.0 and later open such repositories, ignore the settings they cannot decode, and keep them.
- **Feature flags.** Flags other than `move_node`, `create_tag` and `delete_tag` are not enforced. To stop every writer, set the repository status to read-only.
