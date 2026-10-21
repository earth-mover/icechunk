# Feature flags

We want to add to the format the ability to soft-block the repo from executing
certain operations.

## Design

We add to `repo.fbs`

```flatbuffers

table Repo {
  ...
  enabled_features: [u16];
  disabled_features: [u16];
}
```

The full list of feature flag strings is hardcoded in the code. We introduce new
API:

```python

class FeatureFlag:
  it: int
  name: str
  default_enabled: bool
  setting: bool | None  # None for unset

  @property
  def enabled(&self) -> bool:  # takes into account the default and the setting
    ...

class Repository:
  ...
  def feature_flags(self) -> list[FeatureFlag]:
    ...
  def enabled_feature_flags(self) -> list[FeatureFlag]:
    ...
  def disabled_feature_flags(self) -> list[FeatureFlag]:
    ...

  def update_feature_flag(self, feature_name: str, set_to: bool | None):
    ...
```

Icechunk code has (something isomorphic to) a hardcoded list of `FeatureFlag`.

The presence of a string in the `enabled_features` array means that it was
explicitly set by the user. Same for `disabled_features`, a flag that is
not present in either lists is at its default value.

We organize `default_enabled` for existing features so most features are in their
default states. Default feature flags are not serialized so most repos will have
two empty lists of features (which is serialized as null).

Before executing a function Icechunk checks the status of the corresponding
feature flag. Of course feature flags cannot be enforced, since user has access
to the full on-disk repository, but the Icechunk library will honor them.

## List of features

All flags default to enabled. IDs are fixed; the code holds them in
`icechunk/src/feature_flags.rs`.

| id | name                        | status                                             |
| -- | --------------------------- | -------------------------------------------------- |
| 1  | commit                      | implemented                                        |
| 2  | amend                       | implemented                                        |
| 3  | move_node                   | implemented                                        |
| 4  | create_tag                  | implemented                                        |
| 5  | delete_tag                  | implemented                                        |
| 6  | rebase                      | implemented                                        |
| 7  | create_new_nodes            | implemented, checked at commit and flush           |
| 8  | delete_nodes                | implemented, checked at commit and flush           |
| 9  | update_chunks               | implemented, checked at commit and flush           |
| 10 | update_array_metadata       | implemented, checked at commit and flush           |
| 11 | update_group_metadata       | implemented, checked at commit and flush           |
| 12 | create_branch               | implemented                                        |
| 13 | delete_branch               | implemented                                        |
| 14 | reset_branch                | implemented                                        |
| 15 | garbage_collection          | implemented                                        |
| 16 | expiration                  | implemented                                        |
| 17 | upgrade_spec_version        | reserved; no upgrade path starts from a V2 repo    |
| 18 | update_config               | implemented                                        |
| 19 | set_default_commit_metadata | implemented, checked at commit and flush           |
| 20 | update_repository_metadata  | implemented                                        |
| 21 | rewrite_manifests           | implemented                                        |

## Flag interactions

Each flag guards one operation. A flag does not guard a result that a different
operation can also produce.

- The content flags (7 to 11) apply to the change set of a commit or a flush.
  They do not apply to ref operations or to moves.
- `reset_branch` can move a branch to any snapshot. This can remove nodes,
  chunks and metadata changes. To freeze the content of a branch, also disable
  `reset_branch`.
- `delete_branch` followed by `create_branch` gives the same result as
  `reset_branch`. To block resets, disable all three flags.
- `move_node` moves a node to a new path. It does not need `create_new_nodes` or
  `delete_nodes`.
- An expiration that deletes expired branches or tags also needs `delete_branch`
  or `delete_tag`.
- `set_feature_flag` has no flag. Any writer can change any flag.

Rules for `update_chunks`:

- A chunk write to an array that the same session creates needs only
  `create_new_nodes`.
- An array update that removes chunk coordinates needs `update_chunks`. This
  applies when a dimension has fewer chunks, or when the number of dimensions
  changes. An update that adds chunks needs only `update_array_metadata`.
- A chunk delete needs `update_chunks`, also when the chunk was never written.
  The commit does not read the manifests to find deletes that change nothing.

## Other format changes

See [more format changes](./012-some-more-IC2-format-changes.md) in the
Icechunk 2.0 library.
