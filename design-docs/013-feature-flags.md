# Feature flags

We want to add to the format the ability to soft-block the repo from executing
certain operations.

## Design

We add to `repo.fbs`

```flatbuffers

table Repo {
  ...
  features: [string];
}
```

The full list of feature flag strings is hardcoded in the code. We introduce new
API:

```python

class FeatureFlagStatus(Enum):
  Enabled
  Disabled
  Unset

class FeatureFlag:
  name: str
  default_enabled: bool
  status: FeatureFlagStatus

  @property
  def enabled(&self) -> bool:
    ...

class Repository:
  ...
  def feature_flags(self) -> list[FeatureFlag]:
    ...

  def update_feature_flag(self, feature_name: str, status: FeatureFlagStatus):
    ...
```

Icechunk code has (something isomorphic to) a hardcoded list of `FeatureFlag`.

The presence of a string in the `features` array means that the opposite of
its default state is enabled. For features that are `default_enabled = false`
the presence means `enabled`, but if `default_enabled = true` the presence
means `disabled`.

We organize `default_enabled` for existing features so most features are in their
default states. Unset feature flags are not serialized so most repos will have
an empty list of features (which is serialized as null).

Before executing a function Icechunk checks the status of the corresponding
feature flag. Of course feature flags cannot be enforced, since user has access
to the full on-disk repository, but the Icechunk library will honor them.

## Proposed list of features

These are some of the proposed feature flags, with a subjective indication if
they should be released in version 2.0 or later. This judgement is mostly
based on importance of the feature flag and complexity of implementation.

| name                               | default            | implement in   |
| -----------------------------------| -------------------|----------------|
| commit                             | enabled            | > 2.0          |
| amend                              | enabled            |2.0             |
| rebase                             | enabled            |2.0             |
| move_node                          | enabled            |2.0             |
| create_new_nodes                   | enabled            |> 2.0           |
| delete_nodes                       | enabled            |> 2.0           |
| update_chunks                      | enabled            | > 2.0          |
| update_array_metadata              | enabled            | > 2.0          |
| update_group_metadata              | enabled            | > 2.0          |
| tag_create                         | enabled            |2.0             |
| tag_delete                         | enabled            |2.0             |
| branch_create                      | enabled            |2.0             |
| branch_delete                      | enabled            |2.0             |
| branch_reset                       | enabled            |2.0             |
| garbage_collection                 | enabled            |2.0             |
| expiration                         | enabled            |2.0             |
| upgrade_spec_version               | enabled            |2.0             |
| update_config                      | enabled            |2.0             |
| set_default_commit_metadata        | enabled            |> 2.0           |
| update_repository_metadata         | enabled            | > 2.0          |
| rewrite_manifests                  | enabled            |2.0             |

## Other format changes

See [more format changes](./012-some-more-IC2-format-changes.md) in the
Icechunk 2.0 library.
