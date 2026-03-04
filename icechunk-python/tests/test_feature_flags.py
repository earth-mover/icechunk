import pytest

import icechunk as ic


def test_set_and_unset_flags_on_repo() -> None:
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
        spec_version=2,
    )

    # All flags should be present and enabled by default
    all_flags = repo.feature_flags()
    assert len(all_flags) > 0
    for f in all_flags:
        assert f.enabled
        assert f.default_enabled
        assert f.setting is None
    total = len(all_flags)

    enabled = repo.enabled_feature_flags()
    assert len(enabled) == total
    assert len(repo.disabled_feature_flags()) == 0

    # Pick a flag to test with
    flag_name = all_flags[0].name
    flag_id = all_flags[0].id

    # Disable it explicitly
    repo.set_feature_flag(flag_name, False)

    disabled = repo.disabled_feature_flags()
    assert len(disabled) == 1
    assert disabled[0].name == flag_name
    assert not disabled[0].enabled
    assert disabled[0].setting is False
    assert len(repo.enabled_feature_flags()) == total - 1

    # Enable another flag explicitly (already enabled by default, but now set explicitly)
    other_name = all_flags[1].name
    other_id = all_flags[1].id
    repo.set_feature_flag(other_name, True)

    by_name = {f.name: f for f in repo.feature_flags()}
    assert by_name[other_name].enabled
    assert by_name[other_name].setting is True
    # first flag is still disabled
    assert not by_name[flag_name].enabled
    assert len(repo.enabled_feature_flags()) == total - 1

    # Reset first flag to default (which is enabled)
    repo.set_feature_flag(flag_name, None)

    assert len(repo.disabled_feature_flags()) == 0
    by_name = {f.name: f for f in repo.feature_flags()}
    assert by_name[flag_name].enabled
    assert by_name[flag_name].setting is None

    # Check ops log has the right FeatureFlagChanged entries
    ops = list(repo.ops_log())
    # Most recent first: set(flag, None), set(other, True), set(flag, False), RepoInitialized
    assert len(ops) == 4
    assert isinstance(ops[0].kind, ic.UpdateType.FeatureFlagChanged)
    assert ops[0].kind.id == flag_id
    assert ops[0].kind.new_value is None
    assert isinstance(ops[1].kind, ic.UpdateType.FeatureFlagChanged)
    assert ops[1].kind.id == other_id
    assert ops[1].kind.new_value is True
    assert isinstance(ops[2].kind, ic.UpdateType.FeatureFlagChanged)
    assert ops[2].kind.id == flag_id
    assert ops[2].kind.new_value is False
    assert isinstance(ops[3].kind, ic.UpdateType.RepoInitialized)


async def test_set_and_unset_flags_on_repo_async() -> None:
    repo = await ic.Repository.create_async(
        storage=ic.in_memory_storage(),
        spec_version=2,
    )

    all_flags = await repo.feature_flags_async()
    assert len(all_flags) > 0
    total = len(all_flags)

    assert len(await repo.enabled_feature_flags_async()) == total
    assert len(await repo.disabled_feature_flags_async()) == 0

    flag_name = all_flags[0].name

    await repo.set_feature_flag_async(flag_name, False)

    disabled = await repo.disabled_feature_flags_async()
    assert len(disabled) == 1
    assert disabled[0].name == flag_name
    assert not disabled[0].enabled
    assert len(await repo.enabled_feature_flags_async()) == total - 1

    await repo.set_feature_flag_async(flag_name, None)
    assert len(await repo.disabled_feature_flags_async()) == 0


def test_tag_ops_blocked_by_feature_flags() -> None:
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
        spec_version=2,
    )

    snap_id = repo.lookup_branch("main")

    # Tags work while flags are enabled
    repo.create_tag("exists", snap_id)

    # Disable both tag flags
    repo.set_feature_flag("create_tag", False)
    repo.set_feature_flag("delete_tag", False)

    with pytest.raises(ic.IcechunkError):
        repo.create_tag("should_fail", snap_id)

    with pytest.raises(ic.IcechunkError):
        repo.delete_tag("exists")


async def test_tag_ops_blocked_by_feature_flags_async() -> None:
    repo = await ic.Repository.create_async(
        storage=ic.in_memory_storage(),
        spec_version=2,
    )

    snap_id = await repo.lookup_branch_async("main")

    await repo.create_tag_async("exists", snap_id)

    await repo.set_feature_flag_async("create_tag", False)
    await repo.set_feature_flag_async("delete_tag", False)

    with pytest.raises(ic.IcechunkError):
        await repo.create_tag_async("should_fail", snap_id)

    with pytest.raises(ic.IcechunkError):
        await repo.delete_tag_async("exists")
