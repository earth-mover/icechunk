#!/usr/bin/env python3

# Simple test to understand mypy unreachable analysis
def test_function() -> None:
    print("Before")

    # Simulate the pattern from the test
    session_mock = type(
        "Mock",
        (),
        {"discard_changes": lambda self: None, "has_uncommitted_changes": False},
    )()

    session_mock.discard_changes()
    print("After discard_changes - this should be reachable")
    assert not session_mock.has_uncommitted_changes

    # This should also be reachable
    print("This line should be reachable too")


if __name__ == "__main__":
    test_function()
