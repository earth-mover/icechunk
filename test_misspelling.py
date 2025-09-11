#!/usr/bin/env python3
"""
Simple test for misspelling detection functionality
"""

import sys
import subprocess

# Test cases
test_cases = [
    # (filter_string, should_warn, expected_misspelling)
    ("icehunk:trace", True, "icehunk"),
    ("icecunk:debug", True, "icecunk"),
    ("iecchunk:info", True, "iecchunk"),
    ("ichunk:warn", True, "ichunk"),
    ("icechunk:debug", False, None),
    ("icechunk::repository:trace", False, None),
    ("debug", False, None),
    ("trace", False, None),
    ("tokio:debug", False, None),
    ("serde:info", False, None),
    ("icehunk:trace,debug,tokio::io:warn", True, "icehunk"),
    ("debug,icecunk:info,warn", True, "icecunk"),
]


def test_misspelling_detection():
    """Test that misspelling detection works as expected"""

    print("Testing misspelling detection...")
    print("=" * 50)

    passed = 0
    failed = 0

    for filter_str, should_warn, expected_misspelling in test_cases:
        print(f"\nTesting filter: '{filter_str}'")
        print(f"Expected warning: {should_warn}")

        # Run the test
        cmd = f'import icechunk as ic; ic.set_logs_filter("{filter_str}")'
        result = subprocess.run(
            ["python", "-c", cmd],
            capture_output=True,
            text=True,
            cwd="/Users/ian/Documents/dev/icechunk",
        )

        stderr_output = result.stderr
        # Check for either colored or plain "Warning" text
        has_warning = (
            "Warning" in stderr_output
            or "\x1b[93m\x1b[1mWarning\x1b[0m" in stderr_output
        )

        print(f"Got warning: {has_warning}")
        if stderr_output:
            print(f"stderr: {stderr_output.strip()}")

        # Check if result matches expectation
        if has_warning == should_warn:
            if should_warn and expected_misspelling in stderr_output:
                print("✅ PASS")
                passed += 1
            elif not should_warn:
                print("✅ PASS")
                passed += 1
            else:
                print("❌ FAIL - Warning content doesn't match expected")
                failed += 1
        else:
            print("❌ FAIL - Warning presence doesn't match expected")
            failed += 1

    print("\n" + "=" * 50)
    print(f"Results: {passed} passed, {failed} failed")

    if failed == 0:
        print("🎉 All tests passed!")
        return True
    else:
        print(f"💥 {failed} tests failed!")
        return False


if __name__ == "__main__":
    success = test_misspelling_detection()
    sys.exit(0 if success else 1)
