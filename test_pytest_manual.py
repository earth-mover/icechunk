#!/usr/bin/env python3
"""
Manual test to verify our pytest misspelling tests work
"""

import sys
import io
import icechunk as ic


class MockCapfd:
    """Mock pytest's capfd fixture"""

    def __init__(self):
        self._stderr_capture = None

    def __enter__(self):
        self._stderr_capture = io.StringIO()
        self._old_stderr = sys.stderr
        sys.stderr = self._stderr_capture
        return self

    def __exit__(self, *args):
        sys.stderr = self._old_stderr

    def readouterr(self):
        if self._stderr_capture:
            captured = self._stderr_capture.getvalue()
            self._stderr_capture = io.StringIO()  # Reset for next capture
            sys.stderr = self._stderr_capture

            class Result:
                def __init__(self, err):
                    self.err = err
                    self.out = ""

            return Result(captured)
        return Result("")


def test_misspelling_warnings_for_icechunk():
    """Test that misspelled variations of 'icechunk' trigger warnings."""
    print("Testing misspelling warnings...")

    tests = [
        ("icehunk:trace", "icehunk"),
        ("icecunk:debug", "icecunk"),
        ("iecchunk:info", "iecchunk"),
        ("ichunk:warn", "ichunk"),
    ]

    for filter_str, expected_misspelling in tests:
        print(f"  Testing: {filter_str}")

        # Start capturing stderr
        capfd = MockCapfd()
        capfd.__enter__()

        try:
            ic.set_logs_filter(filter_str)
            err_output = capfd.readouterr().err
        finally:
            capfd.__exit__(None, None, None)

        print(f"    Captured: {repr(err_output)}")

        # Check for warning and misspelling
        has_warning = "Warning" in err_output
        has_misspelling = expected_misspelling in err_output

        if has_warning and has_misspelling:
            print("    ✅ PASS")
        else:
            print(
                f"    ❌ FAIL - Warning: {has_warning}, Misspelling: {has_misspelling}"
            )
            return False

    return True


def test_no_false_positives():
    """Test that correct spellings don't trigger warnings."""
    print("\nTesting no false positives...")

    tests = [
        "icechunk:debug",
        "debug",
        "tokio:debug",
    ]

    for filter_str in tests:
        print(f"  Testing: {filter_str}")

        # Start capturing stderr
        capfd = MockCapfd()
        capfd.__enter__()

        try:
            ic.set_logs_filter(filter_str)
            err_output = capfd.readouterr().err
        finally:
            capfd.__exit__(None, None, None)

        print(f"    Captured: {repr(err_output)}")

        has_warning = "Warning" in err_output

        if not has_warning:
            print("    ✅ PASS")
        else:
            print("    ❌ FAIL - Unexpected warning")
            return False

    return True


if __name__ == "__main__":
    success1 = test_misspelling_warnings_for_icechunk()
    success2 = test_no_false_positives()

    if success1 and success2:
        print("\n🎉 All pytest-style tests passed!")
        sys.exit(0)
    else:
        print("\n💥 Some tests failed!")
        sys.exit(1)
