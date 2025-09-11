#!/usr/bin/env python3
"""
Simple pytest test runner for our misspelling tests
"""

import subprocess
import tempfile
import os

# Create a minimal test file with just our misspelling tests
test_content = '''
import icechunk as ic

def test_misspelling_warnings_for_icechunk(capfd):
    """Test that misspelled variations of 'icechunk' trigger warnings."""
    # Test common misspellings
    ic.set_logs_filter("icehunk:trace")
    err_output = capfd.readouterr().err
    assert "Warning" in err_output and "icehunk" in err_output

    ic.set_logs_filter("icecunk:debug")
    err_output = capfd.readouterr().err
    assert "Warning" in err_output and "icecunk" in err_output

def test_no_false_positives_for_correct_spelling(capfd):
    """Test that correct 'icechunk' spelling doesn't trigger warnings."""
    ic.set_logs_filter("icechunk:debug")
    assert "Warning" not in capfd.readouterr().err

def test_no_warnings_for_generic_log_levels(capfd):
    """Test that generic log levels don't trigger warnings."""
    ic.set_logs_filter("debug")
    assert "Warning" not in capfd.readouterr().err

def test_complex_filter_strings_with_misspellings(capfd):
    """Test complex filter strings with multiple modules."""
    ic.set_logs_filter("icehunk:trace,debug,tokio::io:warn")
    err_output = capfd.readouterr().err
    assert "Warning" in err_output and "icehunk" in err_output
'''

if __name__ == "__main__":
    # Write test content to a temporary file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(test_content)
        temp_test_file = f.name

    try:
        # Run pytest on the temporary file
        result = subprocess.run(
            [
                "python",
                "-m",
                "pytest",
                temp_test_file,
                "-v",
                "--tb=short",
                "--no-header",
            ],
            capture_output=True,
            text=True,
            cwd="/Users/ian/Documents/dev/icechunk",
        )

        print("STDOUT:")
        print(result.stdout)
        print("\nSTDERR:")
        print(result.stderr)
        print(f"\nReturn code: {result.returncode}")

        if result.returncode == 0:
            print("\n🎉 All pytest tests passed!")
        else:
            print("\n❌ Some pytest tests failed!")

    finally:
        # Clean up
        os.unlink(temp_test_file)
