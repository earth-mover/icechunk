#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "rich>=13.0.0",
# ]
# ///
"""
Check that parameter documentation in icechunk.xarray.to_icechunk is consistent
with xarray.Dataset.to_zarr.

This script extracts the docstrings for specific parameters and compares them,
accounting for minor formatting differences.

Usage:
    # Using XARRAY_DIR environment variable
    export XARRAY_DIR=~/Documents/dev/xarray
    uv run scripts/check_xarray_docs_sync.py

    # Or specify directly
    uv run scripts/check_xarray_docs_sync.py --xarray-path ~/Documents/dev/xarray

    # Verbose output
    uv run scripts/check_xarray_docs_sync.py --verbose
"""

import argparse
import difflib
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import NamedTuple

from rich.console import Console
from rich.table import Table
from rich.text import Text


class DocParam(NamedTuple):
    """Represents a parameter's documentation."""

    name: str
    doc: str


def extract_all_param_names(docstring: str) -> list[str]:
    """
    Extract all parameter names from a docstring.

    Parameters
    ----------
    docstring : str
        The full docstring to parse

    Returns
    -------
    list[str]
        List of all parameter names found
    """
    # Find the Parameters section
    params_match = re.search(
        r"Parameters\s*\n\s*-+\s*\n(.*?)(?=\n\s*(?:Returns|Raises|Notes|Examples|See Also|\Z))",
        docstring,
        re.DOTALL | re.MULTILINE,
    )

    if not params_match:
        return []

    params_text = params_match.group(1)
    param_names = []

    # Find all parameter declarations
    # Look for lines like "param_name : type" but exclude special sections like ".. Note::"
    for line in params_text.split("\n"):
        # Skip lines that start with ".." (RST directives)
        if line.strip().startswith(".."):
            continue
        param_match = re.match(r"^\s*(\w+)\s*:", line)
        if param_match:
            name = param_match.group(1)
            # Exclude capitalized names that are likely RST directives (Note, Warning, etc.)
            if not name[0].isupper():
                param_names.append(name)

    return param_names


def extract_param_docs(docstring: str, param_names: list[str]) -> dict[str, str]:
    """
    Extract documentation for specific parameters from a docstring.

    Parameters
    ----------
    docstring : str
        The full docstring to parse
    param_names : list[str]
        List of parameter names to extract

    Returns
    -------
    dict[str, str]
        Mapping of parameter name to its documentation text
    """
    # Find the Parameters section
    params_match = re.search(
        r"Parameters\s*\n\s*-+\s*\n(.*?)(?=\n\s*(?:Returns|Raises|Notes|Examples|See Also|\Z))",
        docstring,
        re.DOTALL | re.MULTILINE,
    )

    if not params_match:
        return {}

    params_text = params_match.group(1)
    param_docs = {}

    # Split by lines and parse parameter blocks
    # A parameter starts with "name :" at the beginning of a line (possibly indented)
    # and continues until the next parameter or end
    lines = params_text.split("\n")
    current_param = None
    current_doc_lines = []

    for line in lines:
        # Check if this line starts a new parameter
        param_match = re.match(r"^\s*(\w+)\s*:", line)
        if param_match:
            # Save previous parameter if it was one we're looking for
            if current_param and current_param in param_names and current_doc_lines:
                # Join and normalize the documentation
                doc_text = "\n".join(current_doc_lines)
                param_docs[current_param] = normalize_doc_text(doc_text)

            # Start new parameter
            current_param = param_match.group(1)
            # Get the rest of the line after the colon
            rest_of_line = line[param_match.end() :].strip()
            current_doc_lines = [rest_of_line] if rest_of_line else []
        elif current_param:
            # Continue current parameter documentation
            current_doc_lines.append(line)

    # Don't forget the last parameter
    if current_param and current_param in param_names and current_doc_lines:
        doc_text = "\n".join(current_doc_lines)
        param_docs[current_param] = normalize_doc_text(doc_text)

    return param_docs


def normalize_doc_text(text: str) -> str:
    """
    Normalize documentation text for comparison.

    - Removes extra whitespace within lines
    - Preserves paragraph breaks (double newlines)
    - Strips leading/trailing whitespace
    """
    # First normalize line breaks - collapse multiple blank lines to double newline
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    # Strip leading/trailing whitespace from each line but preserve paragraph breaks
    lines = []
    for line in text.split("\n"):
        stripped = line.strip()
        # Replace multiple spaces with single space
        stripped = re.sub(r" +", " ", stripped)
        lines.append(stripped)
    return "\n".join(lines).strip()


def highlight_line_with_char_diff(
    line: str, other_line: str, style: str, similarity_threshold: float = 0.5
) -> Text:
    """
    Highlight a line with character-level differences if lines are similar.

    Parameters
    ----------
    line : str
        The line to highlight
    other_line : str
        The line to compare against
    style : str
        Style to apply ("red" or "green")
    similarity_threshold : float
        Minimum similarity ratio to apply character-level diff

    Returns
    -------
    Text
        Rich Text object with highlighted differences
    """
    char_matcher = difflib.SequenceMatcher(None, line, other_line)
    similarity = char_matcher.ratio()
    text = Text()

    if similarity > similarity_threshold:
        # Lines are similar enough for character-level diff
        for tag, i1, i2, _j1, _j2 in char_matcher.get_opcodes():
            if tag == "equal":
                text.append(line[i1:i2])
            else:
                # Highlight character differences with bold
                text.append(line[i1:i2], style=f"bold {style}")
    else:
        # Lines too different, highlight entire line
        text.append(line, style=style)

    return text


def build_diff_text(
    lines: list[str], other_lines: list[str], style: str, is_source: bool
) -> Text:
    """
    Build rich Text with highlighted differences between two sets of lines.

    Parameters
    ----------
    lines : list[str]
        Lines to highlight (xarray if is_source=True, icechunk if False)
    other_lines : list[str]
        Lines to compare against
    style : str
        Base style for differences ("red" or "green")
    is_source : bool
        True if these are source lines (xarray), False if target (icechunk)

    Returns
    -------
    Text
        Rich Text object with highlighted differences
    """
    if is_source:
        # For xarray (source), we want to show deletions in red
        matcher = difflib.SequenceMatcher(None, lines, other_lines)
    else:
        # For icechunk (target), we want to show insertions in green
        matcher = difflib.SequenceMatcher(None, other_lines, lines)

    text = Text()

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        # Get the correct indices based on which version we're building
        my_i1, my_i2, other_i1, other_i2 = (
            (i1, i2, j1, j2) if is_source else (j1, j2, i1, i2)
        )

        if tag == "equal":
            text.append("".join(lines[my_i1:my_i2]))
        elif (tag == "delete" and is_source) or (tag == "insert" and not is_source):
            # Lines missing in other version
            text.append("".join(lines[my_i1:my_i2]), style=style)
        elif tag == "replace":
            # Check if it's a single line replacement for character-level diff
            if my_i2 - my_i1 == 1 and other_i2 - other_i1 == 1:
                text.append(
                    highlight_line_with_char_diff(
                        lines[my_i1], other_lines[other_i1], style
                    )
                )
            else:
                # Multiple lines - highlight all
                text.append("".join(lines[my_i1:my_i2]), style=style)

    return text


def compute_diff_hash(xr_normalized: str, ic_normalized: str) -> str:
    """
    Compute a hash of the diff between two normalized documentation strings.

    Parameters
    ----------
    xr_normalized : str
        Normalized xarray documentation
    ic_normalized : str
        Normalized icechunk documentation

    Returns
    -------
    str
        SHA256 hash of the diff
    """
    # Create a unified diff to capture the actual differences
    diff_lines = list(
        difflib.unified_diff(
            xr_normalized.splitlines(keepends=True),
            ic_normalized.splitlines(keepends=True),
            lineterm="",
        )
    )
    diff_text = "".join(diff_lines)
    return hashlib.sha256(diff_text.encode()).hexdigest()[:16]


def create_comparison_table(xr_text: Text, ic_text: Text) -> Table:
    """Create a rich Table for side-by-side documentation comparison."""
    table = Table(show_header=True, show_lines=True, expand=True)
    table.add_column(
        "Expected (xarray)\n[dim][red]missing text shown in red[/red][/dim]",
        style="white",
        width=60,
    )
    table.add_column(
        "Actual (icechunk)\n[dim][green]extra text shown in green[/green][/dim]",
        style="white",
        width=60,
    )
    table.add_row(xr_text, ic_text)
    return table


class ParamDiff(NamedTuple):
    """Represents a documentation difference for a parameter."""

    param: str
    diff_hash: str
    xr_normalized: str
    ic_normalized: str


def load_known_diffs(config_path: Path) -> dict[str, dict[str, str]]:
    """
    Load known acceptable documentation differences from config file.

    Returns dict mapping param name to {"hash": str, "reason": str}
    """
    if not config_path.exists():
        return {}

    with open(config_path) as f:
        data: dict[str, dict[str, dict[str, str]]] = json.load(f)
    return data.get("known_diffs", {})


def save_known_diffs(config_path: Path, known_diffs: dict[str, dict[str, str]]) -> None:
    """Save known acceptable documentation differences to config file."""
    data = {"known_diffs": known_diffs}
    with open(config_path, "w") as f:
        json.dump(data, f, indent=2)


def compare_docs(
    icechunk_docs: dict[str, str],
    xarray_docs: dict[str, str],
    param_names: list[str],
    console: Console,
    known_diffs: dict[str, dict[str, str]] | None = None,
) -> tuple[bool, list[str], list[str], list[ParamDiff]]:
    """
    Compare documentation for specified parameters.

    Parameters
    ----------
    known_diffs : dict[str, dict[str, str]] | None
        Optional dict of known acceptable diffs {param: {"hash": str, "reason": str}}

    Returns
    -------
    tuple[bool, list[str], list[str], list[ParamDiff]]
        (all_match, missing_params, missing_in_xarray, diffs_found)
    """
    all_match = True
    missing_params = []
    missing_in_xarray = []
    diffs_found: list[ParamDiff] = []

    for param in param_names:
        ic_doc = icechunk_docs.get(param, "")
        xr_doc = xarray_docs.get(param, "")

        if not ic_doc:
            missing_params.append(param)
            all_match = False
            continue

        if not xr_doc:
            missing_in_xarray.append(param)
            continue

        # Normalize for comparison
        ic_normalized = normalize_doc_text(ic_doc)
        xr_normalized = normalize_doc_text(xr_doc)

        if ic_normalized != xr_normalized:
            diff_hash = compute_diff_hash(xr_normalized, ic_normalized)
            diffs_found.append(ParamDiff(param, diff_hash, xr_normalized, ic_normalized))

            # Check if this is a known acceptable diff
            is_known_diff = (
                known_diffs is not None
                and param in known_diffs
                and known_diffs[param].get("hash") == diff_hash
            )

            if is_known_diff:
                reason = known_diffs[param].get("reason", "No reason provided")  # type: ignore[index]
                console.print(
                    f"\n[yellow]⚠️  Known difference for parameter '{param}':[/yellow]"
                )
                console.print(f"   Reason: {reason}")
                console.print(f"   Hash: {diff_hash}\n")
            else:
                console.print(
                    f"\n[bold]❌ Documentation mismatch for parameter '{param}':[/bold]"
                )
                console.print(f"[dim]Diff hash: {diff_hash}[/dim]\n")
                all_match = False

            # Split into lines for line-based comparison
            xr_lines = xr_normalized.splitlines(keepends=True)
            ic_lines = ic_normalized.splitlines(keepends=True)

            # Build highlighted text for both versions
            xr_text = build_diff_text(xr_lines, ic_lines, "red", is_source=True)
            ic_text = build_diff_text(ic_lines, xr_lines, "green", is_source=False)

            # Display side-by-side comparison
            table = create_comparison_table(xr_text, ic_text)
            console.print(table)
            console.print()

    return all_match, missing_params, missing_in_xarray, diffs_found


def main() -> int:
    parser = argparse.ArgumentParser(description="Check xarray documentation consistency")
    parser.add_argument(
        "--xarray-path",
        type=Path,
        help="Path to xarray source root (defaults to XARRAY_DIR env var or ../xarray)",
    )
    parser.add_argument(
        "--xarray-file",
        type=str,
        default="xarray/core/dataset.py",
        help="Path to xarray source file relative to xarray root (default: xarray/core/dataset.py)",
    )
    parser.add_argument(
        "--icechunk-path",
        type=Path,
        default=Path(__file__).parent.parent / "python" / "icechunk" / "xarray.py",
        help="Path to icechunk xarray.py file",
    )
    parser.add_argument(
        "--params",
        nargs="+",
        help="Parameters to check for consistency (default: all parameters from xarray function, excluding ignored ones)",
    )
    parser.add_argument(
        "--ignore-params",
        nargs="+",
        default=[
            "store",
            "chunk_store",
            "compute",
            "consolidated",
            "zarr_version",
            "write_empty_chunks",
            "synchronizer",
            "storage_options",
            "zarr_format",
        ],
        help="Parameters to ignore from xarray (not applicable to icechunk)",
    )
    parser.add_argument(
        "--xarray-function",
        default="to_zarr",
        help="Name of the xarray function to compare against (default: to_zarr)",
    )
    parser.add_argument(
        "--icechunk-function",
        default="to_icechunk",
        help="Name of the icechunk function to compare (default: to_icechunk)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed comparison output",
    )
    parser.add_argument(
        "--known-diffs",
        type=Path,
        default=Path(__file__).parent / "known-xarray-doc-diffs.json",
        help="Path to known diffs config file (default: scripts/known-xarray-doc-diffs.json)",
    )
    parser.add_argument(
        "--update-known-diffs",
        action="store_true",
        help="Update known diffs config with current diff hashes",
    )

    args = parser.parse_args()

    # Determine xarray path
    import os

    if args.xarray_path:
        xarray_path = args.xarray_path
    elif xarray_dir := os.environ.get("XARRAY_DIR"):
        xarray_path = Path(xarray_dir)
    else:
        xarray_path = Path(__file__).parent.parent.parent / "xarray"

    # Construct full path to xarray source file
    xarray_source_file = xarray_path / args.xarray_file

    # Create console for rich output
    console = Console()

    if not xarray_source_file.exists():
        console.print(
            f"[red]❌ Xarray source file not found at: {xarray_source_file}[/red]"
        )
        console.print(
            "   Please provide --xarray-path or set XARRAY_DIR environment variable"
        )
        console.print(f"   Or adjust --xarray-file (current: {args.xarray_file})")
        return 1

    if not args.icechunk_path.exists():
        console.print(
            f"[red]❌ Icechunk xarray.py not found at: {args.icechunk_path}[/red]"
        )
        return 1

    # Extract docs from xarray
    xarray_source = xarray_source_file.read_text()
    # Find the xarray function docstring
    xarray_pattern = rf'def {re.escape(args.xarray_function)}\(.*?\).*?:.*?"""(.*?)"""'
    xarray_match = re.search(xarray_pattern, xarray_source, re.DOTALL)

    if not xarray_match:
        console.print(
            f"[red]❌ Could not find {args.xarray_function} docstring in xarray[/red]"
        )
        return 1

    xarray_docstring = xarray_match.group(1)

    # Determine which parameters to check
    if args.params:
        # User specified specific parameters
        PARAMS_TO_CHECK = args.params
        IGNORED_PARAMS = []
    else:
        # Extract all parameters from xarray and filter out ignored ones
        all_xarray_params = extract_all_param_names(xarray_docstring)
        IGNORED_PARAMS = args.ignore_params
        PARAMS_TO_CHECK = [p for p in all_xarray_params if p not in IGNORED_PARAMS]

    console.print(
        f"[bold]Checking documentation consistency for:[/bold] {', '.join(PARAMS_TO_CHECK)}"
    )
    if IGNORED_PARAMS:
        console.print(f"[dim]Ignored parameters:[/dim] {', '.join(IGNORED_PARAMS)}")
    console.print(f"[dim]Xarray source:[/dim] {xarray_source_file}")
    console.print(f"[dim]Xarray function:[/dim] {args.xarray_function}")
    console.print(f"[dim]Icechunk source:[/dim] {args.icechunk_path}")
    console.print(f"[dim]Icechunk function:[/dim] {args.icechunk_function}")
    console.print()

    xarray_docs = extract_param_docs(xarray_docstring, PARAMS_TO_CHECK)

    # Extract docs from icechunk
    icechunk_source = args.icechunk_path.read_text()
    icechunk_pattern = (
        rf'def {re.escape(args.icechunk_function)}\(.*?\).*?:.*?"""(.*?)"""'
    )
    icechunk_match = re.search(icechunk_pattern, icechunk_source, re.DOTALL)

    if not icechunk_match:
        console.print(
            f"[red]❌ Could not find {args.icechunk_function} docstring in icechunk[/red]"
        )
        return 1

    icechunk_docstring = icechunk_match.group(1)
    icechunk_docs = extract_param_docs(icechunk_docstring, PARAMS_TO_CHECK)

    # Load known diffs if available
    known_diffs = load_known_diffs(args.known_diffs)

    # Compare
    all_match, missing_params, missing_in_xarray, diffs_found = compare_docs(
        icechunk_docs, xarray_docs, PARAMS_TO_CHECK, console, known_diffs
    )

    # Update known diffs if requested
    if args.update_known_diffs and diffs_found:
        console.print("\n[bold]Updating known diffs file...[/bold]")
        for diff in diffs_found:
            if diff.param not in known_diffs:
                known_diffs[diff.param] = {
                    "hash": diff.diff_hash,
                    "reason": "TODO: Add reason for this difference",
                }
                console.print(f"  Added {diff.param}: {diff.diff_hash}")
            elif known_diffs[diff.param]["hash"] != diff.diff_hash:
                console.print(
                    f"  [yellow]Hash changed for {diff.param}:[/yellow] {known_diffs[diff.param]['hash']} -> {diff.diff_hash}"
                )
                known_diffs[diff.param]["hash"] = diff.diff_hash
        save_known_diffs(args.known_diffs, known_diffs)
        console.print(f"[green]Saved to {args.known_diffs}[/green]\n")

    # Print summary at the end
    console.print("\n" + "=" * 80)
    console.print("[bold]Summary:[/bold]\n")

    console.print(
        f"[bold cyan]Checked parameters ({len(PARAMS_TO_CHECK)}):[/bold cyan] {', '.join(PARAMS_TO_CHECK)}"
    )
    console.print()

    if IGNORED_PARAMS:
        console.print(
            f"[bold dim]Ignored parameters ({len(IGNORED_PARAMS)}):[/bold dim] {', '.join(IGNORED_PARAMS)}"
        )
        console.print()

    if missing_params:
        console.print("[bold red]Missing in icechunk:[/bold red]")
        for param in missing_params:
            console.print(f"  • {param}")
        console.print()

    if missing_in_xarray:
        console.print("[bold yellow]Not found in xarray:[/bold yellow]")
        for param in missing_in_xarray:
            console.print(f"  • {param}")
        console.print()

    if diffs_found:
        known_count = sum(
            1
            for d in diffs_found
            if d.param in known_diffs and known_diffs[d.param]["hash"] == d.diff_hash
        )
        unknown_count = len(diffs_found) - known_count
        console.print(
            f"[bold]Documentation differences:[/bold] {len(diffs_found)} total "
            f"({known_count} known, {unknown_count} unknown)"
        )
        console.print()

    if all_match:
        console.print("[green]✅ All documentation is consistent![/green]")
        return 0
    else:
        if args.update_known_diffs:
            console.print(
                "[yellow]💡 Known diffs updated. Review and add reasons in {args.known_diffs}[/yellow]"
            )
        else:
            console.print(
                "[yellow]💡 Tip: Update the docstrings in icechunk/xarray.py to match xarray's docs[/yellow]"
            )
            console.print(
                "[dim]   Or use --update-known-diffs to mark current diffs as known[/dim]"
            )
        return 1


if __name__ == "__main__":
    sys.exit(main())
