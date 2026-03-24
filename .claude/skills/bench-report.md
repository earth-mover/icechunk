---
name: bench-report
description: Generate an HTML benchmark comparison report from pytest-benchmark JSON files
user_invocable: true
argument_prompt: "Directory containing benchmark JSON files (default: .benchmarks/)"
---

Generate a self-contained HTML benchmark report with two views (Tables and Box Plots) from pytest-benchmark JSON files.

## Steps

1. Glob `{directory}/*.json` to find all benchmark result files. If the user provided no directory, use `.benchmarks/`.

2. Parse each filename to extract the store and version. Filenames follow the pattern `{store}_{version}_{version}_{timestamp}.json` where `{version}` appears twice. The store portion is everything before the first `pypi-` token. For example `s3_pypi-nightly_pypi-nightly_20260320-2224.json` has store=`s3` and version=`nightly`; `s3_ob_pypi-v1_pypi-v1_20260323-1959.json` has store=`s3-object-store` and version=`v1`. Rename `s3_ob` → `s3-object-store` for display. Also handle filenames from pytest-benchmark's `--benchmark-save` which look like `Linux-CPython-3.14-64bit/0001_s3_pypi-nightly_pypi-nightly.json`.

3. For each unique `{store}_{version}` combination, extract benchmark data — need full box-plot stats:
   ```
   jq -s '[.[] | .benchmarks[] | {name: .name, group: .group, min: .stats.min, q1: .stats.q1, median: .stats.median, q3: .stats.q3, max: .stats.max, rounds: .stats.rounds}]'
   ```

4. Combine all extracted data into a single JSON object keyed by `{store}_{version}` (e.g. `s3_nightly`, `gcs_v1`, `s3-object-store_nightly`).

5. Read the HTML template from `.benchmarks/report.html`. Find the existing `const DATA = ...;` block and replace it with the new combined JSON. If the template doesn't exist, inform the user it needs to be created first.

6. Write the updated HTML back to `.benchmarks/report.html`.

7. Open the report in the browser with `open .benchmarks/report.html`.

## Report layout

The report is a single-page self-contained HTML file (no external dependencies). It has two top-level view modes toggled by prominent buttons: **Tables** and **Box Plots**.

### Shared controls (always visible)

- **Reads / Writes toggle** — splits benchmarks into reads (`test_time_*`) and writes (`test_write_*`, `test_set_*`). Everything re-renders based on selection.
- **Text search** filter
- **Group sections** — benchmarks grouped by their `group` field. For reads: Zarr (`zarr-read`), Xarray (`xarray-read`), Other (null group). For writes: Refs (`refs-write`), Other (null group). Each group gets a colored section/header row.

### Tables view

Contains sub-tabs:

- **Summary cards** with geometric mean ratios across stores and versions (scoped to current read/write selection)
- **v1 vs nightly tab** — compares versions per store with ratio bars. Group header rows.
- **S3 vs S3-object-store vs GCS tab** — compares stores with S3 as baseline (ob/s3 and gcs/s3 ratios). Group header rows.
- **All Results tab** — flat table with best values highlighted. Group header rows.
- **"Hide results within 15%" checkbox**
- **Ratio toggle**: flip between nightly/v1 and v1/nightly direction

### Box Plots view

- **Color-coded legend** at top — one color per `{store}_{version}` key. Fixed color map:
  - `s3_v1`: blue, `s3_nightly`: green
  - `s3-object-store_v1`: orange, `s3-object-store_nightly`: pink
  - `gcs_v1`: purple, `gcs_nightly`: yellow
- **One card per benchmark** containing:
  - Benchmark function name + labeled pills
  - Horizontal box & whisker canvas plot with all store/version combos stacked vertically
  - Shared x-axis with auto-scaled units (μs, ms, s) and grid lines
  - Each box: whisker min→Q1, box Q1→Q3, thick median line, whisker Q3→max, caps
  - Labels on left show `{store}_{version}` key

### Labeled colored pills (both views)

Each fixture parameter shown as `fixture: value` pill (e.g. `dataset: gb-8mb`, `preload: default`, `commit: True`). Distinct color per fixture type. Compound tokens kept as single pills using greedy longest-match against known tokens.

Token-to-fixture mapping: datasets (`gb-8mb`, `gb-128mb`, `large-manifest-*`, `simple-1d`, `large-1d`, `pancake-writes`), preload (`default`, `off`), selector (`single-chunk`, `full-read`), splitting (`no-splitting`, `split-size-10_000`), config (`default-inlined`, `not-inlined`), commit (`True`, `False`), executor (`threads`).
