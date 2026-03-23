# Nightly zarrs Upstream Test Failed

**Workflow:** {{WORKFLOW}}
**Run:** [{{RUN_ID}}]({{RUN_URL}})
**Date:** {{DATE}}

## Issue

The zarrs_icechunk build or test step failed when testing against the latest icechunk `main`.

This likely indicates:
- A breaking API change in icechunk that affects zarrs_icechunk
- A dependency resolution conflict
- A regression in icechunk's public interface

## Build/Test Output

```
{{TEST_OUTPUT}}
```

This issue was automatically generated from the nightly zarrs upstream checks.
