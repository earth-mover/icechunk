# Nightly Wheel Build Failed

**Workflow:** {{WORKFLOW}}
**Run:** [{{RUN_ID}}]({{RUN_URL}})
**Date:** {{DATE}}

## Issue

One or more jobs in the nightly wheel build failed or were cancelled. The
`nightly` job still uploads the wheels that built successfully, so this is a
partial failure unless the table below shows everything failing.

## Job results

| Job | Result |
| --- | --- |
{{RESULTS}}

This issue is updated automatically by the nightly wheel workflow, and closes
automatically once the nightly build passes again.
