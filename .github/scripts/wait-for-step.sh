#!/usr/bin/env bash
# Usage: wait-for-step.sh <job name> <step name>
# Used instead of 'needs' so runner pickup and setup overlap the job waited on.
set -euo pipefail

job=$1
step=$2
# No job waited on reaches its step sooner than this many seconds after it starts
earliest=90
jobs_url="repos/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID/jobs?filter=latest&per_page=100"

deadline=$((SECONDS + 1200))
settle_deadline=
while [ "$SECONDS" -lt "$deadline" ]; do
  if ! state=$(gh api "$jobs_url" --jq "
    .jobs[] | select(.name == \"$job\")
    | \"\(.status) \(.conclusion) \(.started_at // \"-\" | if . == \"-\" then 0 else fromdateiso8601 end) \([.steps[] | select(.name == \"$step\") | .conclusion][0])\""); then
    echo "::warning::failed to read the job states of this run, retrying"
    sleep 10
    continue
  fi
  read -r status conclusion started step_conclusion <<<"${state:-none none 0 none}"
  if [ "$step_conclusion" = success ]; then
    echo "$job: '$step' succeeded"
    exit 0
  fi
  if [ "$status" = completed ]; then
    # The API can report a job as successful before it reports its steps' conclusions
    if [ "$conclusion" = success ] && [ "${settle_deadline:=$((SECONDS + 60))}" -gt "$SECONDS" ]; then
      sleep 5
      continue
    fi
    echo "::error::$job finished with conclusion '$conclusion' before '$step' succeeded"
    exit 1
  fi
  # Poll slowly until the step could be done, then fast, to keep API calls per run low
  wait=$((started > 0 ? started + earliest - $(date +%s) : 10))
  sleep $((wait > 3 ? (wait < 30 ? wait : 30) : 3))
done
echo "::error::timed out after 20 minutes waiting for $job: '$step'"
exit 1
