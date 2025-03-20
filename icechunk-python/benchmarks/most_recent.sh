#!/usr/bin/env sh

LATEST_BENCHMARK=$(ls -t ./.benchmarks/**/* | head -n 1)

echo "$LATEST_BENCHMARK"
pytest-benchmark compare --group=group,func,param --sort=fullname --columns=median --name=normal "$LATEST_BENCHMARK"
aws s3 cp "$LATEST_BENCHMARK" s3://earthmover-scratch/benchmarks/$1
