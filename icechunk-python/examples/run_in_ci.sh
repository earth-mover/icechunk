#!/bin/bash
set -e
TIMESTAMP=$(date +%s)
S3_PATH="s3://icechunk-test/${TIMESTAMP}/foo2"

echo "Running bank examples"
python ./examples/bank_accounts.py

echo "Running mpwrite"
python ./examples/mpwrite.py

echo "Running dask_write"
python ./examples/dask_write.py --url "$S3_PATH" create --t-chunks 100 --x-chunks 4 --y-chunks 4 --chunk-x-size 112 --chunk-y-size 112
python ./examples/dask_write.py --url "$S3_PATH" update --t-from 0 --t-to 10 --workers 4
