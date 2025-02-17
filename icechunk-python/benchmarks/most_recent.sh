#!/usr/bin/env sh

echo $(ls -t ./.benchmarks/**/* | head -n 1)
pytest-benchmark compare --group=group,func,param --sort=fullname --columns=median --name=normal `ls -t ./.benchmarks/**/* | head -n 1`
