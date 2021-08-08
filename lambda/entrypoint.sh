#!/bin/sh

if [ -z "${AWS_LAMBDA_RUNTIME_API}" ]; then
  exec aws-lambda-rie clickhouse lambda-server $@
else
  exec clickhouse lambda-server $@
fi
