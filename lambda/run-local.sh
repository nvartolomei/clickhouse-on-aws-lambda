#!/bin/bash

set -euo pipefail

cleanup() {
  trap - SIGTERM && kill -- -$$
}

trap cleanup SIGINT SIGTERM ERR EXIT

docker run -i --rm -v "$HOME/.aws/credentials":/root/.aws/credentials \
        -e AWS_LAMBDA_FUNCTION_NAME=clickhouse \
        -e AWS_REGION=eu-west-2 \
        -p 9021:8080 clickhouse-on-aws-lambda
