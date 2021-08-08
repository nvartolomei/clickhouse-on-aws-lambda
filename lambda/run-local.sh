#!/bin/bash

set -euo pipefail

cleanup() {
  trap - SIGTERM && kill -- -$$
}

trap cleanup SIGINT SIGTERM ERR EXIT

docker run -i --rm -p 9021:8080 clickhouse-on-aws-lambda
