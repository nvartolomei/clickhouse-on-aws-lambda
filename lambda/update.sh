#!/bin/bash

set -euo pipefail

clickhouse_binary="$1"
docker_repository="$2"

./build.sh "$clickhouse_binary"

docker tag clickhouse-on-aws-lambda:latest "$docker_repository"
docker push "$docker_repository"
