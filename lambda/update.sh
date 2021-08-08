#!/bin/bash

clickhouse_binary="$1"
docker_repository="$2"

set -euo pipefail

if [[ ! -f "$clickhouse_binary" ]]; then
    echo "Couldn't find binary at: $clickhouse_binary"
    exit 1
fi

version=$($clickhouse_binary server --version)

if ! $(echo "$version" | grep -q "Click"); then
    echo "Wrong ClickHouse build, should be aws lambda flavour."
    echo "Actual: $version"
    exit 1
fi

cp "$clickhouse_binary" ./tmp/
objcopy -S "./tmp/$(basename $clickhouse_binary)"

docker build -t clickhouse-on-aws-lambda .

docker tag $(basename "$docker_repository") "$docker_repository"
docker push "$docker_repository"
