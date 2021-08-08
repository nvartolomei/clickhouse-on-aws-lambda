#!/bin/bash

set -euo pipefail

clickhouse_binary="$1"


if [[ ! -f "$clickhouse_binary" ]]; then
    echo "Couldn't find binary at: $clickhouse_binary"
    exit 1
fi

version=$($clickhouse_binary 2>&1 ||:)

if ! echo "$version" | grep -q "lambda-server"; then
    echo "Wrong ClickHouse build, should be aws lambda flavour."
    echo "Actual: $version"
    exit 1
fi

cp "$clickhouse_binary" ./tmp/
objcopy -S "./tmp/$(basename "$clickhouse_binary")"

docker build -t clickhouse-on-aws-lambda:latest .
