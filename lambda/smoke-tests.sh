#!/bin/bash

set -euo pipefail

cleanup() {
  trap - SIGTERM && kill -- -$$
}

trap cleanup SIGINT SIGTERM ERR

docker run -i --rm -p 9000:8080 clickhouse-on-aws-lambda &> /dev/null &
runtime_pid=$!

sleep 3

if ! kill -0 $runtime_pid; then
  echo "failed to start container"
  cleanup
  exit 1
fi

for test in ../lambda-tests/0{0,1}*.json; do
  test_name=$(basename "$test" .json)
  echo "Running test: $test_name"

  test_output=$(curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d @$test)

  data_result=$(echo "$test_output" | jq '.data')
  reference_output=$(cat "$(dirname "$test")/$test_name.reference")

  diff <(echo $data_result) <(echo $reference_output)
done

kill -- $runtime_pid
