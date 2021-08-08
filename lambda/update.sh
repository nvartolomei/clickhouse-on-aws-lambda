#!/bin/bash

clickhouse_binary="$1"
docker_repository="$2"

./buid "$clickhouse_binary"

docker tag clickhouse-on-aws-lambda:latest "$docker_repository"
docker push "$docker_repository"
