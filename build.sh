#!/bin/bash
set -euo pipefail

export CC=$(which llamacc)
export CXX=$(which llamac++)

build_dir="$HOME/builds/clickhouse-on-aws-lambda"


run_clean() {
  rm -rf "$build_dir"
}

run_deps() {
  contribs=(
      contrib/abseil-cpp
      contrib/aws
      contrib/aws-c-common
      contrib/aws-checksums
      contrib/aws-c-event-stream
      contrib/boost
      contrib/boringssl
      contrib/cctz
      contrib/croaring
      contrib/curl
      contrib/double-conversion
      contrib/dragonbox
      contrib/fast_float
      contrib/fmtlib
      contrib/libc-headers
      contrib/libcpuid
      contrib/libcxx
      contrib/libcxxabi
      contrib/libunwind
      contrib/libxml2
      contrib/lz4
      contrib/miniselect
      contrib/poco
      contrib/re2
      contrib/replxx
      contrib/simdjson
      contrib/sparsehash-c11
      contrib/xz
      contrib/zlib-ng
      contrib/zstd
  )

  git submodule update --depth 1 --init -- "${contribs[@]}"
}

run_cmake() {
  mkdir -p "$build_dir"

  cmake -B "$build_dir" \
      -DCMAKE_BUILD_TYPE=Debug \
      -DSANITIZE=address \
      -DPARALLEL_LINK_JOBS=6 \
      -DPARALLEL_COMPILE_JOBS=6 \
      -DENABLE_THINLTO=0 \
      -DENABLE_LIBRARIES=0 \
      -DENABLE_TESTS=0 \
      -DENABLE_UTILS=0 \
      -DENABLE_EMBEDDED_COMPILER=0 \
      -DUSE_UNWIND=1 \
      -DENABLE_REPLXX=0 \
      -DENABLE_SSL=1 \
      -DENABLE_CURL=1 \
      -DENABLE_S3=1 \
      -DENABLE_CLICKHOUSE_ALL=0 \
      -DENABLE_CLICKHOUSE_LAMBDA_SERVER=1 \
      -DENABLE_CLICKHOUSE_SERVER=0 \
      -DENABLE_CLICKHOUSE_CLIENT=0
}

run_ninja() {
  ninja -C "$build_dir" $@
}

CLEAN=0
SKIP_DEPS=0
SKIP_CMAKE=0
NINJA_ARGS="clickhouse"
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    key="$1"

    case $key in
    --clean)
        CLEAN=1
        shift
        ;;
    --skip-deps)
        SKIP_DEPS=1
        shift
        ;;
    --skip-cmake)
        SKIP_CMAKE=1
        shift
        ;;
    --ninja-args)
        shift
        NINJA_ARGS=$1
        shift
        ;;
    *)    # unknown option
        POSITIONAL+=("$1") # save it in an array for later
        shift # past argument
        ;;
    esac
done

if [[ "$CLEAN" -ne 0 ]]; then
  run_clean
fi

if [[ "$SKIP_DEPS" -ne 1 ]]; then
  run_deps
fi

if [[ "$SKIP_CMAKE" -ne 1 ]]; then
  run_cmake
fi

echo ${NINJA_ARGS}

# shellcheck disable=SC2086
run_ninja ${NINJA_ARGS}
