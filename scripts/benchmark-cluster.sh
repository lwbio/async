#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
out="${1:?usage: bash scripts/benchmark-cluster.sh OUTPUT_DIRECTORY}"
mkdir -p "$out"
out=$(cd "$out" && pwd)
if [[ -e "$out/benchmark.txt" || -e "$out/metrics.jsonl" ]]; then
  echo "Use a new output directory to avoid overwriting an earlier run" >&2
  exit 1
fi
export ASYNC_TEST_ARTIFACT_DIR="$out/broker"
export ASYNC_BENCH_METRICS="${ASYNC_BENCH_METRICS:-json}"
export ASYNC_BENCH_METRICS_FILE="$out/metrics.jsonl"
{
  go version
  uname -a
  command -v lscpu >/dev/null && lscpu
  docker info --format '{{.Driver}}'
  echo "image=${ASYNC_TEST_RABBITMQ_IMAGE:-rabbitmq:4.2.9-alpine}"
  echo "metrics=$ASYNC_BENCH_METRICS concurrency=${ASYNC_BENCH_CONCURRENCY:-8} prefetch=${ASYNC_BENCH_PREFETCH:-16}"
  echo "handler_delay=${ASYNC_BENCH_HANDLER_DELAY:-0} expected_replicas=3"
  echo "publishers=${ASYNC_BENCH_PUBLISHERS:-1}"
} > "$out/environment.txt"
bash scripts/test-cluster.sh go test -run '^$' -bench '^BenchmarkRabbitMQ' \
  -benchmem "-benchtime=${ASYNC_BENCH_TIME:-10s}" "-count=${ASYNC_BENCH_COUNT:-3}" \
  "-cpu=${ASYNC_BENCH_CPUS:-1,8}" -timeout=30m \
  "-cpuprofile=$out/cpu.pprof" "-memprofile=$out/heap.pprof" "-o=$out/async.test" . \
  2>&1 | tee "$out/benchmark.txt"
