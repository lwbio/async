#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
prefix="async-quorum-test-$$"
image="${ASYNC_TEST_RABBITMQ_IMAGE:-rabbitmq:4.2.9-alpine}"
# Preserve diagnostics before deleting the isolated nodes, including on failure.
cleanup() {
  if [[ -n "${ASYNC_TEST_ARTIFACT_DIR:-}" ]]; then mkdir -p "$ASYNC_TEST_ARTIFACT_DIR" || true; fi
  for i in 1 2 3; do
    docker unpause "$prefix-$i" >/dev/null 2>&1 || true
    if [[ -n "${ASYNC_TEST_ARTIFACT_DIR:-}" ]]; then
      docker logs "$prefix-$i" > "$ASYNC_TEST_ARTIFACT_DIR/node-$i.log" 2>&1 || true
    fi
    docker rm -fv "$prefix-$i" >/dev/null 2>&1 || true
  done
  docker network rm "$prefix" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker network create "$prefix" >/dev/null
for i in 1 2 3; do
  ports=()
  if [[ "$i" == 1 ]]; then ports=(-p 127.0.0.1::5672); fi
  docker run -d --name "$prefix-$i" --hostname "$prefix-$i" --network "$prefix" \
    "${ports[@]}" -e RABBITMQ_ERLANG_COOKIE=isolated-async-test \
    -e RABBITMQ_DEFAULT_USER=async -e RABBITMQ_DEFAULT_PASS=local-test \
    -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS='+S 2:2' "$image" >/dev/null
  ready=false
  for _ in $(seq 1 60); do
    if docker exec --user rabbitmq "$prefix-$i" rabbitmq-diagnostics -q check_running >/dev/null 2>&1; then ready=true; break; fi
    sleep 1
  done
  if [[ "$ready" != true ]]; then docker logs "$prefix-$i"; exit 1; fi
  if [[ "$i" != 1 ]]; then
    docker exec --user rabbitmq "$prefix-$i" rabbitmqctl stop_app
    docker exec --user rabbitmq "$prefix-$i" rabbitmqctl join_cluster "rabbit@$prefix-1"
    docker exec --user rabbitmq "$prefix-$i" rabbitmqctl start_app
  fi
done
binding=$(docker port "$prefix-1" 5672/tcp)
port=${binding##*:}
export ASYNC_TEST_AMQP_URL="amqp://async:local-test@127.0.0.1:$port/"
export ASYNC_TEST_QUORUM_NODES="$prefix-1,$prefix-2,$prefix-3"
if (( $# > 0 )); then
  # Execute an explicit argv, never eval a caller-provided shell string.
  "$@"
else
  go test -race -run '^TestIntegrationQuorumAvailability$' -count=1 -timeout=5m -v .
fi
