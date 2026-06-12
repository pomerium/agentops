#!/usr/bin/env bash
# Run agentops locally, loading configuration from a .env file.
#
# .env holds bare KEY=value lines (no `export`). Sourcing those sets shell
# variables that are NOT inherited by child processes, so we wrap the source in
# `set -a`/`set +a` (allexport) to export everything to `go run`.
set -euo pipefail

cd "$(dirname "$0")/.."   # repo root, so .env and ./cmd resolve regardless of CWD

if [[ ! -f .env ]]; then
	echo "error: .env not found in $(pwd) (copy deploy/secret.example.yaml values into .env)" >&2
	exit 1
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

exec go run ./cmd/agentops
