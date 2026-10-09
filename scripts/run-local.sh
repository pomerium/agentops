#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

case "${1:-harness}" in
	harness)  module=harness;  cmd=./cmd/harness ;;
	slackbot) module=slackbot; cmd=./cmd/slackbot ;;
	*) echo "usage: $0 [harness|slackbot]" >&2; exit 2 ;;
esac

if [[ ! -f .env ]]; then
	echo "error: .env not found in $(pwd); see DEVELOPING.md" >&2
	exit 1
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

cd "$module"
exec env GOWORK=off go run "$cmd"
