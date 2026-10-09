#!/usr/bin/env bash
set -euo pipefail

workspace="${WORKSPACE_DIR:-/workspace}"

git config --global --add safe.directory "$workspace"

if [ -n "${CLAUDE_CONFIG_DIR:-}" ]; then
  mkdir -p "$CLAUDE_CONFIG_DIR"
  chmod 700 "$CLAUDE_CONFIG_DIR"
fi

exec claude-agent-acp
