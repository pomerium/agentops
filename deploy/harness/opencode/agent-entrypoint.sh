#!/usr/bin/env bash
set -euo pipefail

workspace="${WORKSPACE_DIR:-/workspace}"

git config --global --add safe.directory "$workspace"

exec opencode acp
