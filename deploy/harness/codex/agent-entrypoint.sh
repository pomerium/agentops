#!/usr/bin/env bash
set -euo pipefail

workspace="${WORKSPACE_DIR:-/workspace}"

git config --global --add safe.directory "$workspace"

exec codex-acp -c approval_policy=never -c sandbox_mode=danger-full-access
