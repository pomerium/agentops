#!/usr/bin/env bash
set -euo pipefail

workspace="${WORKSPACE_DIR:-/workspace}"

if [ -z "${GIT_REPO_URL:-}" ]; then
  echo "git-checkout: no GIT_REPO_URL; nothing to do" >&2
  exit 0
fi
if [ -e "${workspace}/.git" ]; then
  echo "git-checkout: ${workspace} already checked out; nothing to do" >&2
  exit 0
fi

ref="${GIT_REPO_REF:-HEAD}"
echo "git-checkout: checking out ${GIT_REPO_URL} (ref=${ref}) into ${workspace}" >&2

git config --global --add safe.directory "$workspace"

if [ -n "${GIT_TOKEN:-}" ]; then
  export GIT_USERNAME="${GIT_USERNAME:-x-access-token}"
  askpass="$(mktemp)"
  cat >"$askpass" <<'ASKPASS'
#!/bin/sh
case "$1" in
  Username*) printf '%s' "${GIT_USERNAME}" ;;
  *)         printf '%s' "${GIT_TOKEN}" ;;
esac
ASKPASS
  chmod +x "$askpass"
  export GIT_ASKPASS="$askpass"
fi
export GIT_TERMINAL_PROMPT=0

git -C "$workspace" init -q
git -C "$workspace" remote add origin "$GIT_REPO_URL" 2>/dev/null \
  || git -C "$workspace" remote set-url origin "$GIT_REPO_URL"
git -C "$workspace" fetch --depth 1 origin "$ref"
git -C "$workspace" checkout -q FETCH_HEAD

if [ -n "${GIT_TOKEN:-}" ]; then
  rm -f "$askpass"
fi
echo "git-checkout: checkout complete" >&2
