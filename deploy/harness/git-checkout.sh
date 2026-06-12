#!/usr/bin/env bash
# git-checkout — init-container entrypoint: check out the workflow's gitRepo
# (when one is defined) into the shared workspace volume, then exit.
#
# It runs as the `git-init` init container of the sandbox pod. agentops
# injects the working-context env vars onto the SandboxClaim targeted at this
# container only (see internal/sandbox/claim.go): GIT_REPO_URL, GIT_REPO_REF,
# GIT_USERNAME, GIT_TOKEN. Because the init container exits before the agent
# container starts, the git token never exists in the agent's environment.
#
# Credentials are passed via GIT_ASKPASS so the token never appears in argv
# (ps) or in the persisted git config — only the bare remote URL is stored.
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

# The workspace is a mounted volume owned by a different uid than this user —
# fsGroup makes it group-writable but the owner uid still mismatches, so git's
# "dubious ownership" guard would reject every command. Whitelist it.
git config --global --add safe.directory "$workspace"

if [ -n "${GIT_TOKEN:-}" ]; then
  # x-access-token is the conventional username for GitHub PAT/app-token HTTPS
  # auth; honor an explicit GIT_USERNAME when the secret provides one.
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

# init + shallow fetch (rather than `git clone`) so a non-empty mount point
# (e.g. a volume with lost+found) doesn't fail the checkout.
git -C "$workspace" init -q
git -C "$workspace" remote add origin "$GIT_REPO_URL" 2>/dev/null \
  || git -C "$workspace" remote set-url origin "$GIT_REPO_URL"
git -C "$workspace" fetch --depth 1 origin "$ref"
git -C "$workspace" checkout -q FETCH_HEAD

if [ -n "${GIT_TOKEN:-}" ]; then
  rm -f "$askpass"
fi
echo "git-checkout: checkout complete" >&2
