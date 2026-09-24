#!/usr/bin/env bash
# Decide whether this workflow run is the top of a PR stack (see
# .github/workflows/stack-top.yaml). Writes top=true|false to $GITHUB_OUTPUT, or
# to stdout when that is unset.
#
# Inputs (environment): EVENT_NAME, REPO; for a pull request also PR_NUMBER,
# HEAD_REF and HEAD_REPO (owner/name of the repository the head branch is in).
set -euo pipefail

out=${GITHUB_OUTPUT:-/dev/stdout}
emit() { echo "top=$1" >> "$out"; }

if [[ "${EVENT_NAME}" != "pull_request" ]]; then
  emit true
  exit 0
fi

# A branch in a fork cannot be the base of a PR in this repository, so nothing
# can be stacked on a fork's PR. Asking anyway would match by branch name alone:
# a fork's "main" would find every open PR into this repository's main.
if [[ "${HEAD_REPO}" != "${REPO}" ]]; then
  emit true
  exit 0
fi

# A PR is never stacked on itself, however the query is answered.
above=$(gh pr list --repo "$REPO" --base "$HEAD_REF" --state open --json number \
  | jq -r --argjson self "$PR_NUMBER" 'map(select(.number != $self) | "#\(.number)") | join(" ")')
if [[ -n "$above" ]]; then
  echo "Stacked under $above; the top of the stack runs the checks." >&2
  emit false
else
  emit true
fi
