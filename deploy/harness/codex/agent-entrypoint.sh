#!/usr/bin/env bash
# agent-entrypoint — exec the Codex ACP adapter.
#
# agentops exec's "/bin/sh -lc 'exec ${ACP_AGENT_CMD}'" in this (agent)
# container. The git checkout happens earlier, in the `git-init` init container
# (see git-checkout.sh) — git credentials never reach this container.
set -euo pipefail

workspace="${WORKSPACE_DIR:-/workspace}"

# The workspace volume's owner uid differs from this (node) user, so git's
# "dubious ownership" guard would reject the agent's own git usage in the
# checked-out repo. Whitelist it.
git config --global --add safe.directory "$workspace"

# approval_policy=never: never ask the client to approve a command, so
# MCP-backed sessions run unattended (Slack would otherwise block on prompts).
# sandbox_mode=danger-full-access: disable Codex's own OS-level sandboxing —
# isolation is the pod's job here, and landlock/seatbelt aren't available in
# the container anyway. (Codex `-c` config overrides pass through the adapter;
# see zed-industries/codex-acp CliConfigOverrides.)
exec codex-acp -c approval_policy=never -c sandbox_mode=danger-full-access
