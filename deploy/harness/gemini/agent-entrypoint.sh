#!/usr/bin/env bash
# agent-entrypoint — exec Gemini CLI in ACP mode.
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

# --approval-mode yolo auto-approves tool calls so MCP-backed sessions run
# unattended (Slack would otherwise block on prompts). ACP clients can also
# flip this per-session via session/set_mode.
exec gemini --acp --approval-mode yolo
