# Agent-sandbox harness running Claude Code as an ACP agent.
#
# @agentclientprotocol/claude-agent-acp is the adapter that exposes Claude Code
# over the Agent Client Protocol (ACP) on stdio. It authenticates with
# ANTHROPIC_API_KEY, which the SandboxTemplate injects from the
# claude-code-credentials Secret. This is the maintained, renamed successor of
# the now-deprecated @zed-industries/claude-code-acp, and the same adapter the
# working pom-interactive-sandbox-agent reference uses.
#
# Two harness requirements for MCP-backed, unattended sessions (both verified by
# internal/e2e against the public https://docs.agno.com/mcp server):
#
#  1. Run as NON-ROOT. The adapter refuses the `bypassPermissions` session mode
#     when running as root ("bypassPermissions is not available when running as
#     root"). Without that mode the agent asks the client to approve every tool
#     call (including read-only MCP calls), which we'd surface as blocking Slack
#     prompts — so MCP workflows can't run unattended. We run as `node`.
#     (The adapter's escape hatch for root images is setting IS_SANDBOX=1; we
#     prefer staying non-root.)
#  2. MCP servers must be sent with `headers` as an array, never null. The app
#     normalizes that in sandbox.OpenSession (acp-go-sdk marshals a nil slice as
#     JSON null, which the agent rejects with -32602, dropping ALL MCP servers).
#
# The agentops orchestrator exec's `/bin/sh -lc "exec ${ACP_AGENT_CMD}"`
# into the pod, so the image needs a shell, the adapter on PATH, and a git
# client for the working-context checkout.
#
# Build (context is deploy/harness so the shared git-checkout.sh is reachable):
#   docker build -t agentops-agent:dev -f deploy/harness/claude-code/Dockerfile deploy/harness
#   (or: make harness-build)
# Fallback demo (no LLM): deploy/harness/demo/Dockerfile
FROM node:22-bookworm-slim

# CLI tooling the agent shells out to: inspect MCP tool-result JSON/YAML (jq, yq),
# search the checked-out repo (ripgrep, fd), fetch (curl, wget), view/edit
# (vim, less), unpack (unzip), build native deps (build-essential), git checkout.
# Debian (glibc) is deliberate over Alpine: pandas/numpy install from prebuilt
# manylinux wheels with no compiling, and it matches Anthropic's own Claude Code
# devcontainer.
RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates curl wget \
      jq ripgrep fd-find \
      vim less unzip build-essential \
      python3 python3-pip python3-venv \
  && rm -rf /var/lib/apt/lists/*

# Debian ships fd as `fdfind`; expose the conventional `fd` name.
RUN ln -s "$(command -v fdfind)" /usr/local/bin/fd

# yq (mikefarah, Go single binary) is not in Debian apt — pin a release, match arch
# (dpkg arch names amd64/arm64 line up with the release asset names).
ARG YQ_VERSION=v4.44.3
RUN arch="$(dpkg --print-architecture)" \
  && curl -fsSL "https://github.com/mikefarah/yq/releases/download/${YQ_VERSION}/yq_linux_${arch}" \
       -o /usr/local/bin/yq \
  && chmod +x /usr/local/bin/yq

# Python data-analysis libs. PEP 668 marks the system env externally-managed;
# --break-system-packages is fine in this single-purpose sandbox image.
RUN pip3 install --no-cache-dir --break-system-packages \
      pandas numpy pyyaml

# Pre-install the adapter so each session starts fast (no npx download at exec).
RUN npm install -g @agentclientprotocol/claude-agent-acp@latest

# agent-entrypoint exec's the adapter (ACP_AGENT_CMD points the orchestrator's
# `/bin/sh -lc "exec ${ACP_AGENT_CMD}"` at it). git-checkout is the `git-init`
# init container's command: it checks out the workflow's gitRepo (if any) into
# /workspace from the claim-injected GIT_* env vars — targeted at the init
# container only, so git credentials never reach the agent container. See
# internal/sandbox/claim.go for the env both consume.
COPY claude-code/agent-entrypoint.sh /usr/local/bin/agent-entrypoint
COPY git-checkout.sh /usr/local/bin/git-checkout
RUN chmod +x /usr/local/bin/agent-entrypoint /usr/local/bin/git-checkout
ENV ACP_AGENT_CMD="agent-entrypoint"
# Run as the image's built-in non-root `node` user (uid 1000) so the adapter
# allows bypassPermissions. /workspace and HOME must be writable by it (Claude
# writes config/cache under HOME; the working context is checked out into
# /workspace). In-cluster, ensure the pod's fsGroup grants uid 1000 access to
# the mounted workspace volume.
ENV HOME=/home/node
RUN mkdir -p /workspace && chown -R node:node /workspace
USER node
WORKDIR /workspace
# Idle so the pod stays Ready; the orchestrator exec's the ACP agent per session.
CMD ["sleep", "infinity"]
