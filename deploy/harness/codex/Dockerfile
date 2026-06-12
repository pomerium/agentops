# Agent-sandbox harness running OpenAI Codex CLI as an ACP agent.
#
# @zed-industries/codex-acp is Zed's first-party Rust adapter exposing Codex
# over the Agent Client Protocol on stdio (the npm package ships prebuilt
# per-platform binaries). It authenticates headlessly with OPENAI_API_KEY (or
# CODEX_API_KEY), which the SandboxTemplate injects — no ChatGPT login needed.
#
# Build (context is deploy/harness so the shared git-checkout.sh is reachable):
#   docker build -t agentops-agent-codex:dev -f deploy/harness/codex/Dockerfile deploy/harness
#   (or: make harness-build HARNESS=codex)
FROM node:22-bookworm-slim

# Minimal CLI tooling the agent shells out to; the claude-code harness is the
# batteries-included reference, this one stays lean.
RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates curl jq ripgrep \
  && rm -rf /var/lib/apt/lists/*

# Pre-install the adapter so each session starts fast (no npx download at
# exec); `command -v` fails the build if the package stops shipping the binary.
RUN npm install -g @zed-industries/codex-acp && command -v codex-acp

COPY codex/agent-entrypoint.sh /usr/local/bin/agent-entrypoint
COPY git-checkout.sh /usr/local/bin/git-checkout
RUN chmod +x /usr/local/bin/agent-entrypoint /usr/local/bin/git-checkout
ENV ACP_AGENT_CMD="agent-entrypoint"
# Non-root `node` user (uid 1000), matching the claude-code harness contract:
# the pod's fsGroup grants uid 1000 access to the workspace volume. Codex
# writes config/cache under HOME.
ENV HOME=/home/node
RUN mkdir -p /workspace && chown -R node:node /workspace
USER node
WORKDIR /workspace
# Idle so the pod stays Ready; the orchestrator exec's the ACP agent per session.
CMD ["sleep", "infinity"]
