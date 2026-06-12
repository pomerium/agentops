# Agent-sandbox harness running pi (earendil-works/pi, Mario Zechner's coding
# agent) as an ACP agent.
#
# pi itself speaks a JSONL RPC, not ACP; `pi-acp` is the community adapter
# (svkozak/pi-acp, listed in the official ACP registry) that bridges it to the
# Agent Client Protocol on stdio. The adapter is young ("MVP, expect breaking
# changes") — treat this harness as experimental. pi executes tools without
# permission gating by design ("run it in a container" — which is exactly what
# this is), so sessions run unattended with no extra yolo flag.
#
# Authenticates headlessly with ANTHROPIC_API_KEY (other providers via pi-ai
# env vars), which the SandboxTemplate injects.
#
# Build (context is deploy/harness so the shared git-checkout.sh is reachable):
#   docker build -t agentops-agent-pi:dev -f deploy/harness/pi/Dockerfile deploy/harness
#   (or: make harness-build HARNESS=pi)
FROM node:22-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates curl jq ripgrep \
  && rm -rf /var/lib/apt/lists/*

# Pre-install the adapter so each session starts fast (no npx download at exec).
RUN npm install -g pi-acp && command -v pi-acp

COPY pi/agent-entrypoint.sh /usr/local/bin/agent-entrypoint
COPY git-checkout.sh /usr/local/bin/git-checkout
RUN chmod +x /usr/local/bin/agent-entrypoint /usr/local/bin/git-checkout
ENV ACP_AGENT_CMD="agent-entrypoint"
# Non-root `node` user (uid 1000), matching the claude-code harness contract:
# the pod's fsGroup grants uid 1000 access to the workspace volume.
ENV HOME=/home/node
RUN mkdir -p /workspace && chown -R node:node /workspace
USER node
WORKDIR /workspace
# Idle so the pod stays Ready; the orchestrator exec's the ACP agent per session.
CMD ["sleep", "infinity"]
