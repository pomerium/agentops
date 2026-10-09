FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates curl jq ripgrep \
  && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir 'hermes-agent[acp]==0.16.0' boto3==1.42.89 \
  && command -v hermes-acp

COPY hermes/agent-entrypoint.sh /usr/local/bin/agent-entrypoint
COPY git-checkout.sh /usr/local/bin/git-checkout
RUN chmod +x /usr/local/bin/agent-entrypoint /usr/local/bin/git-checkout
ENV ACP_AGENT_CMD="agent-entrypoint"
RUN useradd --create-home --uid 1000 agent
ENV HOME=/home/agent
RUN mkdir -p /workspace && chown -R agent:agent /workspace
USER agent
WORKDIR /workspace
CMD ["sleep", "infinity"]
