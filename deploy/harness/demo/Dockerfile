# DEMO harness (fallback) — the ACP "example agent" from coder/acp-go-sdk.
# No real LLM; streams canned output + a sample permission prompt. Useful to
# exercise the pipeline without API credentials. Build with:
#
#     docker build -t agentops-agent:dev -f deploy/harness/demo/Dockerfile deploy/harness
#
# The real harnesses (deploy/harness/<agent>/Dockerfile) run actual coding
# agents over ACP — deploy/harness/claude-code is the reference one.
FROM golang:1.26 AS build
ARG ACP_VERSION=v0.13.5
RUN CGO_ENABLED=0 GOBIN=/out go install github.com/coder/acp-go-sdk/example/agent@${ACP_VERSION}

FROM alpine:3.20
# curl is used by the e2e tests to probe the sidecar proxy from inside the
# agent container.
RUN apk add --no-cache git ca-certificates curl
COPY --from=build /out/agent /usr/local/bin/acp-agent
ENV ACP_AGENT_CMD=/usr/local/bin/acp-agent
WORKDIR /workspace
CMD ["sleep", "infinity"]
