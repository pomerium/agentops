import json
import os
import sys
import uuid

GREETING = """\
Hello from AgentOps!

I am the `hello` demo agent, in sandbox pod `{pod}`. I have no LLM: I give this answer to every prompt. Your prompt was: "{prompt}"

If you can read this, the installation works from end to end:

1. Your client called the Harness API through Pomerium with its ServiceAccount token.
2. The platform asked Pomerium's agentic authorization server for a run, sealed to this pod.
3. You approved the run on Pomerium's consent page.
4. The sidecar in this pod exchanged its own token for the run token and connected to the Agent Link.
5. The agent-runner in this pod started me and sent me your prompt.

Next steps:

- Add a real agent. Build a harness image (`make harness-build HARNESS=claude-code`), then apply a SandboxTemplate, a SandboxWarmPool and an AgentTemplate for it.
- Give your agents an LLM. Set `anthropic.enabled` and `anthropic.apiKey` in your quickstart values and upgrade the release; that adds the LLM route.
- Connect Slack. Install the Slack bot chart and add its ServiceAccount to `clients` in your quickstart values.
- Write a client of your own: sdk/ts, sdk/python, or harness/api/client in Go.

The README of the agentops repository has the steps for each one.
"""


def send(message):
    sys.stdout.write(json.dumps(message) + "\n")
    sys.stdout.flush()


def prompt_text(blocks):
    parts = [block.get("text", "") for block in blocks if block.get("type") == "text"]
    text = " ".join(parts).strip()
    return text if len(text) <= 200 else text[:200] + "..."


def prompt(params):
    text = GREETING.format(pod=os.environ.get("HOSTNAME", "unknown"), prompt=prompt_text(params.get("prompt", [])))
    send({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": params["sessionId"],
            "update": {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": text}},
        },
    })
    return {"stopReason": "end_turn"}


def handle(method, params):
    if method == "initialize":
        return {
            "protocolVersion": 1,
            "agentCapabilities": {"sessionCapabilities": {"resume": {}}},
            "agentInfo": {"name": "agentops-hello", "version": "1"},
            "authMethods": [],
        }
    if method == "session/new":
        return {"sessionId": "hello-" + uuid.uuid4().hex}
    if method in ("session/resume", "session/set_mode"):
        return {}
    if method == "session/prompt":
        return prompt(params)
    return None


def main():
    for line in sys.stdin:
        try:
            message = json.loads(line)
        except ValueError:
            continue
        if not isinstance(message, dict) or "id" not in message or "method" not in message:
            continue
        request_id = message["id"]
        try:
            result = handle(message["method"], message.get("params") or {})
        except Exception as error:
            send({"jsonrpc": "2.0", "id": request_id, "error": {"code": -32603, "message": str(error)}})
            continue
        if result is None:
            send({"jsonrpc": "2.0", "id": request_id, "error": {"code": -32601, "message": "method not found: " + str(message["method"])}})
        else:
            send({"jsonrpc": "2.0", "id": request_id, "result": result})


if __name__ == "__main__":
    main()
