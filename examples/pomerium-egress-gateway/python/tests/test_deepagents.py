"""LangChain DeepAgents, through agent-sandbox's own `deepagents-k8s-agent-sandbox` backend."""

import os
import uuid

import pytest

from conftest import PROBE_COMMAND, assert_reached_verify_as_sandbox
from pomerium_sandbox import SANDBOX_NAMESPACE, SANDBOX_WARMPOOL

pytest.importorskip("deepagents_k8s_agent_sandbox")
from deepagents_k8s_agent_sandbox import K8sAgentSandbox  # noqa: E402
from deepagents_k8s_agent_sandbox.settings import K8sAgentSandboxSettings  # noqa: E402


@pytest.fixture
def backend(client):
    settings = K8sAgentSandboxSettings(warmpool=SANDBOX_WARMPOOL, namespace=SANDBOX_NAMESPACE)
    thread = uuid.uuid4().hex[:12]
    backend = K8sAgentSandbox.from_labels_scope(
        client=client,
        sandbox_settings=settings,
        scope={"thread": thread},
        sandbox_api_cwd="/app",
    )
    try:
        yield backend
    finally:
        # The backend finds its claim by the scope labels; so does the cleanup.
        selector = f"deepagents.agents.x-k8s.io/thread={thread}"
        for claim in client.list_all_sandboxes(SANDBOX_NAMESPACE, label_selector=selector):
            client.delete_sandbox(claim, namespace=SANDBOX_NAMESPACE)


def test_backend_execute_reaches_verify_through_pomerium(backend):
    result = backend.execute(PROBE_COMMAND)
    assert result.exit_code == 0, result.output
    assert_reached_verify_as_sandbox(result.output)


def test_backend_files_and_execute_share_the_sandbox(backend):
    backend.write("/app/work/probe.py", "import urllib.request\n"
                  "print(urllib.request.urlopen('http://127.0.0.1:9000/json', timeout=15).read().decode())\n")
    result = backend.execute("python3 probe.py")
    assert result.exit_code == 0, result.output
    assert_reached_verify_as_sandbox(result.output)


@pytest.mark.skipif(not os.environ.get("ANTHROPIC_API_KEY"), reason="needs ANTHROPIC_API_KEY")
def test_deep_agent_uses_the_sandbox(backend):
    from deepagents import create_deep_agent
    from langchain_anthropic import ChatAnthropic

    agent = create_deep_agent(model=ChatAnthropic(model="claude-haiku-5-5"), backend=backend)
    result = agent.invoke({"messages": [("user",
        "Run this shell command with the execute tool and reply with the exact value of "
        f"identity.sub from its JSON output, nothing else: {PROBE_COMMAND}")]})
    assert "system:serviceaccount" in result["messages"][-1].content
