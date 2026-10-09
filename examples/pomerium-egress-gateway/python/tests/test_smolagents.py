"""Hugging Face smolagents: a `CodeAgent` whose `RemotePythonExecutor` is the sandbox."""

import os

import pytest

from conftest import assert_reached_verify_as_sandbox

pytest.importorskip("smolagents")
from smolagents.default_tools import FinalAnswerTool  # noqa: E402
from smolagents.monitoring import AgentLogger  # noqa: E402

from pomerium_sandbox.frameworks.smolagents_executor import AgentSandboxExecutor  # noqa: E402

PROBE_CELL = (
    "import urllib.request\n"
    "body = urllib.request.urlopen('http://127.0.0.1:9000/json', timeout=15).read().decode()\n"
    "print(body)\n"
)


@pytest.fixture
def executor(sandbox):
    ex = AgentSandboxExecutor(sandbox, additional_imports=[], logger=AgentLogger())
    ex.send_tools({"final_answer": FinalAnswerTool()})
    try:
        yield ex
    finally:
        ex.cleanup()


def test_state_persists_between_cells(executor):
    assert executor("x = 2\n").is_final_answer is False
    out = executor("final_answer(x * 3)\n")
    assert out.is_final_answer and out.output == 6


def test_cell_reaches_verify_through_pomerium(executor):
    out = executor(PROBE_CELL)
    assert_reached_verify_as_sandbox(out.logs)
    out = executor("final_answer(body)\n")
    assert out.is_final_answer
    assert_reached_verify_as_sandbox(out.output)


@pytest.mark.skipif(not os.environ.get("ANTHROPIC_API_KEY"), reason="needs ANTHROPIC_API_KEY")
def test_code_agent_uses_the_sandbox(sandbox):
    from smolagents import CodeAgent, LiteLLMModel

    agent = CodeAgent(
        tools=[],
        model=LiteLLMModel(model_id="anthropic/claude-haiku-5-5"),
        executor=AgentSandboxExecutor(sandbox, additional_imports=[], logger=AgentLogger()),
        additional_authorized_imports=["urllib.request", "json"],
    )
    answer = agent.run(
        "GET http://127.0.0.1:9000/json with urllib and return the value of identity.sub from the JSON."
    )
    assert "system:serviceaccount" in str(answer)
