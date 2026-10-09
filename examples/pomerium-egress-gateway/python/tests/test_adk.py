"""Google ADK: an `LlmAgent` whose `code_executor` is the sandbox."""

import os

import pytest

from conftest import PROBE_COMMAND, assert_reached_verify_as_sandbox
from pomerium_sandbox import SANDBOX_NAMESPACE, SANDBOX_WARMPOOL

pytest.importorskip("google.adk")
from google.adk.code_executors.code_execution_utils import CodeExecutionInput, File  # noqa: E402

from pomerium_sandbox.frameworks.adk_executor import AgentSandboxCodeExecutor  # noqa: E402

PROBE_CODE = (
    "import urllib.request\n"
    "print(urllib.request.urlopen('http://127.0.0.1:9000/json', timeout=15).read().decode())\n"
)


@pytest.fixture
def executor(client):
    ex = AgentSandboxCodeExecutor(client=client, warmpool=SANDBOX_WARMPOOL, namespace=SANDBOX_NAMESPACE)
    try:
        yield ex
    finally:
        ex.close()


def test_execute_code_reaches_verify_through_pomerium(executor):
    result = executor.execute_code(None, CodeExecutionInput(code=PROBE_CODE))
    assert result.exit_code == 0, result.stderr
    assert_reached_verify_as_sandbox(result.stdout)


def test_input_files_are_available_to_the_code(executor):
    result = executor.execute_code(None, CodeExecutionInput(
        code="print(open('data.txt').read())",
        input_files=[File(name="data.txt", content=b"from the agent", mime_type="text/plain")],
    ))
    assert result.exit_code == 0, result.stderr
    assert result.stdout.strip() == "from the agent"


@pytest.mark.skipif(not os.environ.get("ANTHROPIC_API_KEY"), reason="needs ANTHROPIC_API_KEY")
def test_llm_agent_uses_the_sandbox(executor):
    import asyncio

    from google.adk.agents.llm_agent import LlmAgent
    from google.adk.models.anthropic_llm import AnthropicLlm
    from google.adk.runners import InMemoryRunner
    from google.genai import types

    agent = LlmAgent(
        name="prober",
        model=AnthropicLlm(model="claude-haiku-5-5"),
        instruction="Answer by writing Python in a ```python block; it is executed and the output returned to you.",
        code_executor=executor,
    )
    runner = InMemoryRunner(agent=agent, app_name="probe")

    async def main():
        session = await runner.session_service.create_session(app_name="probe", user_id="u")
        text = ""
        async for event in runner.run_async(user_id="u", session_id=session.id,
                new_message=types.Content(role="user", parts=[types.Part(text=
                    "GET http://127.0.0.1:9000/json with urllib and tell me the value of identity.sub.")])):
            if event.content and event.content.parts:
                text += "".join(p.text or "" for p in event.content.parts)
        return text

    assert "system:serviceaccount" in asyncio.run(main())
