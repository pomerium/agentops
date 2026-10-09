"""Pydantic AI: a `WorkspaceBackend` that is the sandbox, checked with Pydantic AI's own conformance suite."""

import os

import pytest

from conftest import PROBE_COMMAND, assert_reached_verify_as_sandbox
from pomerium_sandbox import SANDBOX_NAMESPACE, SANDBOX_WARMPOOL, sandbox_client

pytest.importorskip("pydantic_ai")
from pydantic_ai.workspaces import Workspace, WorkspaceRef  # noqa: E402
from pydantic_ai.workspaces.conformance import WorkspaceBackendSuite  # noqa: E402

from pomerium_sandbox.frameworks.pydantic_ai_workspace import AgentSandboxWorkspace  # noqa: E402

pytestmark = pytest.mark.anyio


def new_backend(client, ref=None):
    return AgentSandboxWorkspace(client, warmpool=SANDBOX_WARMPOOL, namespace=SANDBOX_NAMESPACE, ref=ref)


@pytest.fixture(scope="module")
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def workspace(client):
    backend = new_backend(client)
    try:
        yield Workspace(backend)
    finally:
        if backend.sandbox is not None:
            backend.sandbox.terminate()


async def test_run_reaches_verify_through_pomerium(workspace):
    result = await workspace.run(PROBE_COMMAND, shell=True)
    assert result.exit_code == 0, result.stderr
    assert_reached_verify_as_sandbox(result.stdout)


async def test_files_and_commands_share_the_sandbox(workspace):
    await workspace.write_text("probe.py", "import urllib.request\n"
        "print(urllib.request.urlopen('http://127.0.0.1:9000/json', timeout=15).read().decode())\n")
    result = await workspace.run(["python3", "probe.py"])
    assert result.exit_code == 0, result.stderr
    assert_reached_verify_as_sandbox(result.stdout)


class TestConformance(WorkspaceBackendSuite):
    """Pydantic AI's rules for a backend, run against the sandbox.

    Two rules describe the runtime image, not this backend: its /execute
    endpoint decodes output strictly instead of replacing undecodable bytes,
    and it reports a command's exit only once every process holding its output
    has exited. The first is an expected failure here, the second a declared
    limit.
    """

    @pytest.fixture(scope="class")
    @classmethod
    def anyio_backend(cls):
        return "asyncio"

    @pytest.fixture(scope="class")
    @classmethod
    def shared_client(cls):
        return sandbox_client()

    @pytest.fixture(scope="class")
    @classmethod
    async def backend(cls, shared_client):
        backend = new_backend(shared_client)
        try:
            yield backend
        finally:
            if backend.sandbox is not None:
                backend.sandbox.terminate()

    @pytest.fixture
    def fresh_backend(self, shared_client):
        # The suite never destroys what a fresh backend created; this does.
        created = []

        def make():
            backend = new_backend(shared_client)
            created.append(backend)
            return backend

        yield make
        for backend in created:
            if backend.sandbox is not None:
                try:
                    backend.sandbox.terminate()
                except Exception:
                    pass

    @pytest.fixture
    def attach_backend(self, shared_client):
        return lambda ref: new_backend(shared_client, ref=ref)

    @pytest.fixture
    def destroy_environment(self, shared_client):
        async def destroy(backend):
            shared_client.delete_sandbox(backend.ref.id, namespace=SANDBOX_NAMESPACE)
        return destroy

    @pytest.fixture
    def can_detect_exit_with_inherited_output_pipes(self) -> bool:
        return False

    test_undecodable_command_bytes_are_replaced = pytest.mark.xfail(
        reason="python-runtime-sandbox decodes command output strictly", strict=True
    )(WorkspaceBackendSuite.test_undecodable_command_bytes_are_replaced)


@pytest.mark.skipif(not os.environ.get("ANTHROPIC_API_KEY"), reason="needs ANTHROPIC_API_KEY")
async def test_agent_uses_the_sandbox(workspace):
    from pydantic_ai import Agent, RunContext

    agent = Agent("anthropic:claude-haiku-5-5", instructions="Use the run tool for shell commands.")

    @agent.tool
    async def run(ctx: RunContext[None], command: str) -> str:
        r = await workspace.run(command, shell=True)
        return r.stdout + r.stderr

    result = await agent.run(
        f"Run this shell command and reply with the exact value of identity.sub from its JSON output: {PROBE_COMMAND}")
    assert "system:serviceaccount" in result.output
