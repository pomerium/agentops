"""OpenAI Agents SDK, through agent-sandbox's own `openai-agents-k8s-sandbox` provider."""

import asyncio
import os

import pytest

from conftest import PROBE_COMMAND, assert_reached_verify_as_sandbox
from pomerium_sandbox import SANDBOX_NAMESPACE, SANDBOX_WARMPOOL, async_sandbox_client

pytest.importorskip("openai_agents_k8s_sandbox")
from agents.sandbox import Manifest  # noqa: E402
from openai_agents_k8s_sandbox import K8sSandboxClient, K8sSandboxClientOptions  # noqa: E402

# The SDK materializes its workspace at the manifest root. The runtime image
# owns /app, not /workspace.
MANIFEST = Manifest(root="/app/workspace")


def options() -> K8sSandboxClientOptions:
    return K8sSandboxClientOptions(warm_pool=SANDBOX_WARMPOOL, namespace=SANDBOX_NAMESPACE)


def test_session_exec_reaches_verify_through_pomerium():
    async def main():
        async with async_sandbox_client() as sdk:
            client = K8sSandboxClient(sdk)
            session = await client.create(options=options(), manifest=MANIFEST)
            try:
                async with session:
                    result = await session.exec(PROBE_COMMAND)
                    assert result.exit_code == 0, result.stderr
                    assert_reached_verify_as_sandbox(result.stdout)
            finally:
                await client.delete(session)

    asyncio.run(main())


@pytest.mark.skipif(not os.environ.get("OPENAI_API_KEY"), reason="needs OPENAI_API_KEY")
def test_sandbox_agent_uses_the_sandbox():
    from agents import Runner
    from agents.run import RunConfig
    from agents.sandbox import SandboxAgent, SandboxRunConfig

    async def main():
        async with async_sandbox_client() as sdk:
            client = K8sSandboxClient(sdk)
            agent = SandboxAgent(name="prober", instructions="Use the shell to do what the user asks.")
            run_config = RunConfig(sandbox=SandboxRunConfig(client=client, options=options(), manifest=MANIFEST))
            result = await Runner.run(agent,
                "Run this shell command and reply with the exact value of identity.sub from its JSON "
                f"output, nothing else: {PROBE_COMMAND}", run_config=run_config)
            assert "system:serviceaccount" in result.final_output

    asyncio.run(main())
