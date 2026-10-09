"""A Google ADK `BaseCodeExecutor` backed by a Kubernetes agent-sandbox.

ADK extracts the code block from the model's reply and calls `execute_code`.
This executor claims one sandbox from the warm pool on first use, keeps it for
the executor's lifetime, and runs each block as a script in it. Files the
model receives as input are uploaded next to the script.
"""

from __future__ import annotations

import base64
from typing import Any

from google.adk.agents.invocation_context import InvocationContext
from google.adk.code_executors.base_code_executor import BaseCodeExecutor
from google.adk.code_executors.code_execution_utils import CodeExecutionInput, CodeExecutionResult
from k8s_agent_sandbox import SandboxClient
from k8s_agent_sandbox.sandbox import Sandbox
from pydantic import ConfigDict, PrivateAttr


class AgentSandboxCodeExecutor(BaseCodeExecutor):
    """Executes Python in an agent-sandbox pod.

    Attributes:
        client: The SDK client, pointed at the sandbox route.
        warmpool: The SandboxWarmPool to claim from.
        namespace: Where the claim is created.
        timeout_seconds: Inherited from ADK; the limit for one script.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    client: SandboxClient
    warmpool: str
    namespace: str = "default"
    timeout_seconds: int | None = 300

    _sandbox: Sandbox | None = PrivateAttr(default=None)

    @property
    def sandbox(self) -> Sandbox:
        if self._sandbox is None:
            self._sandbox = self.client.create_sandbox(warmpool=self.warmpool, namespace=self.namespace)
        return self._sandbox

    def execute_code(
        self, invocation_context: InvocationContext, code_execution_input: CodeExecutionInput
    ) -> CodeExecutionResult:
        sb = self.sandbox
        for f in code_execution_input.input_files:
            content: Any = f.content
            if isinstance(content, str):
                content = base64.b64decode(content)
            sb.files.write(f.name, content)
        sb.files.write("script.py", code_execution_input.code)
        result = sb.commands.run(
            "python3 script.py",
            timeout=(self.timeout_seconds or 300) + 30,
            command_timeout=self.timeout_seconds,
        )
        return CodeExecutionResult(stdout=result.stdout, stderr=result.stderr, exit_code=result.exit_code)

    def close(self) -> None:
        """Releases the sandbox. The warm pool replaces it."""
        if self._sandbox is not None:
            self._sandbox.terminate()
            self._sandbox = None
