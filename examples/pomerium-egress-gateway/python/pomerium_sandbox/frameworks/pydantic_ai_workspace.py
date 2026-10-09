"""A Pydantic AI `WorkspaceBackend` backed by a Kubernetes agent-sandbox.

It implements `SupportsCommands` only: Pydantic AI derives the file operations
through the sandbox's shell. The first operation claims a sandbox from the
warm pool, or attaches to the one a `WorkspaceRef` names. The backend never
deletes the sandbox; whoever holds the ref does.
"""

from __future__ import annotations

import shlex
import uuid
from collections.abc import Mapping

import anyio
from k8s_agent_sandbox import SandboxClient
from k8s_agent_sandbox.exceptions import SandboxNotFoundError, SandboxRequestError
from k8s_agent_sandbox.sandbox import Sandbox
from pydantic_ai.workspaces import (
    CommandResult,
    SupportsCommands,
    WorkspaceBackend,
    WorkspaceCommand,
    WorkspaceRef,
    WorkspaceTimeoutError,
    WorkspaceUnavailableError,
)
from pydantic_ai.workspaces.protocol import validate_timeout

PROVIDER = "k8s-agent-sandbox"

# Each command first records its own pid, which is its process group id: the
# runtime starts every command as a session leader. The script ends with
# builtins, so the shell stays the parent of the command instead of replacing
# itself with it, and a cancelled command is killed as a group, leader
# excepted: the leader then reaps the children and exits with its own exit
# code. Nothing is left behind as a zombie, which pid 1 of the runtime image
# would never reap. The image has no procps, so python walks /proc.
_PID_DIR = "/tmp/.pydantic-ai-runs"
_KILL = (
    "python3 -c \"import os, signal\n"
    "leader = int(open({path!r}).read())\n"
    "for p in os.listdir('/proc'):\n"
    "    if not p.isdigit() or int(p) == leader:\n"
    "        continue\n"
    "    try:\n"
    "        pgrp = int(open(f'/proc/{{p}}/stat').read().rsplit(')', 1)[1].split()[2])\n"
    "        if pgrp == leader:\n"
    "            os.kill(int(p), signal.SIGKILL)\n"
    "    except OSError:\n"
    "        pass\n\""
)


class AgentSandboxWorkspace(WorkspaceBackend, SupportsCommands):
    """One sandbox pod as a Pydantic AI workspace.

    Args:
        client: The SDK client, pointed at the sandbox route.
        warmpool: The SandboxWarmPool to claim from.
        namespace: Where the claim is created.
        ref: An existing environment to attach to; its id is the claim name.
        working_dir: The runtime's base directory, where commands start.
    """

    def __init__(
        self,
        client: SandboxClient,
        *,
        warmpool: str,
        namespace: str = "default",
        ref: WorkspaceRef | None = None,
        working_dir: str = "/app",
    ):
        self._client = client
        self._warmpool = warmpool
        self._namespace = namespace
        self._ref = ref
        self._working_dir = working_dir
        self._sandbox: Sandbox | None = None
        self._lock = anyio.Lock()

    @property
    def ref(self) -> WorkspaceRef | None:
        return self._ref

    @property
    def sandbox(self) -> Sandbox | None:
        """The SDK handle once the environment exists, for provider-specific calls."""
        return self._sandbox

    async def _connect(self) -> Sandbox:
        async with self._lock:
            if self._sandbox is None:
                if self._ref is None:
                    # A cancelled caller must not lose the sandbox it created.
                    with anyio.CancelScope(shield=True):
                        sandbox = await anyio.to_thread.run_sync(
                            lambda: self._client.create_sandbox(warmpool=self._warmpool, namespace=self._namespace)
                        )
                    self._sandbox = sandbox
                    self._ref = WorkspaceRef(provider=PROVIDER, id=sandbox.claim_name)
                else:
                    self._sandbox = await anyio.to_thread.run_sync(self._attach)
            return self._sandbox

    def _attach(self) -> Sandbox:
        try:
            return self._client.get_sandbox(self._ref.id, namespace=self._namespace)
        except SandboxNotFoundError as error:
            raise WorkspaceUnavailableError(f"sandbox {self._ref.id!r} no longer exists") from error

    def _gone(self) -> bool:
        try:
            self._client.get_sandbox(self._ref.id, namespace=self._namespace)
        except SandboxNotFoundError:
            return True
        return False

    async def working_dir(self) -> str:
        await self._connect()
        return self._working_dir

    async def run(
        self,
        command: WorkspaceCommand,
        *,
        shell: bool = False,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> CommandResult:
        validate_timeout(timeout)
        if shell:
            if not isinstance(command, str):
                raise TypeError("shell=True takes a command string")
            script = command
        else:
            if isinstance(command, str):
                raise TypeError("an argv sequence is required unless shell=True")
            script = shlex.join(command)
        exports = "".join(f"export {k}={shlex.quote(v)}; " for k, v in (env or {}).items())
        pid_file = f"{_PID_DIR}/{uuid.uuid4().hex}"
        script = (
            f"mkdir -p {_PID_DIR} && echo $$ > {pid_file}; "
            f"cd {shlex.quote(self._working_dir)} && {exports}{script}; "
            f"rc=$?; rm -f {pid_file}; exit $rc"
        )
        sandbox = await self._connect()

        def call():
            return sandbox.commands.run(script, timeout=int((timeout or 600) + 30), command_timeout=timeout)

        try:
            result = await anyio.to_thread.run_sync(call, abandon_on_cancel=True)
        except anyio.get_cancelled_exc_class():
            with anyio.CancelScope(shield=True):
                await anyio.to_thread.run_sync(
                    lambda: sandbox.commands.run(_KILL.format(path=pid_file), timeout=30)
                )
            raise
        except SandboxRequestError as error:
            if await anyio.to_thread.run_sync(self._gone):
                raise WorkspaceUnavailableError(f"sandbox {self._ref.id!r} is gone") from error
            raise
        if result.timed_out:
            raise WorkspaceTimeoutError(f"command exceeded {timeout}s", stdout=result.stdout, stderr=result.stderr)
        return CommandResult(exit_code=result.exit_code, stdout=result.stdout, stderr=result.stderr)
