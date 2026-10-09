"""A smolagents `RemotePythonExecutor` backed by a Kubernetes agent-sandbox.

smolagents' `CodeAgent` runs the model's Python in steps and expects state to
survive between them, as its E2B, Modal and Docker executors do with a Jupyter
kernel. The runtime image has no kernel, so this executor starts one: a small
Python process in the sandbox that executes cells from files in order, in one
namespace, and writes each result as JSON. Everything crosses the agent-sandbox
SDK's `/upload` and `/execute` endpoints; nothing listens on a new port.
"""

from __future__ import annotations

import json
import shlex
import time

from k8s_agent_sandbox.sandbox import Sandbox
from smolagents.local_python_executor import CodeOutput
from smolagents.remote_executors import RemotePythonExecutor
from smolagents.utils import AgentError

KERNEL_DIR = "_smolagents"

# PyPI names whose import name differs.
_IMPORT_NAMES = {"pillow": "PIL", "scikit-learn": "sklearn", "pyyaml": "yaml", "beautifulsoup4": "bs4", "opencv-python": "cv2"}

# The kernel: cells arrive as <n>.py, results leave as <n>.json.
KERNEL = r'''
import contextlib, io, json, os, sys, time, traceback
d = sys.argv[1]
os.makedirs(d, exist_ok=True)
ns = {"__name__": "__main__"}
with open(os.path.join(d, "pid"), "w") as f:
    f.write(str(os.getpid()))
open(os.path.join(d, "ready"), "w").close()
n = 0
while True:
    src = os.path.join(d, f"{n}.py")
    if not os.path.exists(src):
        time.sleep(0.05)
        continue
    with open(src) as f:
        code = f.read()
    out = io.StringIO()
    result = {}
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        try:
            exec(compile(code, f"<cell {n}>", "exec"), ns)
        except BaseException as e:
            result = {"ename": type(e).__name__, "evalue": str(e), "traceback": traceback.format_exc()}
    result["logs"] = out.getvalue()
    tmp = os.path.join(d, f"{n}.json.tmp")
    with open(tmp, "w") as f:
        json.dump(result, f)
    os.replace(tmp, os.path.join(d, f"{n}.json"))
    n += 1
'''


class AgentSandboxExecutor(RemotePythonExecutor):
    """Runs a `CodeAgent`'s code in an agent-sandbox pod.

    Args:
        sandbox: An SDK `Sandbox` handle (from `SandboxClient.create_sandbox`).
            The caller owns its lifetime.
        additional_imports: Packages to pip-install. The sandbox pod reaches
            only Pomerium, so this works only through a Pomerium route to a
            package index; leave it empty otherwise.
        logger: smolagents' logger.
        poll_interval: How often to ask the sandbox whether a cell finished.
    """

    def __init__(self, sandbox: Sandbox, additional_imports: list[str], logger, *, poll_interval: float = 0.2):
        super().__init__(additional_imports, logger)
        self.sandbox = sandbox
        self.poll_interval = poll_interval
        self._cell = 0
        self._start_kernel()
        self.installed_packages = self.install_packages(additional_imports)

    def _start_kernel(self) -> None:
        self.sandbox.files.write(f"{KERNEL_DIR}/kernel.py", KERNEL)
        start = (
            f"mkdir -p {KERNEL_DIR} && "
            f"setsid nohup python3 {KERNEL_DIR}/kernel.py {KERNEL_DIR} "
            f">{KERNEL_DIR}/kernel.log 2>&1 </dev/null &"
        )
        self._run(start)
        self._wait_for(f"{KERNEL_DIR}/ready", timeout=30)

    def _run(self, command: str, timeout: int = 60):
        return self.sandbox.commands.run(command, timeout=timeout)

    def _wait_for(self, path: str, timeout: float) -> str:
        deadline = time.monotonic() + timeout
        while True:
            result = self._run(f"cat {shlex.quote(path)}")
            if result.exit_code == 0:
                return result.stdout
            if time.monotonic() > deadline:
                raise AgentError(f"sandbox kernel did not produce {path} within {timeout}s", self.logger)
            time.sleep(self.poll_interval)

    def install_packages(self, additional_imports: list[str]) -> list[str]:
        """Makes packages importable in the sandbox, as far as the sandbox allows.

        smolagents asks for a tool's requirements (its `final_answer` wants
        `pillow` and `numpy`, which it imports only if present). A package the
        image already has is kept; the rest is pip-installed, which works only
        through a Pomerium route to a package index. A failed install is
        logged, not fatal: code that imports the package then fails in the
        sandbox, where the agent sees the error.
        """
        installed = []
        for package in additional_imports:
            module = _IMPORT_NAMES.get(package, package.replace("-", "_"))
            if self._run(f"python3 -c 'import {module}'").exit_code == 0:
                installed.append(package)
                continue
            result = self._run(f"pip install --quiet --timeout 5 --retries 0 {shlex.quote(package)}", timeout=600)
            if result.exit_code == 0:
                installed.append(package)
            else:
                self.logger.log(f"{package}: not installed in the sandbox (no route to a package index?)")
        return installed

    def run_code_raise_errors(self, code: str) -> CodeOutput:
        n = self._cell
        self._cell += 1
        self.sandbox.files.write(f"{KERNEL_DIR}/{n}.py", code)
        result = json.loads(self._wait_for(f"{KERNEL_DIR}/{n}.json", timeout=600))
        logs = result.get("logs", "")
        if "ename" in result:
            if result["ename"] == RemotePythonExecutor.FINAL_ANSWER_EXCEPTION:
                final_answer = self._deserialize_final_answer(result["evalue"], self.allow_pickle)
                return CodeOutput(output=final_answer, logs=logs, is_final_answer=True)
            raise AgentError(
                f"{logs}\nExecuting code yielded an error:\n{result['ename']}\n{result['evalue']}\n{result['traceback']}",
                self.logger,
            )
        return CodeOutput(output=None, logs=logs, is_final_answer=False)

    def cleanup(self) -> None:
        """Stops the kernel. The sandbox itself belongs to the caller."""
        self._run(f"kill $(cat {KERNEL_DIR}/pid) 2>/dev/null; true")
