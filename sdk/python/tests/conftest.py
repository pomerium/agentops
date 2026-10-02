from __future__ import annotations

import os
import re
import signal
import subprocess
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest
from connectrpc.request import RequestContext

from agentops_harness import (
    Config,
    HarnessAPIServiceClient,
    HarnessAPIServiceClientSync,
    async_client,
    sync_client,
)


def _binary() -> Path:
    raw = os.environ.get("APISTUB_BIN")
    return Path(raw) if raw else Path(__file__).resolve().parents[3] / "bin" / "apistub"


class _Headers:
    def __init__(self, headers: dict[str, str]) -> None:
        self._headers = headers

    async def on_start(self, ctx: RequestContext) -> None:
        ctx.request_headers.update(self._headers)

    async def on_end(self, token: None, ctx: RequestContext, error: Exception | None) -> None:
        pass

    def on_start_sync(self, ctx: RequestContext) -> None:
        ctx.request_headers.update(self._headers)

    def on_end_sync(self, token: None, ctx: RequestContext, error: Exception | None) -> None:
        pass


class Stub:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def config(
        self,
        *,
        client: str | None = None,
        scenario: str | None = None,
        scenario_key: str | None = None,
        **kwargs: Any,
    ) -> Config:
        stub_headers = {
            "x-apistub-client": client,
            "x-apistub-scenario": scenario,
            "x-apistub-scenario-key": scenario_key,
        }
        headers = {k: v for k, v in stub_headers.items() if v is not None}
        return Config(base_url=self.base_url, interceptors=[_Headers(headers)], **kwargs)

    def client(self, **kwargs: Any) -> HarnessAPIServiceClient:
        return async_client(self.config(**kwargs))


@pytest.fixture(scope="session")
def stub() -> Iterator[Stub]:
    binary = _binary()
    if not binary.exists():
        pytest.fail(f"the conformance stub is not built at {binary}. Run `make apistub` (or set APISTUB_BIN).")
    proc = subprocess.Popen(
        [str(binary), "-addr", "127.0.0.1:0"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    assert proc.stdout is not None
    line = proc.stdout.readline()
    match = re.search(r"listening on (\S+)", line)
    if match is None:
        proc.kill()
        pytest.fail(f"the stub said {line!r}, not an address")
    try:
        yield Stub(f"http://{match.group(1)}")
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.fixture
async def client(stub: Stub) -> AsyncIterator[HarnessAPIServiceClient]:
    async with stub.client() as c:
        yield c


@pytest.fixture
def sync(stub: Stub) -> Iterator[HarnessAPIServiceClientSync]:
    with sync_client(stub.config()) as c:
        yield c
