from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from connectrpc.code import Code
from connectrpc.errors import ConnectError
from connectrpc.method import IdempotencyLevel
from connectrpc.request import RequestContext
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    stop_any,
    wait_exponential_jitter,
)

from .gen.harnessapi.v1.harnessapi_connect import HarnessAPIServiceClient, HarnessAPIServiceClientSync

__all__ = ["Config", "async_client", "sync_client"]

_RETRYABLE = frozenset({Code.UNAVAILABLE, Code.DEADLINE_EXCEEDED, Code.UNKNOWN})


@dataclass
class Config:
    base_url: str
    token_file: str | None = None
    request_timeout: float = 30.0
    retries: int = 2
    interceptors: Sequence[Any] = ()
    http_client: Any = None


def async_client(config: Config) -> HarnessAPIServiceClient:
    return HarnessAPIServiceClient(
        config.base_url.rstrip("/"), interceptors=_interceptors(config), http_client=config.http_client
    )


def sync_client(config: Config) -> HarnessAPIServiceClientSync:
    return HarnessAPIServiceClientSync(
        config.base_url.rstrip("/"), interceptors=_interceptors(config), http_client=config.http_client
    )


def _interceptors(config: Config) -> list[Any]:
    bearer = [_Bearer(Path(config.token_file))] if config.token_file else []
    return [_Retry(config), *config.interceptors, *bearer]


class _Retry:
    def __init__(self, config: Config) -> None:
        self._timeout_ms = int(config.request_timeout * 1000)
        self._retries = max(0, config.retries)

    async def intercept_unary(self, call_next: Callable[..., Awaitable[Any]], request: Any, ctx: RequestContext) -> Any:
        async def once() -> Any:
            return await call_next(request, self._attempt(ctx))

        return await AsyncRetrying(**self._policy(request, ctx))(once)

    def intercept_unary_sync(self, call_next: Callable[..., Any], request: Any, ctx: RequestContext) -> Any:
        return Retrying(**self._policy(request, ctx))(lambda: call_next(request, self._attempt(ctx)))

    def _policy(self, request: Any, ctx: RequestContext) -> dict[str, Any]:
        def past_deadline(state: RetryCallState) -> bool:
            remaining = ctx.timeout_ms
            return remaining is not None and remaining <= (state.upcoming_sleep or 0) * 1000

        return {
            "stop": stop_any(stop_after_attempt(self._attempts(request, ctx) + 1), past_deadline),
            "wait": wait_exponential_jitter(initial=0.2, max=2.0, jitter=0.2),
            "retry": retry_if_exception(lambda exc: isinstance(exc, ConnectError) and exc.code in _RETRYABLE),
            "reraise": True,
        }

    def _attempts(self, request: Any, ctx: RequestContext) -> int:
        if ctx.method.idempotency_level != IdempotencyLevel.UNKNOWN or getattr(request, "idempotency_key", ""):
            return self._retries
        return 0

    def _attempt(self, ctx: RequestContext) -> RequestContext:
        remaining = ctx.timeout_ms
        return RequestContext(
            method=ctx.method,
            http_method=ctx.http_method,
            request_headers=ctx.request_headers,
            timeout_ms=self._timeout_ms if remaining is None else min(int(remaining), self._timeout_ms),
        )


class _Bearer:
    def __init__(self, token_file: Path) -> None:
        self._token_file = token_file

    def _set(self, ctx: RequestContext) -> None:
        ctx.request_headers["authorization"] = f"Bearer {self._token_file.read_text(encoding='utf8').strip()}"

    async def on_start(self, ctx: RequestContext) -> None:
        self._set(ctx)

    async def on_end(self, token: None, ctx: RequestContext, error: Exception | None) -> None:
        pass

    def on_start_sync(self, ctx: RequestContext) -> None:
        self._set(ctx)

    def on_end_sync(self, token: None, ctx: RequestContext, error: Exception | None) -> None:
        pass
