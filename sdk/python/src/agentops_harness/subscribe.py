from __future__ import annotations

import asyncio
import copy
from collections.abc import AsyncGenerator, AsyncIterator

from connectrpc.code import Code
from connectrpc.errors import ConnectError

from .errors import sentinel_of
from .gen.harnessapi.v1.harnessapi_connect import HarnessAPIServiceClient
from .gen.harnessapi.v1.harnessapi_pb import Event, Sentinel, SubscribeRequest, SubscribeResponse

__all__ = ["EventFeed", "subscribe"]

_FINAL = frozenset({Sentinel.NOT_FOUND, Sentinel.FORBIDDEN, Sentinel.INVALID_ARGUMENT})

_Opened = tuple[AsyncIterator[SubscribeResponse], SubscribeResponse]


async def subscribe(
    client: HarnessAPIServiceClient,
    request: SubscribeRequest,
    *,
    keepalive_interval: float = 20.0,
    missed_keepalives: int = 3,
    reconnect_backoff: float = 2.0,
) -> EventFeed:
    feed = EventFeed(client, request, keepalive_interval * missed_keepalives, reconnect_backoff)
    try:
        first = await feed._open()
    except BaseException:
        feed._closed_wait.cancel()
        raise
    feed._unread = first
    feed._events = feed._run(first)
    return feed


class EventFeed:
    def __init__(
        self, client: HarnessAPIServiceClient, request: SubscribeRequest, silence: float, backoff: float
    ) -> None:
        self._client = client
        self._request = request
        self._last = request.after_seq
        self._silence = silence
        self._backoff = backoff
        self._closed = asyncio.Event()
        self._closed_wait = asyncio.ensure_future(self._closed.wait())
        self._events: AsyncGenerator[Event, None]
        self._unread: _Opened | None = None

    async def __aenter__(self) -> EventFeed:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    def __aiter__(self) -> AsyncIterator[Event]:
        return self._events

    async def close(self) -> None:
        self._closed.set()
        if not self._events.ag_running:
            await self._events.aclose()
        unread, self._unread = self._unread, None
        if unread is not None:
            await _close(unread[0])

    async def _run(self, opened: _Opened | None) -> AsyncGenerator[Event, None]:
        self._unread = None
        try:
            while opened is not None:
                stream, frame = opened
                try:
                    while frame is not None:
                        event = frame.event
                        if not frame.keepalive and event is not None and event.seq > self._last:
                            self._last = event.seq
                            yield event
                            if event.has_field("session_ended"):
                                return
                        frame = await self._read(stream)
                    return
                except ConnectError:
                    pass
                finally:
                    await _close(stream)
                opened = await self._reopen()
        finally:
            self._closed.set()
            self._closed_wait.cancel()

    async def _open(self) -> _Opened | None:
        request = copy.copy(self._request)
        request.after_seq = self._last
        stream = self._client.subscribe(request).__aiter__()
        try:
            first = await self._read(stream)
        except BaseException:
            await _close(stream)
            raise
        if first is None:
            await _close(stream)
            return None
        return stream, first

    async def _reopen(self) -> _Opened | None:
        while True:
            await asyncio.wait({self._closed_wait}, timeout=self._backoff)
            if self._closed.is_set():
                return None
            try:
                return await self._open()
            except ConnectError as exc:
                if sentinel_of(exc) in _FINAL:
                    return None

    async def _read(self, stream: AsyncIterator[SubscribeResponse]) -> SubscribeResponse | None:
        frame = asyncio.ensure_future(stream.__anext__())
        await asyncio.wait({frame, self._closed_wait}, timeout=self._silence, return_when=asyncio.FIRST_COMPLETED)
        if not frame.done():
            frame.cancel()
            if self._closed.is_set():
                return None
            raise ConnectError(Code.DEADLINE_EXCEEDED, "no events or keepalives within the liveness window")
        try:
            return frame.result()
        except StopAsyncIteration:
            return None


async def _close(stream: AsyncIterator[SubscribeResponse]) -> None:
    closer = getattr(stream, "aclose", None)
    if closer is None:
        return
    try:
        await closer()
    except (ConnectError, OSError, RuntimeError):
        pass
