from __future__ import annotations

import asyncio
import socket
import time
from typing import Any

import protobuf
import pytest
from protobuf import DescMessage

import agentops_harness
from agentops_harness import (
    ApprovalRequired,
    Code,
    Config,
    ConnectError,
    CreateSessionRequest,
    EndReason,
    EndSessionRequest,
    ErrorInfo,
    Event,
    GetSessionRequest,
    HarnessAPIServiceClient,
    HarnessAPIServiceClientSync,
    ListEventsRequest,
    ListSessionsRequest,
    ListTemplatesRequest,
    PermissionRequest,
    PermissionResolved,
    PromptRequest,
    Reason,
    RespondPermissionRequest,
    Sentinel,
    SessionRef,
    SessionState,
    StateChanged,
    SubscribeRequest,
    ToolCallStatus,
    async_client,
    is_live,
    sentinel_of,
    subscribe,
    sync_client,
)
from agentops_harness.gen.harnessapi.v1 import harnessapi_pb

PROMPT_PERMISSION = "stub:permission"
PROMPT_UNKNOWN_EVENT = "stub:unknown-event"
PROMPT_ZERO_EVENT = "stub:zero-event"


def sentinel_ref(sentinel: Sentinel) -> SessionRef:
    name = next(v.name for v in Sentinel.desc().values if v.number == sentinel)
    return SessionRef(session_id=f"sentinel/{name}")


def payloads(events: list[Event], kind: str) -> list[Any]:
    return [e.payload.value for e in events if e.has_field(kind)]


async def open_session(client: HarnessAPIServiceClient, conversation_ref: str, initial_prompt: str = "") -> SessionRef:
    response = await client.create_session(
        CreateSessionRequest(
            template="runid",
            conversation_ref=conversation_ref,
            approval_prompt="ship it",
            initial_prompt=initial_prompt,
        )
    )
    return SessionRef(session_id=response.session.id)


async def events(client: HarnessAPIServiceClient, ref: SessionRef, *, after_seq: int = 0, limit: int = 0) -> list[Event]:
    response = await client.list_events(ListEventsRequest(ref=ref, after_seq=after_seq, limit=limit))
    return list(response.events)


async def end(client: HarnessAPIServiceClient, ref: SessionRef) -> None:
    await client.end_session(EndSessionRequest(ref=ref))


async def collect(feed: Any, budget: float = 20.0) -> list[Event]:
    async def drain() -> list[Event]:
        return [event async for event in feed]

    return await asyncio.wait_for(drain(), timeout=budget)


async def end_after(delay: float, client: HarnessAPIServiceClient, ref: SessionRef) -> None:
    await asyncio.sleep(delay)
    await end(client, ref)


async def test_full_lifecycle(client: HarnessAPIServiceClient) -> None:
    view = (
        await client.create_session(
            CreateSessionRequest(template="runid", conversation_ref="lifecycle", approval_prompt="ship it")
        )
    ).session
    assert view.id
    assert view.state == SessionState.PENDING
    assert is_live(view.state)
    ref = SessionRef(session_id=view.id)

    log = await events(client, ref)
    (approval,) = payloads(log, "approval_required")
    assert isinstance(approval, ApprovalRequired)
    assert "/approve/" in approval.approval_url
    assert approval.expires_at is not None
    assert payloads(log, "state_changed")[0] == StateChanged(
        old=SessionState.PENDING, new=SessionState.LAUNCHING, reason=Reason.LAUNCH
    )
    assert [e.seq for e in log] == list(range(1, len(log) + 1))
    assert log[0].timestamp is not None

    turn = await client.prompt(PromptRequest(ref=ref, content="do the thing"))
    assert turn.turn_id
    assert (await client.list_templates(ListTemplatesRequest())).templates
    assert (await client.list_sessions(ListSessionsRequest())).sessions

    await client.end_session(EndSessionRequest(ref=ref, reason=EndReason.ENDED))
    ended = (await client.get_session(GetSessionRequest(ref=ref))).session
    assert ended.state == SessionState.ENDED
    assert not is_live(ended.state)
    transition, closing = (await events(client, ref))[-2:]
    assert transition.has_field("state_changed")
    assert closing.has_field("session_ended")
    assert transition.payload.value.new == SessionState.ENDED
    assert transition.payload.value.reason == Reason.UNSPECIFIED
    assert closing.payload.value.reason == EndReason.ENDED


async def test_every_published_sentinel_including_shared_codes(stub: Any) -> None:
    expected = {
        Sentinel.NOT_FOUND: Code.NOT_FOUND,
        Sentinel.FORBIDDEN: Code.PERMISSION_DENIED,
        Sentinel.CONFLICT: Code.ALREADY_EXISTS,
        Sentinel.INVALID_STATE: Code.FAILED_PRECONDITION,
        Sentinel.INVALID_ARGUMENT: Code.INVALID_ARGUMENT,
        Sentinel.UNKNOWN_REQUEST: Code.NOT_FOUND,
        Sentinel.NOT_REVIVABLE: Code.FAILED_PRECONDITION,
        Sentinel.UNAVAILABLE: Code.UNAVAILABLE,
        Sentinel.QUOTA_EXCEEDED: Code.RESOURCE_EXHAUSTED,
    }
    assert set(expected) == set(Sentinel) - {Sentinel.UNSPECIFIED}
    async with stub.client(retries=0) as client:
        for sentinel, code in expected.items():
            with pytest.raises(ConnectError) as caught:
                await client.get_session(GetSessionRequest(ref=sentinel_ref(sentinel)))
            assert sentinel_of(caught.value) is sentinel, f"{sentinel!r} lost its sentinel"
            assert caught.value.code == code, f"{sentinel!r} arrived as the wrong code"


@pytest.mark.parametrize("number", [0, 99], ids=["unspecified", "from-a-later-platform"])
def test_a_sentinel_this_build_does_not_know_is_tolerated(number: int) -> None:
    exc = ConnectError(Code.NOT_FOUND, "gone", details=[ErrorInfo(sentinel=Sentinel(number), detail="why")])
    assert sentinel_of(exc) is None
    assert exc.code == Code.NOT_FOUND


def test_an_error_that_is_not_a_connect_error_has_no_sentinel() -> None:
    assert sentinel_of(ValueError("boom")) is None


async def test_cross_client_is_not_found(stub: Any) -> None:
    async with stub.client(client="alice") as alice, stub.client(client="bob") as bob:
        ref = await open_session(alice, "cross")
        with pytest.raises(ConnectError) as caught:
            await bob.get_session(GetSessionRequest(ref=ref))
        assert sentinel_of(caught.value) is Sentinel.NOT_FOUND
        with pytest.raises(ConnectError) as caught:
            await subscribe(bob, SubscribeRequest(ref=ref))
        assert sentinel_of(caught.value) is Sentinel.NOT_FOUND
        assert not (await bob.list_sessions(ListSessionsRequest())).sessions


async def test_feed_stops_on_the_session_ended_event(client: HarnessAPIServiceClient) -> None:
    ref = await open_session(client, "ended")
    feed = await subscribe(client, SubscribeRequest(ref=ref))
    await end(client, ref)
    got = await collect(feed, 10.0)
    assert got[-1].has_field("session_ended")
    assert got[-1].payload.value.reason == EndReason.ENDED


async def test_closing_the_feed_inside_the_loop_ends_it(client: HarnessAPIServiceClient) -> None:
    ref = await open_session(client, "close-inside", "do the thing")
    feed = await subscribe(client, SubscribeRequest(ref=ref))
    got: list[Event] = []
    async for event in feed:
        got.append(event)
        if event.has_field("turn_completed"):
            await feed.close()
    assert got[-1].has_field("turn_completed")


async def test_already_finished_log_is_a_success_with_an_empty_feed(client: HarnessAPIServiceClient) -> None:
    ref = await open_session(client, "finished")
    await end(client, ref)
    feed = await subscribe(client, SubscribeRequest(ref=ref, after_seq=1 << 30))
    assert await collect(feed, 10.0) == []


async def test_stream_that_ends_before_saying_anything(stub: Any) -> None:
    async with stub.client(scenario="no-frames") as client:
        ref = await open_session(client, "noframes")
        assert await collect(await subscribe(client, SubscribeRequest(ref=ref)), 10.0) == []


async def test_resume_after_drop_has_no_gap_and_no_duplicate(stub: Any) -> None:
    async with stub.client() as driver:
        ref = await open_session(driver, "resume", "do the thing")
        before = await events(driver, ref)
        assert len(before) > 3
        async with stub.client(scenario="drop-after=3", scenario_key="py-resume") as reader:
            feed = await subscribe(reader, SubscribeRequest(ref=ref), reconnect_backoff=0.1)
            task = asyncio.create_task(end_after(0.5, driver, ref))
            got = await collect(feed, 20.0)
            await task
    assert len(got) >= len(before)
    assert [e.seq for e in got[: len(before)]] == [e.seq for e in before]
    for previous, current in zip(got, got[1:]):
        assert current.seq > previous.seq, "a duplicate crossed the resume"


async def test_silent_stream_is_given_up_on_and_resumed(stub: Any) -> None:
    async with stub.client() as driver:
        ref = await open_session(driver, "silent", "do the thing")
        async with stub.client(scenario="silent", scenario_key="py-silent") as reader:
            feed = await subscribe(
                reader, SubscribeRequest(ref=ref), keepalive_interval=0.1, missed_keepalives=3, reconnect_backoff=0.1
            )
            task = asyncio.create_task(end_after(1.0, driver, ref))
            got = await collect(feed, 20.0)
            await task
    assert got, "the client never gave up on a silent stream"
    assert got[0].seq == 1
    assert got[-1].has_field("session_ended")


async def test_a_mute_stream_fails_to_open_within_the_liveness_window(stub: Any) -> None:
    async with stub.client(scenario="mute") as client:
        ref = await open_session(client, "mute")
        started = time.monotonic()
        with pytest.raises(ConnectError):
            await subscribe(client, SubscribeRequest(ref=ref), keepalive_interval=0.1, missed_keepalives=3)
        assert time.monotonic() - started < 5


async def test_unknown_response_field_is_tolerated(stub: Any) -> None:
    async with stub.client(scenario="unknown-field") as client:
        ref = await open_session(client, "unknown-field")
        assert ref.session_id
        assert await events(client, ref)
        feed = await subscribe(client, SubscribeRequest(ref=ref))
        await end(client, ref)
        assert await collect(feed, 10.0)


async def test_unknown_payload_is_tolerated(client: HarnessAPIServiceClient) -> None:
    ref = await open_session(client, "unknown-event", PROMPT_UNKNOWN_EVENT)
    await end(client, ref)
    listed = await events(client, ref)
    fed = await collect(await subscribe(client, SubscribeRequest(ref=ref)), 10.0)
    for log in (listed, fed):
        (future,) = [e for e in log if e.payload is None]
        assert future.seq > 0
        assert future.turn_id
        assert future.timestamp is not None
        completed = [e for e in log if e.has_field("turn_completed")]
        assert [e.turn_id for e in completed] == [future.turn_id]


async def test_absent_fields_read_as_proto3_defaults(client: HarnessAPIServiceClient) -> None:
    ref = await open_session(client, "zero")
    feed = await subscribe(client, SubscribeRequest(ref=ref))
    await client.prompt(PromptRequest(ref=ref, content=PROMPT_ZERO_EVENT))
    await end(client, ref)
    got = await collect(feed, 10.0)
    assert got
    assert all(e.seq != 0 for e in got)
    assert got[-1].has_field("session_ended")
    for empty in (Event.from_binary(b""), Event.from_json("{}")):
        assert (empty.seq, empty.turn_id, empty.timestamp, empty.payload) == (0, "", None, None)


async def test_int64_arrives_as_a_string_and_stays_an_int(client: HarnessAPIServiceClient) -> None:
    ref = await open_session(client, "int64", "do the thing")
    view = (await client.get_session(GetSessionRequest(ref=ref))).session
    assert isinstance(view.last_seq, int)
    assert view.last_seq > 5
    rest = await events(client, ref, after_seq=view.last_seq - 1)
    assert [e.seq for e in rest] == [view.last_seq]


def test_every_contract_type_is_exported() -> None:
    file = harnessapi_pb.desc()
    names = {e.name for e in file.enums}
    pending = [m.desc() for m in (harnessapi_pb.Event, harnessapi_pb.SessionView, harnessapi_pb.ErrorInfo)]
    pending += [m.input for m in file.services[0].methods] + [m.output for m in file.services[0].methods]
    while pending:
        message = pending.pop()
        if message.file is not file or message.name in names:
            continue
        names.add(message.name)
        for field in message.fields:
            nested = getattr(field.value, "message", None) or getattr(field.value, "element", None)
            if isinstance(nested, DescMessage):
                pending.append(nested)
    for name in sorted(names):
        assert name in agentops_harness.__all__, f"{name} is not exported"
        assert getattr(agentops_harness, name) is getattr(harnessapi_pb, name), name
    assert agentops_harness.Oneof is protobuf.Oneof


async def test_permission_round_trip(client: HarnessAPIServiceClient) -> None:
    ref = await open_session(client, "perm", PROMPT_PERMISSION)
    log = await events(client, ref)
    (ask,) = payloads(log, "permission_request")
    assert isinstance(ask, PermissionRequest)
    assert [option.id for option in ask.options] == ["allow", "deny"]
    assert ask.deadline is not None
    call = next(c for c in payloads(log, "tool_call") if c.id == ask.tool_call_id)
    assert call.status == ToolCallStatus.PENDING
    assert call.tool_input.to_python() == {"target": "production"}
    assert not payloads(log, "turn_completed")

    answer = RespondPermissionRequest(ref=ref, request_id=ask.request_id, option_id="allow")
    await client.respond_permission(answer)
    after = await events(client, ref)
    (resolved,) = payloads(after, "permission_resolved")
    assert isinstance(resolved, PermissionResolved)
    assert resolved.request_id == ask.request_id
    assert (resolved.resolution.field, resolved.resolution.value) == ("option_id", "allow")
    assert payloads(after, "turn_completed")
    with pytest.raises(ConnectError) as caught:
        await client.respond_permission(answer)
    assert sentinel_of(caught.value) is Sentinel.UNKNOWN_REQUEST


async def test_conversation_ref_addressing(client: HarnessAPIServiceClient) -> None:
    ref = await open_session(client, "byref")
    by_conversation = SessionRef(conversation_ref="byref")
    assert (await client.get_session(GetSessionRequest(ref=by_conversation))).session.id == ref.session_id
    await end(client, ref)
    with pytest.raises(ConnectError):
        await client.get_session(GetSessionRequest(ref=by_conversation))
    past = (
        await client.get_session(GetSessionRequest(ref=SessionRef(conversation_ref="byref", include_terminal=True)))
    ).session
    assert past.id == ref.session_id
    assert past.template == "runid"


async def test_conflict(client: HarnessAPIServiceClient) -> None:
    request = CreateSessionRequest(template="runid", conversation_ref="conflict", approval_prompt="ship it")
    await client.create_session(request)
    with pytest.raises(ConnectError) as caught:
        await client.create_session(request)
    assert sentinel_of(caught.value) is Sentinel.CONFLICT


async def test_approval_prompt_is_required(client: HarnessAPIServiceClient) -> None:
    with pytest.raises(ConnectError) as caught:
        await client.create_session(
            CreateSessionRequest(template="runid", conversation_ref="noprompt", approval_prompt="")
        )
    assert sentinel_of(caught.value) is Sentinel.INVALID_ARGUMENT


async def test_list_events_pages(client: HarnessAPIServiceClient) -> None:
    ref = await open_session(client, "page", "do the thing")
    everything = await events(client, ref)
    assert len(everything) > 4
    page = await events(client, ref, limit=2)
    assert len(page) == 2
    assert page[0].seq == 1
    rest = await events(client, ref, after_seq=page[1].seq)
    assert rest[0].seq == 3
    assert len(rest) == len(everything) - 2


async def test_a_repeated_key_gets_the_first_turn(client: HarnessAPIServiceClient) -> None:
    ref = await open_session(client, "keyed")

    async def prompt(key: str) -> str:
        return (await client.prompt(PromptRequest(ref=ref, content="deploy", idempotency_key=key))).turn_id

    first = await prompt("msg-1")
    assert await prompt("msg-1") == first
    assert await prompt("msg-2") != first


UNAVAILABLE = sentinel_ref(Sentinel.UNAVAILABLE)

RETRY_CASES = [
    pytest.param("get_session", GetSessionRequest(ref=UNAVAILABLE), True, id="an-unavailable-read"),
    pytest.param("prompt", PromptRequest(ref=UNAVAILABLE, content="deploy"), False, id="a-prompt-without-a-key"),
    pytest.param(
        "prompt",
        PromptRequest(ref=UNAVAILABLE, content="deploy", idempotency_key="msg-1"),
        True,
        id="a-keyed-prompt",
    ),
    pytest.param(
        "get_session", GetSessionRequest(ref=sentinel_ref(Sentinel.FORBIDDEN)), False, id="a-considered-refusal"
    ),
]


def assert_resent(elapsed: float, resent: bool) -> None:
    assert elapsed >= 0.55 if resent else elapsed < 0.5


@pytest.mark.parametrize(("method", "request_", "resent"), RETRY_CASES)
async def test_retries(stub: Any, method: str, request_: Any, resent: bool) -> None:
    async with stub.client(retries=2) as client:
        started = time.monotonic()
        with pytest.raises(ConnectError):
            await getattr(client, method)(request_)
    assert_resent(time.monotonic() - started, resent)


@pytest.mark.parametrize(("method", "request_", "resent"), RETRY_CASES)
def test_sync_retries(stub: Any, method: str, request_: Any, resent: bool) -> None:
    with sync_client(stub.config(retries=2)) as client:
        started = time.monotonic()
        with pytest.raises(ConnectError):
            getattr(client, method)(request_)
    assert_resent(time.monotonic() - started, resent)


async def test_each_attempt_gets_its_own_deadline() -> None:
    with socket.socket() as silent:
        silent.bind(("127.0.0.1", 0))
        silent.listen()
        url = f"http://127.0.0.1:{silent.getsockname()[1]}"
        async with async_client(Config(base_url=url, request_timeout=0.1, retries=1)) as client:
            started = time.monotonic()
            with pytest.raises(ConnectError) as caught:
                await client.list_templates(ListTemplatesRequest())
    assert caught.value.code == Code.DEADLINE_EXCEEDED
    assert time.monotonic() - started >= 0.4


def test_sync_client_reports_sentinels(sync: HarnessAPIServiceClientSync) -> None:
    with pytest.raises(ConnectError) as caught:
        sync.get_session(GetSessionRequest(ref=sentinel_ref(Sentinel.NOT_REVIVABLE)))
    assert sentinel_of(caught.value) is Sentinel.NOT_REVIVABLE
    assert caught.value.code == Code.FAILED_PRECONDITION


def test_sync_client_streams_a_session_to_its_end(sync: HarnessAPIServiceClientSync) -> None:
    view = sync.create_session(
        CreateSessionRequest(
            template="runid", conversation_ref="sync-stream", approval_prompt="ship it", initial_prompt="do the thing"
        )
    ).session
    ref = SessionRef(session_id=view.id)
    sync.end_session(EndSessionRequest(ref=ref))
    seen = [r.event for r in sync.subscribe(SubscribeRequest(ref=ref)) if r.event is not None]
    assert seen[-1].has_field("session_ended")
    assert [e.seq for e in seen] == list(range(1, len(seen) + 1))


async def test_the_token_file_is_the_bearer_on_calls_and_streams(stub: Any, tmp_path: Any) -> None:
    token = tmp_path / "token"
    token.write_text("carol\n")
    async with async_client(Config(base_url=stub.base_url, token_file=str(token))) as client:
        ref = await open_session(client, "bearer")
        feed = await subscribe(client, SubscribeRequest(ref=ref))
        await end(client, ref)
        assert (await collect(feed, 10.0))[-1].has_field("session_ended")
    async with stub.client(client="carol") as carol:
        assert (await carol.get_session(GetSessionRequest(ref=ref))).session.id == ref.session_id


async def test_close_from_another_task_ends_the_feed(stub: Any) -> None:
    async with stub.client() as driver:
        ref = await open_session(driver, "close-elsewhere")
        async with stub.client(scenario="drop-after=2", scenario_key="py-close-elsewhere") as reader:
            feed = await subscribe(reader, SubscribeRequest(ref=ref), reconnect_backoff=30.0)
            consumer = asyncio.create_task(collect(feed, 10.0))
            await asyncio.sleep(0.3)
            started = time.monotonic()
            await feed.close()
            await consumer
            assert time.monotonic() - started < 1.0


async def test_closing_an_unread_feed_releases_its_stream(client: HarnessAPIServiceClient) -> None:
    ref = await open_session(client, "close-unread")
    feed = await subscribe(client, SubscribeRequest(ref=ref))
    await feed.close()
    await asyncio.sleep(0)
    assert feed._unread is None
    assert feed._closed_wait.done()
    assert [e async for e in feed] == []
