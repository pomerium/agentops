# agentops-harness

The Python client for the Harness API. The contract is [`proto/harnessapi/v1/harnessapi.proto`](../../proto/harnessapi/v1/harnessapi.proto).

`async_client` and `sync_client` return the generated Connect clients. They add a bearer token from a file, and a timeout and retries for each call. A `Prompt` is retried only if it has an idempotency key. `subscribe` follows a session's events and reconnects after a drop. `sentinel_of` reads the `Sentinel` from an error.

```python
from agentops_harness import Config, CreateSessionRequest, SessionRef, SubscribeRequest, async_client, subscribe

async with async_client(Config(base_url="http://127.0.0.1:8099")) as client:
    created = await client.create_session(
        CreateSessionRequest(template="runid", conversation_ref="demo", approval_prompt="Run?")
    )
    async for event in await subscribe(client, SubscribeRequest(ref=SessionRef(session_id=created.session.id))):
        print(event.payload)
```

Tests run against the conformance stub: `make sdk-test-py`.
