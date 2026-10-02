from __future__ import annotations

import asyncio
import os
import time

from agentops_harness import (
    Config,
    CreateSessionRequest,
    EndSessionRequest,
    Oneof,
    SessionRef,
    SubscribeRequest,
    async_client,
    subscribe,
)


async def main() -> None:
    config = Config(
        base_url=os.environ.get("HARNESS_API_URL", "http://127.0.0.1:8099"),
        token_file=os.environ.get("HARNESS_TOKEN_FILE"),
    )
    async with async_client(config) as client:
        response = await client.create_session(
            CreateSessionRequest(
                template=os.environ.get("HARNESS_TEMPLATE", "runid"),
                conversation_ref=f"hello-{int(time.time())}",
                approval_prompt="Run a hello-world session from the Python example?",
                initial_prompt="Say hello.",
            )
        )
        ref = SessionRef(session_id=response.session.id)
        print(f"session {ref.session_id} is {response.session.state}")

        feed = await subscribe(client, SubscribeRequest(ref=ref))
        async for event in feed:
            match event.payload:
                case Oneof("approval_required", approval):
                    print(f"\n  approve at: {approval.approval_url}\n")
                case Oneof("agent_message", message):
                    print(message.text)
                case Oneof("tool_call", call):
                    print(f"  [{call.status}] {call.title or call.id}")
                case Oneof("turn_completed", _):
                    print("\nturn finished")
                    await feed.close()
                case Oneof("session_ended", ended):
                    print(f"session ended: {ended.reason}")

        await client.end_session(EndSessionRequest(ref=ref))


if __name__ == "__main__":
    asyncio.run(main())
