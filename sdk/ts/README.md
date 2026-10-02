# @pomerium/agentops-harness

The TypeScript client for the Harness API. The contract is [`proto/harnessapi/v1/harnessapi.proto`](../../proto/harnessapi/v1/harnessapi.proto).

`createNodeClient` (from `@pomerium/agentops-harness/node`) and `createWebClient` return the generated Connect client. They add a bearer token (Node only), a timeout and retries for each call, and JSON that ignores unknown fields. A `Prompt` is retried only if it has an idempotency key. `subscribe` follows a session's events and reconnects after a drop. `sentinelOf` reads the `Sentinel` from an error.

```ts
import { subscribe } from "@pomerium/agentops-harness";
import { createNodeClient } from "@pomerium/agentops-harness/node";

const client = createNodeClient({ baseUrl: "http://127.0.0.1:8099" });
const { session } = await client.createSession({ template: "runid", conversationRef: "demo", approvalPrompt: "Run?" });
for await (const event of await subscribe(client, { ref: { sessionId: session!.id } })) {
  console.log(event.payload.case);
}
```

Tests run against the conformance stub: `make sdk-test-ts`.
