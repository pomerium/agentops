# Writing a client

The Harness API is the surface a Slack app, a web app or a CI bot drives an
agent session through, and the durable event log it reads back. This is what you
need to write a client for it, in whatever language you have.

> **What is in the repository today:** the wire contract (`proto/harnessapi`),
> the Go types and client (`harness/api`), and the conformance stub
> (`make apistub`). The companion (Tier 1), the TypeScript and Python SDKs, and
> the runnable examples arrive in follow-up changes; they are described here so
> the contract is documented in one place.

There are three ways to consume it, and they differ in how much of the network
you take on rather than in what they can do. All three reach the same fourteen
verbs.

| | you write | you handle | good for |
|---|---|---|---|
| **Tier 1 — the companion** | plain HTTP against a local socket | nothing | a shell script, a language with no SDK, a page in a browser |
| **Tier 2 — an SDK** | typed calls | reconnects, in the SDK | a long-lived service |
| **Tier 3 — polling** | HTTP + a loop | one retry loop | CI, and anything whose token expires before the work finishes |

The runnable examples (`hello-session` in TypeScript, Python and Go) arrive with
the SDKs: the same flow in three languages, each runnable against the conformance
stub with no cluster and no credentials.

---

## The shape of a session

```
createSession ──▶ pending ──▶ launching ──▶ awaiting_approval ──▶ running
                                                    │                │
                              a human opens the approval URL         │
                                                                     ▼
                                    suspended ◀── idle ──── prompt ──┤
                                        │                            │
                                     prompt (= revive)               ▼
                                        └──────────────────────▶  ended
```

Four things about it surprise everyone once.

**The approval URL is not on the `createSession` response.** A run has to exist
before anyone can be asked to approve it, so the URL arrives on the log as an
`approval_required` event — and on `getSession` once it is set. A client that
reads it off the create response reads an empty string.

**Delivering that URL to a human is your job.** The platform mints it; who sees
it is a question only your client can answer. Render it as chrome the agent
cannot imitate, never as agent-supplied content.

**A revive is a `prompt`, not a verb.** Prompting a `suspended` session brings it
back on a new pod with a fresh approval, and the result says `revived: true`.
There is no queue: consent covers exactly what the approver read, so a prompt in
any other state is `ErrInvalidState`.

**The template is fixed at creation.** It is snapshotted onto the session, and a
revive reuses the stored spec — so a client cannot re-point a consented
conversation at different upstreams.

## Identity

You never send a client id. The server takes it from the assertion Pomerium
verified on the route, and no request message has a field for one — a field a
client could set would be a field a client could lie in.

What you do send is your workload's projected ServiceAccount token as
`Authorization: Bearer …`. **Re-read it on every request.** A projected token is
rotated under a running process, so a copy taken at startup stops working partway
through the day, and the failure looks like an authorization problem rather than
a caching one.

Two subject forms exist and neither works in the other's place:

- the route's policy matches the **raw** token subject —
  `system:serviceaccount:agentops:agentops-slackbot`;
- a `ClientBinding`'s `spec.subject` matches the **minted assertion's** subject,
  which is provider-prefixed — `cluster/system:serviceaccount:agentops:agentops-slackbot`.

The harness logs each client the first time it sees one; that log line is how a
deployment learns what its own subject actually is, rather than guessing.

Another client's session is `not_found`, never `permission_denied`. That is
deliberate: a client that could tell "exists but not yours" from "does not exist"
could enumerate somebody else's conversations.

---

## Tier 1 — the companion

A sidecar that holds your workload's identity and translates the API into plain
REST and SSE on a unix socket. Your process needs an HTTP client and nothing
else: no SDK, no token handling, no Connect.

```sh
curl --unix-socket /var/run/agentops/client.sock -sS \
  -X POST http://companion/v1/sessions \
  -H 'content-type: application/json' \
  -d '{"template":"runid","conversation_ref":"build-4711",
       "approval_prompt":"Deploy build 4711 to staging?"}'

curl --unix-socket /var/run/agentops/client.sock -sSN \
  http://companion/v1/sessions/$ID/events/stream
```

| verb | route |
|---|---|
| CreateSession | `POST /v1/sessions` |
| ListSessions | `GET /v1/sessions?live_only=&updated_since=` |
| ListTemplates | `GET /v1/templates` |
| GetSession | `GET /v1/sessions/{id}` |
| Prompt | `POST /v1/sessions/{id}/prompt` |
| CancelTurn | `POST /v1/sessions/{id}/cancel` |
| RespondPermission | `POST /v1/sessions/{id}/permissions` |
| Suspend | `POST /v1/sessions/{id}/suspend` |
| EndSession | `POST /v1/sessions/{id}/end` |
| ReissueApproval | `POST /v1/sessions/{id}/approval` |
| SetSessionMetadata | `PUT /v1/sessions/{id}/metadata` (body IS the metadata) |
| DeleteWorkspace | `DELETE /v1/sessions/{id}/workspace` |
| ListEvents | `GET /v1/sessions/{id}/events?after_seq=&limit=` |
| Subscribe | `GET /v1/sessions/{id}/events/stream` (SSE) |

To address a session by your own conversation ref instead of by id, put `-` in
the path and the ref in the query: `/v1/sessions/-?conversation_ref=slack:C123/1712345678.9001`.
Refs contain colons and slashes, and escaping one into a path segment is a trap.

Field names here are snake_case — the Go `api` package's json tags — not the
proto3 camelCase of the wire underneath. **This REST shape is the companion's own
translation, not the platform's protocol.** Errors come back as
`{"error":{"sentinel":"ErrNotFound","detail":"…"}}` with a matching status;
branch on the sentinel, because HTTP cannot express every distinction the
sentinel set makes (see the table below).

On the SSE stream, `id:` is the event's sequence, `event:` is its type, and
`data:` is the whole event. Because `id:` is the sequence, a browser's automatic
`Last-Event-ID` resumption lands exactly where it should. **Close the stream
yourself when you see `session_ended`** — an `EventSource` reconnects when the
server closes, forever, and the log is finished.

The socket carries **unauthenticated full session authority** for this pod's
client identity. Never share the volume it lives on with a sandbox or an agent
container.

## Tier 2 — an SDK

TypeScript and Python SDKs arrive in a follow-up change; the Go client is
`harness/api/client`, which satisfies the same `api.API` interface the
in-process implementation does. All of them hide the same six subscription
semantics, listed under [Subscribing](#subscribing) — mirror all six, not a
subset, if you write another.

## Tier 3 — polling

The whole client is: create, deliver the approval URL, then call `ListEvents`
with the last sequence you saw until you get `session_ended`.

```python
for event in client.poll_events(ref, timeout=900):
    ...
```

Use this in CI. A job's projected token lives for about ten minutes and an
approval window can easily outlast it, so a job **cannot** hold a subscription
across one — and does not have to. The delivery contract explicitly allows
reconciling against the log rather than the stream: "the stream is the fast path,
not the only path."

---

## The wire, for a client without an SDK

Connect RPC over HTTP. The server speaks h2c **and** HTTP/1.1, and HTTP/1.1 is
sufficient — Connect rejects it only for bidirectional streams, and server
streaming works fine over chunked HTTP/1.1. Do not require HTTP/2 features.

Every path is `/harnessapi.v1.HarnessAPIService/{Method}`. Send
`Connect-Protocol-Version: 1` on every request.

### JSON mapping

Field names are lowerCamelCase: `sessionId`, `afterSeq`, `approvalPrompt`.

**Zero-valued fields are OMITTED.** The server marshals with stock protojson, so:

- a `seq` of 0 arrives as **no `seq` key at all**;
- an event frame is `{"event":{…}}` with **no `keepalive` key**, rather than
  `keepalive: false`;
- an empty list is **absent**, not `[]`;
- a verb that returns nothing returns `{}`.

The rule, everywhere: **absent means the proto3 default.** A decoder that
distinguishes "missing" from "zero" is reading a difference the wire does not
carry.

Two more conversions:

- **int64 travels as a JSON string** (`"seq": "12"`), because a 64-bit integer
  does not survive a JSON number in every language. Accept both forms.
- **`bytes` travels as base64** — `payload`, `metadata`. Decode it. (In
  TypeScript, protobuf-es has already done this for you: `payload` reaches you as
  a `Uint8Array`. Decoding it again is the mistake to avoid there.)
- **Timestamps are RFC3339**, up to nanosecond precision — more digits than
  Python's `datetime` accepts, so truncate rather than fail.

### Streaming framing

Both directions use Connect envelopes: **one flags byte, four bytes of
big-endian length, then the message**.

```
0x00 00 00 00 4a {"ref":{"sessionId":"abc"}}
 │     └── length          └── the message
 └── flags: bit0 compressed, bit1 end-of-stream
```

**The streaming REQUEST is enveloped too.** A bare JSON body gets a parse error
that reads like a schema problem and is not one. Send
`Content-Type: application/connect+json`.

The final envelope has bit1 set and carries `{}` on success or
`{"error":{…},"metadata":{…}}` on failure — **as JSON in both codecs**; the
end-of-stream frame is part of the Connect streaming protocol, not of the message
codec.

### Errors

A Connect code, plus one error detail of type `harnessapi.v1.ErrorInfo` naming
the sentinel the platform actually raised. **Two pairs share a code**, so the
code alone is not enough:

<!-- BEGIN GENERATED: sentinels (make generate) -->
| sentinel | Connect code | HTTP (companion) | means |
|---|---|---|---|
| `ErrNotFound` | `not_found` | 404 | no such session, or none yours |
| `ErrUnknownRequest` | `not_found` | 404 | an unknown or already-resolved permission request |
| `ErrForbidden` | `permission_denied` | 403 | not your session, or template not in your binding |
| `ErrConflict` | `already_exists` | 409 | the conversation ref already has a live session |
| `ErrInvalidState` | `failed_precondition` | 412 | the verb does not apply in this session's state |
| `ErrNotRevivable` | `failed_precondition` | 412 | suspended but not continuable — start a new session |
| `ErrInvalidArgument` | `invalid_argument` | 400 | a malformed or missing field |
| `ErrNotImplemented` | `unimplemented` | 501 | published, not built yet |
| `ErrUnavailable` | `unavailable` | 503 | a dependency failed; retryable |
| `ErrQuotaExceeded` | `resource_exhausted` | 429 | your ClientBinding caps this; a "not now" |
<!-- END GENERATED: sentinels -->

In a JSON error body the detail appears twice — once as base64 `value` and once
as a decoded `debug` object:

```json
{"code":"failed_precondition","message":"…",
 "details":[{"type":"harnessapi.v1.ErrorInfo",
             "value":"CgxFcnJOb3RSZXZpdmFibGU",
             "debug":{"sentinel":"ErrNotRevivable","detail":"no approver recorded"}}]}
```

Read `debug.sentinel` if you are not decoding protobuf. Note the base64 in
`value` is **unpadded** (connect-go uses `RawStdEncoding`), which most decoders
accept and none of them promise to.

An unrecognized sentinel must be tolerated, not rejected: the set grows.

### Subscribing

`Subscribe{ref, afterSeq}` streams `SubscribeResponse{event, keepalive}`. History
at or before `afterSeq` is not replayed; history after it is.

**The first frame is always a keepalive**, sent as soon as the service admits the
subscription. That is what makes opening one synchronous: a server-streaming call
returns before the server has looked at the request, so without an opening
message a client cannot tell "subscribed" from "refused" — which for a quiet
session would be silence instead of a denial. Treat **any** first frame as the
acknowledgement; waiting for an event would hang on every quiet session.

Keepalives arrive every **20 seconds**. They are not decoration: HTTP/2 PINGs
terminate at each hop, so a proxy can hold a subscription open that has been dead
upstream for minutes. An application-level tick is the only liveness signal that
crosses the whole path.

Six semantics a client must implement. All six, not a subset — each one is a
production failure that development never shows you:

1. **Dead-stream detection.** Three missed intervals (60 s) of *total* silence
   means the stream is dead however healthy the socket looks. Reset the timer on
   every frame, keepalive included. Apply it to the opening frame too, or a hop
   that accepts the connection and then says nothing hangs you forever.
2. **Reconnect with `afterSeq` = the last sequence you delivered**, after a short
   backoff (2 s here).
3. **Dedup: drop `seq <= last`.** Delivery is at-least-once; a resume can repeat
   but cannot skip.
4. **Stop on the `session_ended` EVENT**, not only on end of stream.
   Reconnecting after it loops against a finished log forever.
5. **A clean end of stream is a success, not an error.** The log is finished and
   there is nothing left to deliver. In practice you will see the opening
   keepalive and then a clean close; a bare end-of-stream before any frame comes
   only from a hop that truncated the response, and must be treated the same way.
6. **Terminal errors stop the loop**: `not_found`, `permission_denied`,
   `invalid_argument`. The platform has answered, and retrying hides that.

Unary calls: retry twice on `unavailable`, `deadline_exceeded`, `unknown`, or a
transport failure with no Connect error at all; back off `(attempt+1) × 200 ms`.
Everything else is the platform's considered answer, and asking again only asks
again. `CreateSession` is safe to retry — with an idempotency key you get the
session you already made, and without one you have said you do not care.

### Events

The vocabulary is **additive-only**: tolerate a type you do not know rather than
fail to parse. `type` is a free string and `payload` is raw JSON bytes for
exactly this reason — a proto enum would reject a type this build has not heard
of, and a `oneof` would re-encode a payload the log holds verbatim.

<!-- BEGIN GENERATED: event-types (make generate) -->
`state_changed` · `approval_required` · `approved` · `launch_stalled` ·
`agent_message` · `agent_thought` · `tool_call` · `permission_request` ·
`permission_resolved` · `turn_completed` · `turn_failed` · `usage` ·
`idle_warning` · `suspended` · `revived` · `released` · `session_ended`
<!-- END GENERATED: event-types -->

Payload keys are snake_case (the payload is opaque JSON the platform stores
verbatim — it is not proto3-mapped like the envelope around it).

**Duration fields are nanosecond integers**, named with an `_ns` suffix, because
that is how Go marshals a duration: `waited_ns`, `lead_ns`, `retained_for_ns`.

Here is every shape. A `timestamp` is an RFC 3339 string, an `int` is a JSON
number and never a quoted one, and a key that can be absent is the platform
having nothing to say — not a null.

<!-- BEGIN GENERATED: payload-shapes (make generate) -->
| event or shape | payload keys (`?` = omitted when empty) |
|---|---|
| `state_changed` | `old` string · `new` string · `reason?` string |
| `approval_required` | `approval_url` string · `expires_at` timestamp · `reissued?` bool |
| `approved` | `approver_subject` string |
| `launch_stalled` | `waited_ns` int (ns) |
| `agent_message` | `part_id` string · `text` string · `final` bool |
| `agent_thought` | `text` string |
| `tool_call` | `id` string · `title?` string · `kind?` string · `status` string · `invocation_message?` string · `tool_input?` json · `update?` bool |
| `permission_request` | `request_id` string · `summary` string · `options` PermissionOption[] · `deadline` timestamp · `tool_call_id?` string |
| `permission_resolved` | `request_id` string · `resolution` string |
| `turn_completed` | `stop_reason` string |
| `turn_failed` | `reason` string |
| `usage` | `input_tokens?` int · `output_tokens?` int · `cached_input_tokens?` int · `cache_creation_tokens?` int · `thought_tokens?` int · `total_tokens?` int · `cost_usd?` float · `context_window?` int · `context_used?` int |
| `idle_warning` | `lead_ns` int (ns) |
| `suspended` | `reason?` string · `retained_for_ns?` int (ns) |
| `revived` | — empty |
| `released` | `retained_for_ns?` int (ns) |
| `session_ended` | `reason` string · `detail?` string |
| `PermissionOption` | `id` string · `name` string · `kind?` string |
<!-- END GENERATED: payload-shapes -->

`agent_message` carries a `part_id`: it is a *segment*, not a chunk, and a
renderer updates a part in place rather than diffing text. Its `text` is
attacker-influenceable by construction (prompt injection); the platform carries
it verbatim and never sanitizes it. Rendering it as inert content — no link
unfurling, no mention syntax — is your half of the contract.

`session_ended` carries one of these reasons, and they exist because each calls
for a different next step: `never_approved` and `attach_timeout` and
`revive_failed` send a reader to three different places.

<!-- BEGIN GENERATED: end-reasons (make generate) -->
`revoked` · `expired` · `never_approved` · `agent_exit` · `tunnel_lost` ·
`ended` · `interrupted` · `prepare_failed` · `run_create_failed` ·
`attach_timeout` · `launch_failed`
<!-- END GENERATED: end-reasons -->

### Other limits

- `SetSessionMetadata` caps at **16 KiB**. It is a handle for presentation state,
  not a document store.
- `ListEvents` pages clamp to **500**.
- `RespondPermission` is idempotent for ten minutes after a request resolves, so
  a client that could not tell a slow round-trip from a lost one gets the same
  answer rather than an error.
- `CreateSession` requires a non-blank `approval_prompt`.

---

## Testing your client

`make apistub` builds a conformance server: the real transport and the real
apiserver over a scripted in-memory implementation, with identity from a plain
header. No cluster, no Pomerium, no model key.

[`docs/sdk-conformance.md`](sdk-conformance.md) lists the scenarios every client
should pass, including the ones you cannot provoke against a healthy server — a
stream that dies mid-flight, a response carrying a field your build has never
heard of, a log that ends before it says anything.
