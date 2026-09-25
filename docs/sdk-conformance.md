# SDK conformance

A client for this API is mostly easy and occasionally very hard. The easy part is
the nine verbs. The hard part is a handful of conditions that a healthy
server never produces, that a development laptop never shows you, and that
production produces on the first bad afternoon: a proxy recycling a connection
mid-stream, a hop that accepts a subscription and then goes quiet, a release that
adds a field your build has never heard of.

`make apistub` is a server that produces them on demand. This is the list of what
it can be asked for and what a client must do about each.

```sh
make apistub
./bin/apistub                    # prints: listening on 127.0.0.1:54321
./bin/apistub -addr 127.0.0.1:8099
```

It is the **real** apiserver and the real Connect transport — the same codec,
envelope framing, error details, opening keepalive and streaming lifecycle as
production — in front of a scripted in-memory implementation. The one
substitution is identity: it comes from a header instead of a verified
assertion, so no Pomerium and no cluster are needed. It binds loopback only and
refuses to start otherwise, because it authenticates nobody.

The reference suites arrive with the TypeScript SDK (run over **both** codecs)
and the Python SDK, in a follow-up change. Until then, the stub's own tests
(`harness/internal/apistub`) drive it through the Go client, covering every
scenario below except `no-frames`.

## Steering it

Three headers. Anything a client can set on a request, which is the point — a
scenario reachable only through a side channel would be testing code no user
runs.

| header | effect |
|---|---|
| `x-apistub-client` | the caller's identity. Default `stub`. Two values = two clients. |
| `x-apistub-scenario` | the wire condition to produce (below). |
| `x-apistub-scenario-key` | scopes the scenario to the FIRST request bearing that key. |

`x-apistub-scenario-key` matters more than it looks. A scenario like "drop the
stream" applied to every attempt would drop the reconnect too, and a correct
client would reconnect forever — the test would hang rather than pass. Scoping it
to the first request is what lets you assert that the *recovery* worked.

A bearer token is also accepted as an identity, so a client that can only set
`Authorization` (the Go client, and therefore the companion) can still pick one.

Two more levers ride on ordinary request fields:

- **`sentinel/<Name>` as a session id** — or as a template on `CreateSession` —
  fails with that published sentinel from whichever verb you called. All ten
  names are in [`clients.md`](clients.md#errors).
- **Scripted prompts** make a turn unfold a particular way:
  `stub:permission` stops on a permission request and waits for an answer;
  `stub:unknown-event` emits an event type from a later version of the platform;
  `stub:zero-event` emits an entirely zero-valued event.

## The scenarios

### `unknown-field`

Adds a field no build has ever heard of to every response message — an extra JSON
key on the JSON codec, an unknown protobuf field on the binary one.

**Must:** decode normally and ignore it.

This is the additive-compatibility promise made concrete, and it is the one
protobuf-es rejects by default: without `jsonOptions: {ignoreUnknownFields: true}`
on the transport, the platform's next additive release becomes a parse error in
every client built on that SDK.

### `no-frames`

Ends the stream cleanly before it sends anything at all — the first envelope the
client sees is the end-of-stream.

**Must:** treat it as a SUCCESS with an empty feed, and stop.

The log is finished; there is nothing to deliver and nothing went wrong. A client
that treats it as an error reopens a closed log forever. Against a healthy server
you will not see this — the opening keepalive always comes first — but a hop that
truncates a response produces it, and the handling is the same as for the clean
close that follows a finished log.

### `silent`

Lets the opening keepalive through and then swallows everything, keepalives
included, holding the connection open.

**Must:** give up after the liveness window (3 × 20 s by default), reconnect with
`afterSeq`, and deliver the rest with no gap and no duplicate.

Nothing is wrong with the socket; the far end is simply gone. This is what the
keepalive contract exists for, and no healthy server will ever demonstrate it.

### `mute`

Swallows every envelope **including** the opening keepalive, and holds the
connection open.

**Must:** fail to open, within the liveness window.

This is the one condition a client cannot ride out by waiting, because opening a
subscription is synchronous. A client without a deadline on the opening frame
waits forever, with no error and no stream — and the process it happens to goes on
looking perfectly healthy. (The Go client acquired this deadline because this
scenario found it missing.)

### `drop-after=N`

Ends the stream in mid-flight after N envelopes — as a recycled proxy connection
or a moved pod does. N counts every message envelope, the opening keepalive
included.

**Must:** reconnect with `afterSeq` and deliver the remainder exactly once,
in order.

Use `x-apistub-scenario-key` here, or the reconnect drops too.

## Everything else worth asserting

The two reference suites also cover, with no scenario header:

- **the full lifecycle** — create, read the approval URL **off the log** (never
  off the `createSession` response), prompt, end;
- **every published sentinel** — the suites assert their table covers the whole
  generated set, so an eleventh cannot slip in untested — including that the two
  pairs sharing a Connect code remain distinguishable by name;
- **cross-client isolation** — another client's session is `not_found` from every
  verb, and a refused subscribe fails at *open* rather than becoming a silent
  feed;
- **stop on the `session_ended` event.** The stub deliberately leaves the feed
  open after it, so a client that waits for end-of-stream instead hangs here;
- **an already-finished log** — subscribing succeeds and the feed ends
  immediately;
- **an unknown event type** passes through, and the turn either side of it still
  parses;
- **a zero-valued event.** On the JSON codec it is literally `{}` — no `seq`, no
  `type`, no `timestamp`. It must decode to the proto3 defaults rather than
  raise, and its `seq` of 0 must be dropped as already-seen rather than break the
  feed;
- **int64 as a JSON string** survives the round trip and stays an integer;
- **the permission round trip**, including that answering twice is
  `ErrUnknownRequest` rather than a second decision;
- **conversation-ref addressing**, including `include_terminal` for "what ran
  here before?";
- **conflict** — a second live session on one conversation is refused;
- **`ListEvents` paging**;
- **the retry policy** — a retryable code is re-sent and then reported as the
  platform's own answer; a considered refusal is not retried at all, and neither
  is a prompt without an idempotency key (assert on elapsed time, or you are
  asserting nothing);
- **idempotency keys** — a keyed prompt is re-sent, and a repeated key gets the
  first prompt's turn back rather than a second turn.

## If you are writing an SDK in another language

Work through this list in order; each item is a real production failure. The
subscription semantics are where clients go wrong — implement all six from
[`clients.md`](clients.md#subscribing), not the four that are easy.

If you find a scenario the stub cannot produce that your transport needs, add it:
`harness/internal/apistub/scenario.go` works on the response bytes, because that
is the only place these conditions live. `api.API` cannot express "this stream
dies now", and the generated types cannot express "carrying a field that does not
exist yet".
