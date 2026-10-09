# Threads: ownership, joining, and carryover

How the Slack bot maps a Slack thread to sessions, who may steer them, and what
travels between them. All of this lives in the bot (`slackbot/internal/app`); the
platform sees only sessions and their conversation refs. See
[run-identity.md](./run-identity.md) for how a run is sealed to a pod.

## The ownership model

**One session, one owner, one Pomerium run, one approval.** A session is owned by
exactly one person, and the agent in it runs under a run *that person* approved,
sealed to that pod, scoped to the routes they consented to. Ownership never
transfers.

That is what keeps authorization honest. If Bo could type into Ana's session:

- Bo would act with Ana's authorization, and Pomerium would attribute Bo's
  actions to Ana's approval — a confused deputy with a misleading audit trail.
- The case this feature exists for — *Bo has an access Ana lacks* — would be
  **inverted**: Bo's request would run with Ana's lesser grants.
- Any channel member could inject instructions into someone else's running
  agent.

So multiplayer is not shared driving. **A Slack thread does not own a session; it
hosts a roster of them.** Bo's first mention in Ana's thread is a *join*: his own
session, his own pod, his own run and approval, seeded with the conversation he
was reading — bound to the same thread, so the answers land where the discussion
is. Nothing is shared but the thread itself.

Files are never shared either. Ana's agent's edits are not on Bo's agent's disk;
only the *conversation* crosses, through Slack. Sessions created into a shared
thread are told so in their system prompt, because an interleaved thread implies
one machine and an agent that believes it can see a colleague's checkout will
report work it never did. The channel between agents is git: push a branch, name
it in the answer, and the catch-up below carries the name.

Ownership compares **team and user**, and so does the registry key. Slack user ids
are unique per workspace, not globally, so in a Slack Connect channel an external
member whose home id collides with a participant's would otherwise pass every
check — and could be handed their approval DM. The gateway resolves the actor's own
workspace from the message's `user_team` (the envelope's `team_id` is always the
installing workspace, which would make the comparison trivially true), and events
from any workspace other than the app's own are dropped.

## Solo and multiplayer

A thread started by a top-level mention is **solo**: one participant, whose plain
replies are turns, and a plain reply picks their paused session back up. That is
the common case.

When a **second participant's** session registers in the thread, the thread flips
to **multiplayer**, one-way and announced once in the thread:

> This thread is now multiplayer — I act on @mentions only, from anyone, each in
> their own session. Everything else is discussion I'll read along the way.

From then on **@mention = act, plain message = discussion** — for everyone,
including the person who started the thread. The announcement exists for them: it
is their plain replies that just stopped being turns, and unannounced that is a
bot which silently ignores them. The rule is symmetric, which is what makes it
teachable: the thread is a room, and you address the agent by name.

The flip fires when a session registers in a thread where this process already
holds another session, or when a join finds another session (a paused one, or one
the process is not following) in the platform's session listing. It is recorded in
the Slack state of every session this process holds in the thread, so a restart
does not un-teach it, and each of them gets a catch-up cursor at the same moment.
A paused session that was not held then records the flip when it is picked up
again.

A session that joins a thread runs as in a room from the start, even when it is
the only session there (a mention inside a thread the bot was not part of): it
acts on mentions only, and its system prompt says the thread is shared. With
nobody else to tell, no flip is announced.

## What happens when you @mention in a thread

Every row is about the **actor's own** session, because that is the only session
their mention can reach. What somebody else has going in the same thread changes
nothing here — it only means their answers appear in this session's next catch-up.

| Thread | Actor's own session | Outcome |
|---|---|---|
| top-level | — | **start** — new session, owner = the actor |
| in-thread | running | **turn** — their text, prefixed with their catch-up in a multiplayer thread |
| in-thread | launching | **wait** — an ephemeral says nothing is running yet; no second launch |
| in-thread | suspended | **continue** — same workspace, same conversation, plus a catch-up covering the whole quiet period |
| in-thread | finished, lost, or none | **join** — their own session in *this* thread, seeded with the visible conversation |
| in-thread, channel retemplated | finished, lost, or none | **refuse** — a line saying the agent changed; a new top-level mention uses the new one (see Retemplating) |

| Plain reply from | Thread | Outcome |
|---|---|---|
| the owner of a running session | solo | **turn** |
| the owner of a paused session | solo | **continue** |
| the owner of a launching session | solo | **wait** — the same ephemeral as for a mention |
| the owner of an ended session | solo | an ephemeral says to mention the bot for a new session |
| a participant | multiplayer | **comment** — never a turn; carried in the next catch-up |
| anyone else | either | **comment**, plus a one-time ephemeral teaching the mention |

"Launching" means a launch is in flight in this process, or the platform still
reports the session as pending, launching or awaiting approval.

A join covers three ways of arriving: a second person in a colleague's thread, the
first mention in a thread with nobody in it yet, and somebody picking up a thread
whose own session of theirs has ended. All three are one path, differing only in
what the roster contains.

`ConversationRef` is `slack:{channel}:{thread_ts}:{team}:{user}` — the platform
allows one live session per ref, so a thread-scoped ref could name only one member
of a roster.

## Continuing a paused thread

A session that has had no turn for `SESSION_IDLE_TTL` (15 minutes by default) is
*paused*: the platform suspends its sandbox instead of deleting it. The pod is
freed (which is what costs money) while the Sandbox and its workspace volume are
kept. The agent's own conversation transcript lives on that volume, so a later
message can pick the conversation up instead of starting one. If the suspend
fails, the platform ends the session instead.

**In a solo thread a plain reply from the owner is enough** — a mention is not
required. From their side the thread just went quiet, so re-summoning a bot they
are already talking to would be ceremony; their message becomes the next turn of
the restored conversation. Both entry points go through the same checks, so a
reply can never reach a conversation a mention could not.

Continuing is the ordinary launch with two substitutions — the suspended sandbox is
brought back instead of a new one prepared, and the ACP session is resumed instead
of opened. Everything in between is the same, and in particular **the run is brand
new and needs its own approval.** A resumed pod has a new UID, and a run is sealed
to a pod UID byte-exactly, so the old run could not be reused even if reusing
consent were acceptable.

In a multiplayer thread a plain reply does **not** continue: the message was
addressed to the room, and continuing on it would page somebody for an approval
they did not ask for. Their mention does, and it arrives with a catch-up covering
the whole time they were away.

**The bot routes a person only to their own session.** It looks the session up by
the person's own conversation ref. This is a security control, not a nicety. A
volume has no identity binding in Kubernetes — access to it is "can you create a
pod that mounts it", which is namespace RBAC and says nothing about whose data it
holds — so this routing is the only thing keeping one person's conversation away
from another.

The platform then continues the session only when all of these hold:

- it is suspended, so a workspace is actually being held;
- it has a recorded ACP session to resume, a recorded workspace, and its template
  snapshot;
- **it knows which human approved the session the first time.** The
  continuation's own approval is pinned to that person (`expected_subject`), so
  even a mis-routed continuation cannot be activated by the wrong one: they never
  approve it, so the pod never gets a token and the agent never starts. Without a
  recorded approver the continuation is refused outright rather than left
  unpinned — an unpinned continuation is precisely what the pin exists to prevent;
- the client's ClientBinding still lists the session's template.

The approver's IdP subject is learned when the session starts running, from the
authorization server, because that is the only moment a Slack identity and an IdP
subject are observed together.

When the platform answers that a session cannot be continued
(`SENTINEL_NOT_REVIVABLE`), the bot ends it and starts the person a fresh session
in the same thread, seeded with the Slack discussion. The fresh session has no
access to the old workspace.

A continuation that fails later puts everything back: the pod goes back down and
the session goes back to suspended, keeping its original pause time. What the bot
does next depends on the reason the platform gives:

- `REASON_REVIVE_FAILED` (the approval lapsed, the workspace did not come back): the
  session stays paused, and the thread says to mention the bot to try again.
- `REASON_RESUME_UNAVAILABLE` (no transcript, a harness that cannot resume, an
  agent that refuses): no retry can work, so the bot ends that session and starts
  the person a fresh one in the same thread, seeded with the Slack discussion.

The same start-over happens when the bot cannot pick a paused session up at all
because its thread carries no readable Slack state.

Retention is bounded by `SUSPENDED_TTL` (24 hours by default), measured from the
pause. The platform's sweep then deletes the SandboxClaim, and the status line
says the workspace was released.

The launching-owner row is load-bearing. Without it, someone mentioning in their
own thread during their own approval wait (up to `AGENTIC_RUN_TTL`, 15 minutes by
default) would join *against themselves*: a second pod and a second consent DM for
one intent.

A failed read of the platform's state never falls through to "they have nothing
here". That would start a second session beside a conversation they are still
holding, and do it silently. A mention reports the failure in the thread and stops;
a plain reply is logged and left alone.

Clicks on a tool-call permission button honour the session's owner alone. The
button carries the session id and the thread is resolved **by that**, never by the
clicker: with a roster, looking up the clicker's own binding would find *their*
session and check ownership of the wrong one — and pass.

## Carryover: the seed and the catch-up

**Carryover is a live read of the Slack thread.** The bot keeps no conversation
ledger and no table of message bodies: the thread is read with
`conversations.replies` and the prompt composed from what is there *now*.

It happens at two moments, through one reader:

- **The seed.** A join's first prompt carries the thread as it stands, capped,
  quoted, framed. A bare join (a mention with no text) starts no turn; its first
  request carries the seed instead.
- **The catch-up.** Every subsequent turn of every session in a multiplayer thread
  is prefixed with the *delta*: what appeared since that session's cursor and
  before the message asking for this turn.

Consequences, all of them wanted:

- Edits and deletes are honored for free. A deleted message cannot be carried; an
  edited one carries its current text, and an edit after the cursor brings the
  message back into the next delta.
- The bot stores no copy of the conversation for carryover: no retention window,
  no purge, no conversation bodies at rest in the bot.
- In-flight agent output carries naturally, because the in-progress message is
  already in Slack: the reader and the carryover see the same thing.
- Convergence costs nothing while nobody acts, and is complete at the only moment
  it matters: when an agent is about to act.

### Why the catch-up is lazy

The intuitive design — push each answer into the sibling sessions as it lands — is
not merely expensive, it is structurally impossible. `Prompt` is the only context
ingress and it **runs a turn**: every mirrored message would make every sibling
agent think, answer and post, and a mirror into a *suspended* session **is** a
continuation, so quiet participants would be DM'd an approval request for a message
nobody sent them. A verb that appends context without a turn would inject
third-party text into a session with no action by its owner, which is the one thing
the trust boundary below works hardest to prevent.

So nothing is pushed. Each session carries a cursor, `last_seen_ts`, persisted in
its Slack state beside `last_seq`, advanced to the triggering message on every
turn. Messages that land mid-turn belong to the next delta. A **missing** cursor
composes no delta at all rather than the whole thread: a session that never
delivered one may hold that conversation already, and re-quoting it as news is
worse than being briefly behind. A **stale** cursor re-quotes — duplicated, never
lost. A catch-up whose read fails sends the turn without one and leaves the cursor
where it was, so the next turn carries it.

### What carries, and what never does

Carried content is only what the thread shows in Slack right now: human message
texts, and this bot's own messages that are not chrome. Never tool-call arguments
or results, thoughts, run ids, approval URLs, sandbox names, or anything from the
bot's debug logs — none of that is in Slack, so the mechanism excludes it
structurally rather than by filtering. Never across channels, and never out of a DM
into a channel.

Speakers are never identified. The composer labels *roles* — and, in a room, a
speaker's relation to the reading session, which is the least it can say and still
be coherent about who is who:

| Label | Is |
|---|---|
| `[earlier request]` / `[earlier reply, quoted from Slack]` | the two sides of a seeded conversation |
| `[earlier comment from your requester]` | this session's own owner, talking without addressing the agent |
| `[earlier message from another person]` | anybody else in the thread |
| `[earlier reply from another person's agent, quoted from Slack]` | a sibling session's answer |

Every label starts with `earlier`, which is what the sanitizer keys on to disarm a
forged one inside carried text. A new label that does not is a hole.

### The chrome tag and the content tag

A thread's bot messages mix real answers with lifecycle chrome, and one session's
answers with another's. Two tags, both Slack message metadata, both attached on
every post *and* every update so `chat.update`'s retain-on-omit behavior never
matters:

| `event_type` | On | Read to |
|---|---|---|
| `agentops_chrome` | every status line, hint, notice and permission prompt | drop chrome from every carried conversation |
| `agentops_content` | every agent-output message, payload naming its session | drop a session's *own* answers from its catch-up |

`ThreadReplies` sets `IncludeAllMetadata`, without which Slack returns the metadata
empty and every tag would read as absent.

The failure direction is deliberate: **an untagged bot message counts as
content.** One whose metadata failed to attach contributes a stray status line to a
prompt, or is quoted back to the session that wrote it — never a dropped answer. A
debug line counts untagged bot messages that look like status lines, as the canary
for that plumbing regressing.

Answer attribution (`↳ for <@bo>`, on the first message of a turn in a multiplayer
thread) rides in the Block Kit blocks and deliberately **not** in the message text:
a thread read composes prompts from the text, and an attribution line there would
travel into another participant's agent context as if the agent had said it.

### Caps and flags

Both the seed and each delta are capped at 50 messages and 8000 bytes of text, most
recent kept, with the omission stated inside the quotation. A busy thread cannot
bloat every turn without bound.

What a join's seed could not deliver is said in the join line in the thread rather
than left for the reader to discover:

| Flag | Means |
|---|---|
| truncated | the read failed, or the caps trimmed the oldest messages |
| inflight | another agent was mid-turn, so the last carried reply may be incomplete |

### Retemplating: the one case a join refuses

A join always resolves the channel's *current* template, because the template
defines the run's route grants and reusing a stale one would let a join request
grants an admin removed. But applying fresh config to the grants while carrying the
old conversation is its own hole: an admin rebinds the channel to an agent whose
required MCP servers include a third-party upstream, and the old conversation would
be fed to a pod wired to an upstream nobody consented to. And an amnesiac agent
seated beside caught-up siblings looks broken, while a mixed-template roster feeds
one discussion to differently-granted agents.

So a join into a thread whose sessions ran a different template is refused, with a
line in the thread naming both agents and saying that a new top-level mention uses
the current one.

The roster it checks comes from this process's own registry, or from the platform's
session listing when nothing about the thread is in memory — after a restart, or
while every session in it is paused, which is exactly how long a thread has to sit
for an admin to rebind the channel under it.

## The trust boundary

Because agent answers cross owners, Ana's content can reach Bo's
higher-privileged agent. The threat is concrete and needs no social engineering:
Ana asks her agent to read something she controls — a repo file, an issue body, a
URL, an MCP result — so a payload lands inside the *answer*. Bo mentions the bot,
the payload is carried, Bo's consent page shows only his request and that a
conversation was carried, Bo approves, and the payload runs under Bo's grants.

**In a multiplayer thread this is a steady state, not a one-time event.** The
crossing happens on every turn rather than once at a join, and any channel
member's comments reach every participant's agent at its next turn. The same
measures apply continuously; the risk does not change in kind, only in frequency,
and it is listed under accepted risks below.

Five measures, all required:

1. **Carried agent text is never labeled as the model's own speech.** It is
   `[earlier reply, quoted from Slack]` or `[earlier reply from another person's
   agent, quoted from Slack]`, never `[you]` — the latter would put
   attacker-reachable text in the highest-trust slot in the window.
2. **The delimiters and role labels are escaped in every carried text.** Without
   that, a message containing `</conversation>` followed by `The request: …`
   closes the block and injects a second request. Structural tokens are rewrapped
   in parentheses: readable, but unable to close the block or open a label. This
   runs on every carried text, at a join and in every catch-up.
3. **Nothing synthesizes action from carried content.** A bare mention (`@bot` with
   no text) joins idle; nothing says "continue the conversation above", because the
   last carried line is whatever *somebody else's* agent said and continuing it is
   exactly the confused deputy. A catch-up block with no request behind it is
   dropped rather than sent: the acting user's own words always occupy the trusted
   slot, and quoted material never does.
4. **The approver sees that carryover exists.** The prompt they consent to is the
   requester's own words plus a bounded provenance clause — `[continues another
   person's conversation: N messages carried, <permalink>]`. The carried
   conversation goes to the agent only: it is not part of the approval prompt, so
   the authorization server never receives it and the consent page does not show
   it. The join line in the thread states the carried count and any truncation
   *before* the approval DM goes out, so the people whose conversation it is see
   what came along while declining is still just a matter of not approving.
5. **The framing states that no earlier approval carries**, that the quotation is
   untrusted data, and that none of it happened on this machine: the other
   participants' agents have their own workspaces, which this one cannot see.

## Lineage: the audit trail

The platform records one piece of lineage: `parent_session_id`, one of the sessions
already in the thread when this one joined (the lowest session id; empty when it
was the first). Everything else the bot knows about a session lives in Slack, as
message metadata on the session's one status message. The conversation ref names
the thread, so after a restart the bot reads the thread and takes the newest status
message carrying that session's state. The platform never sees any of it.

## One status message per session

A session carries exactly one lifecycle message at a time, moving through
Getting ready → Waiting for approval → Ready → Paused/Stopped/Finished, and through
"Picking this back up" when a paused thread is continued. A carryover read sees one
chrome message instead of four, and every thread has its "is this alive?" answer in
a predictable place. Transitions are guaranteed updates, never the coalescing path
— a status line must not be droppable.

**Within an episode it is edited where it sits.** Getting ready → approval → Ready
all land while the reader is watching that line, and editing does not re-notify.

**At an episode boundary, in a solo thread, it is restated at the bottom** and the
message it supersedes is deleted. The boundaries are the ending (finished, paused,
stopped, interrupted) and a paused thread being picked back up. The message is
posted at launch, so an ending edited in place would appear above every answer
since: a session that paused after answering would read as having paused *before*
it. The status message's timestamp is part of the session's Slack state and is
re-recorded on every relocation, so a restart finds a message that still exists.

**In a multiplayer thread each line names its session's owner**, after the emoji:
`:rocket: <@bo>'s agent: Ready`. "Ready" is unambiguous in a private thread and
useless where three of them are talking. The marker stays first deliberately — the
canary that spots a lifecycle line which lost its metadata reads those markers, and
a line starting with a user mention would be invisible to it. There is one message
per *session*, not a merged roster line: merging needs cross-session write
coordination on a single message.

**And it does not move.** The restatement exists because the reader is at the bottom
of the thread — but in a room the bottom of the thread is several other people's
conversation, and N sessions each dropping a paused/ended line into it is most of
what makes a busy room unreadable. So in a multiplayer thread the anchor is edited
where it sits, and the person whose session it is gets the same words as an
ephemeral, which lands where they are reading and costs everybody else nothing.

The busy reaction is the exception that goes the other way: it is the **thread's**,
counted across every session under the root, so it goes on for the first turn
anywhere and comes off after the last rather than flapping as each session finishes.

### Who sees which chrome

| | Solo | Multiplayer |
|---|---|---|
| status anchor | public, restated at episode boundaries | public, **edited in place only**, owner-prefixed |
| paused / picking-back-up / ended | the restatement above | anchor edit + **ephemeral to the owner** |
| turn failed, turn refused | public in the thread | **ephemeral to the owner** |
| launching / ended / discussion nudges | ephemeral | ephemeral |
| idle warning | posted in the thread, deleted when answered | **DM to the owner**, edited when answered |
| join line, flip announcement, agent answers, permission prompt | public | public |

Ephemerals are best-effort *by design*, and everything above leans on it: Slack
renders one only if that person is looking at the thread, it cannot be edited or
referred to later, and it never notifies. Nothing depends on delivery — the anchor
carries the same fact durably, and a mention picks a session back up whether or not
the notice was ever seen. They also never appear in `conversations.replies`, which
makes them structurally invisible to every carryover and catch-up: the one piece of
chrome that needs no tag to stay out of a prompt.

The idle warning — sent `SESSION_IDLE_WARN_LEAD` (2 minutes by default) before an
idle session pauses — is its own message, because reaching somebody who has gone
quiet mid-thought is the entire point and an edit would not notify. It goes out at
most once per quiet spell; a turn re-arms it. It is **withdrawn once it is
answered**, by either answer: a turn arrived, or the session paused or ended. Left
standing it promises a pause that is either not coming or already happened.

In a multiplayer thread the idle warning is the one owner-only line that is **not**
an ephemeral, for exactly the reason above: an ephemeral neither notifies nor
renders for a person who is not looking. It goes to the owner's DM with the bot,
links the thread, asks for a mention rather than a reply (in a room a plain reply
keeps nothing going), and is **edited** rather than deleted when its question is
answered — deleting a DM leaves a notification pointing at a message that no
longer exists.

## The approval DM

The approval request is DM'd to one person — posting it in the thread would let
any channel member race the owner, since it is first-approval-wins with no deny.
A DM channel has no thread structure, so the message has to carry its own
context: it names the agent and links the thread it came from, because someone
with two runs in flight has no other way to tell which DM belongs to which
conversation. If the DM cannot be sent, the bot ends the session and says so in
the thread.

The consent-page URL rides on the button, never in the message body. It appears
in the notification fallback only as a labeled link, so no surface displays a raw
URL, and a client that cannot render blocks still has a way through.

Once the session runs, the bot replaces the message's blocks with a line saying it
was approved and linking the thread. It replaces the blocks rather than omitting
them, so the button is gone deterministically rather than depending on
`chat.update`'s clear-on-omit behavior: a link to the consent page would only
invite a visit that does nothing.

## Accepted risks

- **Comments are an injection channel by design.** That is what "the agent reads the
  room" means: any channel member's un-mentioned message reaches every participant's
  agent at its next turn, quoted and escaped. It cannot *make* an agent act —
  acting requires the owner's own mention, and nothing synthesizes action from
  carried text — but it is the same class of exposure carryover already accepts,
  recurring per turn rather than once.
- **The bot does not limit launches per person.** Each top-level mention, and each
  new participant's mention in a thread, launches a pod and sends an approval DM,
  and any channel member can cause one. Within one thread a person holds one
  session: a second mention from them finds their own binding and runs a turn or
  waits rather than launching — a lookup, not a reservation, so there is nothing to
  release and nothing to leak. The platform's limits are per client, so they apply
  to the bot as a whole: the ClientBinding's optional `createRatePerMinute`,
  `maxLiveSessions` and `maxPendingApprovals`. Reclamation is `AGENTIC_RUN_TTL`
  (unapproved, 15 min), `SESSION_IDLE_TTL` (15 min without a turn) and
  `SESSION_TTL` (1 h from the session's start or its latest continuation, for a
  session that is not paused).
- **The etiquette nudge needs a session in memory.** Somebody with no session of
  their own gets the "plain messages are discussion" ephemeral only when this
  process is holding one for that thread; after a restart, or while every session in
  it is paused, their plain message is silently treated as what it is — discussion.
  Finding the roster would cost a session listing on every stray reply in every
  thread, which is a poor trade for a nudge.
- **The multiplayer etiquette is snapshotted at creation.** The system prompt
  appendix cannot be revised, so a session that was solo when it started is never
  told that its thread later gained other agents. What it would have prevented is a
  wrong guess about files it will find missing the moment it looks; what fixing it
  would cost is re-creating sessions to edit a system prompt.
- **A paused session learns of a flip late.** A session that was not held in memory
  when its thread flipped records the flip only when it is picked up again. If, by
  then, this process holds no session in the thread at all (every session in it is
  paused, or ended across a restart), its owner's plain reply continues it as in a
  solo thread.
- **A flip costs its trigger one message.** A participant baselined by a flip that
  came from a continuation has their cursor set at the continuing mention rather
  than just before it, so that one message is missing from their next catch-up. A
  join reads the thread anyway and gets the boundary right; making a continuation
  match would cost an API call to recover a single line that its answer will
  paraphrase regardless.
- **A thread read has a ceiling.** `ThreadReplies` pages through the whole thread,
  200 replies a page, and keeps the last 200 messages — the first 200 would empty a
  catch-up rather than truncate it, since the window is always at the newest edge.
  It stops after 15 pages, so a thread of more than 3,000 replies cannot be read
  whole. A seed then carries nothing and the join line says so; a catch-up reads
  again from one hour before the session's cursor.
- **A paused conversation is a second copy of it, governed only by this routing.**
  The Slack thread is governed by Slack's access controls; the transcript on the
  workspace volume is governed by whether the bot routes someone to it. The
  mitigation is the approval pin described above — a mis-routed continuation is
  never approved, so it never runs — but the copy exists, and the boundary is
  application logic rather than anything Kubernetes enforces.
- **The transcript may grow without bound.** In the example SandboxTemplates it
  shares the 1Gi workspace volume with the git checkout, and it accumulates across
  every continuation of a session. If it ever fills the volume the failure is
  `ENOSPC` in the *workspace* — failing git operations and agent file writes — not
  anything transcript-shaped. The escape hatch is a larger `storage:` request in the
  SandboxTemplate.
- **No thread metrics.** The bot's telemetry is structured logging only, and the
  platform's `/metrics` covers Harness API requests and subscriptions, not threads.
  Everything about threads is log-derived: `session joined a thread` answers "why
  does this session exist?", `thread is now multiplayer` records a flip, and the
  platform's `session created` line carries the live and launching counts so a
  runaway is at least visible.
- **Editing the message that triggered a turn does not change the running
  request.** The composed turn is the durable record.
- **"Refused" and "ignored" approvals are indistinguishable** — the
  authorization server exposes no denial signal, so the lapsed-approval copy names
  both possibilities.
- **Slack rate limits with several streaming sessions in one channel.** Posts and
  updates are rate-limited per channel, across every session in it, and streaming
  updates to one message are coalesced. Several sessions streaming in one channel
  share that budget, so each one's live updates slow down.
- **No cross-channel work.**
