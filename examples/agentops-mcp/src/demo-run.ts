import { createHash } from "node:crypto";
import { inputRequired, inputResponse } from "@modelcontextprotocol/server";
import type { Duration } from "effect";
import { Effect } from "effect";
import { nextSeq, type HarnessClient, type HarnessEvent } from "./harness.ts";
import { createErrorResult, createTextResult } from "./lib/utils.ts";
import { logger } from "./logger.ts";
import { isTerminal, TaskStore, type TaskRecord } from "./tasks.ts";

/**
 * Drives one AgentOps session behind one MCP task, mapping Harness API events
 * to task state:
 *
 *   PENDING / LAUNCHING      → working          "Launching sandbox…"
 *   approvalRequired         → input_required   URL elicitation of the Pomerium consent page
 *   approved / RUNNING       → working          "Approved by …", "Agent running…"
 *   agentThought / toolCall  → working          live statusMessage
 *   turnCompleted            → completed        the agent's answer
 *   turnFailed / sessionEnded before an answer → completed, isError
 *   tasks/cancel             → cancelled        EndSession
 *
 * The approval itself happens on Pomerium's page, as the signed-in user, never
 * in the MCP client: the elicitation only hands the user the link.
 */

/** conversationRef prefix of every session this server creates. */
export const SESSION_REF_PREFIX = "mcp-";

/**
 * A session's conversationRef: the run id plus a tag of the user who launched
 * it. Every session runs as this server's ServiceAccount, so the Harness API
 * can't tell users apart; the tag keeps a run its owner's after this process
 * forgets it.
 */
export function sessionRef(runId: string, owner: string | undefined): string {
  return `${SESSION_REF_PREFIX}${runId}.${ownerTag(owner)}`;
}

/** The run id in a conversationRef this server made. */
export function runIdFromRef(ref: string): string {
  const id = ref.slice(SESSION_REF_PREFIX.length);
  const dot = id.indexOf(".");

  return dot === -1 ? id : id.slice(0, dot);
}

/** Whether `owner` launched the session with this conversationRef. */
export function ownsSession(
  ref: string | undefined,
  owner: string | undefined,
): boolean {
  return (
    ref !== undefined &&
    ref.startsWith(SESSION_REF_PREFIX) &&
    ref.endsWith(`.${ownerTag(owner)}`)
  );
}

function ownerTag(owner: string | undefined): string {
  return createHash("sha256")
    .update(owner ?? "")
    .digest("base64url")
    .slice(0, 16);
}

export type DemoRunOptions = {
  store: TaskStore;
  harness: HarnessClient;
  taskId: string;
  template: string;
  prompt: string;
  owner: string | undefined;
  pollInterval?: Duration.Input;
};

const STATE_MESSAGES = new Map([
  ["SESSION_STATE_PENDING", "Session queued"],
  ["SESSION_STATE_LAUNCHING", "Launching the agent's sandbox pod…"],
  ["SESSION_STATE_RUNNING", "Agent running"],
]);

/** Consecutive failed ListEvents polls before the run gives up. */
const MAX_POLL_FAILURES = 5;

export function approvalKey(seq: string): string {
  return `approve-run-${seq}`;
}

export function runDemoSession(options: DemoRunOptions): Effect.Effect<void> {
  const { store, harness, taskId, template, prompt, owner } = options;
  const pollInterval = options.pollInterval ?? "1500 millis";
  /** Set once CreateSession returns, so a failure can end the session. */
  let createdSessionId: string | undefined;

  return Effect.gen(function* () {
    const record = store.get(taskId, owner);

    if (!record) return;

    // A cancel can arrive while CreateSession is pending: record it now and
    // end the session once it exists.
    record.onCancel = () => {
      store.update(taskId, {
        status: "cancelled",
        statusMessage: "Cancelled before the agent session started",
      });
    };

    const session = yield* harness.createSession({
      template,
      conversationRef: sessionRef(taskId, owner),
      approvalPrompt: `Run the AgentOps "${template}" demo agent${owner ? ` for ${owner}` : ""}?`,
      initialPrompt: prompt,
    });

    const sessionId = session.id;
    createdSessionId = sessionId;
    yield* logger.info({ taskId, sessionId, template }, "Session created");

    const endSession = () =>
      Effect.runFork(
        harness.endSession(sessionId).pipe(Effect.catch(() => Effect.void)),
      );

    if (isTerminal(store.get(taskId, owner)?.status ?? "cancelled")) {
      endSession();

      return;
    }

    record.onCancel = () => {
      store.update(taskId, {
        status: "cancelled",
        statusMessage: "Cancelled; the agent session was ended",
      });
      endSession();
    };

    record.onInputResponses = (responses) => {
      const outstanding = record.inputRequests ?? {};

      for (const key of Object.keys(responses)) {
        if (!(key in outstanding)) continue;
        const response = inputResponse(responses, key);

        // Which answer each client gives (e.g. whether a form is shown or
        // cancelled by the client) is otherwise invisible.
        Effect.runFork(
          logger.info(
            {
              taskId,
              key,
              kind: response.kind,
              action: response.kind === "elicit" ? response.action : null,
            },
            "Input response received",
          ),
        );

        if (response.kind === "elicit" && response.action === "decline") {
          store.update(taskId, {
            status: "cancelled",
            statusMessage: "Approval declined; the agent session was ended",
          });
          endSession();

          return;
        }

        // `cancel`: the prompt was dismissed, or the client couldn't show
        // it (a ChatGPT dot does this). Not a refusal: keep the run and its
        // link; the caller reports the link as plain text.
        if (response.kind === "elicit" && response.action === "cancel") {
          store.update(taskId, {
            statusMessage:
              "The approval prompt was dismissed; still waiting for approval in Pomerium",
          });

          return;
        }

        // The user opened the consent page. The `approved` event, not this
        // response, moves the task on.
        store.update(taskId, {
          status: "working",
          statusMessage: "Waiting for Pomerium to confirm the approval…",
        });
      }
    };

    store.update(taskId, {
      status: "working",
      statusMessage: `Session ${sessionId} created; launching sandbox…`,
      sessionId,
    });

    let afterSeq = 0;
    let failures = 0;
    const answer: string[] = [];

    while (!isTerminal(store.get(taskId, owner)?.status ?? "cancelled")) {
      // A transient Harness or Pomerium error shouldn't end the run: retry a
      // few polls before giving up.
      const events = yield* harness
        .listEvents(sessionId, afterSeq)
        .pipe(
          Effect.catch((error) =>
            ++failures < MAX_POLL_FAILURES
              ? logger
                  .warn(
                    { taskId, sessionId, failures, error: error.message },
                    "ListEvents failed; retrying",
                  )
                  .pipe(Effect.as(undefined))
              : Effect.fail(error),
          ),
        );

      if (events === undefined) {
        yield* Effect.sleep(pollInterval);
        continue;
      }

      failures = 0;
      const before = afterSeq;
      afterSeq = nextSeq(afterSeq, events);

      for (const event of events) {
        applyEvent(store, taskId, sessionId, event, answer);
      }

      // Nothing new (or a page that doesn't advance): wait before polling.
      if (afterSeq === before) yield* Effect.sleep(pollInterval);
    }

    yield* logger.info(
      {
        taskId,
        sessionId,
        status: store.get(taskId, owner)?.status ?? "expired",
      },
      "Task finished",
    );
  }).pipe(
    Effect.catch((error) =>
      Effect.gen(function* () {
        yield* logger.error(
          { taskId, method: error.method, error: error.message },
          "Harness API call failed",
        );

        // Nobody follows the run any more: end its session so it doesn't
        // hold a quota slot or a sandbox, or run after a late approval.
        if (createdSessionId !== undefined) {
          yield* harness
            .endSession(createdSessionId)
            .pipe(Effect.catch(() => Effect.void));
        }

        const quota = error.message.startsWith("resource_exhausted");

        store.update(taskId, {
          status: "completed",
          statusMessage: quota
            ? "Too many AgentOps runs are waiting for approval"
            : "The AgentOps Harness API call failed",
          result: createErrorResult({
            error: quota
              ? `Too many AgentOps runs are already waiting for approval (${error.message}). ` +
                "Call list_pending_approvals to see them with their approval links, then " +
                "approve them or end them with end_session, and launch again."
              : `Harness API ${error.method} failed: ${error.message}`,
          }),
        });
      }),
    ),
  );
}

/**
 * Rebuilds a run's state from its session's events, for runs this process no
 * longer tracks (it restarted, or the client gave up). Same mapping as a live
 * run, so get_run_status reports them the same way.
 */
export function replayRun(
  runId: string,
  sessionId: string,
  events: HarnessEvent[],
): TaskRecord {
  const scratch = new TaskStore();
  const record = scratch.create(undefined);
  const answer: string[] = [];

  scratch.update(record.taskId, {
    statusMessage: `Session ${sessionId}`,
    sessionId,
  });

  for (const event of events) {
    applyEvent(scratch, record.taskId, sessionId, event, answer);
  }

  return { ...record, taskId: runId };
}

function applyEvent(
  store: TaskStore,
  taskId: string,
  sessionId: string,
  event: HarnessEvent,
  answer: string[],
): void {
  if (event.stateChanged?.new) {
    // AWAITING_APPROVAL has no entry: the approvalRequired event that follows
    // it carries the URL and moves the task to input_required.
    const message = STATE_MESSAGES.get(event.stateChanged.new);

    if (message) {
      store.update(taskId, { status: "working", statusMessage: message });
    }

    return;
  }

  if (event.approvalRequired) {
    const { approvalUrl, expiresAt } = event.approvalRequired;
    const until = expiresAt ? ` before ${expiresAt}` : "";
    store.update(taskId, {
      status: "input_required",
      statusMessage: `Waiting for approval in Pomerium${until}`,
      approvalUrl,
      approvalExpiresAt: expiresAt,
      inputRequests: {
        [approvalKey(event.seq)]: inputRequired.elicitUrl({
          message: `Approve the AgentOps agent run on Pomerium's consent page${until}. Sign in as an approver, review what the agent asks for, and click Approve.`,
          url: approvalUrl,
        }),
      },
    });

    return;
  }

  if (event.approved) {
    const by = event.approved.approverSubject
      ? ` (approver ${event.approved.approverSubject})`
      : "";

    store.update(taskId, {
      status: "working",
      statusMessage: `Approved${by}; the agent is starting`,
    });

    return;
  }

  if (event.launchStalled) {
    store.update(taskId, {
      statusMessage:
        "The sandbox has not connected back yet; still waiting (launch stalled)",
    });

    return;
  }

  if (event.agentThought?.text) {
    store.update(taskId, {
      statusMessage: `Thinking: ${truncate(event.agentThought.text)}`,
    });

    return;
  }

  if (event.toolCall?.title) {
    store.update(taskId, {
      statusMessage: `Tool call: ${truncate(event.toolCall.title)}`,
    });

    return;
  }

  if (event.agentMessage?.text) {
    answer.push(event.agentMessage.text);
    store.update(taskId, { statusMessage: "The agent is answering…" });

    return;
  }

  if (event.turnCompleted) {
    store.update(taskId, {
      status: "completed",
      statusMessage: "The agent finished its turn",
      result: createTextResult({ sessionId, answer: answer.join("") }),
    });

    return;
  }

  if (event.turnFailed) {
    store.update(taskId, {
      status: "completed",
      statusMessage: "The agent's turn failed",
      result: createErrorResult({
        sessionId,
        error: event.turnFailed.reason ?? "turn failed",
      }),
    });

    return;
  }

  if (event.sessionEnded) {
    const { reason, detail } = event.sessionEnded;
    store.update(taskId, {
      status: "completed",
      statusMessage: "The session ended before the agent answered",
      result: createErrorResult({
        sessionId,
        error: [reason, detail].filter(Boolean).join(": ") || "session ended",
      }),
    });
  }
}

function truncate(text: string, max = 160): string {
  const line = text.replace(/\s+/g, " ").trim();

  return line.length > max ? `${line.slice(0, max - 1)}…` : line;
}
