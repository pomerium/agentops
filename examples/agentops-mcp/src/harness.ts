import { readFile } from "node:fs/promises";
import { Data, Effect, Result, Schema } from "effect";
import type { JsonObject } from "./lib/json.ts";
import { getConfig } from "./config.ts";

/**
 * Minimal client for the AgentOps Harness API: Connect with JSON, one POST per
 * method to `<base>/harnessapi.v1.HarnessAPIService/<Method>`. Field names are
 * the proto JSON (camelCase) names; see proto/harnessapi/v1/harnessapi.proto in
 * pomerium/agentops.
 */

export class HarnessError extends Data.TaggedError("HarnessError")<{
  readonly method: string;
  readonly status?: number;
  readonly message: string;
}> {}

export type SessionView = {
  id: string;
  conversationRef?: string;
  state?: string;
  template?: string;
  lastSeq?: string;
};

export type TemplateSummary = { name: string; description?: string };

/** One session event. Exactly one payload field is set. */
export type HarnessEvent = {
  seq: string;
  turnId?: string;
  stateChanged?: { old?: string; new?: string; reason?: string };
  approvalRequired?: { approvalUrl: string; expiresAt?: string };
  approved?: { approverSubject?: string };
  launchStalled?: { waited?: string };
  agentMessage?: { partId?: string; text?: string; final?: boolean };
  agentThought?: { text?: string };
  toolCall?: { id?: string; title?: string; status?: string };
  permissionRequest?: { requestId?: string; summary?: string };
  turnCompleted?: { stopReason?: string };
  turnFailed?: { reason?: string };
  sessionEnded?: { reason?: string; detail?: string };
};

export type CreateSessionRequest = {
  template: string;
  conversationRef: string;
  approvalPrompt: string;
  initialPrompt: string;
};

export interface HarnessClient {
  listTemplates(): Effect.Effect<TemplateSummary[], HarnessError>;
  createSession(
    request: CreateSessionRequest,
  ): Effect.Effect<SessionView, HarnessError>;
  listEvents(
    sessionId: string,
    afterSeq: number,
  ): Effect.Effect<HarnessEvent[], HarnessError>;
  endSession(sessionId: string): Effect.Effect<void, HarnessError>;
  listSessions(liveOnly: boolean): Effect.Effect<SessionView[], HarnessError>;
}

type HarnessClientOptions = {
  baseUrl: string;
  token: () => Promise<string>;
  fetch?: typeof globalThis.fetch;
};

export function makeHarnessClient(
  options: HarnessClientOptions,
): HarnessClient {
  const doFetch = options.fetch ?? globalThis.fetch;
  const base = options.baseUrl.replace(/\/+$/, "");

  const call = <T>(method: string, body: JsonObject) =>
    Effect.tryPromise({
      try: async () => {
        const token = await options.token();

        const response = await doFetch(
          `${base}/harnessapi.v1.HarnessAPIService/${method}`,
          {
            method: "POST",
            headers: {
              authorization: `Bearer ${token}`,
              "content-type": "application/json",
            },
            body: JSON.stringify(body),
          },
        );

        const text = await response.text();

        if (!response.ok) {
          throw new HarnessError({
            method,
            status: response.status,
            message: harnessErrorMessage(text, response.status),
          });
        }

        // SAFETY: a 2xx Connect response is the JSON encoding of `method`'s
        // response message (harnessapi.proto); T names that message's fields.
        return (text ? JSON.parse(text) : {}) as T;
      },
      catch: (error) =>
        error instanceof HarnessError
          ? error
          : new HarnessError({
              method,
              message: error instanceof Error ? error.message : String(error),
            }),
    });

  return {
    listTemplates: () =>
      call<{ templates?: TemplateSummary[] }>("ListTemplates", {}).pipe(
        Effect.map((r) => r.templates ?? []),
      ),
    createSession: (request) =>
      call<{ session: SessionView }>("CreateSession", request).pipe(
        Effect.map((r) => r.session),
      ),
    listEvents: (sessionId, afterSeq) =>
      call<{ events?: HarnessEvent[] }>("ListEvents", {
        ref: { sessionId, includeTerminal: true },
        afterSeq: String(afterSeq),
      }).pipe(Effect.map((r) => r.events ?? [])),
    endSession: (sessionId) =>
      call("EndSession", { ref: { sessionId } }).pipe(Effect.asVoid),
    listSessions: (liveOnly) =>
      call<{ sessions?: SessionView[] }>("ListSessions", { liveOnly }).pipe(
        Effect.map((r) => r.sessions ?? []),
      ),
  };
}

const ConnectError = Schema.Struct({
  code: Schema.optional(Schema.String),
  message: Schema.optional(Schema.String),
});

/** Connect errors are `{code, message}` JSON; Pomerium refusals are HTML. */
function harnessErrorMessage(text: string, status: number): string {
  try {
    const parsed = Schema.decodeUnknownResult(ConnectError)(JSON.parse(text));

    if (Result.isFailure(parsed)) return `HTTP ${status}`;

    return [parsed.success.code, parsed.success.message]
      .filter(Boolean)
      .join(": ");
  } catch {
    return `HTTP ${status}`;
  }
}

/** The highest seq in `events`, or `afterSeq`; ignores seqs that don't parse. */
export function nextSeq(afterSeq: number, events: HarnessEvent[]): number {
  return events.reduce((max, event) => {
    const seq = Number(event.seq);

    return Number.isFinite(seq) ? Math.max(max, seq) : max;
  }, afterSeq);
}

/** A session's events after `afterSeq`, following every page to the end. */
export function listAllEvents(
  harness: HarnessClient,
  sessionId: string,
  afterSeq = 0,
): Effect.Effect<HarnessEvent[], HarnessError> {
  return Effect.gen(function* () {
    const all: HarnessEvent[] = [];
    let after = afterSeq;

    for (;;) {
      const page = yield* harness.listEvents(sessionId, after);
      const next = nextSeq(after, page);

      all.push(...page);

      // An empty page is the end; one that doesn't advance would loop.
      if (page.length === 0 || next === after) return all;
      after = next;
    }
  });
}

let defaultClient: HarnessClient | undefined;

/** The process-wide client built from config. */
export function getHarnessClient(): HarnessClient {
  if (!defaultClient) {
    const config = getConfig();
    defaultClient = makeHarnessClient({
      baseUrl: config.HARNESS_API_URL,
      token: async () =>
        config.HARNESS_TOKEN ||
        (await readFile(config.HARNESS_TOKEN_FILE, "utf8")).trim(),
    });
  }

  return defaultClient;
}
