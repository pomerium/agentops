import { SessionState } from "./gen/harnessapi/v1/harnessapi_pb.js";

const liveStates: ReadonlySet<SessionState> = new Set([
  SessionState.PENDING,
  SessionState.LAUNCHING,
  SessionState.AWAITING_APPROVAL,
  SessionState.RUNNING,
  SessionState.SUSPENDED,
]);

export function isLive(state: SessionState): boolean {
  return liveStates.has(state);
}
