---
name: agentops-demo
description: Run the AgentOps hello demo agent through this MCP server and explain what happens at each step - launching a sandbox, approving the run on Pomerium's consent page, following the task's progress, and reading the agent's answer. Trigger when asked to launch, demo, or check on an AgentOps agent run, or to list or end AgentOps sessions. Do not use to approve a run on the user's behalf.
---

# AgentOps demo

This server fronts AgentOps, which runs coding agents in Kubernetes sandbox pods behind Pomerium. It exposes these tools:

- `launch_hello_demo` starts a run of the `hello` demo agent. Pass an optional `prompt`.
- `get_run_status` waits briefly and reports a run by `runId`: the approval link, progress, or the answer.
- `list_pending_approvals` lists the user's runs still waiting for approval, with its link and expiry, including stale runs no client follows any more.
- `list_agents` lists the agent templates this server may run.
- `end_session` ends a session by `sessionId` and frees its pod.

## How a run goes

`launch_hello_demo` returns an MCP **task** when the client supports the Tasks extension (`io.modelcontextprotocol/tasks`). Follow its `statusMessage` as it moves through:

1. `working`: the session is queued and the sandbox pod launches.
2. `input_required`: the run needs approval. The task carries a **URL elicitation** that links to Pomerium's consent page. Open it for the user. The user signs in, reviews what the agent asks for, and clicks Approve. Approval expires after about 15 minutes.
3. `working`: "Approved"; the agent is starting, thinking, calling tools or answering.
4. `completed`: the result holds `sessionId` and the agent's `answer`. A failed turn or a session that ended early (for example, the approval expired) completes with `isError: true` and the reason.

Cancelling the task ends the AgentOps session. Declining the URL elicitation does too. Dismissing it (`cancel`) does not: the run keeps waiting for approval.

If the client supports MCP Events, subscribe to `run.approval_required` and `run.finished` before launching. The launch then returns the `runId` and approval link at once, and the answer arrives as a `run.finished` event: don't poll.

Without events or the Tasks extension, a client that supports URL-mode elicitation gets the approval request in the tool call itself, then the answer when the agent finishes. Any other client (for example ChatGPT) gets a plain result with the approval link and a `runId`: show the user the link, then call `get_run_status` with the `runId` until the run completes, or subscribe to `run.finished` with that `run_id`.

If a launch fails because too many runs are waiting for approval, call `list_pending_approvals`, then approve those runs or end them with `end_session`.

## Boundaries

- Only the user can approve, on Pomerium's page, as themselves. Pomerium checks that they are an approver. Never claim a run is approved until the task says so.
- Each user sees only the tasks they started.
- The `hello` agent has no LLM. It answers every prompt with a fixed text that names its sandbox pod, which shows that the whole path works.
