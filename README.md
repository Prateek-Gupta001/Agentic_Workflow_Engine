# Agentic Workflow Engine

A small backend engine for running a customer support triage workflow as a DAG, with a few of the agent/decision nodes backed by an LLM call (mocked here) instead of fixed logic. Upon reading up on DAG and Graph engineering, I wanted to build like a real infra project — Postgres as the single source of truth for every node's state, crash-safe replay, idempotent retries, and a park-and-resume flow for the human-in-the-loop step.

The whole thing runs on a fixed, hand-authored graph. Nothing about the DAG shape is generated at runtime — only the *path* through it is decided dynamically, based on what the classify step returns.

## What it actually does

A request comes in as free text ("my invoice looks wrong this month"). The engine classifies it as `bug`, `billing`, or `unclear`, pulls some mock customer/account context in parallel, and then branches:

- **bug** → files a mock Linear issue, then drafts a reply
- **billing** → checks a mock invoice, then drafts a reply
- **unclear** → pauses and waits for a human to approve/reject before drafting a reply

```
Input
 ├──> Classify ─────────────┐
 ├──> FetchCustomer ────────┤
 └──> FetchAccount ─────────┴──> ChoosePath
                                    ├─ bug     → CreateLinearIssue → DraftReplyBug
                                    ├─ billing → CheckInvoice      → DraftReplyBilling
                                    └─ unclear → HumanApproval     → DraftReplyUnclear
```

Only one of the three bottom branches ever actually runs per request — the other two get marked `skipped` by the executor once `ChoosePath` resolves.

## Design decisions (and why)

**Every node is a fixed graph vertex, not a shared type dispatching on a task field.** Early on I tried making one generic `AgentDecisionNode` handle both classification and reply-drafting by passing a `task` string through the input map. It got confusing fast — the node ended up needing to know things about the graph (who's calling it, what it depends on) that really belong to the graph, not the node. I split it out: every step in the DAG above is its own `Node` implementation with its own Go type, and a static `Deps` map (`NodeType -> []NodeType`) owns the dependency structure. Nodes are pure functions — `Execute(ctx, input) (output, error)` and nothing else. They don't touch Postgres, don't know their own position in the graph, don't know about retries. All of that lives in the executor.

**Postgres is read on every dispatch, not just on crash recovery.** I went back and forth on this — an in-memory fast path during a live run, falling back to Postgres only if the process restarts, would technically be faster. I skipped it on purpose. Having one code path (`GetNodeStates` → merge outputs → dispatch) that behaves identically whether the run is five milliseconds old or resuming after a crash means there's no separate "resume" logic to get subtly wrong. It costs a few extra round trips per run. Worth it for a system where the main thing being graded is reliability, not throughput.

**Execution is a fixed-point loop, not a fixed number of passes.** Each iteration: read all node states from Postgres, find whatever's `pending` with all its dependencies `completed`/`skipped`, dispatch those in parallel, wait, repeat. It stops when nothing left is both pending and unblocked — which naturally happens once a branch is chosen and the losing branches get pruned, without the loop needing to know in advance which node will end up being terminal.

**Human approval doesn't block a goroutine.** When `HumanApproval` becomes ready, the executor writes `awaiting_approval` straight to `node_states` and returns — nothing sits there waiting on a channel. A separate endpoint resumes the same dispatch loop once a decision comes in. This means a run can sit "paused" for two minutes or two days without holding any resources.

**Idempotency comes from checking `node_states` before doing anything, not from a dedupe key.** Every tool-call node's completed row *is* its cache. If a node is retried, the executor won't re-call a mock if that node instance already has a `completed` row — same pattern the classify/validation checks use.

## Setup

You'll need Go and a Postgres instance reachable from wherever you run this.

```bash
export DATABASE_URL="postgres://user:pass@localhost:5432/workflow_engine?sslmode=disable"
go run ./cmd/server
```

The schema (`workflow_runs`, `node_states`, `node_events`) gets created automatically on startup via `CREATE TABLE IF NOT EXISTS` — no separate migration step, no seed data needed.

Default listen address is `localhost:8080`. The frontend is a single static `index.html` — open it directly in a browser, no build step. If your server isn't on the default address, change the `API_BASE` constant near the top of the `<script>` tag.

## API

| Method | Path | What it does |
|---|---|---|
| `POST` | `/v1/req` | Submit a new request. Returns `{req_id}` immediately; the run itself continues in the background. |
| `GET` | `/v1/runs/{id}/state` | Full snapshot of every node's status/input/output/error/attempt count for that run. This is what the UI polls every 2s. |
| `GET` | `/v1/runs/{id}/result` | Whether the run has finished, and the final reply if so. |
| `GET` | `/v1/runs/{id}/approval` | Whether `human_approval` is currently waiting on a decision. |
| `POST` | `/v1/runs/{id}/approval` | Submit `{decision: "approved"|"rejected"}`. |
| `POST` | `/v1/runs/{id}/nodes/{nodeId}/retry` | Retry a node currently in `failed` status. |
| `GET` | `/v1/runs/{id}/nodes/{nodeId}/events` | Full event history for one node across every attempt — status transitions, timestamps, error messages. |
| `GET` | `/v1/runs` | Last 10 runs, for the "past runs" view. |

## The UI

Plain HTML/CSS/vanilla JS, one file, no dependencies. It's a debugger, not a product — submit a request, watch the DAG light up node by node as things complete, click any node to see its input/output/error and full event log, retry anything that failed, approve or reject when a human decision is needed. There's also a tab for browsing and re-opening past runs.

## Testing the interesting paths

The assignment specifically asks for scenarios covering branching, retry, approval, validation failure, and idempotency, so here's how to exercise each one:

**Branching** — submit three different requests (something with "bug"/"crash"/"error" in it, something with "invoice"/"bill", and something unrelated to either) and watch three different paths light up, with the other two branches showing as `skipped`.

**Retry** — the `CreateLinearIssue` mock is deliberately flaky: it fails on the first attempt and succeeds from the second attempt onward. Submit a bug-classified request, watch that node go `failed`, hit retry (via the UI button or `POST .../retry`), and watch it complete and unblock the draft-reply step. The node's event log will show both attempts.

**Approval** — submit something that classifies as `unclear`. The `human_approval` node parks itself at `awaiting_approval` and the run stalls there (by design — no goroutine is blocked, it's just a status in Postgres). Approve or reject it and confirm the final draft-reply node picks up the decision correctly.

**Validation failure** — classify and human-approval both validate their own output before letting the executor treat them as complete: classify only accepts `bug`/`billing`/`unclear` back from the mock LLM, and the approval endpoint only accepts `approved`/`rejected`. Sending anything else to `POST .../approval` gets rejected with a 400 rather than silently corrupting state.

**Idempotency** — retry an already-completed node, or submit approval twice for the same run. Both are guarded (a `WHERE status = 'failed'` / `WHERE status = 'awaiting_approval'` clause on the reset), so a duplicate call is a no-op rather than a double-execution of a mock external system.

## What's not in here

A few things I decided not to build, on purpose rather than by accident:

- **No automatic crash recovery for a node stuck mid-execution.** If the process dies between marking a node `running` and it actually finishing, nothing currently notices and resets it — you'd have to do that by hand today. The retry endpoint only covers nodes the executor itself marked `failed`. A reconciliation pass (find `running` rows older than some threshold on startup, reset them, redispatch) is the natural next addition and the data model already supports it — I just ran out of scope to wire it in properly.
- **No OpenTelemetry/Jaeger.** I thought about it, since I've used that stack elsewhere. Decided against it here — the whole point of a trace is reconstructing causality across a system that doesn't already have a structured record of who depended on whom. This one does, in Postgres, as a direct consequence of the DAG design. Bolting on a tracing backend on top would be tracking something the schema already tells you.
- **No auth.** Wide open, single-tenant, meant to run locally against your own Postgres.
