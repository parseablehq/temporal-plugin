# @parseable/temporal

Temporal middleware plugin that ships workflow and activity execution events to [Parseable](https://www.parseable.com/) as OpenTelemetry logs and traces.

> **End-user integration guide:** [INTEGRATION.md](./INTEGRATION.md) - install, configure, schema reference, query examples.
>
> **Submission status:** [STATUS.md](./STATUS.md) - what's done and what's pending for the Temporal AI Partner Program submission.

The plugin emits structured logs (workflow/activity start, complete, fail, retry, duration) into a Parseable log stream, alongside OpenTelemetry traces (`RunWorkflow:*`, `StartActivity:*`, `RunActivity:*`) into a Parseable trace stream. Users get a flat queryable schema for analytics plus a waterfall view of workflow execution.

This README is the developer-facing landing page for the repo (architecture, repo layout, how to run the demo, caveats). For end-user consumption see `INTEGRATION.md`.

---

## Repository layout

`src/` is the publishable plugin package (`@parseable/temporal`). `examples/` contains a runnable demo worker and clients. `test/` contains mocha tests against the plugin using the demo workflows as fixtures.

```
src/                              # the integration - published as @parseable/temporal
├── index.ts                      # ParseablePlugin class (extends SimplePlugin)
├── activity-interceptor.ts       # ActivityInbound interceptor (worker process)
├── workflow-interceptor.ts       # WorkflowInbound + Outbound interceptors (workflow isolate, replay-safe via sinks)
├── workflow.ts                   # public workflowEvent() helper (consumer imports from @parseable/temporal/workflow)
├── exporters.ts                  # OTLP HTTP exporters (logs + traces) + SanitizingSpanExporter
├── version.ts                    # PLUGIN_VERSION constant
└── types.ts                      # ParseableEventRecord schema

examples/                         # runnable demo - not published
├── activities.ts                 # greet (success), chargeCard (always fails)
├── workflows.ts                  # example, failingExample, userEventExample, parentExample, signalEventExample, queryUpdateExample, childSignalParent, continueAsNewExample, updateFailureExample, parentFailingExample
├── worker.ts                     # demo worker wired up with ParseablePlugin
├── client.ts                     # triggers happy-path workflow
├── fail-client.ts                # triggers failing workflow
├── event-client.ts               # triggers user-event workflow
└── parent-client.ts              # triggers parent → child workflow

test/                             # mocha tests (use examples/* as fixtures)
├── replay-safety.test.ts         # interceptor coverage: workflow/activity/signal/query/update/child_workflow/continue_as_new + failure paths, asserts zero emissions during history replay
├── activities.test.ts            # unit test for greet activity
├── workflows.test.ts             # workflow integration test (requires TestWorkflowEnvironment)
└── workflows-mocks.test.ts       # workflow integration test with mocked activities
```

---

## Architecture

```
                        ┌───────────────────┐
                        │  Temporal Server  │
                        │ (localhost:7233)  │
                        └─────────┬─────────┘
                                  │ gRPC
                  ┌───────────────┴───────────────┐
                  │           Worker              │
                  │                               │
                  │  ┌─────────────────────────┐  │
                  │  │  Workflow V8 isolate    │  │  ← replay-safe; cannot do I/O
                  │  │                         │  │
                  │  │  WorkflowInbound +      │  │
                  │  │  WorkflowOutbound       │  │
                  │  │  interceptors           │  │
                  │  │                         │  │
                  │  │  proxySinks ──────┐     │  │
                  │  └───────────────────┼─────┘  │
                  │                      ▼        │
                  │  ┌──────────────────────────┐ │
                  │  │ Sink consumer (worker proc)│
                  │  │ enriches with service_name│
                  │  └──────────────┬───────────┘ │
                  │                 │             │
                  │  ┌──────────────▼───────────┐ │
                  │  │ ActivityInbound          │ │
                  │  │ interceptor              │ │
                  │  └──────────────┬───────────┘ │
                  │                 │             │
                  │  ┌──────────────▼───────────┐ │
                  │  │ emit(record)             │ │
                  │  │  → OTel Logger           │ │
                  │  │  → BatchLogRecordProc    │ │
                  │  │  → OTLPLogExporter       │ │
                  │  └──────────────┬───────────┘ │
                  │                 │             │
                  │  ┌──────────────┴────────────┐│
                  │  │ Temporal OpenTelemetryPlug││
                  │  │  → BatchSpanProcessor     ││
                  │  │  → SanitizingSpanExporter ││
                  │  │  → OTLPTraceExporter      ││
                  │  └──────────────┬────────────┘│
                  └─────────────────┼─────────────┘
                                    │ HTTPS
                          ┌─────────▼──────────┐
                          │  Parseable         │
                          │  /v1/logs   (logs) │
                          │  /v1/traces (spans)│
                          └────────────────────┘
```

### Key design points

- **Replay safety.** Workflow events are emitted via `proxySinks` with `callDuringReplay: false`. When Temporal replays a workflow's history (after a worker crash, cache eviction, or manual replay), the sink is skipped - no duplicate logs or spans. Verified by `src/mocha/replay-safety.test.ts`.
- **Two layers, one plugin.** `ParseablePlugin` extends `@temporalio/plugin`'s `SimplePlugin` and internally composes Temporal's official `OpenTelemetryPlugin` for trace emission. Logs are emitted from our own interceptors directly to OTel's log API. Both flow into Parseable through OTLP/HTTP.
- **`SanitizingSpanExporter`.** Temporal's OTel plugin emits spans with nested objects, `Date` instances, and `undefined` fields as attributes. OTLP attribute values are restricted to primitives or arrays of primitives, so Parseable's strict OTLP parser rejects the raw payload with `400 Invalid data for Value`. The sanitizer wraps the trace exporter and flattens nested objects to JSON strings, `Date` to ISO, and drops `undefined`s before serialization.
- **OTel pinned to 1.x.** Temporal's `OpenTelemetryPlugin` pins `@opentelemetry/sdk-trace-base@^1.25.1`. The OTel ecosystem has split between 1.x (mature) and 2.x (newer). We ride the 1.x line - `sdk-trace-base@1.30.x`, `resources@1.30.x`, `exporter-{logs,trace}-otlp-http@0.57.x`, `sdk-logs@0.57.x` - until Temporal moves.

---

## Running the demo locally

### Prerequisites

- Node.js 20+
- [Temporal CLI](https://github.com/temporalio/cli) (`brew install temporal` on macOS)
- A Parseable instance reachable on the network. For dev: a local instance with default credentials.

### Three terminals

**Terminal 1 - Temporal dev server:**

```bash
temporal server start-dev
```

Runs on `localhost:7233` (gRPC) and `http://localhost:8233` (UI).

**Terminal 2 - Worker:**

```bash
npm install
PARSEABLE_URL=https://your-parseable-host \
PARSEABLE_USERNAME=youruser \
PARSEABLE_PASSWORD=yourpass \
npm run examples:worker.watch
```

`PARSEABLE_URL` is required - the worker refuses to start without it. Username/password default to `admin/admin` if unset (matching a default Parseable dev install). The worker connects to Temporal at `localhost:7233`, polls the `hello-world` task queue, and auto-restarts on `src/` or `examples/` changes via nodemon.

**Terminal 3 - Client (run on demand):**

```bash
npm run examples:workflow         # success path: greet activity
npm run examples:workflow:fail    # failure path: chargeCard with 3-retry policy
npm run examples:workflow:event   # user-event path: workflow emits custom domain events via workflowEvent()
npm run examples:workflow:parent  # parent → child workflow path: exercises the outbound interceptor
```

After running, check Parseable at `${PARSEABLE_URL}`:
- Stream `temporal-logs` - workflow/activity records with attributes `workflow_id`, `activity_name`, `attempt`, `status`, `duration_ms`, `service_name`, etc.
- Stream `temporal-traces` - spans `RunWorkflow:example`, `StartActivity:greet`, `RunActivity:greet`.

---

## Tests

```bash
npm test                                                                        # runs all mocha tests
npx mocha --require ts-node/register test/replay-safety.test.ts                 # run only replay-safety
```

The replay-safety suite exercises every interceptor path and asserts that replay re-emits **zero** records (sinks correctly skipped via `callDuringReplay: false`, activities and queries don't re-execute on replay):

| Test | Effects covered | Live invariants asserted |
| --- | --- | --- |
| `example` + `greet` | workflow inbound, activity | 2 workflow records (started+completed), 2 activity records |
| `signalEventExample` | `handleSignal`, `workflowEvent` | 2 signal records, 2 user_event records |
| `queryUpdateExample` | `handleQuery`, `handleUpdate` | 2 query records, 2 update records |
| `childSignalParent` | `startChildWorkflowExecution`, `signalWorkflow` outbound | child_workflow start+complete (after child finishes, not at start RPC), 2 outbound signal records |
| `continueAsNewExample` | `continueAsNew` outbound | single record with `status: 'started'` only |
| `failingExample` | retries + workflow failure | 3 activity started + 3 activity failed (with `attempt` 1/2/3 and `duration_ms`/`error`), 1 failed workflow record |
| `updateFailureExample` | update handler `ApplicationFailure` | 1 update started + 1 update failed (no completed), error propagated |
| `parentFailingExample` | child-workflow run-time failure | outbound child_workflow started + failed, parent workflow failed |

Each test fetches the workflow history, replays it via `Worker.runReplayHistory()` with a fresh plugin, and re-asserts that the record stream is empty for the effect under test.

This requires a running Temporal dev server (`temporal server start-dev`) - the suite connects to `localhost:7233` rather than spinning up an in-process test server.

---

## Caveats

- **OTel ecosystem version split.** We pin to OTel 1.x because Temporal's plugin does. When Temporal moves to 2.x, we follow.
- **Empty-body warning on OTLP success.** Parseable returns HTTP 200 with an empty body for accepted OTLP payloads. OTel's deserializer logs `Export succeeded but could not deserialize response - is the response specification compliant?` - this is benign and only visible at `DiagLogLevel.DEBUG` or above.
- **Span attribute sanitization.** The `SanitizingSpanExporter` is a workaround for an interop gap between Temporal's OTel plugin (emits non-primitive span attributes) and strict OTLP parsers (require primitive attribute values). Without it, Parseable returns `400 Invalid data for Value`.
- **Throw `ApplicationFailure` for graceful handler failures.** Signal/update handlers that throw a plain `Error` are treated by Temporal as a workflow-task failure: the task is retried until it succeeds, and the plugin will emit one `started`+`failed` record pair per retry. To fail an update (or any handler) cleanly without retry storms, throw `ApplicationFailure.create({ message, nonRetryable: true })` from `@temporalio/workflow`. The interceptor then records exactly one `failed` event and the error propagates to the client as an update failure rather than a task failure.
- **`child_workflow` completion is tracked from the child, not the start RPC.** The outbound interceptor wraps the result promise returned by `next(input)` so `status: 'completed'` (or `failed`) fires when the child actually finishes - not when the start call returns. Start-time RPC errors and run-time child failures are reported with distinct `failed` records.
