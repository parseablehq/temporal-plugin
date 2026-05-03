# @parseable/temporal

Temporal middleware plugin that ships workflow and activity execution events to [Parseable](https://www.parseable.com/) as OpenTelemetry logs and traces.

> **End-user integration guide:** [INTEGRATION.md](./INTEGRATION.md) — install, configure, schema reference, query examples.
>
> **Submission status:** [STATUS.md](./STATUS.md) — what's done and what's pending for the Temporal AI Partner Program submission.

The plugin emits structured logs (workflow/activity start, complete, fail, retry, duration) into a Parseable log stream, alongside OpenTelemetry traces (`RunWorkflow:*`, `StartActivity:*`, `RunActivity:*`) into a Parseable trace stream. Users get a flat queryable schema for analytics plus a waterfall view of workflow execution.

This README is the developer-facing landing page for the repo (architecture, repo layout, how to run the demo, caveats). For end-user consumption see `INTEGRATION.md`.

---

## Repository layout

This repo contains both the plugin source and a runnable demo so the integration can be exercised end-to-end. When the plugin is published to npm, `src/plugin/**` will be extracted to a standalone package.

```
src/
├── plugin/                       # the integration — this is what becomes @parseable/temporal
│   ├── index.ts                  # ParseablePlugin class (extends SimplePlugin)
│   ├── activity-interceptor.ts   # ActivityInbound interceptor (worker process)
│   ├── workflow-interceptor.ts   # WorkflowInbound + Outbound interceptors (workflow isolate, replay-safe via sinks)
│   ├── workflow.ts               # public workflowEvent() helper
│   ├── exporters.ts              # OTLP HTTP exporters (logs + traces) + SanitizingSpanExporter
│   ├── version.ts                # PLUGIN_VERSION constant
│   └── types.ts                  # ParseableEventRecord schema
│
├── activities.ts                 # demo: greet (success), chargeCard (always fails)
├── workflows.ts                  # demo: example, failingExample, userEventExample, parentExample
├── worker.ts                     # demo worker — wires up ParseablePlugin
├── client.ts                     # demo: triggers happy-path workflow
├── fail-client.ts                # demo: triggers failing workflow
├── event-client.ts               # demo: triggers user-event workflow
├── parent-client.ts              # demo: triggers parent → child workflow
└── mocha/
    └── replay-safety.test.ts     # asserts workflow sink fires zero times during history replay
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

- **Replay safety.** Workflow events are emitted via `proxySinks` with `callDuringReplay: false`. When Temporal replays a workflow's history (after a worker crash, cache eviction, or manual replay), the sink is skipped — no duplicate logs or spans. Verified by `src/mocha/replay-safety.test.ts`.
- **Two layers, one plugin.** `ParseablePlugin` extends `@temporalio/plugin`'s `SimplePlugin` and internally composes Temporal's official `OpenTelemetryPlugin` for trace emission. Logs are emitted from our own interceptors directly to OTel's log API. Both flow into Parseable through OTLP/HTTP.
- **`SanitizingSpanExporter`.** Temporal's OTel plugin emits spans with nested objects, `Date` instances, and `undefined` fields as attributes. OTLP attribute values are restricted to primitives or arrays of primitives, so Parseable's strict OTLP parser rejects the raw payload with `400 Invalid data for Value`. The sanitizer wraps the trace exporter and flattens nested objects to JSON strings, `Date` to ISO, and drops `undefined`s before serialization.
- **OTel pinned to 1.x.** Temporal's `OpenTelemetryPlugin` pins `@opentelemetry/sdk-trace-base@^1.25.1`. The OTel ecosystem has split between 1.x (mature) and 2.x (newer). We ride the 1.x line — `sdk-trace-base@1.30.x`, `resources@1.30.x`, `exporter-{logs,trace}-otlp-http@0.57.x`, `sdk-logs@0.57.x` — until Temporal moves.

---

## Running the demo locally

### Prerequisites

- Node.js 20+
- [Temporal CLI](https://github.com/temporalio/cli) (`brew install temporal` on macOS)
- A Parseable instance reachable on the network. For dev: a local instance with default credentials.

### Three terminals

**Terminal 1 — Temporal dev server:**

```bash
temporal server start-dev
```

Runs on `localhost:7233` (gRPC) and `http://localhost:8233` (UI).

**Terminal 2 — Worker:**

```bash
npm install
npm run start.watch
```

Worker connects to Temporal and starts polling the `hello-world` task queue. Auto-restarts on `src/` changes via nodemon.

By default the worker ships logs and traces to `http://anton:8010` with credentials `admin:admin`. Override via env:

```bash
PARSEABLE_URL=https://your-parseable-host \
PARSEABLE_USERNAME=youruser \
PARSEABLE_PASSWORD=yourpass \
npm run start.watch
```

**Terminal 3 — Client (run on demand):**

```bash
npm run workflow         # success path: greet activity
npm run workflow:fail    # failure path: chargeCard with 3-retry policy
npm run workflow:event   # user-event path: workflow emits custom domain events via workflowEvent()
npm run workflow:parent  # parent → child workflow path: exercises the outbound interceptor
```

After running, check Parseable at `${PARSEABLE_URL}`:
- Stream `temporal-logs` — workflow/activity records with attributes `workflow_id`, `activity_name`, `attempt`, `status`, `duration_ms`, `service_name`, etc.
- Stream `temporal-traces` — spans `RunWorkflow:example`, `StartActivity:greet`, `RunActivity:greet`.

---

## Tests

```bash
npm test                                     # runs all mocha tests
npx mocha src/mocha/replay-safety.test.ts    # run only replay-safety
```

The replay-safety test:
1. Runs a real workflow with the plugin, captures emitted records.
2. Fetches the workflow's history from the Temporal server.
3. Replays the history via `Worker.runReplayHistory()` with a fresh plugin instance.
4. Asserts the replay emits **zero** workflow records (sink correctly skipped) and **zero** activity records (activities don't re-execute on replay).

This requires a running Temporal dev server (`temporal server start-dev`) — the test connects to `localhost:7233` rather than spinning up an in-process test server.

---

## Caveats

- **OTel ecosystem version split.** We pin to OTel 1.x because Temporal's plugin does. When Temporal moves to 2.x, we follow.
- **Empty-body warning on OTLP success.** Parseable returns HTTP 200 with an empty body for accepted OTLP payloads. OTel's deserializer logs `Export succeeded but could not deserialize response - is the response specification compliant?` — this is benign and only visible at `DiagLogLevel.DEBUG` or above.
- **Span attribute sanitization.** The `SanitizingSpanExporter` is a workaround for an interop gap between Temporal's OTel plugin (emits non-primitive span attributes) and strict OTLP parsers (require primitive attribute values). Without it, Parseable returns `400 Invalid data for Value`.
