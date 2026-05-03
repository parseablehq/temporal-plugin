# Parseable

[Parseable](https://www.parseable.com/) is an observability platform for logs, metrics, and traces. The `@parseable/temporal` plugin ships Temporal workflow and activity execution events to Parseable as both OpenTelemetry traces (a waterfall view of workflow runs) and structured log records (a queryable schema with `workflow_id`, `activity_name`, `attempt`, `duration_ms`, etc.).

Together, the two streams give Temporal users:

- A flame-graph trace of every workflow run, including child workflows and activity calls.
- A flat, queryable log schema for fleet-wide analytics — failure rates by activity name, latency by workflow type, retry hotspots, and custom domain events emitted via a `workflowEvent()` helper.

## Installation

```bash
npm install @parseable/temporal
```

## Configuration

```ts
import { Worker } from '@temporalio/worker';
import { ParseablePlugin } from '@parseable/temporal';

const worker = await Worker.create({
  // ...usual worker options...
  plugins: [
    new ParseablePlugin({
      serviceName: 'temporal-worker',
      endpoint: process.env.PARSEABLE_URL,
      auth: {
        username: process.env.PARSEABLE_USERNAME,
        password: process.env.PARSEABLE_PASSWORD,
      },
      // optional, defaults shown:
      logs: { stream: 'temporal-logs' },
      traces: { stream: 'temporal-traces' },
    }),
  ],
});
```

Logs are POSTed to `${endpoint}/v1/logs`; traces to `${endpoint}/v1/traces`. Both pipelines are independently configurable — pass `logs: false` or `traces: false` to disable either layer.

### Options

| Option        | Type                                       | Default                          | Notes |
|---------------|--------------------------------------------|----------------------------------|-------|
| `serviceName` | `string`                                   | required                         | Becomes `service.name` resource attribute and `service_name` log field. |
| `endpoint`    | `string`                                   | required if logs/traces enabled  | Parseable host base URL, e.g. `http://parseable.example:8010`. |
| `auth`        | `{ username, password }`                   | required if logs/traces enabled  | HTTP basic auth. |
| `logs`        | `false` &#124; `{ stream?: string }`       | `{ stream: 'temporal-logs' }`    | Set to `false` to disable log emission. |
| `traces`      | `false` &#124; `{ stream?: string }`       | `{ stream: 'temporal-traces' }`  | Set to `false` to disable trace emission. |

## What gets captured

### Workflow lifecycle

Every workflow run emits `started` and `completed` (or `failed`) records with `duration_ms`, `workflow_id`, `run_id`, `workflow_name`, and on failure an `error` message.

### Activity lifecycle

Every activity execution emits `started` and `completed` (or `failed`) records carrying `activity_name`, `activity_id`, `attempt`, `duration_ms`, and the parent workflow's identifiers. Retries produce a record per attempt.

### Inbound and outbound messages

In addition to workflow and activity records, the plugin emits **message records** (`type: 'signal' | 'query' | 'update' | 'child_workflow' | 'continue_as_new'`) for:

- Signals, queries, and updates received by the workflow.
- Outgoing signals to other workflows, child workflows started, and continue-as-new transitions.

The `direction` field distinguishes `inbound` (received by this workflow) from `outbound` (sent by this workflow).

| Field                | Type                                                                                                  |
|----------------------|-------------------------------------------------------------------------------------------------------|
| `type`               | `'signal'` &#124; `'query'` &#124; `'update'` &#124; `'child_workflow'` &#124; `'continue_as_new'`   |
| `direction`          | `'inbound'` &#124; `'outbound'`                                                                       |
| `status`             | `'started'` &#124; `'completed'` &#124; `'failed'`                                                    |
| `message_name`       | signal/query/update name, or child workflow type                                                      |
| `target_workflow_id` | for outbound signals/child workflows: the recipient workflow id                                       |
| `workflow_id` / `run_id` / `workflow_name` | the workflow emitting/handling the message                                       |
| `duration_ms`        | on completion/fail                                                                                    |
| `error`              | on fail                                                                                               |

### Custom user events from workflow code

Workflows can emit replay-safe domain events via the `workflowEvent` helper:

```ts
import { workflowEvent } from '@parseable/temporal/workflow';
import { proxyActivities } from '@temporalio/workflow';

export async function agentWorkflow(input: AgentInput): Promise<AgentResult> {
  workflowEvent('agent.started', { user_id: input.userId });

  const plan = await planActivity(input);
  workflowEvent('agent.plan.chosen', { steps: plan.steps.length });

  for (const step of plan.steps) {
    workflowEvent('agent.step.start', { tool: step.tool });
    await runStep(step);
  }

  return result;
}
```

Each call emits a record with `type: 'user_event'`, `event_name`, and an arbitrary serializable `event_data` payload. These records flow through the same sink as built-in workflow events. Useful for AI agents, multi-step orchestration, or any case where you want domain-specific signals alongside Temporal's built-in lifecycle events.

### Replay safety

All workflow-side emission is replay-safe. The plugin uses Temporal Sinks configured with `callDuringReplay: false`, so workflow records and user events are not duplicated when Temporal replays workflow history (during worker recovery, cache eviction, or manual replay). This is verified by an automated test using `Worker.runReplayHistory()`.

## Log schema

| Field            | Type                                                              | Notes |
|------------------|-------------------------------------------------------------------|-------|
| `type`           | `'activity'` &#124; `'workflow'` &#124; `'user_event'` &#124; `'signal'` &#124; `'query'` &#124; `'update'` &#124; `'child_workflow'` &#124; `'continue_as_new'` | discriminator |
| `status`         | `'started'` &#124; `'completed'` &#124; `'failed'`                | not present on `user_event` |
| `service_name`   | `string`                                                          | from plugin config |
| `timestamp`      | ISO 8601 string                                                   | event time |
| `workflow_id`    | `string`                                                          | |
| `run_id`         | `string`                                                          | |
| `workflow_name`  | `string`                                                          | |
| `activity_name`  | `string`                                                          | activity records only |
| `activity_id`    | `string`                                                          | activity records only |
| `attempt`        | `number`                                                          | activity records only |
| `duration_ms`    | `number`                                                          | on completion/fail |
| `error`          | `string`                                                          | on fail |
| `direction`      | `'inbound'` &#124; `'outbound'`                                   | message records only |
| `message_name`   | `string`                                                          | message records only |
| `target_workflow_id` | `string`                                                      | outbound message records |
| `event_name`     | `string`                                                          | user events only |
| `event_data`     | object                                                            | user events only |

All logs and traces carry a `parseable.plugin.version` resource attribute so consumers can correlate behaviour with plugin releases.

Trace spans are emitted by Temporal's `OpenTelemetryPlugin` — see the [Temporal TypeScript observability docs](https://docs.temporal.io/develop/typescript/observability) for the span schema.

## Links

- Plugin source: *[fill in once pushed]*
- Parseable product docs: <https://www.parseable.com/docs>
