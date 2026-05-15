/**
 * Lifecycle event emitted by the activity interceptor for each activity
 * invocation: one `started` record, then either one `completed` or one
 * `failed` record per attempt. Activity retries produce multiple
 * `started`/`failed` pairs with monotonically increasing `attempt`.
 */
export interface ParseableActivityRecord {
  /** ISO-8601 timestamp captured at emit time. */
  timestamp: string;
  type: 'activity';
  /** Lifecycle phase. `started` fires before the handler runs; `completed`/`failed` fires after. */
  status: 'started' | 'completed' | 'failed';
  /** Registered activity name (e.g. `chargeCard`). */
  activity_name: string;
  /** Per-execution activity identifier assigned by Temporal. Stable across retries within one activity execution. */
  activity_id: string;
  /** Retry attempt number, 1-indexed. Incremented on each retry per the activity's retry policy. */
  attempt: number;
  /** Workflow that scheduled this activity. Absent if the activity is run outside a workflow context. */
  workflow_id?: string;
  /** Workflow run id (changes across `continueAsNew`). */
  run_id?: string;
  /** Registered workflow name. */
  workflow_name?: string;
  /** Wall-clock duration in milliseconds for `completed`/`failed` records only. */
  duration_ms?: number;
  /** Error message for `failed` records only. */
  error?: string;
  /** Value of {@link ParseablePluginOptions.serviceName}. */
  service_name: string;
}

/**
 * Lifecycle event emitted by the workflow inbound interceptor for each
 * workflow execution: one `started` and then one `completed` or `failed`.
 * `continueAsNew` produces a separate {@link ParseableContinueAsNewRecord} on
 * the outbound side and starts a new workflow run with its own
 * started/completed pair.
 */
export interface ParseableWorkflowRecord {
  /** ISO-8601 timestamp captured at emit time. */
  timestamp: string;
  type: 'workflow';
  status: 'started' | 'completed' | 'failed';
  workflow_name: string;
  workflow_id: string;
  /** Workflow run id (changes across `continueAsNew`). */
  run_id: string;
  /** Wall-clock duration in milliseconds for `completed`/`failed` records only. */
  duration_ms?: number;
  /** Error message for `failed` records only. */
  error?: string;
  /** Value of {@link ParseablePluginOptions.serviceName}. */
  service_name: string;
}

/**
 * Custom event emitted from workflow code via `workflowEvent(name, data?)`.
 * Use this to record domain-significant moments that aren't tied to a
 * Temporal lifecycle event - e.g. "agent picked tool X", "user approval
 * received". Like all workflow-side records, it routes through a sink with
 * `callDuringReplay: false`, so re-emitted history never duplicates these.
 */
export interface ParseableUserEventRecord {
  timestamp: string;
  type: 'user_event';
  workflow_name: string;
  workflow_id: string;
  /** Workflow run id (changes across `continueAsNew`). */
  run_id: string;
  /** First argument passed to `workflowEvent()`. */
  event_name: string;
  /** Second argument passed to `workflowEvent()`. Free-form JSON payload. */
  event_data?: Record<string, unknown>;
  /** Value of {@link ParseablePluginOptions.serviceName}. */
  service_name: string;
}

/**
 * Fields shared by every workflow-side message record (signals, queries,
 * updates, child-workflow lifecycle, continue-as-new). Not exported on its
 * own - consumers should reference the concrete record types or the
 * {@link ParseableMessageRecord} union.
 */
interface ParseableMessageRecordBase {
  /** ISO-8601 timestamp captured at emit time. */
  timestamp: string;
  /**
   * `inbound` - this workflow is handling something (signal/query/update arriving).
   * `outbound` - this workflow initiated something (sent a signal, started a child, continued-as-new).
   */
  direction: 'inbound' | 'outbound';
  /**
   * Name of the signal/query/update method, or for `child_workflow` and
   * `continue_as_new`, the workflow type being launched.
   */
  message_name?: string;
  /**
   * For `outbound` records that target another workflow (sending a signal,
   * starting a child): the workflow id of the target.
   */
  target_workflow_id?: string;
  /** The current workflow (the one emitting the record). */
  workflow_id: string;
  /** Workflow run id (changes across `continueAsNew`). */
  run_id: string;
  workflow_name: string;
  /** Value of {@link ParseablePluginOptions.serviceName}. */
  service_name: string;
}

/**
 * Shared shape for message records that follow a started/completed/failed
 * lifecycle. Not exported; concrete record types extend this.
 */
interface ParseableLifecycleMessageRecordBase extends ParseableMessageRecordBase {
  status: 'started' | 'completed' | 'failed';
  /** Wall-clock duration in milliseconds for `completed`/`failed` records only. */
  duration_ms?: number;
  /** Error message for `failed` records only. */
  error?: string;
}

/**
 * Signal lifecycle. Emitted twice per signal in normal flow: `started` then
 * `completed`/`failed`.
 *
 * Inbound (`direction: 'inbound'`) is emitted when this workflow's signal
 * handler runs. Outbound (`direction: 'outbound'`) is emitted when this
 * workflow sends a signal to an external or child workflow.
 */
export interface ParseableSignalRecord extends ParseableLifecycleMessageRecordBase {
  type: 'signal';
}

/**
 * Query lifecycle. Emitted twice per query in normal flow: `started` then
 * `completed`/`failed`. Queries are point-in-time, so they appear only on
 * the live worker - replayed history does not re-execute queries and
 * produces zero query records.
 */
export interface ParseableQueryRecord extends ParseableLifecycleMessageRecordBase {
  type: 'query';
}

/**
 * Update lifecycle. Emitted twice per update in normal flow: `started` then
 * `completed`/`failed`. To fail an update cleanly (one `failed` record, no
 * workflow-task retries), the handler should throw
 * `ApplicationFailure.create({ message, nonRetryable: true })` from
 * `@temporalio/workflow`. Throwing a plain `Error` retries the workflow
 * task and produces duplicate started/failed pairs.
 */
export interface ParseableUpdateRecord extends ParseableLifecycleMessageRecordBase {
  type: 'update';
}

/**
 * Outbound child-workflow lifecycle. `started` is emitted when this workflow
 * calls `startChild`/`executeChild`. `completed`/`failed` is emitted when
 * the child workflow actually finishes - the interceptor wraps the child's
 * result promise so this fires on real completion, not at start-RPC return.
 */
export interface ParseableChildWorkflowRecord extends ParseableLifecycleMessageRecordBase {
  type: 'child_workflow';
}

/**
 * Outbound `continueAsNew` event. Has only `status: 'started'` because
 * `continueAsNew()` throws a control-flow signal that never returns to the
 * caller - there is no observable `completed` or `failed`. The newly
 * spawned workflow run emits its own {@link ParseableWorkflowRecord} pair.
 */
export interface ParseableContinueAsNewRecord extends ParseableMessageRecordBase {
  type: 'continue_as_new';
  /** Always `'started'` - see type-level note above. */
  status: 'started';
}

/**
 * Discriminated union of every workflow-side message record. Narrow with
 * `r.type === 'signal' | 'query' | 'update' | 'child_workflow' | 'continue_as_new'`.
 */
export type ParseableMessageRecord =
  | ParseableSignalRecord
  | ParseableQueryRecord
  | ParseableUpdateRecord
  | ParseableChildWorkflowRecord
  | ParseableContinueAsNewRecord;

/**
 * Discriminated union of every record the plugin can emit. The shape passed
 * to a custom {@link Emit} function. Narrow with `r.type`.
 */
export type ParseableEventRecord =
  | ParseableActivityRecord
  | ParseableWorkflowRecord
  | ParseableUserEventRecord
  | ParseableMessageRecord;

/**
 * Signature of the function the plugin calls for every emitted record.
 * Supply your own via {@link ParseablePluginOptions.emit} to bypass the
 * built-in OTel log pipeline and route records elsewhere (e.g. into a Pino
 * logger or an in-memory buffer for tests).
 */
export type Emit = (record: ParseableEventRecord) => void;

/** Omit applied across each member of a union (the built-in `Omit` is not distributive). */
type DistributiveOmit<T, K extends keyof any> = T extends unknown ? Omit<T, K> : never;

/**
 * Payload type for the `parseable.emitWorkflow` sink. Identical to
 * {@link ParseableWorkflowRecord} except `service_name` is added by the
 * worker-side sink consumer (workflow code in the V8 isolate cannot see the
 * plugin's options).
 *
 * @internal Surface used internally to type the sink boundary; consumers
 * almost never need this - interact with {@link ParseableEventRecord} from
 * the `emit` callback or Parseable side.
 */
export type ParseableWorkflowSinkPayload = Omit<ParseableWorkflowRecord, 'service_name'>;

/** @internal Sink-boundary type. See {@link ParseableWorkflowSinkPayload}. */
export type ParseableUserEventSinkPayload = Omit<ParseableUserEventRecord, 'service_name'>;

/** @internal Sink-boundary type. See {@link ParseableWorkflowSinkPayload}. */
export type ParseableMessageSinkPayload = DistributiveOmit<ParseableMessageRecord, 'service_name'>;
