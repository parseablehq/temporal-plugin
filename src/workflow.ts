import { proxySinks, workflowInfo } from '@temporalio/workflow';
import type { ParseableSinks } from './workflow-interceptor';

const { parseable } = proxySinks<ParseableSinks>();

/**
 * Emit a custom user event from inside a Workflow. Imported as a subpath:
 * `import { workflowEvent } from '@parseable/temporal/workflow'`.
 *
 * Use this to capture domain-significant moments from workflow code that
 * aren't tied to a Temporal lifecycle event - for example "agent decided to
 * call tool X", "user approval received", "fraud check passed". Emitted as
 * a {@link ParseableUserEventRecord} with the supplied `name` and optional
 * `data` payload.
 *
 * Replay-safe: routes through a Temporal sink declared with
 * `callDuringReplay: false`, so re-emitted workflow history never produces
 * duplicate user events.
 *
 * @param name - Free-form event name. Indexed in Parseable as `event_name`.
 * @param data - Optional JSON-serializable payload. Indexed as `event_data`.
 *
 * @example
 * ```ts
 * import { workflowEvent } from '@parseable/temporal/workflow';
 *
 * export async function onboardCustomer(input: Input) {
 *   workflowEvent('onboarding.started', { customerId: input.id });
 *   await runKyc(input);
 *   workflowEvent('onboarding.kyc.passed', { customerId: input.id });
 * }
 * ```
 */
export function workflowEvent(name: string, data?: Record<string, unknown>): void {
  const info = workflowInfo();
  parseable.emitUserEvent({
    type: 'user_event',
    timestamp: new Date().toISOString(),
    workflow_name: info.workflowType,
    workflow_id: info.workflowId,
    run_id: info.runId,
    event_name: name,
    event_data: data,
  });
}
