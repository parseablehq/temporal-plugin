import { proxySinks, workflowInfo } from '@temporalio/workflow';
import type { ParseableSinks } from './workflow-interceptor';

const { parseable } = proxySinks<ParseableSinks>();

/**
 * Emit a custom user event from inside a Workflow.
 *
 * Replay-safe: routes through a sink with `callDuringReplay: false`, so events
 * are not duplicated when Temporal replays workflow history.
 *
 * Use this to capture domain-specific signals from workflow code that aren't
 * activity calls — e.g. "agent decided to call tool X", "user approval received".
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
