import {
  proxyActivities,
  executeChild,
  startChild,
  defineSignal,
  defineQuery,
  defineUpdate,
  setHandler,
  condition,
  continueAsNew,
  ApplicationFailure,
} from '@temporalio/workflow';
// Only import the activity types
import type * as activities from './activities';
import { workflowEvent } from '../src/workflow';

export const proceedSignal = defineSignal<[string]>('proceed');
export const incrementUpdate = defineUpdate<number, [number]>('increment');
export const counterQuery = defineQuery<number>('counter');
export const failingUpdate = defineUpdate<void, []>('failingUpdate');

const { greet } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
});

const { chargeCard } = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 seconds',
  retry: {
    maximumAttempts: 3,
    initialInterval: '500ms',
    backoffCoefficient: 1,
  },
});

/** A workflow that simply calls an activity */
export async function example(name: string): Promise<string> {
  return await greet(name);
}

/** A workflow whose activity always fails - exercises the retry/error path */
export async function failingExample(amount: number): Promise<string> {
  return await chargeCard(amount);
}

/** A workflow demonstrating the workflowEvent helper for user-defined events */
export async function userEventExample(name: string): Promise<string> {
  workflowEvent('greeting.requested', { name });
  const result = await greet(name);
  workflowEvent('greeting.delivered', { name, length: result.length });
  return result;
}

/** A workflow that waits for a signal and emits user events - exercises signal + workflowEvent paths */
export async function signalEventExample(): Promise<string> {
  let payload: string | undefined;
  setHandler(proceedSignal, (v) => {
    payload = v;
  });
  workflowEvent('waiting');
  await condition(() => payload !== undefined);
  workflowEvent('proceeded', { payload });
  return payload!;
}

/** A workflow that runs `example` as a child workflow - exercises the outbound interceptor */
export async function parentExample(name: string): Promise<string> {
  return await executeChild(example, {
    args: [name],
    workflowId: 'child-of-parent-' + Date.now(),
  });
}

/** A workflow that handles a query and an update - exercises handleQuery + handleUpdate */
export async function queryUpdateExample(): Promise<number> {
  let counter = 0;
  setHandler(counterQuery, () => counter);
  setHandler(incrementUpdate, (n: number) => {
    counter += n;
    return counter;
  });
  await condition(() => counter >= 1);
  return counter;
}

/** Parent starts a signal-waiting child then signals it - exercises startChildWorkflowExecution + signalWorkflow outbound */
export async function childSignalParent(payload: string): Promise<string> {
  const handle = await startChild(signalEventExample, {
    workflowId: 'signal-child-' + Date.now() + '-' + payload,
  });
  await handle.signal(proceedSignal, payload);
  return await handle.result();
}

/** A workflow whose update handler throws - exercises handleUpdate failure path */
export async function updateFailureExample(): Promise<void> {
  let done = false;
  setHandler(failingUpdate, () => {
    throw ApplicationFailure.create({ message: 'update handler boom', nonRetryable: true });
  });
  setHandler(proceedSignal, () => {
    done = true;
  });
  await condition(() => done);
}

/** Parent that runs `failingExample` as a child - exercises outbound child_workflow failure */
export async function parentFailingExample(amount: number): Promise<string> {
  return await executeChild(failingExample, {
    args: [amount],
    workflowId: 'child-failing-' + Date.now(),
    retry: { maximumAttempts: 1 },
  });
}

/** Workflow that continues-as-new until `remaining` reaches 0 - exercises continueAsNew outbound */
export async function continueAsNewExample(remaining: number): Promise<string> {
  workflowEvent('continueAsNewExample.iter', { remaining });
  if (remaining <= 0) return 'done';
  await continueAsNew<typeof continueAsNewExample>(remaining - 1);
  throw new Error('unreachable');
}
