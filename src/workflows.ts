import { proxyActivities, executeChild } from '@temporalio/workflow';
// Only import the activity types
import type * as activities from './activities';
import { workflowEvent } from './plugin/workflow';

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

/** A workflow whose activity always fails — exercises the retry/error path */
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

/** A workflow that runs `example` as a child workflow — exercises the outbound interceptor */
export async function parentExample(name: string): Promise<string> {
  return await executeChild(example, {
    args: [name],
    workflowId: 'child-of-parent-' + Date.now(),
  });
}
