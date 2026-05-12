import { TestWorkflowEnvironment } from '@temporalio/testing';
import { before, describe, it } from 'mocha';
import { Worker } from '@temporalio/worker';
import { example } from '../examples/workflows';
import * as activities from '../examples/activities';
import assert from 'assert';

describe('Example workflow', () => {
  let testEnv: TestWorkflowEnvironment;

  before(async () => {
    testEnv = await TestWorkflowEnvironment.createLocal();
  });

  after(async () => {
    await testEnv?.teardown();
  });

  it('successfully completes the Workflow', async () => {
    const { client, nativeConnection } = testEnv;
    const taskQueue = 'test';

    const worker = await Worker.create({
      connection: nativeConnection,
      taskQueue,
      workflowsPath: require.resolve('../examples/workflows'),
      activities,
    });

    const result = await worker.runUntil(
      client.workflow.execute(example, {
        args: ['Temporal'],
        workflowId: 'test',
        taskQueue,
      }),
    );
    assert.equal(result, 'Hello, Temporal!');
  });
});
