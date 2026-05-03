import { Client, Connection } from '@temporalio/client';
import { NativeConnection, Worker } from '@temporalio/worker';
import { describe, it } from 'mocha';
import assert from 'assert';
import { example } from '../workflows';
import * as activities from '../activities';
import { ParseablePlugin, ParseableEventRecord } from '../plugin';

describe('ParseablePlugin replay safety', () => {
  it('emits zero events during workflow history replay', async function () {
    this.timeout(60_000);

    const taskQueue = 'replay-safety-test-' + Date.now();
    const workflowId = 'replay-safety-' + Date.now();

    const liveRecords: ParseableEventRecord[] = [];
    const livePlugin = new ParseablePlugin({
      serviceName: 'replay-test',
      emit: (r) => liveRecords.push(r),
      traces: false,
    });

    const nativeConnection = await NativeConnection.connect({ address: 'localhost:7233' });
    const connection = await Connection.connect({ address: 'localhost:7233' });
    const client = new Client({ connection });

    try {
      const worker = await Worker.create({
        connection: nativeConnection,
        namespace: 'default',
        taskQueue,
        workflowsPath: require.resolve('../workflows'),
        activities,
        plugins: [livePlugin],
      });

      const handle = await client.workflow.start(example, {
        taskQueue,
        args: ['Replay'],
        workflowId,
      });
      await worker.runUntil(handle.result());

      const workflowEvents = liveRecords.filter((r) => r.type === 'workflow');
      const activityEvents = liveRecords.filter((r) => r.type === 'activity');
      assert.equal(workflowEvents.length, 2, 'live run should emit 2 workflow records');
      assert.equal(activityEvents.length, 2, 'live run should emit 2 activity records');

      const history = await handle.fetchHistory();

      const replayRecords: ParseableEventRecord[] = [];
      const replayPlugin = new ParseablePlugin({
        serviceName: 'replay-test',
        emit: (r) => replayRecords.push(r),
        traces: false,
      });

      await Worker.runReplayHistory(
        {
          workflowsPath: require.resolve('../workflows'),
          plugins: [replayPlugin],
        },
        history,
      );

      const replayWorkflowEvents = replayRecords.filter((r) => r.type === 'workflow');
      const replayActivityEvents = replayRecords.filter((r) => r.type === 'activity');
      assert.equal(
        replayWorkflowEvents.length,
        0,
        `replay emitted ${replayWorkflowEvents.length} workflow records; expected 0 (callDuringReplay must skip them)`,
      );
      assert.equal(
        replayActivityEvents.length,
        0,
        `replay emitted ${replayActivityEvents.length} activity records; expected 0 (activities should not re-execute during replay)`,
      );
    } finally {
      await connection.close();
      await nativeConnection.close();
    }
  });
});
