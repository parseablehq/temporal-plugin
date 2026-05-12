import { Client, Connection } from '@temporalio/client';
import { NativeConnection, Worker } from '@temporalio/worker';
import { describe, it } from 'mocha';
import assert from 'assert';
import {
  example,
  signalEventExample,
  proceedSignal,
  queryUpdateExample,
  incrementUpdate,
  counterQuery,
  childSignalParent,
  continueAsNewExample,
  failingExample,
  updateFailureExample,
  failingUpdate,
  parentFailingExample,
} from '../examples/workflows';
import * as activities from '../examples/activities';
import { ParseablePlugin, ParseableEventRecord } from '../src';

const TEMPORAL_ADDR = 'localhost:7233';

async function withWorker<T>(
  taskQueue: string,
  liveRecords: ParseableEventRecord[],
  body: (client: Client, worker: Worker) => Promise<T>,
): Promise<T> {
  const livePlugin = new ParseablePlugin({
    serviceName: 'replay-test',
    emit: (r) => liveRecords.push(r),
    traces: false,
  });
  const nativeConnection = await NativeConnection.connect({ address: TEMPORAL_ADDR });
  const connection = await Connection.connect({ address: TEMPORAL_ADDR });
  const client = new Client({ connection });
  try {
    const worker = await Worker.create({
      connection: nativeConnection,
      namespace: 'default',
      taskQueue,
      workflowsPath: require.resolve('../examples/workflows'),
      activities,
      plugins: [livePlugin],
    });
    return await body(client, worker);
  } finally {
    await connection.close();
    await nativeConnection.close();
  }
}

async function replay(history: unknown): Promise<ParseableEventRecord[]> {
  const replayRecords: ParseableEventRecord[] = [];
  const replayPlugin = new ParseablePlugin({
    serviceName: 'replay-test',
    emit: (r) => replayRecords.push(r),
    traces: false,
  });
  try {
    await Worker.runReplayHistory(
      {
        workflowsPath: require.resolve('../examples/workflows'),
        plugins: [replayPlugin],
      },
      history as any,
    );
  } catch {
    // Replay throws when workflow originally failed; we still want to inspect emitted records.
  }
  return replayRecords;
}

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
        workflowsPath: require.resolve('../examples/workflows'),
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
          workflowsPath: require.resolve('../examples/workflows'),
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

  it('emits zero signal and user_event records during replay', async function () {
    this.timeout(60_000);

    const taskQueue = 'replay-safety-signals-' + Date.now();
    const workflowId = 'replay-safety-signals-' + Date.now();

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
        workflowsPath: require.resolve('../examples/workflows'),
        activities,
        plugins: [livePlugin],
      });

      const handle = await client.workflow.start(signalEventExample, {
        taskQueue,
        workflowId,
      });

      await worker.runUntil(
        (async () => {
          await handle.signal(proceedSignal, 'go');
          await handle.result();
        })(),
      );

      const signalEvents = liveRecords.filter((r) => r.type === 'signal');
      const userEvents = liveRecords.filter((r) => r.type === 'user_event');
      assert.equal(
        signalEvents.length,
        2,
        `live run should emit 2 signal records (started+completed); got ${signalEvents.length}`,
      );
      assert.equal(
        userEvents.length,
        2,
        `live run should emit 2 user_event records; got ${userEvents.length}`,
      );

      const history = await handle.fetchHistory();

      const replayRecords: ParseableEventRecord[] = [];
      const replayPlugin = new ParseablePlugin({
        serviceName: 'replay-test',
        emit: (r) => replayRecords.push(r),
        traces: false,
      });

      await Worker.runReplayHistory(
        {
          workflowsPath: require.resolve('../examples/workflows'),
          plugins: [replayPlugin],
        },
        history,
      );

      const replaySignals = replayRecords.filter((r) => r.type === 'signal');
      const replayUserEvents = replayRecords.filter((r) => r.type === 'user_event');
      assert.equal(
        replaySignals.length,
        0,
        `replay emitted ${replaySignals.length} signal records; expected 0 (callDuringReplay must skip them)`,
      );
      assert.equal(
        replayUserEvents.length,
        0,
        `replay emitted ${replayUserEvents.length} user_event records; expected 0 (workflowEvent must respect callDuringReplay)`,
      );
    } finally {
      await connection.close();
      await nativeConnection.close();
    }
  });

  it('emits zero update and query records during replay', async function () {
    this.timeout(60_000);
    const taskQueue = 'replay-safety-update-' + Date.now();
    const workflowId = 'replay-safety-update-' + Date.now();
    const liveRecords: ParseableEventRecord[] = [];

    const history = await withWorker(taskQueue, liveRecords, async (client, worker) => {
      const handle = await client.workflow.start(queryUpdateExample, { taskQueue, workflowId });
      await worker.runUntil(
        (async () => {
          const pre = await handle.query(counterQuery);
          assert.equal(pre, 0);
          const updated = await handle.executeUpdate(incrementUpdate, { args: [1] });
          assert.equal(updated, 1);
          await handle.result();
        })(),
      );
      return handle.fetchHistory();
    });

    const liveUpdates = liveRecords.filter((r) => r.type === 'update');
    const liveQueries = liveRecords.filter((r) => r.type === 'query');
    assert.equal(liveUpdates.length, 2, `live should emit 2 update records; got ${liveUpdates.length}`);
    assert.equal(liveQueries.length, 2, `live should emit 2 query records; got ${liveQueries.length}`);

    const replayRecords = await replay(history);
    const replayUpdates = replayRecords.filter((r) => r.type === 'update');
    const replayQueries = replayRecords.filter((r) => r.type === 'query');
    assert.equal(
      replayUpdates.length,
      0,
      `replay emitted ${replayUpdates.length} update records; expected 0 (callDuringReplay must skip them)`,
    );
    assert.equal(
      replayQueries.length,
      0,
      `replay emitted ${replayQueries.length} query records; expected 0 (queries are point-in-time, not replayed)`,
    );
  });

  it('emits zero child_workflow and outbound signal records during replay', async function () {
    this.timeout(60_000);
    const taskQueue = 'replay-safety-child-' + Date.now();
    const workflowId = 'replay-safety-child-' + Date.now();
    const liveRecords: ParseableEventRecord[] = [];

    const history = await withWorker(taskQueue, liveRecords, async (client, worker) => {
      const handle = await client.workflow.start(childSignalParent, {
        taskQueue,
        workflowId,
        args: ['go'],
      });
      await worker.runUntil(handle.result());
      return handle.fetchHistory();
    });

    const liveChild = liveRecords.filter((r) => r.type === 'child_workflow');
    const liveOutboundSignal = liveRecords.filter(
      (r) => r.type === 'signal' && r.direction === 'outbound',
    );
    assert.ok(
      liveChild.length >= 2,
      `live should emit child_workflow start+complete; got ${liveChild.length}`,
    );
    assert.equal(
      liveOutboundSignal.length,
      2,
      `live should emit 2 outbound signal records; got ${liveOutboundSignal.length}`,
    );

    const replayRecords = await replay(history);
    const replayChild = replayRecords.filter((r) => r.type === 'child_workflow');
    const replayOutboundSignal = replayRecords.filter(
      (r) => r.type === 'signal' && r.direction === 'outbound',
    );
    assert.equal(
      replayChild.length,
      0,
      `replay emitted ${replayChild.length} child_workflow records; expected 0`,
    );
    assert.equal(
      replayOutboundSignal.length,
      0,
      `replay emitted ${replayOutboundSignal.length} outbound signal records; expected 0`,
    );
  });

  it('emits zero continue_as_new records during replay', async function () {
    this.timeout(60_000);
    const taskQueue = 'replay-safety-can-' + Date.now();
    const workflowId = 'replay-safety-can-' + Date.now();
    const liveRecords: ParseableEventRecord[] = [];

    const history = await withWorker(taskQueue, liveRecords, async (client, worker) => {
      const handle = await client.workflow.start(continueAsNewExample, {
        taskQueue,
        workflowId,
        args: [1],
      });
      await worker.runUntil(handle.result());
      return handle.fetchHistory();
    });

    const liveCAN = liveRecords.filter((r) => r.type === 'continue_as_new');
    assert.equal(
      liveCAN.length,
      1,
      `live should emit 1 continue_as_new record (only 'started'); got ${liveCAN.length}`,
    );
    assert.equal(liveCAN[0].status, 'started');

    const replayRecords = await replay(history);
    const replayCAN = replayRecords.filter((r) => r.type === 'continue_as_new');
    assert.equal(
      replayCAN.length,
      0,
      `replay emitted ${replayCAN.length} continue_as_new records; expected 0`,
    );
  });

  it('records every attempt and a failed workflow status on activity retries', async function () {
    this.timeout(60_000);
    const taskQueue = 'replay-safety-fail-' + Date.now();
    const workflowId = 'replay-safety-fail-' + Date.now();
    const liveRecords: ParseableEventRecord[] = [];

    const history = await withWorker(taskQueue, liveRecords, async (client, worker) => {
      const handle = await client.workflow.start(failingExample, {
        taskQueue,
        workflowId,
        args: [100],
      });
      await worker.runUntil(
        handle.result().then(
          () => assert.fail('failingExample should have thrown'),
          () => undefined,
        ),
      );
      return handle.fetchHistory();
    });

    type ActivityRec = Extract<ParseableEventRecord, { type: 'activity' }>;
    type WorkflowRec = Extract<ParseableEventRecord, { type: 'workflow' }>;
    const activities = liveRecords.filter(
      (r): r is ActivityRec => r.type === 'activity',
    );
    const startedAttempts = activities
      .filter((r) => r.status === 'started')
      .map((r) => r.attempt)
      .sort((a, b) => a - b);
    const failedAttempts = activities
      .filter((r) => r.status === 'failed')
      .map((r) => r.attempt)
      .sort((a, b) => a - b);
    assert.deepEqual(
      startedAttempts,
      [1, 2, 3],
      `expected activity start records for attempts 1,2,3; got ${startedAttempts}`,
    );
    assert.deepEqual(
      failedAttempts,
      [1, 2, 3],
      `expected activity failed records for attempts 1,2,3; got ${failedAttempts}`,
    );
    for (const r of activities.filter((r) => r.status === 'failed')) {
      assert.equal(r.activity_name, 'chargeCard');
      assert.equal(r.workflow_id, workflowId);
      assert.equal(r.workflow_name, 'failingExample');
      assert.equal(r.service_name, 'replay-test');
      assert.ok(typeof r.duration_ms === 'number', 'failed activity record missing duration_ms');
      assert.ok(
        r.error?.includes('payment provider unreachable'),
        `failed activity record should carry chargeCard error; got: ${r.error}`,
      );
    }

    const failedWorkflows = liveRecords.filter(
      (r): r is WorkflowRec => r.type === 'workflow' && r.status === 'failed',
    );
    assert.equal(
      failedWorkflows.length,
      1,
      `live should emit 1 failed workflow record; got ${failedWorkflows.length}`,
    );
    const wfFail = failedWorkflows[0];
    assert.ok(wfFail.error, 'failed workflow record should carry error message');
    assert.equal(wfFail.workflow_id, workflowId);
    assert.equal(wfFail.workflow_name, 'failingExample');
    assert.ok(typeof wfFail.duration_ms === 'number', 'failed workflow record missing duration_ms');

    const replayRecords = await replay(history);
    assert.equal(
      replayRecords.filter((r) => r.type === 'workflow').length,
      0,
      `replay emitted workflow records; expected 0`,
    );
    assert.equal(
      replayRecords.filter((r) => r.type === 'activity').length,
      0,
      `replay emitted activity records; expected 0`,
    );
  });

  it('emits failed message record when an update handler throws', async function () {
    this.timeout(60_000);
    const taskQueue = 'replay-safety-update-fail-' + Date.now();
    const workflowId = 'replay-safety-update-fail-' + Date.now();
    const liveRecords: ParseableEventRecord[] = [];

    const history = await withWorker(taskQueue, liveRecords, async (client, worker) => {
      const handle = await client.workflow.start(updateFailureExample, {
        taskQueue,
        workflowId,
      });
      await worker.runUntil(
        (async () => {
          await assert.rejects(() => handle.executeUpdate(failingUpdate));
          await handle.signal(proceedSignal, 'done');
          await handle.result();
        })(),
      );
      return handle.fetchHistory();
    });

    type MessageRec = Extract<ParseableEventRecord, { type: 'update' }>;
    const updates = liveRecords.filter((r): r is MessageRec => r.type === 'update');
    const started = updates.filter((r) => r.status === 'started');
    const failed = updates.filter((r) => r.status === 'failed');
    const completed = updates.filter((r) => r.status === 'completed');
    assert.equal(started.length, 1, `expected 1 update started record; got ${started.length}`);
    assert.equal(failed.length, 1, `expected 1 update failed record; got ${failed.length}`);
    assert.equal(
      completed.length,
      0,
      `expected 0 update completed records on handler throw; got ${completed.length}`,
    );
    assert.equal(failed[0].message_name, 'failingUpdate');
    assert.equal(failed[0].direction, 'inbound');
    assert.ok(
      failed[0].error?.includes('update handler boom'),
      `failed update record should carry handler error; got: ${failed[0].error}`,
    );
    assert.ok(typeof failed[0].duration_ms === 'number');

    const replayRecords = await replay(history);
    assert.equal(
      replayRecords.filter((r) => r.type === 'update').length,
      0,
      `replay emitted update records; expected 0`,
    );
  });

  it('emits failed child_workflow outbound record when a child workflow fails', async function () {
    this.timeout(60_000);
    const taskQueue = 'replay-safety-child-fail-' + Date.now();
    const workflowId = 'replay-safety-child-fail-' + Date.now();
    const liveRecords: ParseableEventRecord[] = [];

    const history = await withWorker(taskQueue, liveRecords, async (client, worker) => {
      const handle = await client.workflow.start(parentFailingExample, {
        taskQueue,
        workflowId,
        args: [100],
      });
      await worker.runUntil(
        handle.result().then(
          () => assert.fail('parentFailingExample should have thrown'),
          () => undefined,
        ),
      );
      return handle.fetchHistory();
    });

    type MessageRec = Extract<ParseableEventRecord, { type: 'child_workflow' }>;
    const children = liveRecords.filter(
      (r): r is MessageRec => r.type === 'child_workflow',
    );
    const started = children.filter((r) => r.status === 'started');
    const failed = children.filter((r) => r.status === 'failed');
    assert.equal(
      started.length,
      1,
      `expected 1 child_workflow started record; got ${started.length}`,
    );
    assert.equal(
      failed.length,
      1,
      `expected 1 child_workflow failed record; got ${failed.length}`,
    );
    assert.equal(failed[0].direction, 'outbound');
    assert.equal(failed[0].message_name, 'failingExample');
    assert.ok(failed[0].error, 'failed child_workflow record should carry error');

    type WorkflowRec = Extract<ParseableEventRecord, { type: 'workflow' }>;
    const parentFailed = liveRecords.filter(
      (r): r is WorkflowRec =>
        r.type === 'workflow' && r.workflow_id === workflowId && r.status === 'failed',
    );
    assert.equal(
      parentFailed.length,
      1,
      `expected parent workflow to record 1 failed status; got ${parentFailed.length}`,
    );
    assert.ok(parentFailed[0].error, 'parent failed workflow record should carry error');

    const replayRecords = await replay(history);
    assert.equal(
      replayRecords.filter((r) => r.type === 'child_workflow').length,
      0,
      `replay emitted child_workflow records; expected 0`,
    );
    assert.equal(
      replayRecords.filter((r) => r.type === 'workflow').length,
      0,
      `replay emitted workflow records; expected 0`,
    );
  });
});
