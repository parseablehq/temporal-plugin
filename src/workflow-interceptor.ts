import { proxySinks, workflowInfo } from '@temporalio/workflow';
import type {
  Sinks,
  WorkflowInterceptorsFactory,
  WorkflowInboundCallsInterceptor,
  WorkflowOutboundCallsInterceptor,
  WorkflowExecuteInput,
  SignalInput,
  QueryInput,
  UpdateInput,
  SignalWorkflowInput,
  StartChildWorkflowExecutionInput,
  ContinueAsNewInput,
  Next,
} from '@temporalio/workflow';
import type {
  ParseableWorkflowSinkPayload,
  ParseableUserEventSinkPayload,
  ParseableMessageSinkPayload,
} from './types';

export interface ParseableSinks extends Sinks {
  parseable: {
    emitWorkflow(payload: ParseableWorkflowSinkPayload): void;
    emitUserEvent(payload: ParseableUserEventSinkPayload): void;
    emitMessage(payload: ParseableMessageSinkPayload): void;
  };
}

const { parseable } = proxySinks<ParseableSinks>();

function workflowBase() {
  const info = workflowInfo();
  return {
    workflow_id: info.workflowId,
    run_id: info.runId,
    workflow_name: info.workflowType,
  };
}

function nowIso(): string {
  return new Date().toISOString();
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

const inbound: WorkflowInboundCallsInterceptor = {
  async execute(
    input: WorkflowExecuteInput,
    next: Next<WorkflowInboundCallsInterceptor, 'execute'>,
  ): Promise<unknown> {
    const base = workflowBase();
    parseable.emitWorkflow({ ...base, type: 'workflow', timestamp: nowIso(), status: 'started' });
    const start = Date.now();
    try {
      const result = await next(input);
      parseable.emitWorkflow({
        ...base,
        type: 'workflow',
        timestamp: nowIso(),
        status: 'completed',
        duration_ms: Date.now() - start,
      });
      return result;
    } catch (err) {
      parseable.emitWorkflow({
        ...base,
        type: 'workflow',
        timestamp: nowIso(),
        status: 'failed',
        duration_ms: Date.now() - start,
        error: errorMessage(err),
      });
      throw err;
    }
  },

  async handleSignal(
    input: SignalInput,
    next: Next<WorkflowInboundCallsInterceptor, 'handleSignal'>,
  ): Promise<void> {
    const base = workflowBase();
    parseable.emitMessage({
      ...base,
      type: 'signal',
      direction: 'inbound',
      timestamp: nowIso(),
      status: 'started',
      message_name: input.signalName,
    });
    const start = Date.now();
    try {
      await next(input);
      parseable.emitMessage({
        ...base,
        type: 'signal',
        direction: 'inbound',
        timestamp: nowIso(),
        status: 'completed',
        message_name: input.signalName,
        duration_ms: Date.now() - start,
      });
    } catch (err) {
      parseable.emitMessage({
        ...base,
        type: 'signal',
        direction: 'inbound',
        timestamp: nowIso(),
        status: 'failed',
        message_name: input.signalName,
        duration_ms: Date.now() - start,
        error: errorMessage(err),
      });
      throw err;
    }
  },

  async handleQuery(
    input: QueryInput,
    next: Next<WorkflowInboundCallsInterceptor, 'handleQuery'>,
  ): Promise<unknown> {
    const base = workflowBase();
    parseable.emitMessage({
      ...base,
      type: 'query',
      direction: 'inbound',
      timestamp: nowIso(),
      status: 'started',
      message_name: input.queryName,
    });
    const start = Date.now();
    try {
      const result = await next(input);
      parseable.emitMessage({
        ...base,
        type: 'query',
        direction: 'inbound',
        timestamp: nowIso(),
        status: 'completed',
        message_name: input.queryName,
        duration_ms: Date.now() - start,
      });
      return result;
    } catch (err) {
      parseable.emitMessage({
        ...base,
        type: 'query',
        direction: 'inbound',
        timestamp: nowIso(),
        status: 'failed',
        message_name: input.queryName,
        duration_ms: Date.now() - start,
        error: errorMessage(err),
      });
      throw err;
    }
  },

  async handleUpdate(
    input: UpdateInput,
    next: Next<WorkflowInboundCallsInterceptor, 'handleUpdate'>,
  ): Promise<unknown> {
    const base = workflowBase();
    parseable.emitMessage({
      ...base,
      type: 'update',
      direction: 'inbound',
      timestamp: nowIso(),
      status: 'started',
      message_name: input.name,
    });
    const start = Date.now();
    try {
      const result = await next(input);
      parseable.emitMessage({
        ...base,
        type: 'update',
        direction: 'inbound',
        timestamp: nowIso(),
        status: 'completed',
        message_name: input.name,
        duration_ms: Date.now() - start,
      });
      return result;
    } catch (err) {
      parseable.emitMessage({
        ...base,
        type: 'update',
        direction: 'inbound',
        timestamp: nowIso(),
        status: 'failed',
        message_name: input.name,
        duration_ms: Date.now() - start,
        error: errorMessage(err),
      });
      throw err;
    }
  },
};

const outbound: WorkflowOutboundCallsInterceptor = {
  async signalWorkflow(
    input: SignalWorkflowInput,
    next: Next<WorkflowOutboundCallsInterceptor, 'signalWorkflow'>,
  ): Promise<void> {
    const base = workflowBase();
    const target_workflow_id =
      (input.target.type === 'external'
        ? input.target.workflowExecution.workflowId
        : input.target.childWorkflowId) ?? undefined;
    parseable.emitMessage({
      ...base,
      type: 'signal',
      direction: 'outbound',
      timestamp: nowIso(),
      status: 'started',
      message_name: input.signalName,
      target_workflow_id,
    });
    const start = Date.now();
    try {
      await next(input);
      parseable.emitMessage({
        ...base,
        type: 'signal',
        direction: 'outbound',
        timestamp: nowIso(),
        status: 'completed',
        message_name: input.signalName,
        target_workflow_id,
        duration_ms: Date.now() - start,
      });
    } catch (err) {
      parseable.emitMessage({
        ...base,
        type: 'signal',
        direction: 'outbound',
        timestamp: nowIso(),
        status: 'failed',
        message_name: input.signalName,
        target_workflow_id,
        duration_ms: Date.now() - start,
        error: errorMessage(err),
      });
      throw err;
    }
  },

  async startChildWorkflowExecution(
    input: StartChildWorkflowExecutionInput,
    next: Next<WorkflowOutboundCallsInterceptor, 'startChildWorkflowExecution'>,
  ): Promise<[Promise<string>, Promise<unknown>]> {
    const base = workflowBase();
    const target_workflow_id = input.options.workflowId ?? undefined;
    parseable.emitMessage({
      ...base,
      type: 'child_workflow',
      direction: 'outbound',
      timestamp: nowIso(),
      status: 'started',
      message_name: input.workflowType,
      target_workflow_id,
    });
    const start = Date.now();
    let started: [Promise<string>, Promise<unknown>];
    try {
      started = await next(input);
    } catch (err) {
      parseable.emitMessage({
        ...base,
        type: 'child_workflow',
        direction: 'outbound',
        timestamp: nowIso(),
        status: 'failed',
        message_name: input.workflowType,
        target_workflow_id,
        duration_ms: Date.now() - start,
        error: errorMessage(err),
      });
      throw err;
    }
    const [startedHandle, resultPromise] = started;
    const wrappedResult = resultPromise.then(
      (r) => {
        parseable.emitMessage({
          ...base,
          type: 'child_workflow',
          direction: 'outbound',
          timestamp: nowIso(),
          status: 'completed',
          message_name: input.workflowType,
          target_workflow_id,
          duration_ms: Date.now() - start,
        });
        return r;
      },
      (err) => {
        parseable.emitMessage({
          ...base,
          type: 'child_workflow',
          direction: 'outbound',
          timestamp: nowIso(),
          status: 'failed',
          message_name: input.workflowType,
          target_workflow_id,
          duration_ms: Date.now() - start,
          error: errorMessage(err),
        });
        throw err;
      },
    );
    return [startedHandle, wrappedResult];
  },

  async continueAsNew(
    input: ContinueAsNewInput,
    next: Next<WorkflowOutboundCallsInterceptor, 'continueAsNew'>,
  ): Promise<never> {
    const base = workflowBase();
    parseable.emitMessage({
      ...base,
      type: 'continue_as_new',
      direction: 'outbound',
      timestamp: nowIso(),
      status: 'started',
      message_name: input.options.workflowType,
    });
    return next(input);
  },
};

export const interceptors: WorkflowInterceptorsFactory = () => ({
  inbound: [inbound],
  outbound: [outbound],
});
