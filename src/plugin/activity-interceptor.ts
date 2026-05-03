import type { Context } from '@temporalio/activity';
import type {
  ActivityInterceptors,
  ActivityInboundCallsInterceptor,
  ActivityExecuteInput,
  Next,
} from '@temporalio/worker';
import type { Emit } from './types';

export function createParseableActivityInterceptor(opts: {
  serviceName: string;
  emit: Emit;
}): (ctx: Context) => ActivityInterceptors {
  return (ctx) => {
    const inbound: ActivityInboundCallsInterceptor = {
      async execute(
        input: ActivityExecuteInput,
        next: Next<ActivityInboundCallsInterceptor, 'execute'>,
      ): Promise<unknown> {
        const info = ctx.info;
        const base = {
          activity_name: info.activityType,
          activity_id: info.activityId,
          attempt: info.attempt,
          workflow_id: info.workflowExecution?.workflowId,
          run_id: info.workflowExecution?.runId,
          workflow_name: info.workflowType,
          service_name: opts.serviceName,
        };

        opts.emit({
          ...base,
          timestamp: new Date().toISOString(),
          type: 'activity',
          status: 'started',
        });

        const start = Date.now();
        try {
          const result = await next(input);
          opts.emit({
            ...base,
            timestamp: new Date().toISOString(),
            type: 'activity',
            status: 'completed',
            duration_ms: Date.now() - start,
          });
          return result;
        } catch (err) {
          opts.emit({
            ...base,
            timestamp: new Date().toISOString(),
            type: 'activity',
            status: 'failed',
            duration_ms: Date.now() - start,
            error: err instanceof Error ? err.message : String(err),
          });
          throw err;
        }
      },
    };
    return { inbound };
  };
}
