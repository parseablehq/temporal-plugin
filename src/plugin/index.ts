import { SimplePlugin } from '@temporalio/plugin';
import type {
  WorkerOptions,
  ReplayWorkerOptions,
  InjectedSinks,
  Worker,
} from '@temporalio/worker';
import type { ClientOptions } from '@temporalio/client';
import { OpenTelemetryPlugin } from '@temporalio/interceptors-opentelemetry';
import type { LoggerProvider } from '@opentelemetry/sdk-logs';
import type { SpanProcessor } from '@opentelemetry/sdk-trace-base';
import { createParseableActivityInterceptor } from './activity-interceptor';
import {
  buildLoggerProvider,
  buildResource,
  buildSpanProcessor,
  createOtelLogEmitter,
  type ParseableEndpoint,
} from './exporters';
import type { Emit } from './types';
import type { ParseableSinks } from './workflow-interceptor';

export interface ParseablePluginOptions {
  serviceName: string;
  endpoint?: string;
  auth?: { username: string; password: string };
  logs?: false | { stream?: string };
  traces?: false | { stream?: string };
  /** Override emit for tests; bypasses the OTel log pipeline. */
  emit?: Emit;
}

const PLUGIN_NAME = 'parseable.ParseablePlugin';
const DEFAULT_LOGS_STREAM = 'temporal-logs';
const DEFAULT_TRACES_STREAM = 'temporal-traces';

const consoleEmit: Emit = (record) => {
  console.log(`[parseable] ${JSON.stringify(record)}`);
};

function requireEndpoint(opts: ParseablePluginOptions, what: string): ParseableEndpoint {
  if (!opts.endpoint || !opts.auth) {
    throw new Error(
      `ParseablePlugin: ${what} requires 'endpoint' and 'auth'. Pass '${what}: false' to disable, or provide a custom 'emit' for logs.`,
    );
  }
  return { endpoint: opts.endpoint, auth: opts.auth };
}

export class ParseablePlugin extends SimplePlugin {
  private readonly emitFn: Emit;
  private readonly serviceName: string;
  private readonly otelPlugin?: OpenTelemetryPlugin;
  private readonly loggerProvider?: LoggerProvider;
  private readonly spanProcessor?: SpanProcessor;

  constructor(opts: ParseablePluginOptions) {
    const { serviceName } = opts;
    const resource = buildResource(serviceName);

    let loggerProvider: LoggerProvider | undefined;
    let emit: Emit;
    if (opts.emit) {
      emit = opts.emit;
    } else if (opts.logs === false) {
      emit = consoleEmit;
    } else {
      const endpoint = requireEndpoint(opts, 'logs');
      const stream = opts.logs?.stream ?? DEFAULT_LOGS_STREAM;
      loggerProvider = buildLoggerProvider(endpoint, stream, resource);
      emit = createOtelLogEmitter(loggerProvider.getLogger(PLUGIN_NAME));
    }

    let otelPlugin: OpenTelemetryPlugin | undefined;
    let spanProcessor: SpanProcessor | undefined;
    if (opts.traces !== false) {
      const endpoint = requireEndpoint(opts, 'traces');
      const stream = opts.traces?.stream ?? DEFAULT_TRACES_STREAM;
      spanProcessor = buildSpanProcessor(endpoint, stream);
      otelPlugin = new OpenTelemetryPlugin({ resource, spanProcessor });
    }

    const activityInterceptor = createParseableActivityInterceptor({ serviceName, emit });

    super({
      name: PLUGIN_NAME,
      workerInterceptors: {
        activity: [activityInterceptor],
        workflowModules: [require.resolve('./workflow-interceptor')],
      },
    });

    this.emitFn = emit;
    this.serviceName = serviceName;
    this.otelPlugin = otelPlugin;
    this.loggerProvider = loggerProvider;
    this.spanProcessor = spanProcessor;
  }

  override configureClient(options: ClientOptions): ClientOptions {
    const fromOtel = this.otelPlugin ? this.otelPlugin.configureClient(options) : options;
    return super.configureClient(fromOtel);
  }

  override configureWorker(options: WorkerOptions): WorkerOptions {
    const fromOtel = this.otelPlugin ? this.otelPlugin.configureWorker(options) : options;
    const merged = super.configureWorker(fromOtel);
    return { ...merged, sinks: this.injectSinks(merged.sinks) };
  }

  override configureReplayWorker(options: ReplayWorkerOptions): ReplayWorkerOptions {
    const fromOtel = this.otelPlugin ? this.otelPlugin.configureReplayWorker(options) : options;
    const merged = super.configureReplayWorker(fromOtel);
    return { ...merged, sinks: this.injectSinks(merged.sinks) };
  }

  override async runWorker(worker: Worker, next: (w: Worker) => Promise<void>): Promise<void> {
    try {
      await super.runWorker(worker, next);
    } finally {
      await Promise.allSettled([
        this.spanProcessor?.shutdown(),
        this.loggerProvider?.shutdown(),
      ]);
    }
  }

  private injectSinks(existing: InjectedSinks<any> | undefined): InjectedSinks<any> {
    const ours: InjectedSinks<ParseableSinks> = {
      parseable: {
        emitWorkflow: {
          fn: (_info, payload) => {
            this.emitFn({ ...payload, service_name: this.serviceName });
          },
          callDuringReplay: false,
        },
        emitUserEvent: {
          fn: (_info, payload) => {
            this.emitFn({ ...payload, service_name: this.serviceName });
          },
          callDuringReplay: false,
        },
        emitMessage: {
          fn: (_info, payload) => {
            this.emitFn({ ...payload, service_name: this.serviceName });
          },
          callDuringReplay: false,
        },
      },
    };
    return { ...(existing ?? {}), ...ours };
  }
}

export type {
  Emit,
  ParseableEventRecord,
  ParseableActivityRecord,
  ParseableWorkflowRecord,
  ParseableUserEventRecord,
  ParseableMessageRecord,
} from './types';
