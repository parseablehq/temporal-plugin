import { SimplePlugin } from '@temporalio/plugin';
import type { WorkerOptions, ReplayWorkerOptions, InjectedSinks, Worker } from '@temporalio/worker';
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

/**
 * Configuration for {@link ParseablePlugin}.
 *
 * @example
 * ```ts
 * new ParseablePlugin({
 *   serviceName: 'orders-worker',
 *   endpoint: 'https://parseable.example.com',
 *   auth: { username: 'admin', password: '...' },
 * });
 * ```
 */
export interface ParseablePluginOptions {
  /**
   * Logical service identifier stamped onto every emitted record as
   * `service_name` and onto every span as `service.name`. Used to filter and
   * group events from this worker in Parseable / OTel-aware tools.
   */
  serviceName: string;

  /**
   * Base URL of the Parseable instance (e.g. `http://localhost:8000` or
   * `https://parseable.example.com`). Must include scheme; OTLP exporters
   * derive `endpoint + /v1/logs` and `endpoint + /v1/traces` from this.
   *
   * Required unless both `logs: false` (or a custom `emit`) and
   * `traces: false` are set.
   */
  endpoint?: string;

  /**
   * Basic-auth credentials for the Parseable HTTP API. Sent as a
   * `Basic …` header on every OTLP request.
   *
   * Required when shipping logs or traces over the network - i.e. when
   * `endpoint` is set and a custom `emit` is not supplied.
   */
  auth?: { username: string; password: string };

  /**
   * Logs pipeline configuration.
   * - `{ stream?: string }` (default) - emit logs to the given Parseable
   *   stream (default `temporal-logs`).
   * - `false` - disable the OTel log pipeline. Records are emitted to
   *   `console.log` instead. Useful for local dev when you don't have a
   *   Parseable instance handy.
   *
   * Ignored if a custom `emit` is provided.
   */
  logs?: false | { stream?: string };

  /**
   * Traces pipeline configuration.
   * - `{ stream?: string }` (default) - emit OTel spans (composed via
   *   Temporal's `OpenTelemetryPlugin`) to the given Parseable stream
   *   (default `temporal-traces`).
   * - `false` - disable trace emission entirely. The plugin's
   *   `OpenTelemetryPlugin` composition is skipped.
   */
  traces?: false | { stream?: string };

  /**
   * Override the emit function used for log records. Bypasses the OTel
   * log pipeline (and `logs` / `endpoint` / `auth`) entirely - the supplied
   * function receives every {@link ParseableEventRecord} directly.
   *
   * Primarily for tests (capture records into an array) and for routing
   * records into a non-OTel sink (e.g. an existing Pino/Winston logger).
   */
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

/**
 * Temporal worker/client plugin that ships workflow and activity execution
 * events to a Parseable instance as OpenTelemetry logs and traces.
 *
 * The plugin installs three things into the worker:
 *
 * 1. An **activity interceptor** that emits `started` / `completed` / `failed`
 *    records (with `attempt`, `duration_ms`, `error`) for every activity
 *    invocation.
 * 2. A pair of **workflow interceptors** (inbound + outbound) that emit records
 *    for workflow execution, signals, queries, updates, child-workflow start
 *    and completion, and `continueAsNew`. These run inside the workflow V8
 *    isolate and route through Temporal **sinks** with `callDuringReplay: false`
 *    so history replay never produces duplicate records.
 * 3. Temporal's official {@link https://www.npmjs.com/package/@temporalio/interceptors-opentelemetry OpenTelemetryPlugin}
 *    composed internally, with its span processor wired to OTLP/HTTP and
 *    pointed at Parseable's trace ingestion stream. Disable via
 *    `traces: false`.
 *
 * Pass the instance via `Worker.create({ plugins: [...] })` and/or
 * `Client.create({ plugins: [...] })`.
 *
 * @example
 * ```ts
 * import { Worker } from '@temporalio/worker';
 * import { ParseablePlugin } from '@parseable/temporal';
 *
 * const plugin = new ParseablePlugin({
 *   serviceName: 'orders-worker',
 *   endpoint: process.env.PARSEABLE_URL!,
 *   auth: { username: 'admin', password: process.env.PARSEABLE_PASSWORD! },
 * });
 *
 * const worker = await Worker.create({
 *   connection,
 *   namespace: 'default',
 *   taskQueue: 'orders',
 *   workflowsPath: require.resolve('./workflows'),
 *   activities,
 *   plugins: [plugin],
 * });
 * ```
 *
 * @remarks
 * Workflow inbound handlers should throw `ApplicationFailure` (not a plain
 * `Error`) when they want to fail gracefully - plain errors cause the
 * workflow task to retry, which produces duplicate `started`/`failed` record
 * pairs in the plugin output. See README "Caveats" for details.
 */
export class ParseablePlugin extends SimplePlugin {
  private readonly emitFn: Emit;
  private readonly serviceName: string;
  private readonly otelPlugin?: OpenTelemetryPlugin;
  private readonly loggerProvider?: LoggerProvider;
  private readonly spanProcessor?: SpanProcessor;

  /**
   * @param opts - Plugin configuration. See {@link ParseablePluginOptions}.
   * @throws If `endpoint`/`auth` are missing while logs and/or traces are
   * enabled (and no custom `emit` is provided).
   */
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

  /**
   * Called by the Temporal client builder. Delegates to the composed
   * OpenTelemetryPlugin (when traces are enabled) so client-issued
   * workflow/signal calls get OTel context propagation, then forwards to the
   * base plugin.
   *
   * @internal Invoked by the Temporal SDK; not part of the user-facing API.
   */
  override configureClient(options: ClientOptions): ClientOptions {
    const fromOtel = this.otelPlugin ? this.otelPlugin.configureClient(options) : options;
    return super.configureClient(fromOtel);
  }

  /**
   * Called by `Worker.create`. Layers in OpenTelemetry tracing (when
   * enabled), then registers the `parseable` sink that the workflow
   * interceptor uses to emit records out of the V8 isolate.
   *
   * Throws if the consumer already registered a `parseable` sink - pick a
   * different namespace to avoid the collision.
   *
   * @internal Invoked by the Temporal SDK; not part of the user-facing API.
   */
  override configureWorker(options: WorkerOptions): WorkerOptions {
    const fromOtel = this.otelPlugin ? this.otelPlugin.configureWorker(options) : options;
    const merged = super.configureWorker(fromOtel);
    return { ...merged, sinks: this.injectSinks(merged.sinks) };
  }

  /**
   * Called by `Worker.runReplayHistory`. Same wiring as
   * {@link ParseablePlugin.configureWorker} but for the replay worker - every
   * record-emitting sink uses `callDuringReplay: false`, so replaying a
   * history never re-emits records.
   *
   * @internal Invoked by the Temporal SDK; not part of the user-facing API.
   */
  override configureReplayWorker(options: ReplayWorkerOptions): ReplayWorkerOptions {
    const fromOtel = this.otelPlugin ? this.otelPlugin.configureReplayWorker(options) : options;
    const merged = super.configureReplayWorker(fromOtel);
    return { ...merged, sinks: this.injectSinks(merged.sinks) };
  }

  /**
   * Wraps the worker's main run loop. Threads control through the composed
   * OpenTelemetryPlugin (when enabled), then runs the worker. On shutdown
   * (whether the worker exits cleanly or throws), the OTel span processor
   * and logger provider are flushed and closed.
   *
   * @internal Invoked by the Temporal SDK; not part of the user-facing API.
   */
  override async runWorker(worker: Worker, next: (w: Worker) => Promise<void>): Promise<void> {
    const wrapped = this.otelPlugin ? (w: Worker) => this.otelPlugin!.runWorker(w, next) : next;
    try {
      await super.runWorker(worker, wrapped);
    } finally {
      await Promise.allSettled([this.spanProcessor?.shutdown(), this.loggerProvider?.shutdown()]);
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
    if (existing && Object.prototype.hasOwnProperty.call(existing, 'parseable')) {
      throw new Error(
        "ParseablePlugin: sink name 'parseable' is already registered on this worker. " +
          'Remove the conflicting sink or wrap ParseablePlugin behind a different sink namespace.',
      );
    }
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
  ParseableSignalRecord,
  ParseableQueryRecord,
  ParseableUpdateRecord,
  ParseableChildWorkflowRecord,
  ParseableContinueAsNewRecord,
} from './types';
