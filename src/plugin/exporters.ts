import { Resource } from '@opentelemetry/resources';
import { SemanticResourceAttributes } from '@opentelemetry/semantic-conventions';
import {
  BatchSpanProcessor,
  type SpanProcessor,
  type SpanExporter,
  type ReadableSpan,
} from '@opentelemetry/sdk-trace-base';
import type { ExportResult } from '@opentelemetry/core';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { OTLPLogExporter } from '@opentelemetry/exporter-logs-otlp-http';
import { LoggerProvider, BatchLogRecordProcessor } from '@opentelemetry/sdk-logs';
import { SeverityNumber, type Logger } from '@opentelemetry/api-logs';
import type { Emit, ParseableEventRecord } from './types';
import { PLUGIN_VERSION } from './version';

export interface ParseableEndpoint {
  endpoint: string;
  auth: { username: string; password: string };
}

function basicAuth(username: string, password: string): string {
  return 'Basic ' + Buffer.from(`${username}:${password}`).toString('base64');
}

function headersFor(
  endpoint: ParseableEndpoint,
  logSource: 'otel-logs' | 'otel-traces',
  stream: string,
): Record<string, string> {
  return {
    Authorization: basicAuth(endpoint.auth.username, endpoint.auth.password),
    'x-p-log-source': logSource,
    'x-p-stream': stream,
  };
}

function joinUrl(base: string, path: string): string {
  return base.replace(/\/$/, '') + path;
}

export function buildResource(serviceName: string): Resource {
  return new Resource({
    [SemanticResourceAttributes.SERVICE_NAME]: serviceName,
    'parseable.plugin.version': PLUGIN_VERSION,
  });
}

type AttrPrimitive = string | number | boolean;
type AttrValue = AttrPrimitive | AttrPrimitive[];

function sanitizeAttributeValue(v: unknown): AttrValue | undefined {
  if (v === undefined || v === null) return undefined;
  if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') return v;
  if (v instanceof Date) return v.toISOString();
  if (Array.isArray(v)) {
    const cleaned: AttrPrimitive[] = [];
    for (const item of v) {
      const s = sanitizeAttributeValue(item);
      if (typeof s === 'string' || typeof s === 'number' || typeof s === 'boolean') cleaned.push(s);
    }
    return cleaned.length ? cleaned : undefined;
  }
  if (typeof v === 'object') {
    try {
      return JSON.stringify(v);
    } catch {
      return undefined;
    }
  }
  return undefined;
}

function sanitizeAttributes(attrs: Record<string, unknown>): Record<string, AttrValue> {
  const out: Record<string, AttrValue> = {};
  for (const [k, v] of Object.entries(attrs)) {
    const s = sanitizeAttributeValue(v);
    if (s !== undefined) out[k] = s;
  }
  return out;
}

class SanitizingSpanExporter implements SpanExporter {
  constructor(private readonly inner: SpanExporter) {}
  export(spans: ReadableSpan[], cb: (result: ExportResult) => void): void {
    const cleaned = spans.map(
      (span) =>
        new Proxy(span, {
          get(target, prop) {
            if (prop === 'attributes') return sanitizeAttributes(target.attributes as Record<string, unknown>);
            return (target as unknown as Record<string | symbol, unknown>)[prop];
          },
        }),
    );
    this.inner.export(cleaned, cb);
  }
  shutdown(): Promise<void> {
    return this.inner.shutdown();
  }
  forceFlush?(): Promise<void> {
    return this.inner.forceFlush?.() ?? Promise.resolve();
  }
}

export function buildSpanProcessor(endpoint: ParseableEndpoint, stream: string): SpanProcessor {
  const otlp = new OTLPTraceExporter({
    url: joinUrl(endpoint.endpoint, '/v1/traces'),
    headers: headersFor(endpoint, 'otel-traces', stream),
  });
  return new BatchSpanProcessor(new SanitizingSpanExporter(otlp));
}

export function buildLoggerProvider(
  endpoint: ParseableEndpoint,
  stream: string,
  resource: Resource,
): LoggerProvider {
  const exporter = new OTLPLogExporter({
    url: joinUrl(endpoint.endpoint, '/v1/logs'),
    headers: headersFor(endpoint, 'otel-logs', stream),
  });
  const provider = new LoggerProvider({ resource });
  provider.addLogRecordProcessor(new BatchLogRecordProcessor(exporter));
  return provider;
}

export function createOtelLogEmitter(logger: Logger): Emit {
  return (record: ParseableEventRecord) => {
    const isError = record.type !== 'user_event' && record.status === 'failed';
    const body =
      record.type === 'user_event'
        ? `user_event ${record.event_name}`
        : `${record.type} ${record.status}`;
    logger.emit({
      severityNumber: isError ? SeverityNumber.ERROR : SeverityNumber.INFO,
      severityText: isError ? 'ERROR' : 'INFO',
      body,
      attributes: sanitizeAttributes(record as unknown as Record<string, unknown>),
      timestamp: new Date(record.timestamp),
    });
  };
}
