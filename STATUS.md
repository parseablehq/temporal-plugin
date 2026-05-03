# v0.1 status

Tracking what's done and what's left for the Temporal AI Partner Program submission. Internal — not part of the user-facing integration guide.

| Item | Status |
|---|---|
| `ParseablePlugin` class extending `SimplePlugin` | done |
| Activity inbound interceptor (start/completed/failed/retry/duration) | done |
| Workflow inbound interceptor via Sink (replay-safe) | done |
| Workflow inbound: signals, queries, updates | done |
| Workflow outbound: child workflows, outgoing signals, continue-as-new | done |
| OTLP HTTP log exporter to Parseable | done |
| OTLP HTTP trace exporter to Parseable (via `OpenTelemetryPlugin`) | done |
| `SanitizingSpanExporter` for OTLP attribute compatibility | done |
| Replay-safety test | done |
| `workflowEvent()` helper for user-defined replay-safe events | done |
| Plugin version metadata on logs and traces | done |
| Workflow client interceptor (additional header propagation) | partial via OTel plugin |
| Standalone npm package extraction | deferred — currently embedded in this demo repo |
| SQL pack / dashboard templates | deferred to v0.1.1 |
| Submission to Temporal partner team | pending |
