export interface ParseableActivityRecord {
  timestamp: string;
  type: 'activity';
  status: 'started' | 'completed' | 'failed';
  activity_name: string;
  activity_id: string;
  attempt: number;
  workflow_id?: string;
  run_id?: string;
  workflow_name?: string;
  duration_ms?: number;
  error?: string;
  service_name: string;
}

export interface ParseableWorkflowRecord {
  timestamp: string;
  type: 'workflow';
  status: 'started' | 'completed' | 'failed';
  workflow_name: string;
  workflow_id: string;
  run_id: string;
  duration_ms?: number;
  error?: string;
  service_name: string;
}

export interface ParseableUserEventRecord {
  timestamp: string;
  type: 'user_event';
  workflow_name: string;
  workflow_id: string;
  run_id: string;
  event_name: string;
  event_data?: Record<string, unknown>;
  service_name: string;
}

export interface ParseableMessageRecord {
  timestamp: string;
  type: 'signal' | 'query' | 'update' | 'child_workflow' | 'continue_as_new';
  direction: 'inbound' | 'outbound';
  status: 'started' | 'completed' | 'failed';
  message_name?: string;
  target_workflow_id?: string;
  workflow_id: string;
  run_id: string;
  workflow_name: string;
  duration_ms?: number;
  error?: string;
  service_name: string;
}

export type ParseableEventRecord =
  | ParseableActivityRecord
  | ParseableWorkflowRecord
  | ParseableUserEventRecord
  | ParseableMessageRecord;

export type Emit = (record: ParseableEventRecord) => void;

export type ParseableWorkflowSinkPayload = Omit<ParseableWorkflowRecord, 'service_name'>;

export type ParseableUserEventSinkPayload = Omit<ParseableUserEventRecord, 'service_name'>;

export type ParseableMessageSinkPayload = Omit<ParseableMessageRecord, 'service_name'>;
