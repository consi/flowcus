/** API client functions for Flowcus backend. */

export interface ServerInfo {
  name: string;
  version: string;
  server: {
    host: string;
    port: number;
    dev_mode: boolean;
  };
  workers: {
    async: number;
    cpu: number;
  };
}

export interface QueryColumn {
  name: string;
  type: string;
}

export interface QueryStats {
  parse_time_us: number;
  execution_time_us: number;
  rows_scanned: number;
  rows_returned: number;
  total_rows: number;
  parts_scanned: number;
  parts_skipped: number;
  bytes_read?: number;
  cached?: boolean;
}

export interface Pagination {
  offset: number;
  limit: number;
  total: number;
  has_more: boolean;
}

export interface TimeRangeBounds {
  start: number;
  end: number;
}

export interface QueryResult {
  columns: QueryColumn[];
  rows: unknown[][];
  stats: QueryStats;
  pagination: Pagination;
  time_range: TimeRangeBounds;
}

export interface QueryError {
  error: string;
  position?: number;
  length?: number;
}

export interface FieldInfo {
  name: string;
  type: string;
  description?: string;
}

export async function fetchInfo(): Promise<ServerInfo> {
  const res = await fetch('/api/info');
  if (!res.ok) throw new Error(`Server info failed: ${res.status}`);
  return res.json();
}

export async function fetchHealth(): Promise<{ status: string }> {
  const res = await fetch('/api/health');
  if (!res.ok) throw new Error(`Health check failed: ${res.status}`);
  return res.json();
}

export async function executeQuery(
  query: string,
  offset?: number,
  limit?: number,
  timeRange?: TimeRangeBounds,
): Promise<QueryResult> {
  const body: Record<string, unknown> = { query, offset, limit };
  if (timeRange) {
    body.time_start = timeRange.start;
    body.time_end = timeRange.end;
  }
  const res = await fetch('/api/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw body as QueryError;
  }
  return res.json();
}

export async function fetchFields(): Promise<FieldInfo[]> {
  const res = await fetch('/api/query/fields');
  if (!res.ok) throw new Error(`Fetch fields failed: ${res.status}`);
  return res.json();
}

export interface InterfaceInfo {
  exporter: string;
  domain_id: number;
  index: number;
  name: string;
}

export async function fetchInterfaces(): Promise<InterfaceInfo[]> {
  const res = await fetch('/api/interfaces');
  if (!res.ok) return [];
  return res.json().then(d => d.interfaces ?? []).catch(() => []);
}

// ── Structured query types ────────────────────

export interface StructuredTimeRange {
  type: 'relative' | 'absolute';
  duration?: string;
  start?: string;
  end?: string;
}

export interface StructuredFilter {
  field: string;
  op: string;
  value: unknown;
  negated?: boolean;
}

export interface StructuredQueryRequest {
  time_range: StructuredTimeRange;
  filters: StructuredFilter[];
  logic?: 'and' | 'or';
  columns?: string[];
  aggregate?: unknown;
  sort?: { field: string; dir: 'asc' | 'desc' };
  offset?: number;
  limit?: number;
  time_start?: number;
  time_end?: number;
}

export interface SchemaField {
  name: string;
  filter_type: string;
  data_type: string;
  description: string;
  semantic_hint: string;
}

export interface SchemaResponse {
  filter_types: Record<string, string[]>;
  fields: SchemaField[];
  op_hints: Record<string, string>;
}

export async function executeStructuredQuery(
  req: StructuredQueryRequest,
): Promise<QueryResult> {
  const res = await fetch('/api/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(req),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw err;
  }
  return res.json();
}

// ── Histogram stats endpoint ──────────────────

export interface HistogramBucket {
  timestamp: number;
  count: number;
}

export interface HistogramResponse {
  buckets: HistogramBucket[];
  total_rows: number;
  time_range: TimeRangeBounds;
  bucket_ms: number;
  done: boolean;
}

export interface HistogramRequest {
  time_range: StructuredTimeRange;
  filters: StructuredFilter[];
  logic: 'and' | 'or';
  buckets?: number;
}

export async function fetchHistogram(
  req: HistogramRequest,
): Promise<HistogramResponse> {
  const res = await fetch('/api/stats/histogram', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(req),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw err;
  }
  return res.json();
}

export async function fetchQuerySchema(): Promise<SchemaResponse> {
  const res = await fetch('/api/query/schema');
  if (!res.ok) throw new Error(`Fetch schema failed: ${res.status}`);
  return res.json();
}

// --- Settings API ---

export interface SettingControl {
  type: 'text' | 'number' | 'bool' | 'select' | 'slider' | 'bytes' | 'duration';
  min?: number;
  max?: number;
  step?: number;
  unit?: string;
  options?: Array<{ value: string; label: string }>;
  min_secs?: number;
  max_secs?: number;
}

export interface SettingField {
  section: string;
  key: string;
  label: string;
  description: string;
  guidance: string;
  control: SettingControl;
  default_value: unknown;
  restart_required: boolean;
}

export interface SettingsSection {
  key: string;
  label: string;
  description: string;
  fields: SettingField[];
}

export interface SettingsSchema {
  sections: SettingsSection[];
}

export interface SettingsValidation {
  errors: Array<{ section: string; field: string; message: string }>;
  warnings: Array<{ section: string; field: string; message: string }>;
}

export interface SettingsSaveResponse {
  config: Record<string, Record<string, unknown>>;
  restart_required: string[];
  validation: SettingsValidation;
}

export async function fetchSettings(): Promise<Record<string, Record<string, unknown>>> {
  const res = await fetch('/api/settings');
  if (!res.ok) throw new Error(`Failed to fetch settings: ${res.status}`);
  return res.json();
}

export async function fetchSettingsSchema(): Promise<SettingsSchema> {
  const res = await fetch('/api/settings/schema');
  if (!res.ok) throw new Error(`Failed to fetch settings schema: ${res.status}`);
  return res.json();
}

export async function fetchSettingsDefaults(): Promise<Record<string, Record<string, unknown>>> {
  const res = await fetch('/api/settings/defaults');
  if (!res.ok) throw new Error(`Failed to fetch settings defaults: ${res.status}`);
  return res.json();
}

export async function saveSettings(config: Record<string, Record<string, unknown>>): Promise<SettingsSaveResponse> {
  const res = await fetch('/api/settings', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(config),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw err;
  }
  return res.json();
}

export async function validateSettings(config: Record<string, Record<string, unknown>>): Promise<SettingsValidation> {
  const res = await fetch('/api/settings/validate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(config),
  });
  if (!res.ok) throw new Error(`Validation request failed: ${res.status}`);
  return res.json();
}

export async function restartServer(): Promise<void> {
  await fetch('/api/restart', { method: 'POST' });
}

// --- Health Stats API ---

export interface HealthStats {
  metrics: Record<string, number>;
  process: {
    rss_bytes: number;
    threads: number;
  };
  cache: {
    used_bytes: number;
    max_bytes: number;
    hits: number;
    misses: number;
    partitions: Array<{ name: string; used_bytes: number; max_bytes: number }>;
  };
  storage: {
    parts_total: number;
    parts_gen0: number;
    parts_merged: number;
    disk_bytes: number;
  };
}

export async function fetchHealthStats(): Promise<HealthStats> {
  const res = await fetch('/api/health/stats');
  if (!res.ok) throw new Error(`Health stats failed: ${res.status}`);
  return res.json();
}

