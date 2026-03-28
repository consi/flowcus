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
  parts_scanned: number;
  parts_skipped: number;
}

export interface QueryResult {
  columns: QueryColumn[];
  rows: unknown[][];
  stats: QueryStats;
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

export async function executeQuery(query: string): Promise<QueryResult> {
  const res = await fetch('/api/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query }),
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
