import { useCallback, useEffect, useRef, useState } from 'react';
import { ResultsTable } from './ResultsTable';
import { FlowSidebar } from './FlowSidebar';
import { TimeRangePicker } from './TimeRangePicker';
import { SearchBar } from './SearchBar';
import { ExplainGantt, type PlanStep } from './ExplainGantt';
import { ThemeSwitcher } from './ThemeSwitcher';
import { AboutDialog } from './AboutDialog';
import { SettingsPanel } from './SettingsPanel';
import { HealthPanel } from './HealthPanel';
import { TimeHistogram } from './TimeHistogram';
import {
  executeStructuredQuery,
  fetchInfo,
  fetchInterfaces,
  fetchQuerySchema,
  type Pagination,
  type QueryError,
  type QueryStats,
  type QueryColumn,
  type ServerInfo,
  type StructuredTimeRange,
  type StructuredFilter,
  type StructuredQueryRequest,
  type SchemaResponse,
} from './api';
import { setInterfaceNames, getTimezone, setTimezone, getAvailableTimezones } from './formatters';
import { formatCache } from './formatCache';
import type { SchemaField } from './api';

// ---------------------------------------------------------------------------
// Value coercion — convert user-friendly strings into JSON the backend expects
// ---------------------------------------------------------------------------

/** Parse a human-readable duration string to milliseconds. Returns null if not a duration. */
function parseDurationMs(s: string): number | null {
  const m = s.trim().match(/^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)$/i);
  if (!m) return null;
  const n = parseFloat(m[1]);
  switch (m[2].toLowerCase()) {
    case 'ms': return n;
    case 's': return n * 1000;
    case 'm': return n * 60_000;
    case 'h': return n * 3_600_000;
    case 'd': return n * 86_400_000;
    default: return null;
  }
}

/** Parse a number with optional size/magnitude suffix. Returns null if not parseable. */
function parseNumericWithSuffix(s: string): number | null {
  const m = s.trim().match(/^(\d+(?:\.\d+)?)\s*(k|m|g|t|kb|mb|gb|tb|ki|mi|gi|ti|kib|mib|gib|tib)?$/i);
  if (!m) return null;
  const n = parseFloat(m[1]);
  if (!m[2]) return n;
  switch (m[2].toLowerCase()) {
    case 'k': case 'kb': return n * 1_000;
    case 'm': case 'mb': return n * 1_000_000;
    case 'g': case 'gb': return n * 1_000_000_000;
    case 't': case 'tb': return n * 1_000_000_000_000;
    case 'ki': case 'kib': return n * 1024;
    case 'mi': case 'mib': return n * 1024 * 1024;
    case 'gi': case 'gib': return n * 1024 * 1024 * 1024;
    case 'ti': case 'tib': return n * 1024 * 1024 * 1024 * 1024;
    default: return null;
  }
}

/** Coerce a single scalar value based on field semantics. */
function coerceScalar(raw: string, field: SchemaField | undefined): unknown {
  const s = raw.trim();
  if (!field) return s;

  // Duration fields: accept human-readable durations
  if (field.semantic_hint === 'duration_ms') {
    const ms = parseDurationMs(s);
    if (ms !== null) return ms;
  }

  // Numeric types: try plain number first, then suffixed
  if (field.filter_type === 'numeric' || field.filter_type === 'port' || field.filter_type === 'protocol') {
    const plain = Number(s);
    if (!isNaN(plain) && s.length > 0) return plain;
    const suffixed = parseNumericWithSuffix(s);
    if (suffixed !== null) return suffixed;
  }

  return s;
}

/** Split a comma-separated value string into individual items. */
function splitListValue(raw: string): string[] {
  return raw.split(',').map(s => s.trim()).filter(Boolean);
}

/**
 * Coerce a StructuredFilter's value into the JSON shape the backend expects.
 * - `between`, `port_range`: [low, high] array
 * - `in`, `not_in`: array of values
 * - Duration fields: human strings → ms numbers
 * - Numeric scalars: string → number when possible
 */
function coerceFilterValue(
  filter: StructuredFilter,
  schema: SchemaResponse | null,
): StructuredFilter {
  const field = schema?.fields.find(f => f.name === filter.field);
  const raw = typeof filter.value === 'string' ? filter.value : String(filter.value);

  switch (filter.op) {
    case 'between':
    case 'port_range': {
      // Accept "low,high" or "low-high" — split into [low, high] array
      // But only if it's currently a string (not already an array)
      if (typeof filter.value !== 'string') return filter;
      const sep = raw.includes(',') ? ',' : '-';
      const parts = raw.split(sep).map(s => s.trim()).filter(Boolean);
      if (parts.length === 2) {
        return { ...filter, value: [coerceScalar(parts[0], field), coerceScalar(parts[1], field)] };
      }
      return filter;
    }

    case 'in':
    case 'not_in': {
      // Split comma-separated into array
      if (typeof filter.value !== 'string') return filter;
      const items = splitListValue(raw);
      return { ...filter, value: items.map(s => coerceScalar(s, field)) };
    }

    default: {
      // Scalar coercion
      if (typeof filter.value === 'string') {
        return { ...filter, value: coerceScalar(raw, field) };
      }
      return filter;
    }
  }
}

// ---------------------------------------------------------------------------
// URL query parameter persistence
// ---------------------------------------------------------------------------

const VALID_RELATIVE_DURATION = /^(\d+)(ms|s|m|h|d)$/;

function parseTimeRangeParam(t: string): StructuredTimeRange | null {
  // Absolute: two numeric timestamps separated by dash
  const dashIdx = t.indexOf('-', 1); // start at 1 to skip potential negative (won't happen with ms timestamps, but safe)
  if (dashIdx > 0) {
    const startStr = t.slice(0, dashIdx);
    const endStr = t.slice(dashIdx + 1);
    if (/^\d+$/.test(startStr) && /^\d+$/.test(endStr)) {
      const startMs = Number(startStr);
      const endMs = Number(endStr);
      if (startMs < endMs) {
        return {
          type: 'absolute',
          start: new Date(startMs).toISOString(),
          end: new Date(endMs).toISOString(),
        };
      }
    }
  }
  // Relative: duration string like 5m, 1h, 7d
  if (VALID_RELATIVE_DURATION.test(t)) {
    return { type: 'relative', duration: t };
  }
  return null;
}

function parseFiltersParam(f: string): StructuredFilter[] {
  if (!f) return [];
  return f.split(',').map((segment) => {
    let s = segment;
    let negated = false;
    if (s.startsWith('!')) {
      negated = true;
      s = s.slice(1);
    }
    // Split on dots: field.op.value (value may contain dots, e.g. IP addresses)
    const firstDot = s.indexOf('.');
    if (firstDot < 0) return null;
    const secondDot = s.indexOf('.', firstDot + 1);
    if (secondDot < 0) return null;
    const field = s.slice(0, firstDot);
    const op = s.slice(firstDot + 1, secondDot);
    const value = decodeURIComponent(s.slice(secondDot + 1));
    if (!field || !op) return null;
    return { field, op, value, negated: negated || undefined } as StructuredFilter;
  }).filter((f): f is StructuredFilter => f !== null);
}

function parseRefreshParam(r: string): number {
  const n = parseInt(r, 10);
  return isNaN(n) || n < 0 ? 0 : n;
}

function serializeTimeRange(tr: StructuredTimeRange): string {
  if (tr.type === 'relative' && tr.duration) {
    return tr.duration;
  }
  if (tr.type === 'absolute' && tr.start && tr.end) {
    const startMs = new Date(tr.start).getTime();
    const endMs = new Date(tr.end).getTime();
    return `${startMs}-${endMs}`;
  }
  return '5m';
}

function serializeFilters(filters: StructuredFilter[]): string {
  return filters.map((f) => {
    const prefix = f.negated ? '!' : '';
    const encodedValue = encodeURIComponent(String(f.value));
    return `${prefix}${f.field}.${f.op}.${encodedValue}`;
  }).join(',');
}

function readInitialStateFromURL(): {
  timeRange?: StructuredTimeRange;
  filters?: StructuredFilter[];
  refreshInterval?: number;
} {
  const params = new URLSearchParams(window.location.search);
  const result: ReturnType<typeof readInitialStateFromURL> = {};

  const t = params.get('t');
  if (t) {
    const parsed = parseTimeRangeParam(t);
    if (parsed) result.timeRange = parsed;
  }

  const f = params.get('f');
  if (f) {
    const parsed = parseFiltersParam(f);
    if (parsed.length > 0) result.filters = parsed;
  }

  const r = params.get('r');
  if (r) {
    const parsed = parseRefreshParam(r);
    if (parsed > 0) result.refreshInterval = parsed;
  }

  return result;
}

const _initialURLState = readInitialStateFromURL();

function formatMicros(us: number): string {
  if (us < 1000) return `${us}\u00B5s`;
  if (us < 1_000_000) return `${(us / 1000).toFixed(1)}ms`;
  return `${(us / 1_000_000).toFixed(2)}s`;
}

function humanBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

export function App() {
  const [info, setInfo] = useState<ServerInfo | null>(null);
  const [columns, setColumns] = useState<QueryColumn[]>([]);
  const [allRows, setAllRows] = useState<unknown[][]>([]);
  const [stats, setStats] = useState<QueryStats | null>(null);
  const [explain, setExplain] = useState<unknown[] | null>(null);
  const [pagination, setPagination] = useState<Pagination | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [queryError, setQueryError] = useState<QueryError | null>(null);
  const [selectedRow, setSelectedRow] = useState<number | null>(null);
  const [showStats, setShowStats] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [healthOpen, setHealthOpen] = useState(false);
  const [aboutOpen, setAboutOpen] = useState(false);
  const [queryGen, setQueryGen] = useState(0);

  // Query state — initialized from URL params if present
  const [timeRange, setTimeRange] = useState<StructuredTimeRange>(
    _initialURLState.timeRange ?? { type: 'relative', duration: '5m' },
  );
  const [filters, setFilters] = useState<StructuredFilter[]>(
    _initialURLState.filters ?? [],
  );
  const filterLogic: 'and' | 'or' = 'and';
  const [schema, setSchema] = useState<SchemaResponse | null>(null);
  const [refreshInterval, setRefreshInterval] = useState(
    _initialURLState.refreshInterval ?? 0,
  );

  // URL sync — write state changes back to the URL bar.
  // Uses a ref to suppress the effect on initial mount (state came FROM the URL).
  const urlSyncMounted = useRef(false);
  useEffect(() => {
    if (!urlSyncMounted.current) {
      urlSyncMounted.current = true;
      return;
    }
    const params = new URLSearchParams();
    params.set('t', serializeTimeRange(timeRange));
    if (filters.length > 0) {
      params.set('f', serializeFilters(filters));
    }
    if (refreshInterval > 0) {
      params.set('r', String(refreshInterval));
    }
    const qs = params.toString();
    const newURL = qs ? `?${qs}` : window.location.pathname;
    window.history.replaceState(null, '', newURL);
  }, [timeRange, filters, refreshInterval]);

  const setFiltersKeepLimitLast = useCallback((filtersOrUpdater: StructuredFilter[] | ((prev: StructuredFilter[]) => StructuredFilter[])) => {
    setFilters((prev) => {
      const next = typeof filtersOrUpdater === 'function' ? filtersOrUpdater(prev) : filtersOrUpdater;
      const nonLimit = next.filter((f) => f.field !== 'limit');
      const limitF = next.filter((f) => f.field === 'limit');
      if (limitF.length === 0) return next;
      return [...nonLimit, ...limitF];
    });
  }, []);

  const handleAddFilter = useCallback((field: string, value: unknown, negated: boolean) => {
    const op = negated ? 'ne' : 'eq';
    const strValue = String(value);
    const exists = filters.some((f) => f.field === field && f.op === op && String(f.value) === strValue);
    if (exists) return;
    setFiltersKeepLimitLast((prev) => [...prev, { field, op, value: strValue }]);
  }, [filters, setFiltersKeepLimitLast]);

  // Column visibility (persisted in localStorage)
  const [visibleColumns, setVisibleColumns] = useState<string[] | null>(() => {
    try {
      const saved = localStorage.getItem('flowcus:columns');
      return saved ? JSON.parse(saved) : null;
    } catch { return null; }
  });

  // Timezone
  const [timezone, setTz] = useState(getTimezone);
  const handleTimezoneChange = useCallback((e: React.ChangeEvent<HTMLSelectElement>) => {
    const tz = e.target.value;
    setTz(tz);
    setTimezone(tz);
    formatCache.clear();
  }, []);

  // Persist visible columns
  useEffect(() => {
    if (visibleColumns) {
      localStorage.setItem('flowcus:columns', JSON.stringify(visibleColumns));
    }
  }, [visibleColumns]);

  // Load initial data on mount
  useEffect(() => {
    fetchInfo().then(setInfo).catch(() => {});
    fetchInterfaces().then(setInterfaceNames).catch(() => {});
    fetchQuerySchema().then(setSchema).catch(() => {});
  }, []);

  // Pagination & cancellation refs
  const lastStructuredReq = useRef<StructuredQueryRequest | null>(null);
  const initialQueryFired = useRef(false);
  const abortRef = useRef<AbortController | null>(null);
  const histogramChanged = useRef(false);

  const handleExecute = useCallback(async () => {
    // Cancel any in-flight query
    if (abortRef.current) abortRef.current.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    // Extract limit from filters if present — pass as aggregate stage
    const limitFilter = filters.find((f) => f.field === 'limit');
    const limitN = limitFilter ? Math.max(1, Math.min(Number(limitFilter.value) || 100, 10000)) : 0;
    const realFilters = filters
      .filter((f) => f.field !== 'limit' && f.field && f.op)
      .map((f) => coerceFilterValue(f, schema));

    const pageLimit = limitN > 0 ? limitN : 100;
    const req: StructuredQueryRequest = {
      time_range: timeRange,
      filters: realFilters,
      logic: filterLogic,
      aggregate: limitN > 0 ? { type: 'limit', n: limitN } : undefined,
      offset: 0,
      limit: pageLimit + 1,
    };

    setLoading(true);
    setQueryError(null);
    setSelectedRow(null);
    setQueryGen((g) => g + 1);
    lastStructuredReq.current = req;

    try {
      const res = await executeStructuredQuery(req);
      if (controller.signal.aborted) return;

      const gotExtra = res.rows.length > pageLimit;
      const displayRows = gotExtra ? res.rows.slice(0, pageLimit) : res.rows;

      setColumns(res.columns);
      setAllRows(displayRows);
      setStats(res.stats);
      setExplain((res as unknown as Record<string, unknown>).explain as unknown[] ?? null);
      setPagination({
        offset: 0,
        limit: pageLimit,
        total: res.pagination.total,
        has_more: gotExtra,
      });

      // Pin time range for cursor-based scroll. Use the +1 row's
      // timestamp as the initial cursor so loadMore can start there.
      const timeColIdx = res.columns.findIndex((c) => c.name === 'flowcusExportTime');
      let cursorEnd = res.time_range.end;
      if (gotExtra && timeColIdx >= 0) {
        const extraTime = res.rows[pageLimit][timeColIdx];
        if (typeof extraTime === 'number') cursorEnd = extraTime;
      }
      lastStructuredReq.current = {
        ...req,
        limit: pageLimit,
        time_start: res.time_range.start,
        time_end: cursorEnd,
        pinned_columns: res.schema_columns,
      };
    } catch (err: unknown) {
      if (controller.signal.aborted) return;
      if (err && typeof err === 'object' && 'error' in err) {
        setQueryError(err as QueryError);
      } else {
        setQueryError({ error: err instanceof Error ? err.message : 'Unknown error' });
      }
    } finally {
      if (!controller.signal.aborted) {
        setLoading(false);
      }
    }
  }, [timeRange, filters]);

  const handleCancel = useCallback(() => {
    if (abortRef.current) {
      abortRef.current.abort();
      abortRef.current = null;
    }
    setLoading(false);
  }, []);

  const handleHistogramTimeChange = useCallback((range: StructuredTimeRange) => {
    setTimeRange(range);
    histogramChanged.current = true;
  }, []);

  // Auto-execute on first visit
  useEffect(() => {
    if (!initialQueryFired.current) {
      initialQueryFired.current = true;
      handleExecute();
    }
  }, [handleExecute]);

  // Auto-execute when time range changes (from picker or histogram),
  // debounced to avoid rapid re-queries during interaction.
  const prevTimeRange = useRef(timeRange);
  useEffect(() => {
    // Skip if this is the initial render (handled above)
    if (prevTimeRange.current === timeRange) return;
    prevTimeRange.current = timeRange;

    // Histogram changes fire immediately (no debounce needed)
    if (histogramChanged.current) {
      histogramChanged.current = false;
      handleExecute();
      return;
    }

    // Picker changes are debounced
    const timer = setTimeout(() => {
      handleExecute();
    }, 400);
    return () => clearTimeout(timer);
  }, [timeRange, handleExecute]);

  // Auto-refresh timer
  useEffect(() => {
    if (refreshInterval <= 0) return;
    const timer = setInterval(() => {
      handleExecute();
    }, refreshInterval * 1000);
    return () => clearInterval(timer);
  }, [refreshInterval, handleExecute]);

  const loadMore = useCallback(async () => {
    if (!pagination?.has_more || loadingMore || !lastStructuredReq.current) return;

    const pageLimit = pagination.limit;
    const timeColIdx = columns.findIndex((c) => c.name === 'flowcusExportTime');
    if (timeColIdx < 0) return;

    setLoadingMore(true);
    try {
      // Cursor-based pagination: fetch limit+1 using stored time_end as
      // cursor. The +1 row's timestamp becomes the next cursor, giving us
      // a clean ms-level boundary that avoids re-fetching displayed rows.
      const req = {
        ...lastStructuredReq.current,
        offset: 0,
        limit: pageLimit + 1,
      };
      const res = await executeStructuredQuery(req);
      const gotExtra = res.rows.length > pageLimit;
      const displayRows = gotExtra ? res.rows.slice(0, pageLimit) : res.rows;

      if (displayRows.length === 0) {
        setPagination((prev) => prev ? { ...prev, has_more: false } : prev);
        return;
      }

      // Advance cursor: use the +1 row's timestamp as next time_end
      if (gotExtra) {
        const extraRow = res.rows[pageLimit];
        const extraTime = extraRow[timeColIdx];
        if (typeof extraTime === 'number') {
          lastStructuredReq.current = {
            ...lastStructuredReq.current,
            time_end: extraTime,
            pinned_columns: res.schema_columns,
          };
        }
      }

      // Merge new columns into existing set (columns only grow)
      setColumns((prev) => {
        const names = new Set(prev.map((c) => c.name));
        const added = res.columns.filter((c) => !names.has(c.name));
        return added.length > 0 ? [...prev, ...added] : prev;
      });

      setAllRows((prev) => [...prev, ...displayRows]);
      setPagination({
        offset: 0,
        limit: pageLimit,
        total: res.pagination.total,
        has_more: gotExtra,
      });
      setStats(res.stats);
    } catch {
      // silently fail on scroll-load errors
    } finally {
      setLoadingMore(false);
    }
  }, [pagination, loadingMore, columns]);

  const handleRowSelect = useCallback((index: number) => {
    setSelectedRow(index);
  }, []);

  const handleSidebarClose = useCallback(() => {
    setSelectedRow(null);
  }, []);

  const handleSidebarNavigate = useCallback((index: number) => {
    setSelectedRow(Math.max(0, Math.min(index, allRows.length - 1)));
  }, [allRows.length]);

  const timezones = getAvailableTimezones();

  return (
    <div className="app">
      <header className="app-header">
        <svg className="app-logo" viewBox="208 208 290 290" xmlns="http://www.w3.org/2000/svg" width="25" height="25">
          <g transform="translate(340,340)" fill="none" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round">
            <circle cx="-20" cy="-20" r="100" strokeWidth="10" />
            <line x1="62" y1="62" x2="135" y2="135" strokeWidth="18" />
            <g fill="currentColor" stroke="none">
              <circle cx="10" cy="-80" r="7" />
              <circle cx="-10" cy="-80" r="4.5" opacity="0.5" />
              <circle cx="-23" cy="-80" r="2.5" opacity="0.2" />
              <circle cx="-50" cy="-48" r="7" />
              <circle cx="-70" cy="-48" r="4.5" opacity="0.5" />
              <circle cx="-83" cy="-48" r="2.5" opacity="0.2" />
              <circle cx="42" cy="-52" r="7" />
              <circle cx="22" cy="-52" r="4.5" opacity="0.5" />
              <circle cx="9" cy="-52" r="2.5" opacity="0.2" />
              <circle cx="-28" cy="-18" r="7" />
              <circle cx="-48" cy="-18" r="4.5" opacity="0.5" />
              <circle cx="-61" cy="-18" r="2.5" opacity="0.2" />
              <circle cx="50" cy="-22" r="7" />
              <circle cx="30" cy="-22" r="4.5" opacity="0.5" />
              <circle cx="17" cy="-22" r="2.5" opacity="0.2" />
              <circle cx="-58" cy="12" r="7" />
              <circle cx="-78" cy="12" r="4.5" opacity="0.5" />
              <circle cx="30" cy="8" r="7" />
              <circle cx="10" cy="8" r="4.5" opacity="0.5" />
              <circle cx="-3" cy="8" r="2.5" opacity="0.2" />
              <circle cx="2" cy="40" r="7" />
              <circle cx="-18" cy="40" r="4.5" opacity="0.5" />
              <circle cx="-31" cy="40" r="2.5" opacity="0.2" />
              <circle cx="-15" cy="62" r="7" />
              <circle cx="-35" cy="62" r="4.5" opacity="0.5" />
            </g>
          </g>
        </svg>
        <h1 className="app-title">Flowcus</h1>
        <div className="header-actions">
          <button
            className="settings-trigger"
            onClick={() => setHealthOpen(true)}
            title="System Health"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M22 12h-2.48a2 2 0 0 0-1.93 1.46l-2.35 8.36a.25.25 0 0 1-.48 0L9.24 2.18a.25.25 0 0 0-.48 0l-2.35 8.36A2 2 0 0 1 4.49 12H2" />
            </svg>
          </button>
          <button
            className="settings-trigger"
            onClick={() => setSettingsOpen(true)}
            title="Settings"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z" />
              <circle cx="12" cy="12" r="3" />
            </svg>
          </button>
          <button
            className="settings-trigger"
            onClick={() => setAboutOpen(true)}
            title="About Flowcus"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M19 14c1.49-1.46 3-3.21 3-5.5A5.5 5.5 0 0 0 16.5 3c-1.76 0-3 .5-4.5 2-1.5-1.5-2.74-2-4.5-2A5.5 5.5 0 0 0 2 8.5c0 2.3 1.5 4.05 3 5.5l7 7Z" />
            </svg>
          </button>
          <ThemeSwitcher />
        </div>
      </header>

      <section className="query-section">
        <div className="query-bar">
          <TimeRangePicker
            value={timeRange}
            onChange={setTimeRange}
            refreshInterval={refreshInterval}
            onRefreshIntervalChange={setRefreshInterval}
          />

          <select
            className="tz-select"
            value={timezone}
            onChange={handleTimezoneChange}
            title="Timezone for timestamps"
          >
            {timezones.map((tz) => (
              <option key={tz} value={tz}>{tz}</option>
            ))}
          </select>
        </div>

        <SearchBar
          filters={filters}
          onChange={setFiltersKeepLimitLast}
          schema={schema}
          onExecute={handleExecute}
          onCancel={handleCancel}
          loading={loading}
        />

        {queryError && (
          <div className="query-error">
            <span className="query-error-icon">!</span>
            {queryError.error}
          </div>
        )}

        {stats && (
          <div className="query-stats-bar">
            <button
              className={`stats-toggle ${showStats ? 'active' : ''}`}
              onClick={() => setShowStats(!showStats)}
              title="Query stats & execution plan"
            >
              <svg width="14" height="14" viewBox="0 0 16 16" fill="currentColor">
                <path d="M8 1.5a6.5 6.5 0 100 13 6.5 6.5 0 000-13zM0 8a8 8 0 1116 0A8 8 0 010 8zm6.5-.25A.75.75 0 017.25 7h1a.75.75 0 01.75.75v2.75h.25a.75.75 0 010 1.5h-2a.75.75 0 010-1.5h.25v-2h-.25a.75.75 0 01-.75-.75zM8 6a1 1 0 100-2 1 1 0 000 2z"/>
              </svg>
            </button>
            <span className="stats-summary">
              {stats.rows_returned.toLocaleString()} rows
              {stats.total_rows > 0 && ` out of ${stats.total_rows.toLocaleString()}`}
              {' in '}{formatMicros(stats.execution_time_us)}
              {stats.cached ? ' (cached)' : ''}
            </span>
          </div>
        )}

        {showStats && stats && (
          <div className="stats-panel">
            {stats.cached && (
              <div className="stats-cache-hit">Served from cache</div>
            )}
            <div className="stats-grid">
              <div className="stats-cell">
                <span className="stat-label">Parse</span>
                <span className="stat-value">{formatMicros(stats.parse_time_us)}</span>
              </div>
              <div className="stats-cell">
                <span className="stat-label">Execute</span>
                <span className="stat-value">{formatMicros(stats.execution_time_us)}{stats.cached ? ' (cached)' : ''}</span>
              </div>
              <div className="stats-cell">
                <span className="stat-label">Scanned</span>
                <span className="stat-value">{stats.rows_scanned.toLocaleString()} rows</span>
              </div>
              <div className="stats-cell">
                <span className="stat-label">Returned</span>
                <span className="stat-value">{stats.rows_returned.toLocaleString()} rows</span>
              </div>
              <div className="stats-cell">
                <span className="stat-label">Parts</span>
                <span className="stat-value">{stats.parts_scanned} scanned, {stats.parts_skipped} skipped</span>
              </div>
              <div className="stats-cell">
                <span className="stat-label">Disk read</span>
                <span className="stat-value">{humanBytes(stats.bytes_read ?? 0)}</span>
              </div>
            </div>
            {explain && explain.length > 0 && (
              <div className="explain-section">
                <div className="explain-title">Execution plan</div>
                <ExplainGantt steps={explain as PlanStep[]} stats={stats} />
              </div>
            )}
          </div>
        )}
        <TimeHistogram
          timeRange={timeRange}
          filters={filters.map(f => coerceFilterValue(f, schema))}
          onTimeRangeChange={handleHistogramTimeChange}
          queryGen={queryGen}
          timezone={timezone}
        />
      </section>

      {(allRows.length > 0 || loading) && (
        <section className="results-section">
          <ResultsTable
            columns={columns}
            rows={allRows}
            pagination={pagination}
            onLoadMore={loadMore}
            loadingMore={loadingMore}
            onRowSelect={handleRowSelect}
            selectedRow={selectedRow}
            visibleColumns={visibleColumns}
            onColumnConfigChange={setVisibleColumns}
            onAddFilter={handleAddFilter}
          />
        </section>
      )}


      {selectedRow !== null && (
        <FlowSidebar
          columns={columns}
          rows={allRows}
          selectedIndex={selectedRow}
          onClose={handleSidebarClose}
          onNavigate={handleSidebarNavigate}
          totalRows={pagination?.total ?? allRows.length}
          onAddFilter={handleAddFilter}
        />
      )}

      <SettingsPanel open={settingsOpen} onClose={() => setSettingsOpen(false)} />
      <HealthPanel open={healthOpen} onClose={() => setHealthOpen(false)} />
      <AboutDialog open={aboutOpen} onClose={() => setAboutOpen(false)} version={info?.version} />
    </div>
  );
}
