import { useMemo } from 'react';
import type { QueryStats } from './api';

interface ExplainGanttProps {
  steps: PlanStep[];
  stats: QueryStats;
}

type PlanStep =
  | { type: 'TimeRangePrune'; start: number; end: number; parts_before: number; parts_after: number; duration_us?: number | null }
  | { type: 'ColumnIndexFilter'; column: string; predicate: string; parts_skipped: number; duration_us?: number | null }
  | { type: 'BloomFilter'; column: string; value: string; granules_skipped: number; duration_us?: number | null }
  | { type: 'MarkSeek'; column: string; granule_range: [number, number]; duration_us?: number | null }
  | { type: 'ColumnRead'; column: string; bytes: number; duration_us?: number | null }
  | { type: 'FilterApply'; expression: string; rows_before: number; rows_after: number; duration_us?: number | null }
  | { type: 'Aggregate'; function: string; groups: number; duration_us?: number | null }
  | { type: 'ParallelScan'; parts_count: number; duration_us?: number | null }
  | { type: 'PartSkippedMerge'; path: string }
  | { type: 'PartSkippedIncomplete'; path: string }
  | { type: 'PartSkippedMerging'; path: string }
  | { type: 'PartSkippedSubsumed'; path: string; subsumed_by: string }
  | { type: 'StatsShortcut'; parts_used: number; duration_us?: number | null }
  | { type: 'TopNFastPath'; sort_field: string; n: number; parts_scanned: number; rows_scanned: number; duration_us?: number | null }
  | { type: 'CacheStats'; hits: number; misses: number };

interface GanttRow {
  label: string;
  durationUs: number;
  color: string;
  badge?: string;
  parallel?: boolean;  // rendered inside the parallel block
}

const COLORS: Record<string, string> = {
  prune: '#5b7fb8', index: '#7c6ca8', bloom: '#5e9e7a', seek: '#5a8e9e',
  read: '#c49a5c', filter: '#b8a040', aggregate: '#c46b6b',
  skip: '#9494a8', shortcut: '#5e9e7a', parallel: '#5b7fb8',
};

function humanBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function formatUs(us: number): string {
  if (us < 1000) return `${Math.round(us)}\u00B5s`;
  if (us < 1_000_000) return `${(us / 1000).toFixed(1)}ms`;
  return `${(us / 1_000_000).toFixed(2)}s`;
}

function stepDur(s: PlanStep): number | null {
  if ('duration_us' in s && typeof s.duration_us === 'number') return s.duration_us;
  return null;
}

function hasRealTiming(steps: PlanStep[]): boolean {
  return steps.some(s => stepDur(s) !== null);
}

function buildRows(steps: PlanStep[], totalUs: number): GanttRow[] {
  const rows: GanttRow[] = [];
  const useReal = hasRealTiming(steps);

  // Estimate weights when no real timing
  let totalBytes = 0, totalRowsProc = 0;
  if (!useReal) {
    for (const s of steps) {
      if (s.type === 'ColumnRead') totalBytes += s.bytes;
      if (s.type === 'FilterApply') totalRowsProc += s.rows_before;
    }
  }
  const io = 0.6, cpu = 0.3, meta = 0.1;
  const est = (weight: number) => weight * totalUs;

  // Find the ParallelScan step to determine its scope
  const parallelStep = steps.find(s => s.type === 'ParallelScan') as
    (PlanStep & { type: 'ParallelScan' }) | undefined;
  const parallelIdx = steps.findIndex(s => s.type === 'ParallelScan');

  // Collect parallel-phase stats (steps that came from inside parts)
  let parallelReads = 0, parallelBlooms = 0, parallelFilters = 0;
  let parallelBytesRead = 0, parallelRowsScanned = 0, parallelRowsMatched = 0;
  if (parallelIdx >= 0) {
    for (let i = parallelIdx + 1; i < steps.length; i++) {
      const s = steps[i];
      // Stop when we hit a non-part step (like Aggregate)
      if (s.type === 'Aggregate' || s.type === 'StatsShortcut' || s.type === 'TopNFastPath') break;
      if (s.type === 'ColumnRead') { parallelReads++; parallelBytesRead += s.bytes; }
      if (s.type === 'BloomFilter') parallelBlooms++;
      if (s.type === 'FilterApply') {
        parallelFilters++;
        parallelRowsScanned += s.rows_before;
        parallelRowsMatched += s.rows_after;
      }
    }
  }

  // Build sequential pre-parallel steps
  for (let i = 0; i < steps.length; i++) {
    const s = steps[i];
    if (i === parallelIdx) {
      // The parallel scan block — single row representing all parts
      const dur = stepDur(s) ?? est(io + cpu * 0.5);
      const parts = parallelStep?.parts_count ?? 0;
      const summary = [
        `${parts} parts`,
        parallelBytesRead > 0 ? humanBytes(parallelBytesRead) + ' read' : null,
        parallelRowsScanned > 0 ? `${parallelRowsScanned.toLocaleString()} rows scanned` : null,
        parallelRowsMatched > 0 ? `${parallelRowsMatched.toLocaleString()} matched` : null,
      ].filter(Boolean).join(', ');

      rows.push({
        label: `Parallel scan: ${parts} parts`,
        durationUs: dur,
        color: COLORS.parallel,
        badge: summary,
        parallel: true,
      });

      // Skip all per-part steps that follow
      while (i + 1 < steps.length) {
        const next = steps[i + 1];
        if (next.type === 'Aggregate' || next.type === 'StatsShortcut' || next.type === 'TopNFastPath') break;
        if (next.type === 'TimeRangePrune') break;  // shouldn't happen but safety
        i++;
      }
      continue;
    }

    switch (s.type) {
      case 'TimeRangePrune': {
        const pruned = s.parts_before - s.parts_after;
        rows.push({ label: 'Time range prune', durationUs: stepDur(s) ?? est(meta * 0.4), color: COLORS.prune, badge: pruned > 0 ? `${s.parts_after} parts (${pruned} pruned)` : `${s.parts_after} parts` });
        break;
      }
      case 'ColumnIndexFilter':
        rows.push({ label: `Column index filter: ${s.column}`, durationUs: stepDur(s) ?? est(meta * 0.3), color: COLORS.index, badge: s.parts_skipped > 0 ? `-${s.parts_skipped} parts` : undefined });
        break;
      case 'BloomFilter':
        rows.push({ label: `Bloom filter: ${s.column}`, durationUs: stepDur(s) ?? est(meta * 0.3), color: COLORS.bloom, badge: s.granules_skipped > 0 ? `-${s.granules_skipped} granules` : undefined });
        break;
      case 'MarkSeek':
        rows.push({ label: `Mark seek: ${s.column}`, durationUs: stepDur(s) ?? est(meta * 0.1), color: COLORS.seek, badge: `granules ${s.granule_range[0]}\u2013${s.granule_range[1]}` });
        break;
      case 'ColumnRead': {
        const pct = totalBytes > 0 ? s.bytes / totalBytes : 0.5;
        rows.push({ label: `Column read: ${s.column}`, durationUs: stepDur(s) ?? est(io * pct), color: COLORS.read, badge: humanBytes(s.bytes) });
        break;
      }
      case 'FilterApply': {
        const sel = s.rows_before > 0 ? ((s.rows_after / s.rows_before) * 100).toFixed(1) : '0';
        const pct = totalRowsProc > 0 ? s.rows_before / totalRowsProc : 0.5;
        rows.push({ label: `Filter: ${s.expression}`, durationUs: stepDur(s) ?? est(cpu * pct), color: COLORS.filter, badge: `${s.rows_before.toLocaleString()} \u2192 ${s.rows_after.toLocaleString()} (${sel}%)` });
        break;
      }
      case 'Aggregate':
        rows.push({ label: `Aggregate: ${s.function}`, durationUs: stepDur(s) ?? est(cpu * 0.3), color: COLORS.aggregate, badge: `${s.groups.toLocaleString()} groups` });
        break;
      case 'StatsShortcut':
        rows.push({ label: 'Stats shortcut (index-only)', durationUs: stepDur(s) ?? est(0.9), color: COLORS.shortcut, badge: `${s.parts_used} parts` });
        break;
      case 'TopNFastPath':
        rows.push({ label: `TopN fast path: ${s.sort_field}`, durationUs: stepDur(s) ?? est(0.8), color: COLORS.shortcut, badge: `top ${s.n}, ${s.rows_scanned.toLocaleString()} rows` });
        break;
      // Skip part-skipped steps entirely
      default:
        break;
    }
  }

  return rows;
}

export function ExplainGantt({ steps, stats }: ExplainGanttProps) {
  const rows = useMemo(() => buildRows(steps, stats.execution_time_us), [steps, stats.execution_time_us]);

  if (rows.length === 0) {
    return <div className="gantt-empty">No execution steps recorded.</div>;
  }

  const sumUs = rows.reduce((s, r) => s + r.durationUs, 0) || 1;
  let offset = 0;

  const totalBytesRead = stats.bytes_read ?? steps.reduce(
    (acc, s) => s.type === 'ColumnRead' ? acc + s.bytes : acc, 0,
  );

  // Extract cache stats from steps
  const cacheStep = steps.find(s => s.type === 'CacheStats') as
    (PlanStep & { type: 'CacheStats' }) | undefined;
  const cacheHits = cacheStep?.hits ?? 0;
  const cacheMisses = cacheStep?.misses ?? 0;
  const cacheTotal = cacheHits + cacheMisses;

  return (
    <div className="gantt-container">
      {rows.map((row, i) => {
        const widthPct = Math.max((row.durationUs / sumUs) * 100, 2);
        const offsetPct = (offset / sumUs) * 100;
        offset += row.durationUs;

        return (
          <div key={i} className={`gantt-row ${row.parallel ? 'gantt-parallel' : ''}`} title={row.badge ?? row.label}>
            <div className="gantt-label">
              <span className="gantt-label-text">{row.label}</span>
              <span className="gantt-est">{formatUs(row.durationUs)}</span>
            </div>
            <div className="gantt-track">
              <div
                className={`gantt-bar ${row.parallel ? 'gantt-bar-parallel' : ''}`}
                style={{
                  left: `${offsetPct}%`,
                  width: `${widthPct}%`,
                  backgroundColor: row.color,
                }}
              >
                {row.badge && <span className="gantt-badge">{row.badge}</span>}
              </div>
            </div>
          </div>
        );
      })}
      <div className="gantt-row gantt-total">
        <div className="gantt-label">
          <span className="gantt-label-text">Total</span>
          <span className="gantt-est gantt-est-total">{formatUs(stats.execution_time_us)}</span>
        </div>
        <div className="gantt-track">
          <div className="gantt-bar gantt-bar-total" style={{ width: '100%' }} />
        </div>
      </div>
      <div className="gantt-summary">
        {totalBytesRead > 0 && <>Disk: {humanBytes(totalBytesRead)} {' \u00b7 '}</>}
        {stats.rows_scanned.toLocaleString()} rows scanned
        {' \u00b7 '}
        {stats.parts_scanned} parts
        {cacheTotal > 0 && (
          <>
            {' \u00b7 '}
            <span className={cacheHits > 0 ? 'gantt-cache-hit' : ''}>
              Storage cache: {cacheHits}/{cacheTotal} hits
              {cacheHits > 0 && ` (${((cacheHits / cacheTotal) * 100).toFixed(0)}%)`}
            </span>
          </>
        )}
      </div>
    </div>
  );
}

export type { PlanStep };
