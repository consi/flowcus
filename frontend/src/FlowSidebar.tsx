import { useCallback, useEffect, useMemo, useState, useRef } from 'react';
import type { QueryColumn } from './api';
import { fetchRowDetail } from './api';
import { getSidebarFormatter, getColumnLabel, computeDerivedFields } from './formatters';

interface FlowSidebarProps {
  columns: QueryColumn[];
  rows: unknown[][];
  selectedIndex: number;
  onClose: () => void;
  onNavigate: (index: number) => void;
  totalRows: number;
  onAddFilter?: (field: string, value: unknown, negated: boolean) => void;
  rowId?: string | null;
}

export function FlowSidebar({
  columns,
  rows,
  selectedIndex,
  onClose,
  onNavigate,
  totalRows: _totalRows,
  onAddFilter,
  rowId,
}: FlowSidebarProps) {
  const row = rows[selectedIndex];

  const canPrev = selectedIndex > 0;
  const canNext = selectedIndex < rows.length - 1;

  const go = useCallback((delta: number) => {
    const next = selectedIndex + delta;
    if (next >= 0 && next < rows.length) onNavigate(next);
  }, [selectedIndex, rows.length, onNavigate]);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      const tag = (e.target as HTMLElement)?.tagName;
      if (tag === 'TEXTAREA' || tag === 'INPUT' || tag === 'SELECT') return;

      if (e.key === 'Escape') { onClose(); return; }
      if (e.key === 'ArrowUp' || e.key === 'k') { e.preventDefault(); go(-1); }
      if (e.key === 'ArrowDown' || e.key === 'j') { e.preventDefault(); go(1); }
      if (e.key === 'PageUp') { e.preventDefault(); go(-10); }
      if (e.key === 'PageDown') { e.preventDefault(); go(10); }
      if (e.key === 'Home') { e.preventDefault(); onNavigate(0); }
      if (e.key === 'End') { e.preventDefault(); onNavigate(rows.length - 1); }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [go, onClose, onNavigate, rows.length]);

  // Compute derived fields (absolute flow times, duration, rates, avg pkt size)
  const derived = useMemo(
    () => computeDerivedFields(columns, row),
    [columns, row],
  );

  // Lazy-load full row details when rowId is available
  const [detailValues, setDetailValues] = useState<Record<string, unknown> | null>(null);
  const [detailColumns, setDetailColumns] = useState<QueryColumn[]>([]);
  const [detailLoading, setDetailLoading] = useState(false);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const lastFetchedId = useRef<string | null>(null);

  useEffect(() => {
    if (!rowId || rowId === lastFetchedId.current) return;

    setDetailLoading(true);
    if (debounceRef.current) clearTimeout(debounceRef.current);

    debounceRef.current = setTimeout(async () => {
      try {
        const detail = await fetchRowDetail(rowId);
        lastFetchedId.current = rowId;
        setDetailValues(detail.values);
        setDetailColumns(detail.columns);
      } catch {
        // Silently fail — show partial data from list query
      } finally {
        setDetailLoading(false);
      }
    }, 150);

    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, [rowId]);

  // Reset detail state when selectedIndex changes and rowId changes
  useEffect(() => {
    lastFetchedId.current = null;
    setDetailValues(null);
    setDetailColumns([]);
  }, [selectedIndex]);

  // Build merged columns: list-query columns + any extra from detail
  const mergedColumns = useMemo(() => {
    const listNames = new Set(columns.map(c => c.name));
    const extra = detailColumns.filter(c => !listNames.has(c.name) && c.name !== 'flowcusRowId');
    return [...columns.filter(c => c.name !== 'flowcusRowId'), ...extra];
  }, [columns, detailColumns]);

  // Get value for a column — prefer detail values if available
  const getValue = (colName: string, colIdx: number): unknown => {
    if (detailValues && colName in detailValues) return detailValues[colName];
    if (row && colIdx >= 0 && colIdx < row.length) return row[colIdx];
    return null;
  };

  // Get flowcusRowId value
  const rowIdValue = useMemo(() => {
    if (detailValues && 'flowcusRowId' in detailValues) return detailValues['flowcusRowId'];
    if (row) {
      const idx = columns.findIndex(c => c.name === 'flowcusRowId');
      if (idx >= 0 && idx < row.length) return row[idx];
    }
    return rowId ?? null;
  }, [columns, row, detailValues, rowId]);

  if (!row) return null;

  return (
    <aside className="flow-sidebar">
      <div className="sidebar-header">
        <div className="sidebar-title">Flow details</div>
        <button className="sidebar-close" onClick={onClose} title="Close (Esc)">
          {'\u2715'}
        </button>
      </div>

      <nav className="sidebar-nav">
        <button disabled={!canPrev} onClick={() => go(-100)} title="-100">
          {'\u00ab'}
        </button>
        <button disabled={!canPrev} onClick={() => go(-10)} title="-10">
          {'\u2039\u2039'}
        </button>
        <button disabled={!canPrev} onClick={() => go(-1)} title="Previous">
          {'\u2039'}
        </button>
        <span className="sidebar-nav-pos">{selectedIndex + 1}</span>
        <button disabled={!canNext} onClick={() => go(1)} title="Next">
          {'\u203a'}
        </button>
        <button disabled={!canNext} onClick={() => go(10)} title="+10">
          {'\u203a\u203a'}
        </button>
        <button disabled={!canNext} onClick={() => go(100)} title="+100">
          {'\u00bb'}
        </button>
      </nav>

      <div className="sidebar-fields">
        {/* Derived fields (computed from raw data) */}
        {derived.length > 0 && (
          <>
            {derived.map((f) => (
              <div key={f.label} className="sidebar-field sidebar-field-computed">
                <div className="sidebar-field-label">
                  {f.label}
                </div>
                <div className="sidebar-field-value">
                  {f.value}
                </div>
              </div>
            ))}
            <div className="sidebar-divider" />
          </>
        )}

        {mergedColumns.map((col) => {
          const formatter = getSidebarFormatter(col.name);
          const raw = getValue(col.name, columns.findIndex(c => c.name === col.name));
          const formatted = formatter(raw);
          const isNull = raw === null || raw === undefined;

          return (
            <div key={col.name} className="sidebar-field">
              <div className="sidebar-field-label">
                {getColumnLabel(col.name)}
                <span className="sidebar-field-name">{col.name}</span>
              </div>
              <div className="sidebar-field-value-row">
                <div className={`sidebar-field-value ${isNull ? 'null-value' : ''}`}>
                  {formatted}
                  {!isNull && formatted !== String(raw) && (
                    <span className="sidebar-field-raw">{String(raw)}</span>
                  )}
                </div>
                {onAddFilter && !isNull && (
                  <div className="sidebar-filter-actions">
                    <button className="sidebar-filter-btn include" title={`Filter by ${col.name} = ${String(raw)}`}
                      onClick={() => onAddFilter(col.name, raw, false)}>+</button>
                    <button className="sidebar-filter-btn exclude" title={`Filter out ${col.name} = ${String(raw)}`}
                      onClick={() => onAddFilter(col.name, raw, true)}>{'\u2212'}</button>
                  </div>
                )}
              </div>
            </div>
          );
        })}

        {detailLoading && (
          <div className="sidebar-field sidebar-field-computed">
            <div className="sidebar-field-value">Loading details...</div>
          </div>
        )}

        {/* Flowcus Row UUID — special field at the bottom */}
        {rowIdValue != null && (
          <>
            <div className="sidebar-divider" />
            <div className="sidebar-field sidebar-field-computed">
              <div className="sidebar-field-label">
                Flowcus Row UUID
                <span className="sidebar-field-name">flowcusRowId</span>
              </div>
              <div className="sidebar-field-value">
                {String(rowIdValue)}
              </div>
            </div>
          </>
        )}
      </div>

      <div className="sidebar-footer">
        <span className="sidebar-hint">
          {'\u2191\u2193'} navigate &middot; PgUp/PgDn &plusmn;10 &middot; Esc close
        </span>
      </div>
    </aside>
  );
}
