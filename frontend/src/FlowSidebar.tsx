import { useCallback, useEffect, useMemo } from 'react';
import type { QueryColumn } from './api';
import { getSidebarFormatter, getColumnLabel, computeDerivedFields } from './formatters';

interface FlowSidebarProps {
  columns: QueryColumn[];
  rows: unknown[][];
  selectedIndex: number;
  onClose: () => void;
  onNavigate: (index: number) => void;
  totalRows: number;
  onAddFilter?: (field: string, value: unknown, negated: boolean) => void;
}

export function FlowSidebar({
  columns,
  rows,
  selectedIndex,
  onClose,
  onNavigate,
  totalRows,
  onAddFilter,
}: FlowSidebarProps) {
  const row = rows[selectedIndex];
  if (!row) return null;

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

  return (
    <aside className="flow-sidebar">
      <div className="sidebar-header">
        <div className="sidebar-title">
          Flow #{selectedIndex + 1}
          <span className="sidebar-total"> of {totalRows.toLocaleString()}</span>
        </div>
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

        {columns.map((col, i) => {
          const formatter = getSidebarFormatter(col.name);
          const raw = row[i];
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
      </div>

      <div className="sidebar-footer">
        <span className="sidebar-hint">
          {'\u2191\u2193'} navigate &middot; PgUp/PgDn &plusmn;10 &middot; Esc close
        </span>
      </div>
    </aside>
  );
}
