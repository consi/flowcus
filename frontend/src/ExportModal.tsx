import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import type { QueryColumn } from './api';
import { fetchRowDetail } from './api';

type ExportFormat = 'ndjson' | 'json' | 'csv' | 'tsv' | 'ltsv';

interface ExportModalProps {
  open: boolean;
  onClose: () => void;
  columns: QueryColumn[];
  visibleColumns: string[];
  rows: unknown[][];
}

const FORMAT_OPTIONS: { value: ExportFormat; label: string; ext: string }[] = [
  { value: 'ndjson', label: 'NDJSON', ext: 'ndjson' },
  { value: 'json', label: 'JSON', ext: 'json' },
  { value: 'csv', label: 'CSV', ext: 'csv' },
  { value: 'tsv', label: 'TSV', ext: 'tsv' },
  { value: 'ltsv', label: 'LTSV', ext: 'ltsv' },
];

function csvEscape(val: string): string {
  if (val.includes(',') || val.includes('"') || val.includes('\n') || val.includes('\r')) {
    return `"${val.replace(/"/g, '""')}"`;
  }
  return val;
}

function tsvEscape(val: string): string {
  return val.replace(/[\t\n\r]/g, ' ');
}

function formatRow(
  values: Record<string, unknown>,
  cols: string[],
  format: ExportFormat,
): string {
  switch (format) {
    case 'ndjson':
    case 'json': {
      const obj: Record<string, unknown> = {};
      for (const c of cols) {
        if (c in values) obj[c] = values[c];
      }
      return JSON.stringify(obj);
    }
    case 'csv':
      return cols.map((c) => csvEscape(String(values[c] ?? ''))).join(',');
    case 'tsv':
      return cols.map((c) => tsvEscape(String(values[c] ?? ''))).join('\t');
    case 'ltsv':
      return cols
        .filter((c) => c in values && values[c] != null)
        .map((c) => `${c}:${String(values[c]).replace(/[\t\n\r]/g, ' ')}`)
        .join('\t');
  }
}

function buildOutput(
  rows: string[],
  cols: string[],
  format: ExportFormat,
): string {
  switch (format) {
    case 'ndjson':
      return rows.join('\n') + '\n';
    case 'json':
      return '[\n' + rows.map((r) => '  ' + r).join(',\n') + '\n]\n';
    case 'csv':
      return cols.map((c) => csvEscape(c)).join(',') + '\n' + rows.join('\n') + '\n';
    case 'tsv':
      return cols.map((c) => tsvEscape(c)).join('\t') + '\n' + rows.join('\n') + '\n';
    case 'ltsv':
      return rows.join('\n') + '\n';
  }
}

/** Build a Record<string, unknown> from the in-memory row array. */
function rowToRecord(row: unknown[], columns: QueryColumn[]): Record<string, unknown> {
  const rec: Record<string, unknown> = {};
  for (let i = 0; i < columns.length; i++) {
    if (i < row.length && row[i] != null) rec[columns[i].name] = row[i];
  }
  return rec;
}

async function fetchWithConcurrency(
  rows: unknown[][],
  rowIdColIdx: number,
  columns: QueryColumn[],
  selectedCols: string[],
  format: ExportFormat,
  concurrency: number,
  onProgress: (completed: number) => void,
  signal: AbortSignal,
): Promise<string[]> {
  const results: (string | null)[] = new Array(rows.length).fill(null);
  let nextIdx = 0;
  let completed = 0;

  async function worker() {
    while (nextIdx < rows.length) {
      if (signal.aborted) return;
      const idx = nextIdx++;
      const row = rows[idx];
      const rowId = rowIdColIdx >= 0 ? (row[rowIdColIdx] as string | null) : null;

      let values: Record<string, unknown>;
      if (rowId) {
        try {
          const detail = await fetchRowDetail(rowId);
          if (signal.aborted) return;
          values = detail.values as Record<string, unknown>;
        } catch {
          // Fetch failed (merge race, etc.) — fall back to in-memory data
          values = rowToRecord(row, columns);
        }
      } else {
        // No rowId available (v1 parts) — use in-memory data directly
        values = rowToRecord(row, columns);
      }

      results[idx] = formatRow(values, selectedCols, format);
      completed++;
      onProgress(completed);
    }
  }

  const workers = Array.from({ length: Math.min(concurrency, rows.length) }, () => worker());
  await Promise.all(workers);
  return results.filter((r): r is string => r !== null);
}

export function ExportModal({
  open,
  onClose,
  columns,
  visibleColumns,
  rows,
}: ExportModalProps) {
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);
  const [format, setFormat] = useState<ExportFormat>('ndjson');
  const [search, setSearch] = useState('');
  const [exporting, setExporting] = useState(false);
  const [progress, setProgress] = useState(0);
  const [total, setTotal] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const modalRef = useRef<HTMLDivElement>(null);

  // Reset state when modal opens
  useEffect(() => {
    if (open) {
      setSelectedColumns(visibleColumns.length > 0 ? [...visibleColumns] : columns.map((c) => c.name));
      setSearch('');
      setExporting(false);
      setProgress(0);
      setTotal(0);
      setError(null);
    }
  }, [open, visibleColumns, columns]);

  // Close on Escape
  useEffect(() => {
    if (!open) return;
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !exporting) onClose();
    };
    document.addEventListener('keydown', handleKey);
    return () => document.removeEventListener('keydown', handleKey);
  }, [open, exporting, onClose]);

  const handleOverlayClick = useCallback(
    (e: React.MouseEvent) => {
      if (!exporting && modalRef.current && !modalRef.current.contains(e.target as Node)) {
        onClose();
      }
    },
    [onClose, exporting],
  );

  const selectedSet = useMemo(() => new Set(selectedColumns), [selectedColumns]);

  const filteredColumns = useMemo(() => {
    if (!search) return columns;
    const q = search.toLowerCase();
    return columns.filter((c) => c.name.toLowerCase().includes(q));
  }, [columns, search]);

  const toggleColumn = useCallback(
    (name: string) => {
      if (selectedSet.has(name)) {
        setSelectedColumns((prev) => prev.filter((c) => c !== name));
      } else {
        setSelectedColumns((prev) => [...prev, name]);
      }
    },
    [selectedSet],
  );

  const selectAll = useCallback(() => {
    setSelectedColumns(columns.map((c) => c.name));
  }, [columns]);

  const resetToDefault = useCallback(() => {
    setSelectedColumns(visibleColumns.length > 0 ? [...visibleColumns] : columns.map((c) => c.name));
  }, [visibleColumns, columns]);

  const rowIdColIdx = useMemo(
    () => columns.findIndex((c) => c.name === 'flowcusRowId'),
    [columns],
  );

  const handleExport = useCallback(async () => {
    if (selectedColumns.length === 0 || rows.length === 0) return;

    const controller = new AbortController();
    abortRef.current = controller;
    setExporting(true);
    setProgress(0);
    setTotal(rows.length);
    setError(null);

    try {
      const formatted = await fetchWithConcurrency(
        rows,
        rowIdColIdx,
        columns,
        selectedColumns,
        format,
        5,
        (n) => setProgress(n),
        controller.signal,
      );

      if (controller.signal.aborted) return;

      const output = buildOutput(formatted, selectedColumns, format);
      const formatInfo = FORMAT_OPTIONS.find((f) => f.value === format)!;
      const mime = format === 'json' ? 'application/json' : 'text/plain';
      const blob = new Blob([output], { type: `${mime};charset=utf-8` });
      const url = URL.createObjectURL(blob);
      const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
      const a = document.createElement('a');
      a.href = url;
      a.download = `flowcus-export-${ts}.${formatInfo.ext}`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
      onClose();
    } catch (err) {
      if (!controller.signal.aborted) {
        setError(
          err instanceof Error ? err.message
          : typeof err === 'object' && err !== null && 'error' in err ? String((err as Record<string, unknown>).error)
          : String(err),
        );
      }
    } finally {
      if (!controller.signal.aborted) {
        setExporting(false);
      }
    }
  }, [selectedColumns, rows, rowIdColIdx, columns, format, onClose]);

  const handleCancel = useCallback(() => {
    if (abortRef.current) {
      abortRef.current.abort();
      abortRef.current = null;
    }
    setExporting(false);
  }, []);

  if (!open) return null;

  const pct = total > 0 ? Math.round((progress / total) * 100) : 0;

  return createPortal(
    <div className="settings-overlay" onClick={handleOverlayClick}>
      <div className="export-modal" ref={modalRef}>
        <div className="export-header">
          <h2>
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ marginRight: 8, verticalAlign: -3 }}>
              <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
              <polyline points="7 10 12 15 17 10" />
              <line x1="12" y1="15" x2="12" y2="3" />
            </svg>
            Export {rows.length} rows
          </h2>
          <button className="settings-close" onClick={exporting ? handleCancel : onClose} title="Close">{'\u00D7'}</button>
        </div>

        <div className="export-body">
          {/* Column picker */}
          <div className="export-section">
            <div className="export-section-title">
              Columns
              <span className="export-column-count">{selectedColumns.length}/{columns.length}</span>
            </div>
            <input
              type="text"
              className="export-column-search"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search columns..."
              disabled={exporting}
            />
            <div className="export-column-list">
              {filteredColumns.map((col) => (
                <label key={col.name} className="export-column-item">
                  <input
                    type="checkbox"
                    checked={selectedSet.has(col.name)}
                    onChange={() => toggleColumn(col.name)}
                    disabled={exporting}
                  />
                  <span className="export-column-name">{col.name}</span>
                  <span className="export-column-type">{col.type}</span>
                </label>
              ))}
              {filteredColumns.length === 0 && (
                <div className="export-column-empty">No matching columns</div>
              )}
            </div>
            <div className="export-column-actions">
              <button onClick={selectAll} disabled={exporting}>Select all</button>
              <button onClick={resetToDefault} disabled={exporting}>Reset</button>
            </div>
          </div>

          {/* Format picker */}
          <div className="export-section">
            <div className="export-section-title">Format</div>
            <select
              className="export-format-select"
              value={format}
              onChange={(e) => setFormat(e.target.value as ExportFormat)}
              disabled={exporting}
            >
              {FORMAT_OPTIONS.map((f) => (
                <option key={f.value} value={f.value}>{f.label}</option>
              ))}
            </select>
          </div>

          {/* Progress */}
          {exporting && (
            <div className="export-section">
              <div className="export-progress-label">{progress}/{total} rows ({pct}%)</div>
              <div className="export-progress-track">
                <div className="export-progress-fill" style={{ width: `${pct}%` }} />
              </div>
            </div>
          )}

          {error && (
            <div className="export-error">{error}</div>
          )}

          {/* Actions */}
          <div className="export-actions">
            {exporting ? (
              <button className="export-cancel-btn" onClick={handleCancel}>Cancel</button>
            ) : (
              <button
                className="export-start-btn"
                onClick={handleExport}
                disabled={selectedColumns.length === 0 || rows.length === 0}
              >
                Export
              </button>
            )}
          </div>
        </div>
      </div>
    </div>,
    document.body,
  );
}
