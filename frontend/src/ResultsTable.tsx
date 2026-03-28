import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { Pagination, QueryColumn } from './api';

interface ResultsTableProps {
  columns: QueryColumn[];
  rows: unknown[][];
  pagination?: Pagination | null;
  onLoadMore?: () => void;
  loadingMore?: boolean;
}

type SortDir = 'asc' | 'desc' | null;

export function ResultsTable({
  columns,
  rows,
  pagination,
  onLoadMore,
  loadingMore,
}: ResultsTableProps) {
  const [sortCol, setSortCol] = useState<number | null>(null);
  const [sortDir, setSortDir] = useState<SortDir>(null);
  const sentinelRef = useRef<HTMLDivElement | null>(null);

  const handleSort = useCallback(
    (colIndex: number) => {
      if (sortCol === colIndex) {
        if (sortDir === 'asc') setSortDir('desc');
        else if (sortDir === 'desc') {
          setSortCol(null);
          setSortDir(null);
        } else setSortDir('asc');
      } else {
        setSortCol(colIndex);
        setSortDir('asc');
      }
    },
    [sortCol, sortDir],
  );

  const sortedRows = useMemo(() => {
    if (sortCol === null || sortDir === null) return rows;
    const col = sortCol;
    const dir = sortDir === 'asc' ? 1 : -1;
    return [...rows].sort((a, b) => {
      const va = a[col];
      const vb = b[col];
      if (va === null || va === undefined) return dir;
      if (vb === null || vb === undefined) return -dir;
      if (typeof va === 'number' && typeof vb === 'number') return (va - vb) * dir;
      return String(va).localeCompare(String(vb)) * dir;
    });
  }, [rows, sortCol, sortDir]);

  // Infinite scroll: observe sentinel element at bottom of table
  useEffect(() => {
    if (!sentinelRef.current || !onLoadMore) return;

    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && pagination?.has_more && !loadingMore) {
          onLoadMore();
        }
      },
      { threshold: 0.1 },
    );

    observer.observe(sentinelRef.current);
    return () => observer.disconnect();
  }, [onLoadMore, pagination?.has_more, loadingMore]);

  if (columns.length === 0) {
    return <div className="results-empty">No results to display.</div>;
  }

  if (rows.length === 0) {
    return (
      <div className="results-empty">
        <p>Query returned 0 rows.</p>
      </div>
    );
  }

  const sortIndicator = (colIndex: number) => {
    if (sortCol !== colIndex || sortDir === null) return '';
    return sortDir === 'asc' ? ' \u25B2' : ' \u25BC';
  };

  return (
    <div className="results-table-wrapper">
      <table className="results-table">
        <thead>
          <tr>
            {columns.map((col, i) => (
              <th
                key={col.name}
                onClick={() => handleSort(i)}
                className="results-th"
                title={`${col.name} (${col.type}) — click to sort`}
              >
                {col.name}
                <span className="sort-indicator">{sortIndicator(i)}</span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sortedRows.map((row, ri) => (
            <tr key={ri}>
              {row.map((cell, ci) => (
                <td key={ci} className="results-td">
                  {formatCell(cell)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>

      {/* Infinite scroll sentinel + status */}
      <div className="results-scroll-status">
        {loadingMore && <div className="scroll-loading">Loading more rows...</div>}
        {pagination && (
          <div className="scroll-info">
            Showing {rows.length.toLocaleString()} of {pagination.total.toLocaleString()} rows
            {pagination.has_more && !loadingMore && (
              <span className="scroll-hint"> — scroll down for more</span>
            )}
          </div>
        )}
        {/* Invisible sentinel that triggers loading when scrolled into view */}
        <div ref={sentinelRef} className="scroll-sentinel" />
      </div>
    </div>
  );
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  if (typeof value === 'number') {
    if (Number.isInteger(value) && Math.abs(value) >= 1000) {
      return value.toLocaleString();
    }
    if (!Number.isInteger(value)) {
      return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
    }
    return String(value);
  }
  return String(value);
}
