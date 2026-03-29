import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { Pagination, QueryColumn } from './api';
import { getFormatter, getColumnLabel, selectVisibleColumns } from './formatters';
import { ColumnConfig } from './ColumnConfig';

interface ResultsTableProps {
  columns: QueryColumn[];
  rows: unknown[][];
  pagination?: Pagination | null;
  onLoadMore?: () => void;
  loadingMore?: boolean;
  onRowSelect?: (index: number) => void;
  selectedRow?: number | null;
  visibleColumns?: string[] | null;
  onColumnConfigChange?: (columns: string[]) => void;
  onAddFilter?: (field: string, value: unknown, negated: boolean) => void;
}

type SortDir = 'asc' | 'desc' | null;

export function ResultsTable({
  columns,
  rows,
  pagination,
  onLoadMore,
  loadingMore,
  onRowSelect,
  selectedRow,
  visibleColumns,
  onColumnConfigChange,
  onAddFilter,
}: ResultsTableProps) {
  const [sortCol, setSortCol] = useState<number | null>(null);
  const [sortDir, setSortDir] = useState<SortDir>(null);
  const sentinelRef = useRef<HTMLDivElement | null>(null);

  const visibleIndices = useMemo(() => {
    if (visibleColumns && visibleColumns.length > 0) {
      // Map visible column names to indices, filtering to those that exist in result
      const nameToIndex = new Map(columns.map((c, i) => [c.name, i]));
      const indices = visibleColumns
        .map((name) => nameToIndex.get(name))
        .filter((i): i is number => i !== undefined);
      if (indices.length > 0) return indices;
    }
    return selectVisibleColumns(columns);
  }, [columns, visibleColumns]);

  // Derive the current visible column names for the config component
  const visibleColumnNames = useMemo(
    () => visibleIndices.map((i) => columns[i].name),
    [visibleIndices, columns],
  );

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

  // Infinite scroll
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

  const allVisible = visibleIndices.length === columns.length;

  const sortIndicator = (colIndex: number) => {
    if (sortCol !== colIndex || sortDir === null) return '';
    return sortDir === 'asc' ? ' \u25B2' : ' \u25BC';
  };

  return (
    <div className="results-table-wrapper">
      <table className="results-table">
        <thead>
          <tr>
            {onColumnConfigChange && (
              <th className="results-th results-th-config">
                <ColumnConfig
                  columns={columns}
                  visibleColumns={visibleColumnNames}
                  onChange={onColumnConfigChange}
                />
              </th>
            )}
            {visibleIndices.map((ci) => {
              const col = columns[ci];
              return (
                <th
                  key={col.name}
                  onClick={() => handleSort(ci)}
                  className="results-th"
                  title={`${col.name} (${col.type}) \u2014 click to sort`}
                >
                  {getColumnLabel(col.name)}
                  <span className="sort-indicator">{sortIndicator(ci)}</span>
                </th>
              );
            })}
            {!allVisible && !onColumnConfigChange && <th className="results-th results-th-more">&hellip;</th>}
          </tr>
        </thead>
        <tbody>
          {sortedRows.map((row, ri) => {
            const isSelected = selectedRow === ri;
            return (
              <tr
                key={ri}
                className={`results-row ${isSelected ? 'selected' : ''}`}
                onClick={() => onRowSelect?.(ri)}
              >
                {onColumnConfigChange && <td className="results-td results-td-config" />}
                {visibleIndices.map((ci) => {
                  const col = columns[ci];
                  const formatter = getFormatter(col.name);
                  return (
                    <td key={ci} className="results-td">
                      <span className="results-td-content">{formatter(row[ci])}</span>
                      {onAddFilter && row[ci] != null && (
                        <span className="results-td-filters">
                          <button className="td-filter-btn include" title={`Filter by ${col.name}`}
                            onClick={(e) => { e.stopPropagation(); onAddFilter(col.name, row[ci], false); }}>+</button>
                          <button className="td-filter-btn exclude" title={`Filter out ${col.name}`}
                            onClick={(e) => { e.stopPropagation(); onAddFilter(col.name, row[ci], true); }}>{'\u2212'}</button>
                        </span>
                      )}
                    </td>
                  );
                })}
                {!allVisible && !onColumnConfigChange && (
                  <td className="results-td results-td-more">
                    +{columns.length - visibleIndices.length}
                  </td>
                )}
              </tr>
            );
          })}
        </tbody>
      </table>

      <div className="results-scroll-status">
        {loadingMore && <div className="scroll-loading">Loading more rows...</div>}
        {pagination && (
          <div className="scroll-info">
            Showing {rows.length.toLocaleString()} of {pagination.total.toLocaleString()} rows
            {pagination.has_more && !loadingMore && (
              <span className="scroll-hint"> &mdash; scroll down for more</span>
            )}
          </div>
        )}
        <div ref={sentinelRef} className="scroll-sentinel" />
      </div>
    </div>
  );
}
