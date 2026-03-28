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
  const [dragCol, setDragCol] = useState<number | null>(null);
  const [dragOverCol, setDragOverCol] = useState<number | null>(null);
  const didDrag = useRef(false);
  const theadRef = useRef<HTMLTableSectionElement>(null);
  const touchDragRef = useRef(false);
  const touchStartX = useRef(0);

  const visibleIndices = useMemo(() => {
    if (visibleColumns && visibleColumns.length > 0) {
      const nameToIndex = new Map(columns.map((c, i) => [c.name, i]));
      const indices = visibleColumns
        .map((name) => nameToIndex.get(name))
        .filter((i): i is number => i !== undefined);
      if (indices.length > 0) return indices;
    }
    return selectVisibleColumns(columns);
  }, [columns, visibleColumns]);

  const visibleColumnNames = useMemo(
    () => visibleIndices.map((i) => columns[i].name),
    [visibleIndices, columns],
  );

  const handleSort = useCallback(
    (colIndex: number) => {
      if (didDrag.current) {
        didDrag.current = false;
        return;
      }
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

  // Commit column reorder
  const commitColReorder = useCallback((fromIdx: number, toIdx: number) => {
    if (fromIdx === toIdx || !onColumnConfigChange) return;
    didDrag.current = true;
    const next = [...visibleColumnNames];
    const [moved] = next.splice(fromIdx, 1);
    next.splice(toIdx, 0, moved);
    onColumnConfigChange(next);
  }, [visibleColumnNames, onColumnConfigChange]);

  // HTML5 drag handlers (desktop)
  const handleColDragStart = useCallback((e: React.DragEvent, visIdx: number) => {
    setDragCol(visIdx);
    didDrag.current = false;
    e.dataTransfer.effectAllowed = 'move';
    e.dataTransfer.setData('text/plain', String(visIdx));
  }, []);

  const handleColDragOver = useCallback((e: React.DragEvent, visIdx: number) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'move';
    if (dragCol !== visIdx) setDragOverCol(visIdx);
  }, [dragCol]);

  const handleColDragEnd = useCallback(() => {
    if (dragCol !== null && dragOverCol !== null) {
      commitColReorder(dragCol, dragOverCol);
    }
    setDragCol(null);
    setDragOverCol(null);
  }, [dragCol, dragOverCol, commitColReorder]);

  const handleColDragLeave = useCallback(() => {
    setDragOverCol(null);
  }, []);

  // Touch drag handlers for column headers (mobile)
  const getVisIdxFromTouchX = useCallback((clientX: number): number | null => {
    const thead = theadRef.current;
    if (!thead) return null;
    const ths = thead.querySelectorAll<HTMLElement>('.results-th[data-visidx]');
    for (const th of ths) {
      const rect = th.getBoundingClientRect();
      if (clientX >= rect.left && clientX <= rect.right) {
        const idx = parseInt(th.dataset.visidx!, 10);
        return isNaN(idx) ? null : idx;
      }
    }
    return null;
  }, []);

  const handleThTouchStart = useCallback((e: React.TouchEvent, visIdx: number) => {
    touchStartX.current = e.touches[0].clientX;
    touchDragRef.current = false;
    setDragCol(visIdx);
    setDragOverCol(visIdx);
  }, []);

  const handleThTouchMove = useCallback((e: React.TouchEvent) => {
    const touch = e.touches[0];
    // Only enter drag mode after 10px horizontal movement
    if (!touchDragRef.current && Math.abs(touch.clientX - touchStartX.current) > 10) {
      touchDragRef.current = true;
    }
    if (!touchDragRef.current) return;
    e.preventDefault();
    const overIdx = getVisIdxFromTouchX(touch.clientX);
    if (overIdx !== null) setDragOverCol(overIdx);
  }, [getVisIdxFromTouchX]);

  const handleThTouchEnd = useCallback(() => {
    if (touchDragRef.current && dragCol !== null && dragOverCol !== null) {
      commitColReorder(dragCol, dragOverCol);
    }
    touchDragRef.current = false;
    setDragCol(null);
    setDragOverCol(null);
  }, [dragCol, dragOverCol, commitColReorder]);

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
        <thead ref={theadRef}>
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
            {visibleIndices.map((ci, visIdx) => {
              const col = columns[ci];
              const isDragging = dragCol === visIdx;
              const isDragOver = dragOverCol === visIdx && dragCol !== visIdx;
              const dropSide = isDragOver && dragCol !== null
                ? (dragCol < visIdx ? 'right' : 'left')
                : null;
              return (
                <th
                  key={col.name}
                  data-visidx={visIdx}
                  onClick={() => handleSort(ci)}
                  className={`results-th${isDragging ? ' th-dragging' : ''}${dropSide ? ` drag-over-${dropSide}` : ''}`}
                  title={`${col.name} (${col.type}) \u2014 click to sort`}
                  draggable={!!onColumnConfigChange}
                  onDragStart={(e) => handleColDragStart(e, visIdx)}
                  onDragOver={(e) => handleColDragOver(e, visIdx)}
                  onDragEnd={handleColDragEnd}
                  onDragLeave={handleColDragLeave}
                  onTouchStart={onColumnConfigChange ? (e) => handleThTouchStart(e, visIdx) : undefined}
                  onTouchMove={onColumnConfigChange ? handleThTouchMove : undefined}
                  onTouchEnd={onColumnConfigChange ? handleThTouchEnd : undefined}
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
        <div ref={sentinelRef} className="scroll-sentinel" />
      </div>
    </div>
  );
}
