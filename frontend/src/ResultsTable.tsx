import { useCallback, useMemo, useState } from 'react';
import type { QueryColumn } from './api';

interface ResultsTableProps {
  columns: QueryColumn[];
  rows: unknown[][];
}

type SortDir = 'asc' | 'desc' | null;

export function ResultsTable({ columns, rows }: ResultsTableProps) {
  const [sortCol, setSortCol] = useState<number | null>(null);
  const [sortDir, setSortDir] = useState<SortDir>(null);

  const handleSort = useCallback(
    (colIndex: number) => {
      if (sortCol === colIndex) {
        // Cycle: asc -> desc -> none
        if (sortDir === 'asc') {
          setSortDir('desc');
        } else if (sortDir === 'desc') {
          setSortCol(null);
          setSortDir(null);
        } else {
          setSortDir('asc');
        }
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
      if (typeof va === 'number' && typeof vb === 'number') {
        return (va - vb) * dir;
      }
      return String(va).localeCompare(String(vb)) * dir;
    });
  }, [rows, sortCol, sortDir]);

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
    </div>
  );
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  if (typeof value === 'number') {
    // Format large numbers with locale separators
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
