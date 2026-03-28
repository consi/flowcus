import { useCallback, useEffect, useRef, useState } from 'react';
import { QueryEditor } from './QueryEditor';
import { ResultsTable } from './ResultsTable';
import { StatsBar } from './StatsBar';
import {
  executeQuery,
  fetchInfo,
  type Pagination,
  type QueryError,
  type QueryStats,
  type QueryColumn,
  type ServerInfo,
} from './api';

export function App() {
  const [info, setInfo] = useState<ServerInfo | null>(null);
  const [columns, setColumns] = useState<QueryColumn[]>([]);
  const [allRows, setAllRows] = useState<unknown[][]>([]);
  const [stats, setStats] = useState<QueryStats | null>(null);
  const [pagination, setPagination] = useState<Pagination | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [queryError, setQueryError] = useState<QueryError | null>(null);
  const lastQuery = useRef<string>('');

  useEffect(() => {
    fetchInfo().then(setInfo).catch(() => {});
  }, []);

  const handleExecute = useCallback(async (query: string) => {
    setLoading(true);
    setQueryError(null);
    setAllRows([]);
    setColumns([]);
    setStats(null);
    setPagination(null);
    lastQuery.current = query;

    try {
      const res = await executeQuery(query, 0);
      setColumns(res.columns);
      setAllRows(res.rows);
      setStats(res.stats);
      setPagination(res.pagination);
    } catch (err: unknown) {
      if (err && typeof err === 'object' && 'error' in err) {
        setQueryError(err as QueryError);
      } else {
        setQueryError({ error: err instanceof Error ? err.message : 'Unknown error' });
      }
    } finally {
      setLoading(false);
    }
  }, []);

  const loadMore = useCallback(async () => {
    if (!pagination?.has_more || loadingMore || !lastQuery.current) return;

    setLoadingMore(true);
    try {
      const nextOffset = pagination.offset + pagination.limit;
      const res = await executeQuery(lastQuery.current, nextOffset);
      setAllRows((prev) => [...prev, ...res.rows]);
      setPagination(res.pagination);
      setStats(res.stats);
    } catch {
      // silently fail on scroll-load errors
    } finally {
      setLoadingMore(false);
    }
  }, [pagination, loadingMore]);

  return (
    <div className="app">
      <header className="app-header">
        <h1 className="app-title">Flowcus</h1>
        {info && (
          <span className="app-version">
            v{info.version}
            {info.server.dev_mode && <span className="dev-badge">DEV</span>}
          </span>
        )}
      </header>

      <section className="query-section">
        <QueryEditor onExecute={handleExecute} loading={loading} error={queryError} />
      </section>

      {(allRows.length > 0 || loading) && (
        <>
          <section className="results-section">
            <ResultsTable
              columns={columns}
              rows={allRows}
              pagination={pagination}
              onLoadMore={loadMore}
              loadingMore={loadingMore}
            />
          </section>
          {stats && <StatsBar stats={stats} />}
        </>
      )}

      <footer className="app-footer">
        {info && (
          <span>
            {info.name} v{info.version} &mdash; {info.server.host}:{info.server.port}
          </span>
        )}
      </footer>
    </div>
  );
}
