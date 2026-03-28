import { useCallback, useEffect, useState } from 'react';
import { QueryEditor } from './QueryEditor';
import { ResultsTable } from './ResultsTable';
import { StatsBar } from './StatsBar';
import { executeQuery, fetchInfo, type QueryError, type QueryResult, type ServerInfo } from './api';

export function App() {
  const [info, setInfo] = useState<ServerInfo | null>(null);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [queryError, setQueryError] = useState<QueryError | null>(null);

  useEffect(() => {
    fetchInfo().then(setInfo).catch(() => {});
  }, []);

  const handleExecute = useCallback(async (query: string) => {
    setLoading(true);
    setQueryError(null);
    setResult(null);
    try {
      const res = await executeQuery(query);
      setResult(res);
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

      {result && (
        <>
          <section className="results-section">
            <ResultsTable columns={result.columns} rows={result.rows} />
          </section>
          <StatsBar stats={result.stats} />
        </>
      )}

      <footer className="app-footer">
        {info && (
          <span>
            {info.name} v{info.version} &mdash; {info.server.host}:{info.server.port} &mdash;
            {' '}{info.workers.cpu} CPU + {info.workers.async} async workers
          </span>
        )}
      </footer>
    </div>
  );
}
