import { useEffect, useState } from 'react';

interface ServerInfo {
  name: string;
  version: string;
  server: {
    host: string;
    port: number;
    dev_mode: boolean;
  };
  workers: {
    async: number;
    cpu: number;
  };
}

export function App() {
  const [info, setInfo] = useState<ServerInfo | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch('/api/info')
      .then((res) => res.json())
      .then(setInfo)
      .catch((err) => setError(err.message));
  }, []);

  return (
    <main>
      <h1>Flowcus</h1>
      {error && <p className="error">Error: {error}</p>}
      {info && (
        <section>
          <h2>Server Info</h2>
          <dl>
            <dt>Version</dt>
            <dd>{info.version}</dd>
            <dt>Port</dt>
            <dd>{info.server.port}</dd>
            <dt>Dev Mode</dt>
            <dd>{info.server.dev_mode ? 'Yes' : 'No'}</dd>
            <dt>Async Workers</dt>
            <dd>{info.workers.async}</dd>
            <dt>CPU Workers</dt>
            <dd>{info.workers.cpu}</dd>
          </dl>
        </section>
      )}
    </main>
  );
}
