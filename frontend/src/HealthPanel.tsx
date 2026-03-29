import { useCallback, useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { fetchHealthStats, type HealthStats } from './api';

function formatBytes(bytes: number): string {
  if (bytes >= 1073741824) return `${(bytes / 1073741824).toFixed(2)} GB`;
  if (bytes >= 1048576) return `${(bytes / 1048576).toFixed(1)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${bytes} B`;
}

function formatNumber(n: number): string {
  if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(2)}B`;
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

function formatUptime(startSecs: number): string {
  const now = Math.floor(Date.now() / 1000);
  let diff = now - startSecs;
  if (diff < 0) diff = 0;
  const days = Math.floor(diff / 86400);
  const hours = Math.floor((diff % 86400) / 3600);
  const mins = Math.floor((diff % 3600) / 60);
  const secs = diff % 60;
  if (days > 0) return `${days}d ${hours}h ${mins}m`;
  if (hours > 0) return `${hours}h ${mins}m ${secs}s`;
  if (mins > 0) return `${mins}m ${secs}s`;
  return `${secs}s`;
}

const PARTITION_COLORS: Record<string, string> = {
  marks: '#7c6fc4',
  blooms: '#4ea8de',
  column_index: '#4ade80',
  metadata: '#fbbf24',
};

function StackedCacheGauge({ partitions, totalUsed, totalMax }: {
  partitions: Array<{ name: string; used_bytes: number; max_bytes: number }>;
  totalUsed: number;
  totalMax: number;
}) {
  const totalPct = totalMax > 0 ? Math.min(100, (totalUsed / totalMax) * 100) : 0;
  return (
    <div className="health-gauge">
      <div className="health-gauge-header">
        <span className="health-gauge-label">Storage Cache</span>
        <span className="health-gauge-value">{formatBytes(totalUsed)} / {formatBytes(totalMax)} ({totalPct.toFixed(1)}%)</span>
      </div>
      <div className="health-gauge-track health-gauge-track-stacked">
        {partitions.map((p) => {
          const pct = totalMax > 0 ? (p.used_bytes / totalMax) * 100 : 0;
          if (pct < 0.1) return null;
          return (
            <div
              key={p.name}
              className="health-gauge-segment"
              style={{ width: `${pct}%`, background: PARTITION_COLORS[p.name] ?? '#888' }}
              title={`${p.name}: ${formatBytes(p.used_bytes)} / ${formatBytes(p.max_bytes)}`}
            />
          );
        })}
      </div>
      <div className="health-cache-legend">
        {partitions.map((p) => {
          const pct = p.max_bytes > 0 ? ((p.used_bytes / p.max_bytes) * 100).toFixed(0) : '0';
          return (
            <div key={p.name} className="health-cache-legend-item">
              <span className="health-cache-legend-dot" style={{ background: PARTITION_COLORS[p.name] ?? '#888' }} />
              <span className="health-cache-legend-name">{p.name}</span>
              <span className="health-cache-legend-val">{formatBytes(p.used_bytes)} / {formatBytes(p.max_bytes)} ({pct}%)</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function StatCard({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="health-stat">
      <div className="health-stat-value">{value}</div>
      <div className="health-stat-label">{label}</div>
      {sub && <div className="health-stat-sub">{sub}</div>}
    </div>
  );
}

interface HealthPanelProps {
  open: boolean;
  onClose: () => void;
}

export function HealthPanel({ open, onClose }: HealthPanelProps) {
  const [stats, setStats] = useState<HealthStats | null>(null);
  const [error, setError] = useState<string | null>(null);
  const modalRef = useRef<HTMLDivElement>(null);
  const intervalRef = useRef<ReturnType<typeof setInterval>>(undefined);

  useEffect(() => {
    if (!open) return;
    setError(null);

    const load = () => {
      fetchHealthStats()
        .then(setStats)
        .catch((err) => setError(err instanceof Error ? err.message : String(err)));
    };

    load();
    intervalRef.current = setInterval(load, 2000);
    return () => clearInterval(intervalRef.current);
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', handleKey);
    return () => document.removeEventListener('keydown', handleKey);
  }, [open, onClose]);

  const handleOverlayClick = useCallback(
    (e: React.MouseEvent) => {
      if (modalRef.current && !modalRef.current.contains(e.target as Node)) {
        onClose();
      }
    },
    [onClose],
  );

  if (!open) return null;

  const m = stats?.metrics ?? {};
  const cacheHitRate = (stats?.cache.hits ?? 0) + (stats?.cache.misses ?? 0) > 0
    ? ((stats!.cache.hits / (stats!.cache.hits + stats!.cache.misses)) * 100).toFixed(1)
    : '—';

  const queryAvgMs = (m.query_requests ?? 0) > 0
    ? ((m.query_duration_us ?? 0) / (m.query_requests ?? 1) / 1000).toFixed(1)
    : '—';

  return createPortal(
    <div className="settings-overlay" onClick={handleOverlayClick}>
      <div className="health-modal" ref={modalRef}>
        <div className="health-header">
          <h2>
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ marginRight: 8, verticalAlign: -3 }}>
              <path d="M22 12h-2.48a2 2 0 0 0-1.93 1.46l-2.35 8.36a.25.25 0 0 1-.48 0L9.24 2.18a.25.25 0 0 0-.48 0l-2.35 8.36A2 2 0 0 1 4.49 12H2" />
            </svg>
            System Health
          </h2>
          <button className="settings-close" onClick={onClose} title="Close">{'\u00D7'}</button>
        </div>

        {error ? (
          <div className="settings-loading">Failed to load stats: {error}</div>
        ) : !stats ? (
          <div className="settings-loading">Loading...</div>
        ) : (
          <div className="health-body">
            {/* Uptime */}
            <div className="health-section">
              <div className="health-section-title">Overview</div>
              <div className="health-stats-grid">
                <StatCard label="Uptime" value={formatUptime(m.process_start_time_secs ?? 0)} />
                <StatCard label="Memory (RSS)" value={formatBytes(stats.process.rss_bytes)} />
                <StatCard label="Threads" value={String(stats.process.threads)} />
                <StatCard label="Active Exporters" value={String(m.ipfix_exporters_active ?? 0)} />
              </div>
            </div>

            {/* Cache */}
            <div className="health-section">
              <div className="health-section-title">Cache</div>
              <StackedCacheGauge
                partitions={stats.cache.partitions}
                totalUsed={stats.cache.used_bytes}
                totalMax={stats.cache.max_bytes}
              />
              <div className="health-stats-grid" style={{ marginTop: '0.75rem' }}>
                <StatCard label="Hit Rate" value={`${cacheHitRate}%`} />
                <StatCard label="Hits" value={formatNumber(stats.cache.hits)} />
                <StatCard label="Misses" value={formatNumber(stats.cache.misses)} />
              </div>
            </div>

            {/* Storage */}
            <div className="health-section">
              <div className="health-section-title">Storage</div>
              <div className="health-stats-grid">
                <StatCard label="Disk Usage" value={formatBytes(stats.storage.disk_bytes)} />
                <StatCard label="Total Parts" value={formatNumber(stats.storage.parts_total)} sub={`Gen0: ${stats.storage.parts_gen0} | Merged: ${stats.storage.parts_merged}`} />
                <StatCard label="Records Ingested" value={formatNumber(m.writer_records_ingested ?? 0)} />
                <StatCard label="Parts Flushed" value={formatNumber(m.writer_parts_flushed ?? 0)} sub={`${formatBytes(m.writer_bytes_flushed ?? 0)} written`} />
              </div>
            </div>

            {/* Merge */}
            <div className="health-section">
              <div className="health-section-title">Merge</div>
              <div className="health-stats-grid">
                <StatCard label="Completed" value={formatNumber(m.merge_completed ?? 0)} sub={`${m.merge_failed ?? 0} failed`} />
                <StatCard label="Rows Merged" value={formatNumber(m.merge_rows_processed ?? 0)} sub={formatBytes(m.merge_bytes_written ?? 0)} />
                <StatCard label="Active Workers" value={String(m.merge_active_workers ?? 0)} />
                <StatCard label="Pending Hours" value={String(m.merge_pending_hours ?? 0)} />
                <StatCard label="Parts Removed" value={formatNumber(m.merge_parts_removed ?? 0)} />
                <StatCard label="Throttled" value={formatNumber(m.merge_throttled ?? 0)} />
              </div>
            </div>

            {/* Retention */}
            <div className="health-section">
              <div className="health-section-title">Retention</div>
              <div className="health-stats-grid">
                <StatCard label="Scan Passes" value={formatNumber(m.retention_passes ?? 0)} />
                <StatCard label="Parts Removed" value={formatNumber(m.retention_parts_removed ?? 0)} />
              </div>
            </div>

            {/* IPFIX */}
            <div className="health-section">
              <div className="health-section-title">IPFIX</div>
              <div className="health-stats-grid">
                <StatCard label="Packets Received" value={formatNumber(m.ipfix_packets_received ?? 0)} sub={formatBytes(m.ipfix_bytes_received ?? 0)} />
                <StatCard label="Packets Parsed" value={formatNumber(m.ipfix_packets_parsed ?? 0)} sub={`${m.ipfix_packets_errors ?? 0} errors`} />
                <StatCard label="Records Decoded" value={formatNumber(m.ipfix_records_decoded ?? 0)} />
                <StatCard label="Templates" value={String(m.ipfix_templates_active ?? 0)} sub={`${m.ipfix_unknown_ies ?? 0} unknown IEs`} />
              </div>
            </div>

            {/* Query */}
            <div className="health-section">
              <div className="health-section-title">Query</div>
              <div className="health-stats-grid">
                <StatCard label="Total Queries" value={formatNumber(m.query_requests ?? 0)} sub={`${m.query_errors ?? 0} errors`} />
                <StatCard label="Avg Latency" value={`${queryAvgMs} ms`} />
                <StatCard label="Rows Scanned" value={formatNumber(m.query_rows_scanned ?? 0)} sub={`${formatNumber(m.query_rows_returned ?? 0)} returned`} />
                <StatCard label="Parts Scanned" value={formatNumber(m.query_parts_scanned ?? 0)} sub={`${formatNumber(m.query_parts_skipped ?? 0)} skipped`} />
              </div>
            </div>

            {/* Writer / Channel */}
            <div className="health-section">
              <div className="health-section-title">Writer</div>
              <div className="health-stats-grid">
                <StatCard label="Active Buffers" value={String(m.writer_active_buffers ?? 0)} sub={formatBytes(m.writer_buffer_bytes ?? 0)} />
                <StatCard label="Channel Depth" value={String(m.writer_channel_depth ?? 0)} sub={`${m.writer_channel_drops ?? 0} drops`} />
                <StatCard label="Flush Errors" value={String(m.writer_flush_errors ?? 0)} />
              </div>
            </div>
          </div>
        )}
      </div>
    </div>,
    document.body,
  );
}
