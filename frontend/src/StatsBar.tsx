import type { QueryStats } from './api';

interface StatsBarProps {
  stats: QueryStats;
}

export function StatsBar({ stats }: StatsBarProps) {
  return (
    <div className="stats-bar">
      <span className="stat-item">
        <span className="stat-label">Parse</span>
        <span className="stat-value">{formatDuration(stats.parse_time_us)}</span>
      </span>
      <span className="stat-sep" />
      <span className="stat-item">
        <span className="stat-label">Execute</span>
        <span className="stat-value">{formatDuration(stats.execution_time_us)}</span>
      </span>
      <span className="stat-sep" />
      <span className="stat-item">
        <span className="stat-label">Rows scanned</span>
        <span className="stat-value">{stats.rows_scanned.toLocaleString()}</span>
      </span>
      <span className="stat-sep" />
      <span className="stat-item">
        <span className="stat-label">Rows returned</span>
        <span className="stat-value">{stats.rows_returned.toLocaleString()}</span>
      </span>
      <span className="stat-sep" />
      <span className="stat-item">
        <span className="stat-label">Parts scanned</span>
        <span className="stat-value">{stats.parts_scanned.toLocaleString()}</span>
      </span>
      <span className="stat-sep" />
      <span className="stat-item">
        <span className="stat-label">Parts skipped</span>
        <span className="stat-value">{stats.parts_skipped.toLocaleString()}</span>
      </span>
    </div>
  );
}

function formatDuration(us: number): string {
  if (us < 1000) return `${us}\u00B5s`;
  if (us < 1_000_000) return `${(us / 1000).toFixed(1)}ms`;
  return `${(us / 1_000_000).toFixed(2)}s`;
}
