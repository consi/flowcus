import { useCallback, useEffect, useRef, useState } from 'react';
import type { StructuredTimeRange } from './api';

const REFRESH_OPTIONS: { label: string; seconds: number }[] = [
  { label: 'Off', seconds: 0 },
  { label: '5s', seconds: 5 },
  { label: '15s', seconds: 15 },
  { label: '30s', seconds: 30 },
  { label: '1m', seconds: 60 },
  { label: '3m', seconds: 180 },
  { label: '5m', seconds: 300 },
  { label: '10m', seconds: 600 },
];

interface TimeRangePickerProps {
  value: StructuredTimeRange;
  onChange: (range: StructuredTimeRange) => void;
  refreshInterval: number;
  onRefreshIntervalChange: (seconds: number) => void;
}

const PRESETS: [string, string[]][] = [
  ['', ['5s', '10s', '30s', '45s', '1m', '5m']],
  ['', ['15m', '1h', '6h', '24h', '7d', '30d']],
];
const UNITS = [
  { value: 's', label: 'seconds' },
  { value: 'm', label: 'minutes' },
  { value: 'h', label: 'hours' },
  { value: 'd', label: 'days' },
  { value: 'w', label: 'weeks' },
] as const;

/** Convert a duration string like "1h", "30m", "7d" to milliseconds. */
function durationToMs(dur: string): number {
  const match = dur.match(/^(\d+)([smhdw])$/);
  if (!match) return 3600_000;
  const n = parseInt(match[1], 10);
  switch (match[2]) {
    case 's': return n * 1000;
    case 'm': return n * 60_000;
    case 'h': return n * 3600_000;
    case 'd': return n * 86_400_000;
    case 'w': return n * 604_800_000;
    default: return n * 3600_000;
  }
}

/** Format a Date to the datetime-local input value (YYYY-MM-DDTHH:mm:ss.sss). */
function toLocalInputValue(d: Date): string {
  const pad = (n: number, w = 2) => String(n).padStart(w, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
}

/** Resolve a StructuredTimeRange to absolute local datetime-local strings. */
function resolveToAbsInputs(range: StructuredTimeRange): [string, string] {
  if (range.type === 'absolute' && range.start && range.end) {
    return [toLocalInputValue(new Date(range.start)), toLocalInputValue(new Date(range.end))];
  }
  const now = Date.now();
  const ms = durationToMs(range.duration ?? '1h');
  return [toLocalInputValue(new Date(now - ms)), toLocalInputValue(new Date(now))];
}

function formatTimeRange(range: StructuredTimeRange): string {
  if (range.type === 'relative') {
    return `Last ${range.duration ?? '1h'}`;
  }
  if (range.start && range.end) {
    const fmt = (iso: string) => {
      const d = new Date(iso);
      return d.toLocaleString(undefined, {
        month: 'short', day: 'numeric',
        hour: '2-digit', minute: '2-digit',
      });
    };
    return `${fmt(range.start)} \u2014 ${fmt(range.end)}`;
  }
  return 'Custom range';
}

export function TimeRangePicker({ value, onChange, refreshInterval, onRefreshIntervalChange }: TimeRangePickerProps) {
  const [open, setOpen] = useState(false);
  const [refreshOpen, setRefreshOpen] = useState(false);
  const [mode, setMode] = useState<'relative' | 'absolute'>(value.type);
  const [customAmount, setCustomAmount] = useState('1');
  const [customUnit, setCustomUnit] = useState('h');
  const [absStart, setAbsStart] = useState('');
  const [absEnd, setAbsEnd] = useState('');
  const popoverRef = useRef<HTMLDivElement>(null);
  const refreshRef = useRef<HTMLDivElement>(null);

  // Close on outside click or Escape
  useEffect(() => {
    if (!open) return;
    const handleClick = (e: MouseEvent) => {
      if (popoverRef.current && !popoverRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false);
    };
    document.addEventListener('mousedown', handleClick);
    document.addEventListener('keydown', handleKey);
    return () => {
      document.removeEventListener('mousedown', handleClick);
      document.removeEventListener('keydown', handleKey);
    };
  }, [open]);

  // Close refresh dropdown on outside click
  useEffect(() => {
    if (!refreshOpen) return;
    const handleClick = (e: MouseEvent) => {
      if (refreshRef.current && !refreshRef.current.contains(e.target as Node)) {
        setRefreshOpen(false);
      }
    };
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setRefreshOpen(false);
    };
    document.addEventListener('mousedown', handleClick);
    document.addEventListener('keydown', handleKey);
    return () => {
      document.removeEventListener('mousedown', handleClick);
      document.removeEventListener('keydown', handleKey);
    };
  }, [refreshOpen]);

  const selectPreset = useCallback((duration: string) => {
    onChange({ type: 'relative', duration });
    setMode('relative');
    setOpen(false);
  }, [onChange]);

  const applyCustomRelative = useCallback(() => {
    const n = parseInt(customAmount, 10);
    if (n > 0) {
      onChange({ type: 'relative', duration: `${n}${customUnit}` });
      setOpen(false);
    }
  }, [customAmount, customUnit, onChange]);

  const applyAbsolute = useCallback(() => {
    if (absStart && absEnd) {
      onChange({
        type: 'absolute',
        start: new Date(absStart).toISOString(),
        end: new Date(absEnd).toISOString(),
      });
      setOpen(false);
    }
  }, [absStart, absEnd, onChange]);

  const activeRefresh = REFRESH_OPTIONS.find((r) => r.seconds === refreshInterval);

  return (
    <div className="time-range-group">
    <div className="time-range-picker" ref={popoverRef}>
      <button
        className="time-range-trigger"
        onClick={() => {
          if (!open) {
            const [s, e] = resolveToAbsInputs(value);
            setAbsStart(s);
            setAbsEnd(e);
          }
          setOpen(!open);
          setRefreshOpen(false);
        }}
        title="Select time range"
      >
        <svg width="14" height="14" viewBox="0 0 16 16" fill="currentColor">
          <path d="M8 3.5a.5.5 0 00-1 0V8a.5.5 0 00.252.434l3.5 2a.5.5 0 00.496-.868L8 7.71V3.5z"/>
          <path d="M8 16A8 8 0 108 0a8 8 0 000 16zm7-8A7 7 0 111 8a7 7 0 0114 0z"/>
        </svg>
        <span className="time-range-label">{formatTimeRange(value)}</span>
        <svg width="10" height="10" viewBox="0 0 16 16" fill="currentColor" style={{ opacity: 0.5 }}>
          <path d="M4.427 7.427l3.396 3.396a.25.25 0 00.354 0l3.396-3.396A.25.25 0 0011.396 7H4.604a.25.25 0 00-.177.427z"/>
        </svg>
      </button>

      {open && (
        <div className="time-range-popover">
          <div className="time-range-mode-tabs">
            <button
              className={mode === 'relative' ? 'active' : ''}
              onClick={() => setMode('relative')}
            >Relative</button>
            <button
              className={mode === 'absolute' ? 'active' : ''}
              onClick={() => setMode('absolute')}
            >Absolute</button>
          </div>

          {mode === 'relative' && (
            <div className="time-range-relative">
              <div className="time-range-presets">
                {PRESETS.map(([, items], rowIdx) => (
                  <div key={rowIdx} className="time-range-preset-row">
                    {items.map((p) => (
                      <button
                        key={p}
                        className={`time-range-preset ${value.type === 'relative' && value.duration === p ? 'active' : ''}`}
                        onClick={() => selectPreset(p)}
                      >{p}</button>
                    ))}
                  </div>
                ))}
              </div>
              <div className="time-range-custom">
                <span className="time-range-custom-label">Custom:</span>
                <input
                  type="number"
                  min="1"
                  value={customAmount}
                  onChange={(e) => setCustomAmount(e.target.value)}
                  className="time-range-custom-input"
                  onKeyDown={(e) => { if (e.key === 'Enter') applyCustomRelative(); }}
                />
                <select
                  value={customUnit}
                  onChange={(e) => setCustomUnit(e.target.value)}
                  className="time-range-custom-unit"
                >
                  {UNITS.map((u) => (
                    <option key={u.value} value={u.value}>{u.label}</option>
                  ))}
                </select>
                <button className="time-range-apply" onClick={applyCustomRelative}>Apply</button>
              </div>
            </div>
          )}

          {mode === 'absolute' && (
            <div className="time-range-absolute">
              <label className="time-range-abs-label">
                Start
                <input
                  type="datetime-local"
                  step="0.001"
                  value={absStart}
                  onChange={(e) => setAbsStart(e.target.value)}
                  className="time-range-abs-input"
                />
              </label>
              <label className="time-range-abs-label">
                End
                <input
                  type="datetime-local"
                  step="0.001"
                  value={absEnd}
                  onChange={(e) => setAbsEnd(e.target.value)}
                  className="time-range-abs-input"
                />
              </label>
              <button
                className="time-range-apply"
                onClick={applyAbsolute}
                disabled={!absStart || !absEnd}
              >Apply</button>
            </div>
          )}
        </div>
      )}
    </div>

    <div className="refresh-picker" ref={refreshRef}>
      <button
        className={`refresh-trigger${refreshInterval > 0 ? ' active' : ''}`}
        onClick={() => { setRefreshOpen(!refreshOpen); setOpen(false); }}
        title={refreshInterval > 0 ? `Auto-refresh every ${activeRefresh?.label}` : 'Auto-refresh off'}
      >
        <svg className="refresh-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
          <path d="M21 2v6h-6"/>
          <path d="M3 12a9 9 0 0115.36-6.36L21 8"/>
          <path d="M3 22v-6h6"/>
          <path d="M21 12a9 9 0 01-15.36 6.36L3 16"/>
        </svg>
        {refreshInterval > 0 && <span className="refresh-label">{activeRefresh?.label}</span>}
      </button>

      {refreshOpen && (
        <div className="refresh-popover">
          {REFRESH_OPTIONS.map((opt) => (
            <button
              key={opt.seconds}
              className={`refresh-option${refreshInterval === opt.seconds ? ' active' : ''}`}
              onClick={() => { onRefreshIntervalChange(opt.seconds); setRefreshOpen(false); }}
            >
              {opt.label}
            </button>
          ))}
        </div>
      )}
    </div>
    </div>
  );
}
