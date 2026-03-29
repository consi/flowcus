import { useCallback, useEffect, useRef, useState } from 'react';
import {
  type StructuredTimeRange,
  type StructuredFilter,
} from './api';
import { getTimezone } from './formatters';

interface TimeHistogramProps {
  timeRange: StructuredTimeRange;
  filters: StructuredFilter[];
  onTimeRangeChange: (range: StructuredTimeRange) => void;
  /** Incremented each time the main query is executed, forces histogram refresh. */
  queryGen: number;
  timezone?: string;
}

interface HistogramBucket {
  timestamp: number;
  count: number;
}

// Bucket durations are computed server-side; the client receives them via SSE.

// ── Canvas drawing constants ──────────────────────────────────

const CHART_HEIGHT = 120;
const PADDING_LEFT = 52;
const PADDING_RIGHT = 12;
const PADDING_TOP = 8;
const PADDING_BOTTOM = 22;
const BAR_GAP = 1;
const TOOLTIP_PADDING = 8;

// ── Time formatting ───────────────────────────────────────────

function formatAxisTime(ts: number, windowSecs: number, tz: string): string {
  const d = new Date(ts * 1000);
  if (windowSecs > 86400 * 2) {
    return d.toLocaleDateString(undefined, { timeZone: tz, month: 'short', day: 'numeric' });
  }
  if (windowSecs > 86400) {
    return d.toLocaleString(undefined, { timeZone: tz, month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
  }
  return d.toLocaleTimeString(undefined, { timeZone: tz, hour: '2-digit', minute: '2-digit' });
}

function formatTooltipTime(ts: number, bucketSecs: number, tz: string): string {
  const d = new Date(ts * 1000);
  const end = new Date((ts + bucketSecs) * 1000);
  const fmt = (dt: Date) => dt.toLocaleTimeString(undefined, {
    timeZone: tz, hour: '2-digit', minute: '2-digit', second: bucketSecs < 60 ? '2-digit' : undefined,
  });
  return `${fmt(d)} - ${fmt(end)}`;
}

function formatCount(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toString();
}

function computeYTicks(maxVal: number): number[] {
  if (maxVal <= 0) return [0];
  // Nice tick values
  const rough = maxVal / 4;
  const mag = Math.pow(10, Math.floor(Math.log10(rough)));
  const niceSteps = [1, 2, 2.5, 5, 10];
  let step = mag;
  for (const s of niceSteps) {
    if (s * mag >= rough) { step = s * mag; break; }
  }
  const ticks: number[] = [];
  for (let v = 0; v <= maxVal; v += step) {
    ticks.push(v);
  }
  // Always include a tick at or above max
  if (ticks[ticks.length - 1] < maxVal) {
    ticks.push(ticks[ticks.length - 1] + step);
  }
  return ticks;
}

// ── Component ─────────────────────────────────────────────────

export function TimeHistogram({
  timeRange,
  filters,
  onTimeRangeChange,
  queryGen,
  timezone,
}: TimeHistogramProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [buckets, setBuckets] = useState<HistogramBucket[]>([]);
  // Re-draw when theme changes
  const [themeKey, setThemeKey] = useState(0);
  useEffect(() => {
    const obs = new MutationObserver(() => setThemeKey((k) => k + 1));
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
    return () => obs.disconnect();
  }, []);

  const [totalRows, setTotalRows] = useState<number>(0);
  const [histLoading, setHistLoading] = useState(false);
  const [canvasWidth, setCanvasWidth] = useState(800);
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);
  const [mousePos, setMousePos] = useState<{ x: number; y: number } | null>(null);
  const [brushStart, setBrushStart] = useState<number | null>(null);
  const [brushEnd, setBrushEnd] = useState<number | null>(null);
  const brushingRef = useRef(false);
  const abortRef = useRef<AbortController | null>(null);
  const bucketDurationRef = useRef(60);
  const windowBoundsRef = useRef<[number, number]>([0, 0]);

  // ── Fetch histogram data ──────────────────────────────────

  const hasFilters = filters.some((f) => f.field !== 'limit' && f.field && f.op);

  // Refs to capture current props at fetch time without triggering re-creation
  const timeRangeRef = useRef(timeRange);
  const filtersRef = useRef(filters);
  timeRangeRef.current = timeRange;
  filtersRef.current = filters;

  const applyEvent = useCallback((data: { buckets?: { timestamp: number; count: number }[]; total_rows?: number; time_range?: { start: number; end: number }; bucket_seconds?: number; done?: boolean }) => {
    if (data.bucket_seconds) bucketDurationRef.current = data.bucket_seconds;
    if (data.time_range) windowBoundsRef.current = [data.time_range.start, data.time_range.end];
    if (data.buckets) {
      setBuckets(data.buckets.map((b) => ({ timestamp: b.timestamp, count: b.count })));
    }
    if (data.total_rows !== undefined) setTotalRows(data.total_rows);
    if (data.done) setHistLoading(false);
  }, []);

  const doFetch = useCallback(() => {
    if (abortRef.current) abortRef.current.abort();

    const realFilters = filtersRef.current.filter((f) => f.field !== 'limit' && f.field && f.op);

    // Clear previous data immediately so stale results never linger
    setBuckets([]);
    setTotalRows(0);
    setHistLoading(true);

    const controller = new AbortController();
    abortRef.current = controller;

    fetch('/api/stats/histogram', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Accept': 'text/event-stream' },
      body: JSON.stringify({
        time_range: timeRangeRef.current,
        filters: realFilters,
        logic: 'and',
        buckets: 60,
      }),
      signal: controller.signal,
    }).then(async (response) => {
      if (!response.ok) {
        setBuckets([]);
        setTotalRows(0);
        setHistLoading(false);
        return;
      }

      const contentType = response.headers.get('content-type') ?? '';
      const isSSE = contentType.includes('text/event-stream');
      const reader = isSSE ? response.body?.getReader() : null;

      if (reader) {
        // Stream SSE events progressively — render buckets as they arrive
        const decoder = new TextDecoder();
        let buffer = '';

        for (;;) {
          const { done, value } = await reader.read();
          if (controller.signal.aborted) { reader.cancel(); return; }

          if (value) buffer += decoder.decode(value, { stream: true });

          // Process complete SSE event blocks (delimited by blank lines).
          // Handle both \n\n and \r\n\r\n separators.
          let sepIdx: number;
          while ((sepIdx = buffer.search(/\r?\n\r?\n/)) !== -1) {
            const sepLen = buffer.slice(sepIdx).match(/^\r?\n\r?\n/)![0].length;
            const block = buffer.slice(0, sepIdx);
            buffer = buffer.slice(sepIdx + sepLen);
            for (const line of block.split(/\r?\n/)) {
              if (line.startsWith('data:')) {
                const json = line.slice(line.charAt(5) === ' ' ? 6 : 5);
                try { applyEvent(JSON.parse(json)); } catch { /* skip */ }
              }
            }
          }

          if (done) break;
        }

        // Process any trailing data left in buffer (final event may lack trailing \n\n)
        if (buffer.trim()) {
          for (const line of buffer.split(/\r?\n/)) {
            if (line.startsWith('data:')) {
              const json = line.slice(line.charAt(5) === ' ' ? 6 : 5);
              try { applyEvent(JSON.parse(json)); } catch { /* skip */ }
            }
          }
        }
      } else {
        // Non-streaming: buffer entire response then parse all SSE events
        const text = await response.text();
        if (controller.signal.aborted) return;
        for (const block of text.split(/\r?\n\r?\n/)) {
          for (const line of block.split(/\r?\n/)) {
            if (line.startsWith('data:')) {
              const json = line.slice(line.charAt(5) === ' ' ? 6 : 5);
              try { applyEvent(JSON.parse(json)); } catch { /* skip */ }
            }
          }
        }
      }

      setHistLoading(false);
    }).catch(() => {
      if (!controller.signal.aborted) {
        setBuckets([]);
        setTotalRows(0);
        setHistLoading(false);
      }
    });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [applyEvent]);

  // Fetch only when queryGen changes (same trigger as the main query)
  useEffect(() => {
    doFetch();
  }, [queryGen, doFetch]);

  // Cleanup abort on unmount
  useEffect(() => {
    return () => {
      if (abortRef.current) abortRef.current.abort();
    };
  }, []);

  // ── Resize observer ───────────────────────────────────────

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const obs = new ResizeObserver((entries) => {
      for (const entry of entries) {
        setCanvasWidth(entry.contentRect.width);
      }
    });
    obs.observe(el);
    setCanvasWidth(el.clientWidth);
    return () => obs.disconnect();
  }, []);

  // ── Canvas drawing ────────────────────────────────────────

  const tz = timezone ?? getTimezone();

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const dpr = window.devicePixelRatio || 1;
    const w = canvasWidth;
    const h = CHART_HEIGHT;

    canvas.width = w * dpr;
    canvas.height = h * dpr;
    canvas.style.width = `${w}px`;
    canvas.style.height = `${h}px`;

    const ctx = canvas.getContext('2d');
    if (!ctx) return;
    ctx.scale(dpr, dpr);

    // Read theme colors from CSS variables
    const css = getComputedStyle(document.documentElement);
    const cssVar = (name: string) => css.getPropertyValue(name).trim();
    const textMuted = cssVar('--text-secondary') || '#9494a8';
    const gridColor = cssVar('--border') || '#d4d4de';
    const accent = cssVar('--accent') || '#7c6ca8';
    const textPrimary = cssVar('--text-primary') || '#1e1e2e';

    // Clear
    ctx.clearRect(0, 0, w, h);

    const chartW = w - PADDING_LEFT - PADDING_RIGHT;
    const chartH = h - PADDING_TOP - PADDING_BOTTOM;

    if (buckets.length === 0) {
      // "No data" message
      ctx.fillStyle = textMuted;
      ctx.font = '12px system-ui, sans-serif';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.fillText(histLoading ? '' : 'No data', w / 2, h / 2);
      return;
    }

    const maxCount = Math.max(...buckets.map((b) => b.count), 1);
    const yTicks = computeYTicks(maxCount);
    const yMax = yTicks[yTicks.length - 1] || 1;

    // ── Grid lines ────────────────────────────────────────
    ctx.strokeStyle = gridColor;
    ctx.lineWidth = 1;
    for (const tick of yTicks) {
      const y = PADDING_TOP + chartH - (tick / yMax) * chartH;
      ctx.beginPath();
      ctx.moveTo(PADDING_LEFT, Math.round(y) + 0.5);
      ctx.lineTo(w - PADDING_RIGHT, Math.round(y) + 0.5);
      ctx.stroke();
    }

    // ── Y-axis labels ─────────────────────────────────────
    ctx.fillStyle = textMuted;
    ctx.font = '10px system-ui, sans-serif';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    for (const tick of yTicks) {
      const y = PADDING_TOP + chartH - (tick / yMax) * chartH;
      ctx.fillText(formatCount(tick), PADDING_LEFT - 6, y);
    }

    // ── Bars ──────────────────────────────────────────────
    const barTotalW = chartW / buckets.length;
    const barW = Math.max(barTotalW - BAR_GAP, 1);

    // Create gradient
    const grad = ctx.createLinearGradient(0, PADDING_TOP, 0, PADDING_TOP + chartH);
    grad.addColorStop(0, accent);
    grad.addColorStop(1, cssVar('--purple') || accent);

    const hoverGrad = ctx.createLinearGradient(0, PADDING_TOP, 0, PADDING_TOP + chartH);
    hoverGrad.addColorStop(0, cssVar('--blue') || accent);
    hoverGrad.addColorStop(1, accent);

    for (let i = 0; i < buckets.length; i++) {
      const bucket = buckets[i];
      const barH = bucket.count > 0 ? Math.max((bucket.count / yMax) * chartH, 1) : 0;
      const x = PADDING_LEFT + i * barTotalW;
      const y = PADDING_TOP + chartH - barH;

      if (barH > 0) {
        ctx.fillStyle = i === hoverIndex ? hoverGrad : grad;

        // Draw bar with rounded top
        const radius = Math.min(2, barW / 2, barH / 2);
        ctx.beginPath();
        ctx.moveTo(x, PADDING_TOP + chartH);
        ctx.lineTo(x, y + radius);
        ctx.arcTo(x, y, x + radius, y, radius);
        ctx.arcTo(x + barW, y, x + barW, y + radius, radius);
        ctx.lineTo(x + barW, PADDING_TOP + chartH);
        ctx.closePath();
        ctx.fill();
      }
    }

    // ── X-axis labels ─────────────────────────────────────
    const windowSecs = windowBoundsRef.current[1] - windowBoundsRef.current[0];
    ctx.fillStyle = textMuted;
    ctx.font = '10px system-ui, sans-serif';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';

    // Space labels at least 80px apart
    const labelSpacing = Math.max(1, Math.ceil(80 / barTotalW));
    for (let i = 0; i < buckets.length; i += labelSpacing) {
      const x = PADDING_LEFT + i * barTotalW + barW / 2;
      const label = formatAxisTime(buckets[i].timestamp, windowSecs, tz);
      ctx.fillText(label, x, PADDING_TOP + chartH + 4);
    }

    // ── Brush selection overlay ───────────────────────────
    if (brushStart !== null && brushEnd !== null) {
      const x1 = Math.min(brushStart, brushEnd);
      const x2 = Math.max(brushStart, brushEnd);
      ctx.fillStyle = cssVar('--accent-light') || 'rgba(124, 108, 168, 0.15)';
      ctx.fillRect(x1, PADDING_TOP, x2 - x1, chartH);
      ctx.strokeStyle = accent;
      ctx.lineWidth = 1;
      ctx.strokeRect(x1 + 0.5, PADDING_TOP + 0.5, x2 - x1 - 1, chartH - 1);
    }

    // ── Tooltip ───────────────────────────────────────────
    if (hoverIndex !== null && mousePos && !brushingRef.current) {
      const bucket = buckets[hoverIndex];
      if (bucket) {
        const timeStr = formatTooltipTime(bucket.timestamp, bucketDurationRef.current, tz);
        const countStr = bucket.count.toLocaleString();
        const lines = [timeStr, `${countStr} flows`];

        ctx.font = '11px system-ui, sans-serif';
        const maxLineW = Math.max(...lines.map((l) => ctx.measureText(l).width));
        const tooltipW = maxLineW + TOOLTIP_PADDING * 2;
        const tooltipH = lines.length * 16 + TOOLTIP_PADDING * 2 - 4;

        let tx = mousePos.x + 12;
        let ty = mousePos.y - tooltipH - 8;
        // Keep tooltip within canvas
        if (tx + tooltipW > w) tx = mousePos.x - tooltipW - 12;
        if (ty < 0) ty = mousePos.y + 16;

        // Background
        ctx.fillStyle = cssVar('--bg-surface-2') || '#2e2f4a';
        ctx.beginPath();
        const r = 4;
        ctx.moveTo(tx + r, ty);
        ctx.lineTo(tx + tooltipW - r, ty);
        ctx.arcTo(tx + tooltipW, ty, tx + tooltipW, ty + r, r);
        ctx.lineTo(tx + tooltipW, ty + tooltipH - r);
        ctx.arcTo(tx + tooltipW, ty + tooltipH, tx + tooltipW - r, ty + tooltipH, r);
        ctx.lineTo(tx + r, ty + tooltipH);
        ctx.arcTo(tx, ty + tooltipH, tx, ty + tooltipH - r, r);
        ctx.lineTo(tx, ty + r);
        ctx.arcTo(tx, ty, tx + r, ty, r);
        ctx.closePath();
        ctx.fill();
        ctx.strokeStyle = gridColor;
        ctx.lineWidth = 1;
        ctx.stroke();

        // Text
        ctx.fillStyle = textMuted;
        ctx.textAlign = 'left';
        ctx.textBaseline = 'top';
        ctx.fillText(lines[0], tx + TOOLTIP_PADDING, ty + TOOLTIP_PADDING);

        ctx.fillStyle = textPrimary;
        ctx.font = 'bold 11px system-ui, sans-serif';
        ctx.fillText(lines[1], tx + TOOLTIP_PADDING, ty + TOOLTIP_PADDING + 16);
      }
    }
  }, [buckets, canvasWidth, hoverIndex, mousePos, brushStart, brushEnd, histLoading, tz, themeKey]);

  // ── Mouse interaction helpers ─────────────────────────────

  const getBucketIndexAtX = useCallback(
    (clientX: number): number | null => {
      const canvas = canvasRef.current;
      if (!canvas || buckets.length === 0) return null;
      const rect = canvas.getBoundingClientRect();
      const x = clientX - rect.left;
      const chartW = canvasWidth - PADDING_LEFT - PADDING_RIGHT;
      const barTotalW = chartW / buckets.length;
      const idx = Math.floor((x - PADDING_LEFT) / barTotalW);
      if (idx < 0 || idx >= buckets.length) return null;
      return idx;
    },
    [buckets.length, canvasWidth],
  );

  const getCanvasX = useCallback(
    (clientX: number): number => {
      const canvas = canvasRef.current;
      if (!canvas) return 0;
      return clientX - canvas.getBoundingClientRect().left;
    },
    [],
  );

  const getCanvasY = useCallback(
    (clientY: number): number => {
      const canvas = canvasRef.current;
      if (!canvas) return 0;
      return clientY - canvas.getBoundingClientRect().top;
    },
    [],
  );

  // ── Mouse event handlers ──────────────────────────────────

  const handleMouseMove = useCallback(
    (e: React.MouseEvent) => {
      const idx = getBucketIndexAtX(e.clientX);
      setHoverIndex(idx);
      setMousePos({ x: getCanvasX(e.clientX), y: getCanvasY(e.clientY) });

      if (brushingRef.current) {
        const x = getCanvasX(e.clientX);
        // Clamp to chart area
        const clampedX = Math.max(PADDING_LEFT, Math.min(x, canvasWidth - PADDING_RIGHT));
        setBrushEnd(clampedX);
      }
    },
    [getBucketIndexAtX, getCanvasX, getCanvasY, canvasWidth],
  );

  const handleMouseLeave = useCallback(() => {
    setHoverIndex(null);
    setMousePos(null);
    if (!brushingRef.current) {
      setBrushStart(null);
      setBrushEnd(null);
    }
  }, []);

  const handleMouseDown = useCallback(
    (e: React.MouseEvent) => {
      if (e.button !== 0) return; // left button only
      const x = getCanvasX(e.clientX);
      const clampedX = Math.max(PADDING_LEFT, Math.min(x, canvasWidth - PADDING_RIGHT));
      brushingRef.current = true;
      setBrushStart(clampedX);
      setBrushEnd(clampedX);
    },
    [getCanvasX, canvasWidth],
  );

  const handleMouseUp = useCallback(
    (e: React.MouseEvent) => {
      if (!brushingRef.current) return;
      brushingRef.current = false;

      const x = getCanvasX(e.clientX);
      const clampedX = Math.max(PADDING_LEFT, Math.min(x, canvasWidth - PADDING_RIGHT));
      const startX = brushStart ?? clampedX;
      const endX = clampedX;

      setBrushStart(null);
      setBrushEnd(null);

      // Only trigger if dragged more than 5px
      if (Math.abs(endX - startX) < 5) return;
      if (buckets.length === 0) return;

      // Convert pixel range to time range
      const chartW = canvasWidth - PADDING_LEFT - PADDING_RIGHT;
      const frac1 = (Math.min(startX, endX) - PADDING_LEFT) / chartW;
      const frac2 = (Math.max(startX, endX) - PADDING_LEFT) / chartW;

      const [winStart, winEnd] = windowBoundsRef.current;
      const windowSecs = winEnd - winStart;

      const t1 = winStart + frac1 * windowSecs;
      const t2 = winStart + frac2 * windowSecs;

      const newStart = new Date(Math.floor(t1) * 1000).toISOString();
      const newEnd = new Date(Math.ceil(t2) * 1000).toISOString();

      onTimeRangeChange({
        type: 'absolute',
        start: newStart,
        end: newEnd,
      });
    },
    [brushStart, canvasWidth, buckets.length, onTimeRangeChange, getCanvasX],
  );

  const handleContextMenu = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      if (buckets.length === 0) return;

      const [winStart, winEnd] = windowBoundsRef.current;
      const windowSecs = winEnd - winStart;
      const halfWindow = windowSecs / 2;

      const now = Math.floor(Date.now() / 1000);
      const expandedEnd = Math.min(winEnd + halfWindow, now);
      const newStart = new Date((winStart - halfWindow) * 1000).toISOString();
      const newEnd = new Date(expandedEnd * 1000).toISOString();

      onTimeRangeChange({
        type: 'absolute',
        start: newStart,
        end: newEnd,
      });
    },
    [buckets.length, onTimeRangeChange],
  );

  return (
    <div ref={containerRef} className="time-histogram">
      <canvas
        ref={canvasRef}
        className="time-histogram-canvas"
        onMouseMove={handleMouseMove}
        onMouseLeave={handleMouseLeave}
        onMouseDown={handleMouseDown}
        onMouseUp={handleMouseUp}
        onContextMenu={handleContextMenu}
      />
      <div className="time-histogram-footer">
        <span className="time-histogram-total">
          {totalRows > 0
            ? `${hasFilters ? '~' : ''}${totalRows.toLocaleString()} flows in selected range`
            : histLoading
              ? 'Scanning\u2026'
              : '\u00A0'}
        </span>
        {histLoading && (
          <span className="time-histogram-streaming" />
        )}
      </div>
    </div>
  );
}
