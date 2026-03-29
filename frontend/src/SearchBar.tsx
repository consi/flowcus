import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { StructuredFilter, SchemaResponse, SchemaField } from './api';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface SearchBarProps {
  filters: StructuredFilter[];
  onChange: (filters: StructuredFilter[]) => void;
  schema: SchemaResponse | null;
  onExecute: () => void;
  onCancel: () => void;
  loading: boolean;
}

type CompletionState = 'idle' | 'field' | 'op' | 'value';
type EditTarget = { index: number; part: 'field' | 'op' | 'value' } | null;

interface HistoryEntry {
  filters: StructuredFilter[];
}

interface SuggestionItem {
  key: string;
  label: string;
  detail: string;
  data: unknown;
  group?: string; // optional group header
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const OP_LABELS: Record<string, string> = {
  eq: '=', ne: '!=', gt: '>', ge: '>=', lt: '<', le: '<=',
  in: 'in', not_in: 'not in', between: 'between',
  cidr: 'cidr', wildcard: 'wildcard', ip_range: 'range',
  regex: '~', not_regex: '!~',
  contains: 'contains', not_contains: '!contains',
  starts_with: 'starts with', ends_with: 'ends with',
  port_range: 'range', named: 'is', prefix: 'prefix',
};

/** Special pseudo-field for row limit */
const LIMIT_FIELD = 'limit';

function isLimitFilter(f: StructuredFilter): boolean {
  return f.field === LIMIT_FIELD;
}

const OP_FROM_LABEL = Object.fromEntries(
  Object.entries(OP_LABELS).map(([k, v]) => [v, k]),
);

const COMMON_PROTOCOLS: { name: string; num: number }[] = [
  { name: 'tcp', num: 6 }, { name: 'udp', num: 17 }, { name: 'icmp', num: 1 },
  { name: 'gre', num: 47 }, { name: 'sctp', num: 132 }, { name: 'esp', num: 50 },
  { name: 'ah', num: 51 },
];
const COMMON_SERVICES: { name: string; port: number }[] = [
  { name: 'http', port: 80 }, { name: 'https', port: 443 }, { name: 'dns', port: 53 },
  { name: 'ssh', port: 22 }, { name: 'ftp', port: 21 }, { name: 'smtp', port: 25 },
  { name: 'snmp', port: 161 }, { name: 'telnet', port: 23 }, { name: 'ntp', port: 123 },
  { name: 'imap', port: 143 }, { name: 'pop3', port: 110 }, { name: 'ldap', port: 389 },
  { name: 'rdp', port: 3389 }, { name: 'mysql', port: 3306 }, { name: 'postgresql', port: 5432 },
  { name: 'redis', port: 6379 }, { name: 'syslog', port: 514 }, { name: 'bgp', port: 179 },
  { name: 'netflow', port: 2055 },
];

const TCP_FLAGS: { label: string; detail: string; value: number }[] = [
  { label: 'SYN', detail: '0x02 (2)', value: 2 },
  { label: 'ACK', detail: '0x10 (16)', value: 16 },
  { label: 'FIN', detail: '0x01 (1)', value: 1 },
  { label: 'RST', detail: '0x04 (4)', value: 4 },
  { label: 'PSH', detail: '0x08 (8)', value: 8 },
  { label: 'URG', detail: '0x20 (32)', value: 32 },
  { label: 'SYN+ACK', detail: '0x12 (18)', value: 18 },
  { label: 'FIN+ACK', detail: '0x11 (17)', value: 17 },
  { label: 'RST+ACK', detail: '0x14 (20)', value: 20 },
  { label: 'PSH+ACK', detail: '0x18 (24)', value: 24 },
];

const ICMP_TYPES: { label: string; value: number }[] = [
  { label: 'Echo Reply', value: 0 },
  { label: 'Destination Unreachable', value: 3 },
  { label: 'Redirect', value: 5 },
  { label: 'Echo Request', value: 8 },
  { label: 'Time Exceeded', value: 11 },
  { label: 'Timestamp', value: 13 },
];

const DSCP_VALUES: { label: string; value: number }[] = [
  { label: 'Best Effort (BE)', value: 0 },
  { label: 'EF (Expedited)', value: 46 },
  { label: 'AF11', value: 10 }, { label: 'AF12', value: 12 }, { label: 'AF13', value: 14 },
  { label: 'AF21', value: 18 }, { label: 'AF22', value: 20 }, { label: 'AF23', value: 22 },
  { label: 'AF31', value: 26 }, { label: 'AF32', value: 28 }, { label: 'AF33', value: 30 },
  { label: 'AF41', value: 34 }, { label: 'AF42', value: 36 }, { label: 'AF43', value: 38 },
  { label: 'CS1', value: 8 }, { label: 'CS2', value: 16 }, { label: 'CS3', value: 24 },
  { label: 'CS4', value: 32 }, { label: 'CS5', value: 40 }, { label: 'CS6', value: 48 },
  { label: 'CS7', value: 56 },
];

const DURATION_SHORTCUTS: { label: string; detail: string; data: string }[] = [
  { label: '1s', detail: '1 second', data: '1000' },
  { label: '5s', detail: '5 seconds', data: '5000' },
  { label: '10s', detail: '10 seconds', data: '10000' },
  { label: '30s', detail: '30 seconds', data: '30000' },
  { label: '1m', detail: '1 minute', data: '60000' },
  { label: '5m', detail: '5 minutes', data: '300000' },
  { label: '10m', detail: '10 minutes', data: '600000' },
];

const COUNTER_MAGNITUDES: { label: string; detail: string; data: string }[] = [
  { label: '1K', detail: '1,000', data: '1000' },
  { label: '10K', detail: '10,000', data: '10000' },
  { label: '100K', detail: '100,000', data: '100000' },
  { label: '1M', detail: '1,000,000', data: '1000000' },
  { label: '10M', detail: '10,000,000', data: '10000000' },
  { label: '100M', detail: '100,000,000', data: '100000000' },
  { label: '1G', detail: '1,000,000,000', data: '1000000000' },
];

function filterByQuery(items: SuggestionItem[], query: string): SuggestionItem[] {
  if (!query) return items;
  return items.filter((it) =>
    it.label.toLowerCase().includes(query) || it.detail.toLowerCase().includes(query),
  );
}

function getValueSuggestions(field: SchemaField, op: string, query: string): SuggestionItem[] {
  const q = query.toLowerCase();
  const hint = field.semantic_hint ?? '';
  const ft = field.filter_type ?? '';

  // TCP flags
  if (hint === 'tcp_flags') {
    return filterByQuery(TCP_FLAGS.map((f) => ({
      key: `flag-${f.value}`, label: f.label, detail: f.detail, data: String(f.value),
    })), q);
  }

  // ICMP types
  if (hint === 'icmp_type') {
    return filterByQuery(ICMP_TYPES.map((t) => ({
      key: `icmp-${t.value}`, label: t.label, detail: String(t.value), data: String(t.value),
    })), q);
  }

  // DSCP
  if (hint === 'dscp') {
    return filterByQuery(DSCP_VALUES.map((d) => ({
      key: `dscp-${d.value}`, label: d.label, detail: String(d.value), data: String(d.value),
    })), q);
  }

  // Timestamps
  if (hint === 'timestamp_s' || hint === 'timestamp_ms') {
    const mul = hint === 'timestamp_ms' ? 1000 : 1;
    const nowS = Math.floor(Date.now() / 1000);
    const items: SuggestionItem[] = [
      { key: 'ts-now', label: 'now', detail: 'Current time', data: String(nowS * mul), group: 'Time shortcuts' },
      { key: 'ts-1h', label: '1h ago', detail: '1 hour ago', data: String((nowS - 3600) * mul) },
      { key: 'ts-24h', label: '24h ago', detail: '24 hours ago', data: String((nowS - 86400) * mul) },
      { key: 'ts-7d', label: '7d ago', detail: '7 days ago', data: String((nowS - 604800) * mul) },
    ];
    return filterByQuery(items, q);
  }

  // Duration
  if (hint === 'duration_ms') {
    return filterByQuery(DURATION_SHORTCUTS.map((d) => ({
      key: `dur-${d.data}`, label: d.label, detail: d.detail, data: d.data,
    })), q);
  }

  // Boolean
  if (ft === 'boolean') {
    return filterByQuery([
      { key: 'bool-true', label: 'true', detail: 'True', data: 'true' },
      { key: 'bool-false', label: 'false', detail: 'False', data: 'false' },
    ], q);
  }

  // Protocol
  if (ft === 'protocol') {
    return filterByQuery(COMMON_PROTOCOLS.map((p) => ({
      key: `proto-${p.name}`,
      label: p.name,
      detail: `protocol ${p.num}`,
      data: op === 'named' ? p.name : String(p.num),
    })), q);
  }

  // Port (for eq, ne, in, named)
  if (ft === 'port' && ['eq', 'ne', 'in', 'named'].includes(op)) {
    return filterByQuery(COMMON_SERVICES.map((s) => ({
      key: `svc-${s.name}`,
      label: s.name,
      detail: `port ${s.port}`,
      data: op === 'named' ? s.name : String(s.port),
    })), q);
  }

  // Counter magnitudes
  if (hint === 'counter' && ['gt', 'ge', 'lt', 'le', 'between'].includes(op)) {
    return filterByQuery(COUNTER_MAGNITUDES.map((m) => ({
      key: `mag-${m.data}`, label: m.label, detail: m.detail, data: m.data,
    })), q);
  }

  return [];
}

function getValuePlaceholder(field: SchemaField, op: string): string {
  const hint = field.semantic_hint ?? '';
  const ft = field.filter_type ?? '';

  // Semantic hints take priority
  if (hint === 'timestamp_s') return 'epoch seconds or shortcut';
  if (hint === 'timestamp_ms') return 'epoch ms or shortcut';
  if (hint === 'duration_ms') {
    if (op === 'between') return 'e.g. 5s,1m or 5000,60000';
    return 'e.g. 5s, 1m, 5000';
  }
  if (hint === 'tcp_flags') return 'e.g. SYN, 0x02, 2';
  if (hint === 'icmp_type') return 'e.g. Echo Request, 8';
  if (hint === 'dscp') return 'e.g. EF, AF11, 46';

  // Filter type + op combinations
  if (ft === 'boolean') return 'true or false';

  if (ft === 'ipv4') {
    if (op === 'eq') return 'e.g. 10.0.0.1';
    if (op === 'cidr') return 'e.g. 10.0.0.0/24';
    if (op === 'wildcard') return 'e.g. 10.*.*.1';
    if (op === 'ip_range') return 'e.g. 10.0.0.1-10.0.0.255';
    if (op === 'in' || op === 'not_in') return 'e.g. 10.0.0.1,10.0.0.2';
  }
  if (ft === 'ipv6') {
    if (op === 'eq') return 'e.g. 2001:db8::1';
    if (op === 'cidr') return 'e.g. 2001:db8::/32';
    if (op === 'in' || op === 'not_in') return 'e.g. 2001:db8::1,2001:db8::2';
  }
  if (ft === 'port') {
    if (op === 'eq') return 'e.g. 443 or https';
    if (op === 'port_range') return 'e.g. 1024,65535';
    if (op === 'named') return 'e.g. https, ssh, dns';
    if (op === 'in' || op === 'not_in') return 'e.g. 80,443,8080';
  }
  if (ft === 'protocol') {
    if (op === 'eq') return 'e.g. 6 or tcp';
    if (op === 'named') return 'e.g. tcp, udp, icmp';
  }
  if (ft === 'mac') {
    if (op === 'eq') return 'e.g. aa:bb:cc:dd:ee:ff';
    if (op === 'prefix') return 'e.g. aa:bb:cc';
  }
  if (ft === 'string') {
    if (op === 'eq') return 'e.g. SSH';
    if (op === 'regex') return 'e.g. ^HTTP.*';
    if (op === 'contains') return 'e.g. HTTP';
    if (op === 'in' || op === 'not_in') return 'e.g. SSH,HTTP,DNS';
  }
  if (ft === 'numeric') {
    if (op === 'eq') return 'e.g. 1000, 1K, 1M';
    if (op === 'between') return 'e.g. 100,1000';
    if (op === 'in' || op === 'not_in') return 'e.g. 100,200,300';
    if (op === 'gt' || op === 'ge' || op === 'lt' || op === 'le') return 'e.g. 1000';
  }

  return `value for ${field.name}...`;
}

function getOpPriority(filterType: string, semanticHint: string, op: string): number {
  const hint = semanticHint ?? '';
  const ft = filterType ?? '';

  if (ft === 'ipv4' || ft === 'ipv6') {
    const m: Record<string, number> = { cidr: 0, eq: 1, ne: 2, in: 3, not_in: 4, wildcard: 5, ip_range: 6 };
    return m[op] ?? 99;
  }
  if (ft === 'port') {
    const m: Record<string, number> = { eq: 0, named: 1, ne: 2, port_range: 3, gt: 4, ge: 5, lt: 6, le: 7, in: 8, not_in: 9 };
    return m[op] ?? 99;
  }
  if (ft === 'protocol') {
    const m: Record<string, number> = { named: 0, eq: 1, ne: 2, in: 3, not_in: 4 };
    return m[op] ?? 99;
  }
  if (ft === 'boolean') {
    return op === 'eq' ? 0 : 99;
  }
  if (ft === 'mac') {
    const m: Record<string, number> = { eq: 0, prefix: 1, ne: 2, in: 3, not_in: 4 };
    return m[op] ?? 99;
  }
  if (ft === 'string') {
    const m: Record<string, number> = {
      contains: 0, eq: 1, ne: 2, starts_with: 3, ends_with: 4,
      regex: 5, not_regex: 6, not_contains: 7, in: 8, not_in: 9,
    };
    return m[op] ?? 99;
  }
  if (ft === 'numeric') {
    if (hint === 'counter') {
      const m: Record<string, number> = { gt: 0, ge: 1, lt: 2, le: 3, between: 4, eq: 5, ne: 6, in: 7, not_in: 8 };
      return m[op] ?? 99;
    }
    if (hint === 'timestamp_s' || hint === 'timestamp_ms') {
      const m: Record<string, number> = { gt: 0, lt: 1, ge: 2, le: 3, between: 4, eq: 8, ne: 9 };
      return m[op] ?? 99;
    }
    if (hint === 'duration_ms') {
      const m: Record<string, number> = { gt: 0, lt: 1, between: 2, ge: 3, le: 4, eq: 5, ne: 6 };
      return m[op] ?? 99;
    }
  }

  return 99; // default: preserve schema order
}

// Aliases ordered by network engineer workflow:
// 1. 5-tuple identity (what you always filter on)
// 2. Traffic volume (bytes/packets — the next question is always "how much")
// 3. TCP/QoS flags (next: what kind of traffic)
// 4. Routing & interfaces (where did it go)
// 5. L2 / VLAN (switching context)
// 6. ICMP
// 7. Timing / duration
// 8. Application layer
// 9. Exporter metadata
const ALIAS_ORDER: string[] = [
  // 5-tuple
  'src', 'dst', 'sport', 'dport', 'port', 'proto',
  // Counters
  'bytes', 'packets',
  // TCP / QoS
  'flags', 'tcpflags', 'tos', 'dscp',
  // Routing
  'nexthop', 'nexthop6', 'bgp_nexthop', 'src_as', 'dst_as', 'srcas', 'dstas',
  // Interfaces
  'ingress', 'egress', 'in_if', 'out_if',
  // L2
  'vlan', 'src_mac', 'dst_mac', 'srcmac', 'dstmac',
  // ICMP
  'icmp_type', 'icmp6_type',
  // Timing
  'duration', 'start', 'end',
  // Application
  'app',
  // Exporter
  'exporter', 'domain_id',
];
const ALIAS_RANK = new Map(ALIAS_ORDER.map((name, i) => [name, i]));
const ALIASES = new Set(ALIAS_ORDER);

const FIELD_PRIORITY: Record<string, number> = {
  sourceIPv4Address: 0, destinationIPv4Address: 0,
  sourceIPv6Address: 0, destinationIPv6Address: 0,
  sourceTransportPort: 0, destinationTransportPort: 0,
  protocolIdentifier: 0,
  octetDeltaCount: 1, packetDeltaCount: 1,
  flowStartMilliseconds: 1, flowEndMilliseconds: 1,
  flowStartSeconds: 1, flowEndSeconds: 1,
  flowDurationMilliseconds: 1,
  ingressInterface: 2, egressInterface: 2,
  bgpSourceAsNumber: 2, bgpDestinationAsNumber: 2,
  ipNextHopIPv4Address: 2, ipNextHopIPv6Address: 2,
  bgpNextHopIPv4Address: 2, bgpNextHopIPv6Address: 2,
  vlanId: 2, postVlanId: 2, dot1qVlanId: 2,
  ipClassOfService: 2, tcpControlBits: 2,
  sourceMacAddress: 2, destinationMacAddress: 2,
  icmpTypeCodeIPv4: 2, icmpTypeCodeIPv6: 2,
  applicationName: 2, applicationId: 2,
  mplsTopLabelStackSection: 2,
  flowcusExporterIPv4: 3, flowcusExporterPort: 3,
  flowcusExportTime: 3, flowcusObservationDomainId: 3,
  flowcusFlowDuration: 3,
};

function rankField(f: SchemaField): [number, number] {
  const aliasIdx = ALIAS_RANK.get(f.name);
  if (aliasIdx !== undefined) return [-1, aliasIdx];
  return [FIELD_PRIORITY[f.name] ?? 4, 0];
}

const HISTORY_KEY = 'flowcus:query-history';
const HISTORY_MAX = 15;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function loadHistory(): HistoryEntry[] {
  try {
    const raw = localStorage.getItem(HISTORY_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.slice(0, HISTORY_MAX) : [];
  } catch { return []; }
}

function saveHistory(entry: HistoryEntry): void {
  const history = loadHistory();
  const key = JSON.stringify(entry);
  const filtered = history.filter((h) => JSON.stringify(h) !== key);
  filtered.unshift(entry);
  if (filtered.length > HISTORY_MAX) filtered.length = HISTORY_MAX;
  try { localStorage.setItem(HISTORY_KEY, JSON.stringify(filtered)); } catch { /* full */ }
}

function filtersToText(filters: StructuredFilter[]): string {
  if (filters.length === 0) return '';
  return filters
    .map((f) => isLimitFilter(f) ? `limit ${String(f.value)}` : `${f.field} ${OP_LABELS[f.op] ?? f.op} ${String(f.value)}`)
    .join(' and ');
}

function textToFilters(text: string): { filters: StructuredFilter[] } | null {
  const trimmed = text.trim();
  if (!trimmed) return null;

  // Split on " and " separator
  let parts: string[];
  if (/\s+and\s+/.test(trimmed)) {
    parts = trimmed.split(/\s+and\s+/);
  } else {
    parts = [trimmed];
  }

  // Sorted ops by label length desc for greedy matching
  const sortedOps = Object.entries(OP_LABELS)
    .sort((a, b) => b[1].length - a[1].length);

  const filters: StructuredFilter[] = [];
  for (const part of parts) {
    const p = part.trim();
    if (!p) continue;

    // Handle "limit N" special case
    const limitMatch = p.match(/^limit\s+(\d+)$/i);
    if (limitMatch) {
      filters.push({ field: LIMIT_FIELD, op: 'eq', value: limitMatch[1] });
      continue;
    }

    let matched = false;
    for (const [opKey, opLabel] of sortedOps) {
      const idx = p.indexOf(` ${opLabel} `);
      if (idx > 0) {
        const field = p.slice(0, idx).trim();
        const value = p.slice(idx + opLabel.length + 2).trim();
        if (field && value) {
          filters.push({ field, op: opKey, value });
          matched = true;
          break;
        }
      }
    }
    if (!matched) {
      // Fallback: split field op value by spaces
      const tokens = p.split(/\s+/);
      if (tokens.length >= 3) {
        const field = tokens[0];
        const opLabel = tokens[1];
        const opKey = OP_FROM_LABEL[opLabel] ?? opLabel;
        const value = tokens.slice(2).join(' ');
        filters.push({ field, op: opKey, value });
      } else if (tokens.length === 2) {
        // "field value" — assume eq
        filters.push({ field: tokens[0], op: 'eq', value: tokens[1] });
      }
    }
  }

  return filters.length > 0 ? { filters } : null;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function SearchBar({
  filters, onChange, schema,
  onExecute, onCancel, loading,
}: SearchBarProps) {
  const [inputValue, setInputValue] = useState('');
  const [completionState, setCompletionState] = useState<CompletionState>('idle');
  const [pendingField, setPendingField] = useState<SchemaField | null>(null);
  const [pendingOp, setPendingOp] = useState<string | null>(null);
  const [activeIndex, setActiveIndex] = useState(0);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [showHistory, setShowHistory] = useState(false);
  const [editTarget, setEditTarget] = useState<EditTarget>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const editInputRef = useRef<HTMLInputElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const suggestionsRef = useRef<HTMLDivElement>(null);

  /** The currently active input — either the main one or the inline edit input */
  const activeInput = editTarget ? editInputRef : inputRef;

  // Close on outside click
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setShowSuggestions(false);
        setShowHistory(false);
        if (editTarget) cancelEditRef.current();
      }
    }
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [editTarget]);

  // ---------------------------------------------------------------------------
  // Inline editing — edit happens inside the chip
  // ---------------------------------------------------------------------------

  const cancelEdit = useCallback(() => {
    setEditTarget(null);
    setInputValue('');
    setCompletionState('idle');
    setPendingField(null);
    setPendingOp(null);
    setShowSuggestions(false);
    setActiveIndex(0);
  }, []);
  const cancelEditRef = useRef(cancelEdit);
  cancelEditRef.current = cancelEdit;

  const startEdit = useCallback((index: number, part: 'field' | 'op' | 'value') => {
    cancelEdit();
    const f = filters[index];
    setEditTarget({ index, part });

    if (part === 'field') {
      setInputValue(f.field);
      setCompletionState('field');
    } else if (part === 'op') {
      const field = schema?.fields.find((sf) => sf.name === f.field) ?? null;
      setPendingField(field);
      setInputValue(OP_LABELS[f.op] ?? f.op);
      setCompletionState('op');
    } else {
      const field = schema?.fields.find((sf) => sf.name === f.field) ?? null;
      setPendingField(field);
      setPendingOp(f.op);
      setInputValue(String(f.value));
      setCompletionState('value');
    }

    setShowSuggestions(true);
    setActiveIndex(0);
    // Focus the inline input after render
    setTimeout(() => { editInputRef.current?.focus(); editInputRef.current?.select(); }, 0);
  }, [filters, schema, cancelEdit]);

  const commitEditField = useCallback((field: SchemaField) => {
    if (!editTarget || editTarget.part !== 'field') return;
    const updated = [...filters];
    updated[editTarget.index] = { ...updated[editTarget.index], field: field.name };
    onChange(updated);
    cancelEdit();
    inputRef.current?.focus();
  }, [editTarget, filters, onChange, cancelEdit]);

  const commitEditOp = useCallback((op: string) => {
    if (!editTarget || editTarget.part !== 'op') return;
    const updated = [...filters];
    updated[editTarget.index] = { ...updated[editTarget.index], op };
    onChange(updated);
    cancelEdit();
    inputRef.current?.focus();
  }, [editTarget, filters, onChange, cancelEdit]);

  const commitEditValue = useCallback((value: string) => {
    if (!editTarget || editTarget.part !== 'value') return;
    const trimmed = value.trim();
    if (!trimmed) { cancelEdit(); return; }
    const updated = [...filters];
    updated[editTarget.index] = { ...updated[editTarget.index], value: trimmed };
    onChange(updated);
    cancelEdit();
    inputRef.current?.focus();
  }, [editTarget, filters, onChange, cancelEdit]);

  // ---------------------------------------------------------------------------
  // Suggestions
  // ---------------------------------------------------------------------------

  const suggestions: SuggestionItem[] = useMemo(() => {
    if (!schema) return [];
    const query = inputValue.toLowerCase();

    if (completionState === 'idle' || completionState === 'field') {
      const sorted = schema.fields
        .filter((f) => !query || f.name.toLowerCase().includes(query) || f.description.toLowerCase().includes(query))
        .sort((a, b) => {
          const [ga, sa] = rankField(a);
          const [gb, sb] = rankField(b);
          if (ga !== gb) return ga - gb;
          if (ga === -1) return sa - sb; // aliases: use explicit ordering
          return a.name.localeCompare(b.name); // non-aliases: alphabetical within group
        });

      // Add group headers
      const items: SuggestionItem[] = [];
      let lastGroup = '';
      for (const f of sorted) {
        const group = ALIASES.has(f.name) ? 'Aliases' : 'Fields';
        items.push({
          key: f.name,
          label: f.name,
          detail: f.description,
          data: f,
          group: group !== lastGroup ? group : undefined,
        });
        lastGroup = group;
      }

      // Inject "limit" pseudo-field — only when at least one real filter exists
      const hasRealFilter = filters.some((f) => !isLimitFilter(f));
      const limitLabel = LIMIT_FIELD;
      if (hasRealFilter && (!query || limitLabel.includes(query))) {
        const limitField: SchemaField = { name: LIMIT_FIELD, filter_type: 'numeric', data_type: 'limit', description: 'Limit result rows', semantic_hint: '' };
        // Insert after aliases, before Fields header
        const fieldsIdx = items.findIndex((it) => it.group === 'Fields');
        const limitItem: SuggestionItem = { key: LIMIT_FIELD, label: LIMIT_FIELD, detail: 'Limit result rows', data: limitField };
        if (fieldsIdx >= 0) items.splice(fieldsIdx, 0, limitItem);
        else items.push(limitItem);
      }

      return items;
    }

    if (completionState === 'op' && (pendingField || editTarget?.part === 'op')) {
      const ft = pendingField?.filter_type ?? '';
      const hint = pendingField?.semantic_hint ?? '';
      const ops = schema.filter_types[ft] ?? [];
      return ops
        .filter((op) => {
          if (!query) return true;
          const label = OP_LABELS[op] ?? op;
          const hintText = schema.op_hints?.[op] ?? '';
          return label.toLowerCase().includes(query) || op.toLowerCase().includes(query)
            || hintText.toLowerCase().includes(query);
        })
        .sort((a, b) => getOpPriority(ft, hint, a) - getOpPriority(ft, hint, b))
        .map((op) => ({
          key: op,
          label: OP_LABELS[op] ?? op,
          detail: schema.op_hints?.[op] ?? op,
          data: op,
        }));
    }

    if (completionState === 'value' && (pendingField || editTarget?.part === 'value')) {
      if (pendingField) {
        return getValueSuggestions(pendingField, pendingOp ?? '', query);
      }
    }

    return [];
  }, [schema, inputValue, completionState, pendingField, pendingOp, editTarget, filters]);

  useEffect(() => {
    setActiveIndex((prev) => Math.min(prev, Math.max(0, suggestions.length - 1)));
  }, [suggestions.length]);

  useEffect(() => {
    if (!suggestionsRef.current) return;
    const active = suggestionsRef.current.querySelector('.search-suggestion.active');
    if (active) active.scrollIntoView({ block: 'nearest' });
  }, [activeIndex]);

  // ---------------------------------------------------------------------------
  // History
  // ---------------------------------------------------------------------------

  const history = useMemo(() => loadHistory(), [showHistory]); // eslint-disable-line react-hooks/exhaustive-deps

  const applyHistory = useCallback((entry: HistoryEntry) => {
    onChange(entry.filters);
    setShowHistory(false);
    inputRef.current?.focus();
  }, [onChange]);

  const saveCurrentToHistory = useCallback(() => {
    if (filters.length > 0) saveHistory({ filters });
  }, [filters]);

  // ---------------------------------------------------------------------------
  // Copy / Paste
  // ---------------------------------------------------------------------------

  const [copied, setCopied] = useState(false);
  const copyQueryToClipboard = useCallback(() => {
    const text = filtersToText(filters);
    if (!text) return;
    // Try modern API first, fall back to execCommand
    if (navigator.clipboard?.writeText) {
      navigator.clipboard.writeText(text).then(() => {
        setCopied(true);
        setTimeout(() => setCopied(false), 1500);
      }).catch(() => {
        // Fallback
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
        setCopied(true);
        setTimeout(() => setCopied(false), 1500);
      });
    } else {
      const ta = document.createElement('textarea');
      ta.value = text;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    }
  }, [filters]);

  const handlePaste = useCallback((e: React.ClipboardEvent) => {
    if (editTarget) return;
    if (completionState !== 'idle' && completionState !== 'field') return;
    const text = e.clipboardData.getData('text/plain');
    const parsed = textToFilters(text);
    if (parsed) {
      e.preventDefault();
      onChange(parsed.filters);
      setInputValue('');
      setCompletionState('idle');
      setShowSuggestions(false);
    }
  }, [editTarget, completionState, onChange]);

  // ---------------------------------------------------------------------------
  // Composition actions (new filters)
  // ---------------------------------------------------------------------------

  const resetComposition = useCallback(() => {
    setInputValue('');
    setCompletionState('idle');
    setPendingField(null);
    setPendingOp(null);
    setEditTarget(null);
    setActiveIndex(0);
    setShowSuggestions(false);
  }, []);

  const selectField = useCallback((field: SchemaField) => {
    if (editTarget?.part === 'field') { commitEditField(field); return; }
    setPendingField(field);
    setInputValue('');
    // Limit skips op — goes straight to value
    if (field.name === LIMIT_FIELD) {
      setPendingOp('eq');
      setCompletionState('value');
    } else {
      setCompletionState('op');
    }
    setActiveIndex(0);
    setShowSuggestions(true);
  }, [editTarget, commitEditField]);

  const selectOp = useCallback((op: string) => {
    if (editTarget?.part === 'op') { commitEditOp(op); return; }
    setPendingOp(op);
    setInputValue('');
    setCompletionState('value');
    setActiveIndex(0);
    setShowSuggestions(true);
  }, [editTarget, commitEditOp]);

  const commitFilter = useCallback((value: string) => {
    if (editTarget?.part === 'value') { commitEditValue(value); return; }
    if (!pendingField || !pendingOp) return;
    const trimmed = value.trim();
    if (!trimmed) return;
    const newFilter: StructuredFilter = { field: pendingField.name, op: pendingOp, value: trimmed };
    // If adding a non-limit filter and limit already exists, keep limit last
    if (!isLimitFilter(newFilter)) {
      const nonLimit = filters.filter((f) => !isLimitFilter(f));
      const limitF = filters.filter((f) => isLimitFilter(f));
      onChange([...nonLimit, newFilter, ...limitF]);
    } else {
      onChange([...filters, newFilter]);
    }
    resetComposition();
    inputRef.current?.focus();
  }, [editTarget, commitEditValue, pendingField, pendingOp, filters, onChange, resetComposition]);

  const removeFilter = useCallback((index: number) => {
    onChange(filters.filter((_, i) => i !== index));
    cancelEdit();
    inputRef.current?.focus();
  }, [filters, onChange, cancelEdit]);

  const selectSuggestion = useCallback((index: number) => {
    const item = suggestions[index];
    if (!item) return;
    if (completionState === 'idle' || completionState === 'field') selectField(item.data as SchemaField);
    else if (completionState === 'op') selectOp(item.data as string);
    else if (completionState === 'value') commitFilter(item.data as string);
  }, [suggestions, completionState, selectField, selectOp, commitFilter]);

  // ---------------------------------------------------------------------------
  // Keyboard — shared between main input and inline edit input
  // ---------------------------------------------------------------------------

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
      e.preventDefault();
      if (editTarget && completionState === 'value' && inputValue.trim()) commitEditValue(inputValue);
      else if (completionState === 'value' && inputValue.trim()) commitFilter(inputValue);
      saveCurrentToHistory();
      onExecute();
      return;
    }

    if (e.key === 'Escape') {
      e.preventDefault();
      if (editTarget) { cancelEdit(); inputRef.current?.focus(); return; }
      if (showSuggestions || showHistory) { setShowSuggestions(false); setShowHistory(false); }
      else resetComposition();
      return;
    }

    if (e.key === 'Backspace' && inputValue === '') {
      e.preventDefault();
      if (editTarget) { cancelEdit(); inputRef.current?.focus(); return; }
      if (completionState === 'value') {
        setCompletionState('op'); setPendingOp(null); setShowSuggestions(true); setActiveIndex(0);
      } else if (completionState === 'op') {
        setCompletionState('idle'); setPendingField(null); setShowSuggestions(true); setActiveIndex(0);
      } else if (filters.length > 0) {
        removeFilter(filters.length - 1);
      }
      return;
    }

    if ((showSuggestions && suggestions.length > 0) || showHistory) {
      const listLen = showHistory ? history.length : suggestions.length;
      if (e.key === 'ArrowDown') { e.preventDefault(); setActiveIndex((p) => (p + 1) % listLen); return; }
      if (e.key === 'ArrowUp') { e.preventDefault(); setActiveIndex((p) => (p - 1 + listLen) % listLen); return; }
    }

    if (showHistory && (e.key === 'Tab' || e.key === 'Enter') && history.length > 0) {
      e.preventDefault(); applyHistory(history[activeIndex]); return;
    }

    if (showSuggestions && suggestions.length > 0 && (e.key === 'Tab' || e.key === 'Enter')) {
      e.preventDefault(); selectSuggestion(activeIndex); return;
    }

    if ((e.key === 'Enter' || e.key === 'Tab') && completionState === 'value' && inputValue.trim()) {
      e.preventDefault(); commitFilter(inputValue); return;
    }

    // Enter in idle → execute immediately
    if (e.key === 'Enter' && completionState === 'idle' && !inputValue.trim() && !showSuggestions && !editTarget) {
      e.preventDefault();
      saveCurrentToHistory(); onExecute();
    }
  }, [
    inputValue, showSuggestions, showHistory, suggestions, history, activeIndex,
    completionState, filters, editTarget, commitFilter, commitEditValue, onExecute,
    onCancel, removeFilter, resetComposition, cancelEdit, selectSuggestion,
    applyHistory, saveCurrentToHistory,
  ]);

  const handleInputChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const val = e.target.value;
    setInputValue(val);
    setActiveIndex(0);
    setShowHistory(false);
    if (completionState === 'idle' && val.length > 0) {
      setCompletionState('field');
      setShowSuggestions(true);
    } else if (val.length > 0) {
      setShowSuggestions(true);
    }
  }, [completionState]);

  const handleInputFocus = useCallback(() => {
    if (editTarget) { setShowSuggestions(true); return; }
    if (completionState === 'idle' || completionState === 'field') {
      setCompletionState(inputValue ? 'field' : 'idle');
      setShowSuggestions(true);
    } else {
      setShowSuggestions(true);
    }
  }, [completionState, inputValue, editTarget]);

  const handleSuggestionClick = useCallback((index: number) => {
    selectSuggestion(index);
    activeInput.current?.focus();
  }, [selectSuggestion, activeInput]);

  const handleContainerClick = useCallback(() => {
    if (!editTarget) inputRef.current?.focus();
  }, [editTarget]);

  // Placeholder
  const placeholder = useMemo(() => {
    if (completionState === 'op' && pendingField) return `operator for ${pendingField.name}...`;
    if (completionState === 'value' && pendingField && pendingOp)
      return getValuePlaceholder(pendingField, pendingOp);
    if (filters.length === 0) return 'Type to add filters (e.g. src, proto, dport)...';
    return 'Add filter...';
  }, [completionState, pendingField, pendingOp, filters.length]);

  // ---------------------------------------------------------------------------
  // Render chip — with inline edit support
  // ---------------------------------------------------------------------------

  const renderChipPart = useCallback(
    (text: string, cls: string, editable: boolean, index: number | null, part: 'field' | 'op' | 'value') => {
      const isEditing = editTarget && index !== null && editTarget.index === index && editTarget.part === part;

      if (isEditing) {
        // Measure: at least as wide as the original text, grows with typed text
        const charWidth = part === 'op' ? 0.65 : 0.62; // em per char approximation
        const chars = Math.max(inputValue.length + 1, text.length, 6);
        return (
          <span className={`search-chip-edit-wrap ${cls}`}>
            <input
              ref={editInputRef}
              className="search-chip-edit-input"
              value={inputValue}
              onChange={handleInputChange}
              onKeyDown={handleKeyDown}
              onFocus={handleInputFocus}
              spellCheck={false}
              autoComplete="off"
              style={{ width: `${chars * charWidth}em` }}
            />
          </span>
        );
      }

      return (
        <span
          className={`${cls}${editable ? ' editable' : ''}`}
          onClick={editable && index !== null ? (e) => { e.stopPropagation(); startEdit(index, part); } : undefined}
        >
          {text}
        </span>
      );
    },
    [editTarget, inputValue, handleInputChange, handleKeyDown, handleInputFocus, startEdit],
  );

  const renderChip = useCallback((f: StructuredFilter, index: number | null, editable: boolean) => {
    if (isLimitFilter(f)) {
      // Limit: 2-part chip (label + value), amber/yellow colored
      return (
        <span className="search-chip search-chip-limit">
          <span className="search-chip-limit-label">limit</span>
          {renderChipPart(String(f.value), 'search-chip-limit-value', editable, index, 'value')}
          {editable && index !== null && (
            <button className="search-chip-remove"
              onClick={(e) => { e.stopPropagation(); removeFilter(index); }}>
              {'\u00D7'}
            </button>
          )}
        </span>
      );
    }
    return (
      <span className="search-chip">
        {renderChipPart(f.field, 'search-chip-field', editable, index, 'field')}
        {renderChipPart(OP_LABELS[f.op] ?? f.op, 'search-chip-op', editable, index, 'op')}
        {renderChipPart(String(f.value), 'search-chip-value', editable, index, 'value')}
        {editable && index !== null && (
          <button className="search-chip-remove"
            onClick={(e) => { e.stopPropagation(); removeFilter(index); }}>
            {'\u00D7'}
          </button>
        )}
      </span>
    );
  }, [renderChipPart, removeFilter]);

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  return (
    <div className="search-bar" ref={containerRef}>
      <div className="search-bar-inner" onClick={handleContainerClick}>
        <div className="search-bar-chips">
          {filters.map((f, i) => (
            <span key={i} className="search-chip-group">
              {renderChip(f, i, true)}
            </span>
          ))}

          {pendingField && !editTarget && (
            <span className="search-chip search-chip-pending">
              <span className="search-chip-field">{pendingField.name}</span>
              {pendingOp && <span className="search-chip-op">{OP_LABELS[pendingOp] ?? pendingOp}</span>}
            </span>
          )}

          {/* Main input — hidden when editing inline */}
          <input
            ref={inputRef}
            className="search-input"
            type="text"
            value={editTarget ? '' : inputValue}
            onChange={editTarget ? undefined : handleInputChange}
            onKeyDown={editTarget ? undefined : handleKeyDown}
            onFocus={editTarget ? undefined : handleInputFocus}
            onPaste={editTarget ? undefined : handlePaste}
            placeholder={editTarget ? '' : placeholder}
            spellCheck={false}
            autoComplete="off"
            style={editTarget ? { width: 0, minWidth: 0, padding: 0, opacity: 0 } : undefined}
          />
        </div>

        <div className="search-bar-actions">
          {filters.length > 0 && (
            <button className={`search-copy-btn${copied ? ' copied' : ''}`}
              onClick={(e) => { e.stopPropagation(); copyQueryToClipboard(); }}
              title={copied ? 'Copied!' : 'Copy query to clipboard'}>
              {copied ? (
                <svg width="14" height="14" viewBox="0 0 16 16" fill="currentColor">
                  <path d="M13.78 4.22a.75.75 0 010 1.06l-7.25 7.25a.75.75 0 01-1.06 0L2.22 9.28a.75.75 0 011.06-1.06L6 10.94l6.72-6.72a.75.75 0 011.06 0z"/>
                </svg>
              ) : (
                <svg width="14" height="14" viewBox="0 0 16 16" fill="currentColor">
                  <path d="M0 6.75C0 5.784.784 5 1.75 5h1.5a.75.75 0 010 1.5h-1.5a.25.25 0 00-.25.25v7.5c0 .138.112.25.25.25h7.5a.25.25 0 00.25-.25v-1.5a.75.75 0 011.5 0v1.5A1.75 1.75 0 019.25 16h-7.5A1.75 1.75 0 010 14.25v-7.5z"/>
                  <path d="M5 1.75C5 .784 5.784 0 6.75 0h7.5C15.216 0 16 .784 16 1.75v7.5A1.75 1.75 0 0114.25 11h-7.5A1.75 1.75 0 015 9.25v-7.5zm1.75-.25a.25.25 0 00-.25.25v7.5c0 .138.112.25.25.25h7.5a.25.25 0 00.25-.25v-7.5a.25.25 0 00-.25-.25h-7.5z"/>
                </svg>
              )}
            </button>
          )}
          <button className="search-history-btn"
            onClick={(e) => { e.stopPropagation(); setShowHistory(!showHistory); setShowSuggestions(false); setActiveIndex(0); }}
            title="Query history">
            <svg width="15" height="15" viewBox="0 0 16 16" fill="currentColor">
              <path d="M8 1a7 7 0 11-4.95 2.05l.71.71A6 6 0 108 2V1zm0 3v4.5l3.5 2.1-.5.87L7 9V4h1z"/>
            </svg>
          </button>
          <button className={`search-run-btn${loading ? ' loading' : ''}`}
            onClick={(e) => {
              e.stopPropagation();
              if (loading) onCancel(); else { saveCurrentToHistory(); onExecute(); }
            }}
            title={loading ? 'Cancel query' : 'Run query (Enter)'}>
            {loading ? (
              <span className="search-run-spinner" />
            ) : (
              <svg width="14" height="14" viewBox="0 0 16 16" fill="currentColor">
                <path d="M4 2l10 6-10 6V2z"/>
              </svg>
            )}
          </button>
        </div>
      </div>

      {/* Suggestions with group headers */}
      {showSuggestions && suggestions.length > 0 && !showHistory && (
        <div className="search-suggestions" ref={suggestionsRef}>
          {completionState !== 'field' && completionState !== 'idle' && (
            <div className="search-suggestions-header">
              {completionState === 'op' ? 'Operators' : 'Values'}
            </div>
          )}
          {suggestions.map((s, i) => (
            <div key={s.key}>
              {s.group && <div className="search-suggestions-header">{s.group}</div>}
              <div
                className={`search-suggestion${i === activeIndex ? ' active' : ''}`}
                onMouseDown={(e) => { e.preventDefault(); handleSuggestionClick(i); }}
                onMouseEnter={() => setActiveIndex(i)}
              >
                <span className="search-suggestion-label">{s.label}</span>
                {s.detail && <span className="search-suggestion-detail">{s.detail}</span>}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* History */}
      {showHistory && history.length > 0 && (
        <div className="search-suggestions" ref={suggestionsRef}>
          <div className="search-suggestions-header">Recent queries</div>
          {history.map((entry, i) => (
            <div key={i}
              className={`search-suggestion search-suggestion-history${i === activeIndex ? ' active' : ''}`}
              onMouseDown={(e) => { e.preventDefault(); applyHistory(entry); }}
              onMouseEnter={() => setActiveIndex(i)}>
              <span className="search-suggestion-label">
                {entry.filters.map((f, j) => (
                  <span key={j} className="history-filter-group">
                    {j > 0 && <span className="history-logic">and</span>}
                    {renderChip(f, null, false)}
                  </span>
                ))}
                {entry.filters.length === 0 && <span className="history-empty">No filters</span>}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
