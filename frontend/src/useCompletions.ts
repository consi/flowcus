import { useCallback, useEffect, useMemo, useState } from 'react';
import { fetchFields, type FieldInfo } from './api';

export interface Completion {
  label: string;
  kind: string; // 'keyword' | 'operator' | 'direction' | 'function' | 'field' | 'port' | 'unit'
}

const TIME_KEYWORDS: Completion[] = [
  { label: 'last', kind: 'keyword' },
  { label: 'at', kind: 'keyword' },
  { label: 'offset', kind: 'keyword' },
];

const DURATION_UNITS: Completion[] = [
  { label: 's', kind: 'unit' },
  { label: 'm', kind: 'unit' },
  { label: 'h', kind: 'unit' },
  { label: 'd', kind: 'unit' },
  { label: 'w', kind: 'unit' },
  { label: 'M', kind: 'unit' },
];

const FILTER_KEYWORDS: Completion[] = [
  { label: 'src', kind: 'direction' },
  { label: 'dst', kind: 'direction' },
  { label: 'ip', kind: 'direction' },
  { label: 'port', kind: 'direction' },
  { label: 'sport', kind: 'direction' },
  { label: 'dport', kind: 'direction' },
  { label: 'proto', kind: 'direction' },
  { label: 'flags', kind: 'direction' },
  { label: 'bytes', kind: 'field' },
  { label: 'packets', kind: 'field' },
  { label: 'duration', kind: 'field' },
  { label: 'bps', kind: 'field' },
  { label: 'pps', kind: 'field' },
];

const NAMED_PORTS: Completion[] = [
  { label: 'dns', kind: 'port' },
  { label: 'http', kind: 'port' },
  { label: 'https', kind: 'port' },
  { label: 'ssh', kind: 'port' },
  { label: 'ftp', kind: 'port' },
  { label: 'smtp', kind: 'port' },
  { label: 'ntp', kind: 'port' },
  { label: 'snmp', kind: 'port' },
  { label: 'syslog', kind: 'port' },
  { label: 'rdp', kind: 'port' },
  { label: 'mysql', kind: 'port' },
  { label: 'postgres', kind: 'port' },
  { label: 'redis', kind: 'port' },
  { label: 'bgp', kind: 'port' },
];

const OPERATORS: Completion[] = [
  { label: 'and', kind: 'operator' },
  { label: 'or', kind: 'operator' },
  { label: 'not', kind: 'operator' },
  { label: 'in', kind: 'operator' },
  { label: 'select', kind: 'keyword' },
  { label: 'group by', kind: 'keyword' },
  { label: 'top', kind: 'keyword' },
  { label: 'bottom', kind: 'keyword' },
  { label: 'sort', kind: 'keyword' },
  { label: 'limit', kind: 'keyword' },
];

const AGG_FUNCTIONS: Completion[] = [
  { label: 'sum', kind: 'function' },
  { label: 'avg', kind: 'function' },
  { label: 'count', kind: 'function' },
  { label: 'min', kind: 'function' },
  { label: 'max', kind: 'function' },
  { label: 'uniq', kind: 'function' },
  { label: 'p50', kind: 'function' },
  { label: 'p95', kind: 'function' },
  { label: 'p99', kind: 'function' },
  { label: 'stddev', kind: 'function' },
  { label: 'rate', kind: 'function' },
  { label: 'first', kind: 'function' },
  { label: 'last', kind: 'function' },
];

/** Extract the current word fragment being typed at the cursor position. */
function getCurrentWord(text: string, cursorPos: number): { word: string; start: number } {
  let start = cursorPos;
  while (start > 0 && /[a-zA-Z0-9_]/.test(text[start - 1])) {
    start--;
  }
  return { word: text.slice(start, cursorPos), start };
}

export function useCompletions() {
  const [remoteFields, setRemoteFields] = useState<FieldInfo[]>([]);

  useEffect(() => {
    fetchFields().then(setRemoteFields).catch(() => {
      // Fields endpoint may not be available yet — use empty list
    });
  }, []);

  const allCompletions = useMemo<Completion[]>(() => {
    const fieldCompletions: Completion[] = remoteFields.map((f) => ({
      label: f.name,
      kind: 'field',
    }));
    return [
      ...TIME_KEYWORDS,
      ...DURATION_UNITS,
      ...FILTER_KEYWORDS,
      ...NAMED_PORTS,
      ...OPERATORS,
      ...AGG_FUNCTIONS,
      ...fieldCompletions,
    ];
  }, [remoteFields]);

  const getCompletions = useCallback(
    (text: string, cursorPos: number): { items: Completion[]; wordStart: number } => {
      const { word, start } = getCurrentWord(text, cursorPos);
      if (word.length === 0) {
        return { items: [], wordStart: start };
      }
      const lower = word.toLowerCase();
      const items = allCompletions.filter((c) =>
        c.label.toLowerCase().startsWith(lower) && c.label.toLowerCase() !== lower,
      );
      // Deduplicate by label
      const seen = new Set<string>();
      const unique = items.filter((c) => {
        if (seen.has(c.label)) return false;
        seen.add(c.label);
        return true;
      });
      return { items: unique.slice(0, 15), wordStart: start };
    },
    [allCompletions],
  );

  return { getCompletions };
}
