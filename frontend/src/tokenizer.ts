/** FQL tokenizer for syntax highlighting. */

export type TokenType =
  | 'keyword'
  | 'operator'
  | 'direction'
  | 'number'
  | 'string'
  | 'function'
  | 'field'
  | 'pipe'
  | 'error'
  | 'whitespace';

export interface Token {
  type: TokenType;
  value: string;
  start: number;
  end: number;
}

const KEYWORDS = new Set([
  'last', 'at', 'select', 'group', 'by', 'top', 'bottom', 'sort', 'limit',
  'offset', 'daily', 'weekly', 'every', 'asc', 'desc', 'except', 'as',
]);

const OPERATORS = new Set([
  'and', 'or', 'not', 'in',
]);

const DIRECTIONS = new Set([
  'src', 'dst', 'ip', 'port', 'sport', 'dport', 'proto', 'flags',
]);

const FUNCTIONS = new Set([
  'sum', 'avg', 'count', 'min', 'max', 'uniq',
  'p50', 'p95', 'p99', 'stddev', 'rate',
  'first', 'last',
]);

const KNOWN_FIELDS = new Set([
  'bytes', 'packets', 'duration', 'bps', 'pps', 'tos', 'ttl', 'vlan',
  'applicationName', 'dnsQueryName', 'httpRequestHost', 'ntopTlsServerName',
  'interfaceName', 'VRFname', 'flowEndReason', 'forwardingStatus',
  'ingressInterface', 'egressInterface', 'icmpTypeIPv4', 'vlanId',
  'mplsTopLabelExp', 'httpStatusCode', 'ntopTlsVersion',
  'flowcusExporterIPv4', 'flowcusObservationDomainId',
  'sourceIPv4Address', 'destinationIPv4Address',
  'sourceIPv6Address', 'destinationIPv6Address',
  'sourceTransportPort', 'destinationTransportPort',
  'protocolIdentifier', 'octetDeltaCount', 'packetDeltaCount',
  'tcpControlBits', 'ipClassOfService', 'ipTTL',
  'flowDurationMilliseconds', 'flowStartSeconds', 'flowEndSeconds',
]);

export function tokenize(input: string): Token[] {
  const tokens: Token[] = [];
  let pos = 0;

  while (pos < input.length) {
    const ch = input[pos];

    // Whitespace
    if (/\s/.test(ch)) {
      const start = pos;
      while (pos < input.length && /\s/.test(input[pos])) pos++;
      tokens.push({ type: 'whitespace', value: input.slice(start, pos), start, end: pos });
      continue;
    }

    // Pipe
    if (ch === '|') {
      tokens.push({ type: 'pipe', value: '|', start: pos, end: pos + 1 });
      pos++;
      continue;
    }

    // String literal
    if (ch === '"') {
      const start = pos;
      pos++; // skip opening quote
      while (pos < input.length && input[pos] !== '"') {
        if (input[pos] === '\\') pos++; // skip escaped char
        pos++;
      }
      if (pos < input.length) pos++; // skip closing quote
      tokens.push({ type: 'string', value: input.slice(start, pos), start, end: pos });
      continue;
    }

    // Operators that are symbols: =, !=, >=, <=, >, <, ~, !~, +-
    if ('=!><~'.includes(ch)) {
      const start = pos;
      // Two-char operators
      if (pos + 1 < input.length) {
        const two = input.slice(pos, pos + 2);
        if (['!=', '>=', '<=', '!~', '+-'].includes(two)) {
          pos += 2;
          tokens.push({ type: 'operator', value: two, start, end: pos });
          continue;
        }
      }
      pos++;
      tokens.push({ type: 'operator', value: ch, start, end: pos });
      continue;
    }

    // Parentheses, commas, dots — treat as operator/punctuation
    if ('(),'.includes(ch)) {
      tokens.push({ type: 'operator', value: ch, start: pos, end: pos + 1 });
      pos++;
      continue;
    }

    // Range operator ..
    if (ch === '.' && pos + 1 < input.length && input[pos + 1] === '.') {
      tokens.push({ type: 'operator', value: '..', start: pos, end: pos + 2 });
      pos += 2;
      continue;
    }

    // Number-like: digits, IPs, CIDRs, durations, date-times, port ranges
    // Matches: 10.0.0.0/8, 192.168.1.0/24, 80, 1024-65535, 1M, 5m, 1h30m,
    // 2024-03-15T14:30, 08:00, etc.
    if (/[0-9]/.test(ch)) {
      const start = pos;
      // Consume a liberal number-like token
      while (pos < input.length && /[0-9a-fA-F.:/*\-+TZKMGi]/.test(input[pos])) {
        pos++;
      }
      // Also consume trailing duration-like suffixes (s, m, h, d, w, M) if preceded by digit
      // They may already be consumed above via the character class
      tokens.push({ type: 'number', value: input.slice(start, pos), start, end: pos });
      continue;
    }

    // Words (identifiers / keywords)
    if (/[a-zA-Z_]/.test(ch)) {
      const start = pos;
      while (pos < input.length && /[a-zA-Z0-9_]/.test(input[pos])) pos++;
      const word = input.slice(start, pos);
      const lower = word.toLowerCase();

      let type: TokenType;
      if (OPERATORS.has(lower)) {
        type = 'operator';
      } else if (DIRECTIONS.has(lower)) {
        type = 'direction';
      } else if (FUNCTIONS.has(lower)) {
        // Check if followed by '(' to distinguish function `last(...)` from keyword `last 5m`
        const nextNonSpace = input.slice(pos).match(/^\s*\(/);
        if (nextNonSpace) {
          type = 'function';
        } else if (KEYWORDS.has(lower)) {
          type = 'keyword';
        } else {
          type = 'function';
        }
      } else if (KEYWORDS.has(lower)) {
        type = 'keyword';
      } else if (KNOWN_FIELDS.has(word)) {
        type = 'field';
      } else {
        // Could be a named port (dns, http, etc.) or unknown field — treat as field
        type = 'field';
      }

      tokens.push({ type, value: word, start, end: pos });
      continue;
    }

    // IPv6 starting with ::
    if (ch === ':') {
      const start = pos;
      while (pos < input.length && /[0-9a-fA-F:./]/.test(input[pos])) pos++;
      if (pos === start) pos++; // consume at least one char
      tokens.push({ type: 'number', value: input.slice(start, pos), start, end: pos });
      continue;
    }

    // Wildcard *
    if (ch === '*') {
      tokens.push({ type: 'operator', value: '*', start: pos, end: pos + 1 });
      pos++;
      continue;
    }

    // Unknown character — consume and mark as error
    tokens.push({ type: 'error', value: ch, start: pos, end: pos + 1 });
    pos++;
  }

  return tokens;
}

/** Map token type to CSS class name. */
export function tokenClass(type: TokenType): string {
  switch (type) {
    case 'keyword': return 'fql-keyword';
    case 'operator': return 'fql-operator';
    case 'direction': return 'fql-direction';
    case 'number': return 'fql-number';
    case 'string': return 'fql-string';
    case 'function': return 'fql-function';
    case 'field': return 'fql-field';
    case 'pipe': return 'fql-pipe';
    case 'error': return 'fql-error';
    case 'whitespace': return '';
    default: return '';
  }
}
