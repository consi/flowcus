/** Human-friendly formatters for IPFIX / network flow fields. */

import { formatCache, cached } from './formatCache';

// ── Timezone state ────────────────────────────
let _timezone: string = Intl.DateTimeFormat().resolvedOptions().timeZone;

export function setTimezone(tz: string): void {
  _timezone = tz;
  formatCache.clear(); // Invalidate cached timestamps
}

export function getTimezone(): string {
  return _timezone;
}

// ── Interface name enrichment ────────────────────
// Map key: "ifIndex" → name (simplified: we collapse across exporters for now)
let _interfaceNames = new Map<number, string>();

export function setInterfaceNames(entries: { index: number; name: string }[]): void {
  _interfaceNames = new Map(entries.map(e => [e.index, e.name]));
  formatCache.clear();
}

export function getInterfaceName(index: number): string | undefined {
  return _interfaceNames.get(index);
}

/** Get all available IANA timezones, sorted: local first, UTC second, rest alphabetical. */
export function getAvailableTimezones(): string[] {
  const local = Intl.DateTimeFormat().resolvedOptions().timeZone;
  let all: string[];
  try {
    all = Intl.supportedValuesOf('timeZone');
  } catch {
    // Fallback for older browsers
    all = ['UTC', 'America/New_York', 'America/Chicago', 'America/Denver', 'America/Los_Angeles',
      'Europe/London', 'Europe/Paris', 'Europe/Berlin', 'Asia/Tokyo', 'Asia/Shanghai',
      'Australia/Sydney', 'Pacific/Auckland'];
  }
  const rest = all.filter(tz => tz !== local && tz !== 'UTC').sort();
  return [local, 'UTC', ...rest];
}

const PROTOCOLS: Record<number, string> = {
  0: 'HOPOPT', 1: 'ICMP', 2: 'IGMP', 6: 'TCP', 8: 'EGP', 9: 'IGP',
  17: 'UDP', 27: 'RDP', 41: 'IPv6', 43: 'IPv6-Route', 44: 'IPv6-Frag',
  46: 'RSVP', 47: 'GRE', 50: 'ESP', 51: 'AH', 58: 'ICMPv6',
  59: 'IPv6-NoNxt', 60: 'IPv6-Opts', 88: 'EIGRP', 89: 'OSPF',
  103: 'PIM', 112: 'VRRP', 132: 'SCTP', 137: 'MPLS-in-IP',
};

const WELL_KNOWN_PORTS: Record<number, string> = {
  7: 'Echo', 20: 'FTP-Data', 21: 'FTP', 22: 'SSH', 23: 'Telnet',
  25: 'SMTP', 43: 'WHOIS', 53: 'DNS', 67: 'DHCP-S', 68: 'DHCP-C',
  69: 'TFTP', 80: 'HTTP', 88: 'Kerberos', 110: 'POP3', 119: 'NNTP',
  123: 'NTP', 135: 'MS-RPC', 137: 'NetBIOS-NS', 138: 'NetBIOS-DGM',
  139: 'NetBIOS-SSN', 143: 'IMAP', 161: 'SNMP', 162: 'SNMP-Trap',
  179: 'BGP', 389: 'LDAP', 443: 'HTTPS', 445: 'SMB', 465: 'SMTPS',
  500: 'IKE', 514: 'Syslog', 520: 'RIP', 546: 'DHCPv6-C',
  547: 'DHCPv6-S', 587: 'Submission', 636: 'LDAPS', 853: 'DNS-TLS',
  873: 'Rsync', 993: 'IMAPS', 995: 'POP3S', 1080: 'SOCKS',
  1194: 'OpenVPN', 1433: 'MSSQL', 1521: 'Oracle', 1723: 'PPTP',
  2049: 'NFS', 2082: 'cPanel', 2483: 'Oracle-TLS', 3306: 'MySQL',
  3389: 'RDP', 3478: 'STUN', 4500: 'IPSec-NAT', 4739: 'IPFIX',
  5060: 'SIP', 5061: 'SIPS', 5432: 'PostgreSQL', 5900: 'VNC',
  5938: 'TeamViewer', 6379: 'Redis', 6443: 'K8s-API', 8080: 'HTTP-Alt',
  8443: 'HTTPS-Alt', 8888: 'HTTP-Alt2', 9090: 'Prometheus',
  9200: 'Elasticsearch', 9300: 'ES-Transport', 27017: 'MongoDB',
};

const TCP_FLAG_NAMES: [number, string][] = [
  [0x001, 'FIN'], [0x002, 'SYN'], [0x004, 'RST'], [0x008, 'PSH'],
  [0x010, 'ACK'], [0x020, 'URG'], [0x040, 'ECE'], [0x080, 'CWR'],
];

const DSCP_NAMES: Record<number, string> = {
  0: 'BE', 8: 'CS1', 10: 'AF11', 12: 'AF12', 14: 'AF13',
  16: 'CS2', 18: 'AF21', 20: 'AF22', 22: 'AF23', 24: 'CS3',
  26: 'AF31', 28: 'AF32', 30: 'AF33', 32: 'CS4', 34: 'AF41',
  36: 'AF42', 38: 'AF43', 40: 'CS5', 46: 'EF', 48: 'CS6', 56: 'CS7',
};

const FLOW_END_REASONS: Record<number, string> = {
  0: 'Unknown', 1: 'Idle timeout', 2: 'Active timeout',
  3: 'End of flow', 4: 'Forced end', 5: 'Lack of resources',
};

const FORWARDING_STATUS: Record<number, string> = {
  0: 'Unknown', 1: 'Forwarded', 2: 'Dropped', 3: 'Consumed',
};

const ICMP_TYPES: Record<number, string> = {
  0: 'Echo Reply', 3: 'Dest Unreachable', 4: 'Source Quench',
  5: 'Redirect', 8: 'Echo Request', 9: 'Router Advertisement',
  10: 'Router Solicitation', 11: 'Time Exceeded',
  12: 'Parameter Problem', 13: 'Timestamp', 14: 'Timestamp Reply',
};

// RFC 8158 - NAT event types
const NAT_EVENTS: Record<number, string> = {
  0: 'Reserved', 1: 'NAT Create', 2: 'NAT Delete', 3: 'NAT Pool Exhausted',
  4: 'NAT Quota Exceeded', 5: 'NAT Binding Refresh',
  6: 'NAT Port Alloc', 7: 'NAT Port Dealloc',
  8: 'NAT Thresh Reached', 9: 'NAT Thresh Cleared',
};

// RFC 8158 - NAT types
const NAT_TYPES: Record<number, string> = {
  0: 'Unknown', 1: 'NAT44', 2: 'NAT64', 3: 'NAT46',
  4: 'IPv4 no-NAT', 5: 'NAT66', 6: 'IPv6 no-NAT',
};

// RFC 5610 - Flow direction
const FLOW_DIRECTIONS: Record<number, string> = {
  0: 'Ingress', 1: 'Egress',
};

// RFC 5102 - IP version
const IP_VERSIONS: Record<number, string> = {
  4: 'IPv4', 6: 'IPv6',
};

// RFC 5655 - Firewall event types
const FIREWALL_EVENTS: Record<number, string> = {
  0: 'Ignored', 1: 'Flow Created', 2: 'Flow Deleted',
  3: 'Flow Denied', 4: 'Flow Alert', 5: 'Flow Updated',
};

// RFC 5102 - Sampling algorithm
const SAMPLING_ALGORITHMS: Record<number, string> = {
  0: 'Unassigned', 1: 'Systematic count', 2: 'Systematic time',
  3: 'Random n-out-of-N', 4: 'Uniform probabilistic',
  5: 'Property match', 6: 'Hash-based', 7: 'Hash-based + deterministic',
};

// RFC 5102 - MPLS top label type
const MPLS_LABEL_TYPES: Record<number, string> = {
  0x00: 'Unknown', 0x01: 'TE-MIDPT', 0x02: 'Pseudowire',
  0x03: 'VPN', 0x04: 'BGP', 0x05: 'LDP',
};

// IANA IPFIX - Observation point type
const OBSERVATION_POINT_TYPES: Record<number, string> = {
  0: 'Invalid', 1: 'Physical port', 2: 'Port channel',
  3: 'VLAN',
};

// RFC 8158 - NAT address realm
const ADDRESS_REALMS: Record<number, string> = {
  0: 'Reserved', 1: 'Internal', 2: 'External',
};

// RFC 7011 - Value distribution method
const VALUE_DIST_METHODS: Record<number, string> = {
  0: 'Unspecified', 1: 'Start interval', 2: 'End interval',
  3: 'Mid interval', 4: 'Simple uniform',
};

// IANA IPFIX - Anonymization technique
const ANON_TECHNIQUES: Record<number, string> = {
  0: 'Undefined', 1: 'None', 2: 'Precision Degradation/Truncation',
  3: 'Binning', 4: 'Enumeration', 5: 'Permutation',
  6: 'Structured Permutation', 7: 'Reverse Truncation',
  8: 'Noise',
};

// RFC 5102 - ICMP dest unreachable codes (for verbose display)
const ICMP_UNREACH_CODES: Record<number, string> = {
  0: 'Net Unreachable', 1: 'Host Unreachable', 2: 'Protocol Unreachable',
  3: 'Port Unreachable', 4: 'Frag Needed', 5: 'Source Route Failed',
  6: 'Dst Net Unknown', 7: 'Dst Host Unknown', 9: 'Net Admin Prohib',
  10: 'Host Admin Prohib', 11: 'Net TOS Unreach', 12: 'Host TOS Unreach',
  13: 'Admin Prohib',
};

/** Format a protocol number to its name. */
export function formatProtocol(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const num = Number(value);
  if (!Number.isFinite(num)) return '\u2014';
  const name = PROTOCOLS[num];
  return name ? `${name}` : String(num);
}

/** Format a port number. */
export function formatPort(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const num = Number(value);
  if (!Number.isFinite(num)) return '\u2014';
  return String(num);
}

/** Format a port with service name (for sidebar detail view). */
export function formatPortVerbose(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const num = Number(value);
  if (!Number.isFinite(num)) return '\u2014';
  const name = WELL_KNOWN_PORTS[num];
  return name ?? String(num);
}

/** Format TCP flags bitmask to human-readable. */
export function formatTcpFlags(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const bits = Number(value);
  if (!Number.isFinite(bits)) return '\u2014';
  if (bits === 0) return '0x000';
  const flags = TCP_FLAG_NAMES
    .filter(([mask]) => bits & mask)
    .map(([, name]) => name);
  return flags.length > 0 ? flags.join(',') : `0x${bits.toString(16).padStart(3, '0')}`;
}

/** Format DSCP/TOS value. */
export function formatTos(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const num = Number(value);
  const dscp = num >> 2; // DSCP is top 6 bits of TOS byte
  const name = DSCP_NAMES[dscp];
  return name ? `${name} (${num})` : String(num);
}

/** Collapse an IPv6 address string. */
export function collapseIPv6(addr: string): string {
  // Already collapsed or not a full IPv6
  if (!addr.includes(':')) return addr;

  // Normalize: expand any :: first, then find longest run of 0-groups to collapse
  const parts = addr.split(':');
  if (parts.length <= 2) return addr;

  // Find longest run of "0" or "0000" groups
  let bestStart = -1, bestLen = 0, curStart = -1, curLen = 0;
  for (let i = 0; i < parts.length; i++) {
    if (/^0+$/.test(parts[i]) || parts[i] === '') {
      if (curStart === -1) curStart = i;
      curLen++;
    } else {
      if (curLen > bestLen) { bestStart = curStart; bestLen = curLen; }
      curStart = -1;
      curLen = 0;
    }
  }
  if (curLen > bestLen) { bestStart = curStart; bestLen = curLen; }

  if (bestLen >= 2) {
    const before = parts.slice(0, bestStart).map(g => g.replace(/^0+/, '') || '0');
    const after = parts.slice(bestStart + bestLen).map(g => g.replace(/^0+/, '') || '0');
    const prefix = before.length === 0 ? '' : before.join(':');
    const suffix = after.length === 0 ? '' : after.join(':');
    return `${prefix}::${suffix}`;
  }

  // Just strip leading zeros
  return parts.map(g => g.replace(/^0+/, '') || '0').join(':');
}

/** Format bytes into human-readable units. */
export function formatBytes(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const num = Number(value);
  if (!Number.isFinite(num)) return '\u2014';
  if (num < 1024) return `${num} B`;
  if (num < 1024 * 1024) return `${(num / 1024).toFixed(1)} KB`;
  if (num < 1024 * 1024 * 1024) return `${(num / (1024 * 1024)).toFixed(1)} MB`;
  if (num < 1024 * 1024 * 1024 * 1024) return `${(num / (1024 * 1024 * 1024)).toFixed(2)} GB`;
  return `${(num / (1024 * 1024 * 1024 * 1024)).toFixed(2)} TB`;
}

/** Format a large number with locale-aware separators. */
export function formatNumber(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const num = Number(value);
  if (!Number.isFinite(num)) return '\u2014';
  if (!Number.isInteger(num)) {
    return num.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }
  return num.toLocaleString();
}

/** Format a packets count — just number formatting with "pkt" suffix for large values. */
export function formatPackets(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const num = Number(value);
  if (!Number.isFinite(num)) return '\u2014';
  if (num >= 1_000_000_000) return `${(num / 1_000_000_000).toFixed(2)}G`;
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`;
  if (num >= 10_000) return `${(num / 1_000).toFixed(1)}K`;
  return num.toLocaleString();
}

/** Format seconds/milliseconds duration to human-readable. */
export function formatDuration(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  let ms = Number(value);
  if (!Number.isFinite(ms)) return '\u2014';
  if (ms === 0) return '0s';

  // If it looks like milliseconds (from flowDurationMilliseconds)
  // Keep as ms for the breakdown
  const parts: string[] = [];

  const days = Math.floor(ms / 86_400_000);
  if (days > 0) { parts.push(`${days}d`); ms -= days * 86_400_000; }
  const hours = Math.floor(ms / 3_600_000);
  if (hours > 0) { parts.push(`${hours}h`); ms -= hours * 3_600_000; }
  const mins = Math.floor(ms / 60_000);
  if (mins > 0) { parts.push(`${mins}m`); ms -= mins * 60_000; }
  const secs = Math.floor(ms / 1000);
  const remaining_ms = ms % 1000;
  if (secs > 0 || parts.length === 0) {
    if (remaining_ms > 0 && parts.length === 0) {
      parts.push(`${(secs + remaining_ms / 1000).toFixed(1)}s`);
    } else {
      parts.push(`${secs}s`);
    }
  }

  return parts.join(' ');
}

/** Format timeout (seconds) to human-readable. Used for flowActiveTimeout, flowIdleTimeout. */
export function formatUptime(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  let sec = Number(value);
  if (!Number.isFinite(sec)) return '\u2014';
  if (sec === 0) return '0s';

  const parts: string[] = [];
  const days = Math.floor(sec / 86400);
  if (days > 0) { parts.push(`${days}d`); sec -= days * 86400; }
  const hours = Math.floor(sec / 3600);
  if (hours > 0) { parts.push(`${hours}h`); sec -= hours * 3600; }
  const mins = Math.floor(sec / 60);
  if (mins > 0) { parts.push(`${mins}m`); sec -= mins * 60; }
  if (sec > 0 || parts.length === 0) parts.push(`${sec}s`);

  return parts.join(' ');
}

/** Format sysUpTime (milliseconds per RFC 5102) to human-readable. */
export function formatSysUpTime(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const ms = Number(value);
  if (!Number.isFinite(ms)) return '\u2014';
  if (ms === 0) return '0s';
  return formatDuration(ms);
}

/** Format a unix timestamp using the current timezone setting. */
export function formatTimestamp(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const num = Number(value);
  if (!Number.isFinite(num) || num === 0) return '\u2014';
  const ts = num < 1e12 ? num * 1000 : num;
  return formatDateInTz(ts, _timezone);
}

/** Format a unix timestamp with full detail (for sidebar). */
export function formatTimestampAbsolute(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const num = Number(value);
  if (!Number.isFinite(num) || num === 0) return '\u2014';
  const ts = num < 1e12 ? num * 1000 : num;
  return formatDateInTz(ts, _timezone, true);
}

function formatDateInTz(tsMs: number, tz: string, _full = false): string {
  try {
    const d = new Date(tsMs);
    const now = new Date();
    const thisYear = now.getFullYear();
    const year = d.toLocaleDateString('en-US', { timeZone: tz, year: 'numeric' });
    const timeStr = d.toLocaleTimeString('sv-SE', {
      timeZone: tz, hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
    });

    // Sub-second precision: always show at least milliseconds (.000),
    // and extend to microseconds when fractional ms are present.
    const ms = tsMs % 1000;
    const subMs = ms % 1; // fractional milliseconds (µs precision)
    const micros = Math.round(subMs * 1000);
    const msStr = String(Math.floor(ms)).padStart(3, '0');
    const microStr = micros > 0
      ? `.${msStr}${String(micros).padStart(3, '0').replace(/0+$/, '')}`
      : `.${msStr}`;
    const fullTime = `${timeStr}${microStr}`;

    if (Number(year) === thisYear) {
      const mon = d.toLocaleDateString('en-US', { timeZone: tz, month: 'short' });
      const day = d.toLocaleDateString('en-US', { timeZone: tz, day: 'numeric' });
      return `${mon} ${day} ${fullTime}`;
    }
    // Older: "2024-01-15 08:12:33.123456"
    const dateStr = d.toLocaleDateString('sv-SE', { timeZone: tz });
    return `${dateStr} ${fullTime}`;
  } catch {
    return String(tsMs);
  }
}

/** Format a timestamp as relative time (e.g. "2m ago"). */
export function formatRelativeTime(tsMs: number): string {
  const now = Date.now();
  const diff = now - tsMs;
  if (diff < 0) return 'in the future';
  if (diff < 5_000) return 'just now';
  if (diff < 60_000) return `${Math.floor(diff / 1000)}s ago`;
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`;
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`;
  if (diff < 604_800_000) return `${Math.floor(diff / 86_400_000)}d ago`;
  return `${Math.floor(diff / 604_800_000)}w ago`;
}

/** Format a MAC address with colon separators. */
export function formatMac(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const s = String(value);
  // Already formatted
  if (s.includes(':') || s.includes('-')) return s.toLowerCase();
  // Raw hex string
  if (/^[0-9a-fA-F]{12}$/.test(s)) {
    return s.match(/.{2}/g)!.join(':').toLowerCase();
  }
  return s;
}

/** Format flow end reason. */
export function formatFlowEndReason(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const num = Number(value);
  return FLOW_END_REASONS[num] ?? String(num);
}

/** Format forwarding status. */
export function formatForwardingStatus(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const num = Number(value);
  // Bottom 2 bits are status, top 6 are reason
  const status = num & 0x03;
  return FORWARDING_STATUS[status] ?? String(num);
}

/** Format ICMP type/code combined value. */
export function formatIcmpTypeCode(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const num = Number(value);
  const type = (num >> 8) & 0xff;
  const code = num & 0xff;
  const name = ICMP_TYPES[type];
  return name ? `${name} (${type}/${code})` : `${type}/${code}`;
}

/** Verbose ICMP type/code — includes dest unreachable sub-codes. */
function formatIcmpTypeCodeVerbose(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const num = Number(value);
  const type = (num >> 8) & 0xff;
  const code = num & 0xff;
  const typeName = ICMP_TYPES[type];
  if (type === 3) {
    const codeName = ICMP_UNREACH_CODES[code];
    return codeName ? `${codeName} (3/${code})` : `Dest Unreachable (3/${code})`;
  }
  return typeName ? `${typeName} (${type}/${code})` : `${type}/${code}`;
}

/** Helper for simple enum lookup formatters. */
function enumFormatter(table: Record<number, string>): (value: unknown) => string {
  return (value) => {
    if (value === null || value === undefined) return '\u2014';
    const num = Number(value);
    return table[num] ?? String(num);
  };
}


const formatNatEvent = enumFormatter(NAT_EVENTS);
const formatNatType = enumFormatter(NAT_TYPES);
const formatFlowDirection = enumFormatter(FLOW_DIRECTIONS);
const formatIpVersion = enumFormatter(IP_VERSIONS);
const formatFirewallEvent = enumFormatter(FIREWALL_EVENTS);
const formatSamplingAlgorithm = enumFormatter(SAMPLING_ALGORITHMS);
const formatMplsTopLabelType = enumFormatter(MPLS_LABEL_TYPES);
const formatObservationPointType = enumFormatter(OBSERVATION_POINT_TYPES);
const formatAddressRealm = enumFormatter(ADDRESS_REALMS);
const formatValueDistMethod = enumFormatter(VALUE_DIST_METHODS);
const formatAnonTechnique = enumFormatter(ANON_TECHNIQUES);

/** Format an IP address (handles both v4 and v6). */
export function formatIp(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const s = String(value);
  if (s.includes(':')) return collapseIPv6(s);
  return s;
}

/** Format a CIDR prefix (IP with prefix length). */
export function formatCidr(value: unknown, prefixLen?: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const ip = formatIp(value);
  if (prefixLen !== null && prefixLen !== undefined) return `${ip}/${prefixLen}`;
  return ip;
}

/** Format interface index, enriched with name from IPFIX option data if available. */
export function formatInterface(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const num = Number(value);
  if (!Number.isFinite(num)) return '\u2014';
  const name = _interfaceNames.get(num);
  return name ? name : `if${num}`;
}

/** Verbose interface format for sidebar. */
export function formatInterfaceVerbose(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const num = Number(value);
  if (!Number.isFinite(num)) return '\u2014';
  const name = _interfaceNames.get(num);
  return name ? `${name} (${num})` : `if${num}`;
}

/** Format ASN. */
export function formatAsn(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const num = Number(value);
  if (num === 0) return '\u2014';
  return `AS${num}`;
}

/** Format sampling rate as 1:N. */
export function formatSamplingRate(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const num = Number(value);
  if (num === 0) return '\u2014';
  return `1:${num}`;
}

/** Format bits per second. */
export function formatBps(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const num = Number(value);
  if (num >= 1e12) return `${(num / 1e12).toFixed(2)} Tbps`;
  if (num >= 1e9) return `${(num / 1e9).toFixed(2)} Gbps`;
  if (num >= 1e6) return `${(num / 1e6).toFixed(1)} Mbps`;
  if (num >= 1e3) return `${(num / 1e3).toFixed(1)} Kbps`;
  return `${num} bps`;
}

/** Format packets per second. */
export function formatPps(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  const num = Number(value);
  if (num >= 1e9) return `${(num / 1e9).toFixed(2)} Gpps`;
  if (num >= 1e6) return `${(num / 1e6).toFixed(1)} Mpps`;
  if (num >= 1e3) return `${(num / 1e3).toFixed(1)} Kpps`;
  return `${num} pps`;
}

// ──────────────────────────────────────────────
// Field → formatter mapping
// ──────────────────────────────────────────────

type Formatter = (value: unknown) => string;

const FIELD_FORMATTERS: Record<string, Formatter> = {
  // Protocol
  protocolIdentifier: formatProtocol,

  // IP addresses
  sourceIPv4Address: formatIp,
  destinationIPv4Address: formatIp,
  sourceIPv6Address: formatIp,
  destinationIPv6Address: formatIp,
  ipNextHopIPv4Address: formatIp,
  ipNextHopIPv6Address: formatIp,
  bgpNextHopIPv4Address: formatIp,
  bgpNextHopIPv6Address: formatIp,
  postNATSourceIPv4Address: formatIp,
  postNATDestinationIPv4Address: formatIp,
  postNATSourceIPv6Address: formatIp,
  postNATDestinationIPv6Address: formatIp,
  flowcusExporterIPv4: formatIp,

  // Ports
  sourceTransportPort: formatPort,
  destinationTransportPort: formatPort,
  postNAPTSourceTransportPort: formatPort,
  postNAPTDestinationTransportPort: formatPort,
  flowcusExporterPort: formatPort,
  exporterTransportPort: formatPort,
  collectorTransportPort: formatPort,

  // Bytes
  octetDeltaCount: formatBytes,
  octetTotalCount: formatBytes,
  postOctetDeltaCount: formatBytes,
  postOctetTotalCount: formatBytes,
  layer2OctetDeltaCount: formatBytes,
  layer2OctetTotalCount: formatBytes,

  // Packets
  packetDeltaCount: formatPackets,
  packetTotalCount: formatPackets,
  postPacketDeltaCount: formatPackets,
  postPacketTotalCount: formatPackets,

  // TCP Flags
  tcpControlBits: formatTcpFlags,

  // TOS/DSCP
  ipClassOfService: formatTos,

  // Duration (milliseconds)
  flowDurationMilliseconds: formatDuration,
  flowcusFlowDuration: formatDuration,
  flowDurationMicroseconds: (v) => formatDuration(Number(v) / 1000),

  // SysUpTime (milliseconds per RFC 5102 section 5.11)
  flowStartSysUpTime: formatSysUpTime,
  flowEndSysUpTime: formatSysUpTime,

  // System init (epoch milliseconds)
  systemInitTimeMilliseconds: formatTimestamp,

  // Timestamps
  flowcusExportTime: formatTimestamp,
  flowStartSeconds: formatTimestamp,
  flowEndSeconds: formatTimestamp,
  flowStartMilliseconds: formatTimestamp,
  flowEndMilliseconds: formatTimestamp,

  // Interfaces
  ingressInterface: formatInterface,
  egressInterface: formatInterface,

  // BGP / ASN
  bgpSourceAsNumber: formatAsn,
  bgpDestinationAsNumber: formatAsn,

  // ICMP
  icmpTypeCodeIPv4: formatIcmpTypeCode,
  icmpTypeCodeIPv6: formatIcmpTypeCode,

  // MAC addresses
  sourceMacAddress: formatMac,
  destinationMacAddress: formatMac,
  postSourceMacAddress: formatMac,
  postDestinationMacAddress: formatMac,

  // Sampling
  samplingInterval: formatSamplingRate,
  samplingPacketInterval: formatSamplingRate,

  // Timeouts (seconds)
  flowActiveTimeout: formatUptime,
  flowIdleTimeout: formatUptime,

  // Flow end reason
  flowEndReason: formatFlowEndReason,

  // Forwarding status
  forwardingStatus: formatForwardingStatus,

  // NAT (RFC 8158)
  natEvent: formatNatEvent,
  natType: formatNatType,
  natOriginatingAddressRealm: formatAddressRealm,
  natQuotaExceededEvent: formatNatEvent,
  natThresholdEvent: formatNatEvent,

  // Flow direction (RFC 5610)
  flowDirection: formatFlowDirection,

  // IP version
  ipVersion: formatIpVersion,

  // Firewall (RFC 5655)
  firewallEvent: formatFirewallEvent,

  // Sampling algorithm
  samplingAlgorithm: formatSamplingAlgorithm,

  // MPLS
  mplsTopLabelType: formatMplsTopLabelType,

  // Observation / metering
  observationPointType: formatObservationPointType,
  valueDistributionMethod: formatValueDistMethod,

  // Anonymization
  anonymizationTechnique: formatAnonTechnique,
};

// Verbose formatters for sidebar — show more detail
const SIDEBAR_FORMATTERS: Record<string, Formatter> = {
  sourceTransportPort: formatPortVerbose,
  destinationTransportPort: formatPortVerbose,
  postNAPTSourceTransportPort: formatPortVerbose,
  postNAPTDestinationTransportPort: formatPortVerbose,
  systemInitTimeMilliseconds: formatTimestampAbsolute,
  flowcusExportTime: formatTimestampAbsolute,
  flowStartSeconds: formatTimestampAbsolute,
  flowEndSeconds: formatTimestampAbsolute,
  flowStartMilliseconds: formatTimestampAbsolute,
  flowEndMilliseconds: formatTimestampAbsolute,
  ingressInterface: formatInterfaceVerbose,
  egressInterface: formatInterfaceVerbose,
  icmpTypeCodeIPv4: formatIcmpTypeCodeVerbose,
  icmpTypeCodeIPv6: formatIcmpTypeCodeVerbose,
};

// Timestamp columns should NOT be cached (timezone changes invalidate)
const TIMESTAMP_COLUMNS = new Set([
  'flowcusExportTime', 'flowStartSeconds', 'flowEndSeconds',
  'flowStartMilliseconds', 'flowEndMilliseconds',
]);

/** Get the appropriate formatter for a column name, with LRU caching. */
export function getFormatter(columnName: string): Formatter {
  const fn = FIELD_FORMATTERS[columnName] ?? formatGeneric;
  // Don't cache timestamp formatters (they depend on mutable timezone)
  if (TIMESTAMP_COLUMNS.has(columnName)) return fn;
  return cached(columnName, fn);
}

/** Get verbose formatter for sidebar detail view, with LRU caching. */
export function getSidebarFormatter(columnName: string): Formatter {
  const fn = SIDEBAR_FORMATTERS[columnName] ?? FIELD_FORMATTERS[columnName] ?? formatGeneric;
  if (TIMESTAMP_COLUMNS.has(columnName)) return fn;
  return cached(`sidebar:${columnName}`, fn);
}

/** Generic formatter: numbers with locale, null/NaN as dash. */
export function formatGeneric(value: unknown): string {
  if (value === null || value === undefined) return '\u2014';
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return '\u2014';
    if (Number.isInteger(value) && Math.abs(value) >= 1000) return value.toLocaleString();
    if (!Number.isInteger(value)) return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
    return String(value);
  }
  return String(value);
}

// ──────────────────────────────────────────────
// Human-friendly column labels
// ──────────────────────────────────────────────

const COLUMN_LABELS: Record<string, string> = {
  // Core flow fields
  sourceIPv4Address: 'Src IP',
  destinationIPv4Address: 'Dst IP',
  sourceIPv6Address: 'Src IPv6',
  destinationIPv6Address: 'Dst IPv6',
  sourceTransportPort: 'Src Port',
  destinationTransportPort: 'Dst Port',
  protocolIdentifier: 'Proto',
  octetDeltaCount: 'Bytes',
  packetDeltaCount: 'Packets',
  octetTotalCount: 'Total Bytes',
  packetTotalCount: 'Total Pkts',
  tcpControlBits: 'TCP Flags',
  ipClassOfService: 'ToS/DSCP',

  // Time
  flowDurationMilliseconds: 'Duration',
  flowDurationMicroseconds: 'Duration',
  flowcusFlowDuration: 'Duration',
  flowStartSeconds: 'Flow Start',
  flowEndSeconds: 'Flow End',
  flowStartMilliseconds: 'Flow Start',
  flowEndMilliseconds: 'Flow End',
  flowStartSysUpTime: 'Flow Start Uptime',
  flowEndSysUpTime: 'Flow End Uptime',
  systemInitTimeMilliseconds: 'System Init',
  flowActiveTimeout: 'Active TMO',
  flowIdleTimeout: 'Idle TMO',

  // Flowcus system
  flowcusExportTime: 'Time',
  flowcusExporterIPv4: 'Exporter',
  flowcusExporterPort: 'Exp Port',
  flowcusObservationDomainId: 'Obs Domain',
  flowcusRowId: 'Flowcus Row UUID',

  // Interfaces
  ingressInterface: 'In If',
  egressInterface: 'Out If',

  // Routing / BGP
  bgpSourceAsNumber: 'Src AS',
  bgpDestinationAsNumber: 'Dst AS',
  ipNextHopIPv4Address: 'Next Hop',
  ipNextHopIPv6Address: 'Next Hop v6',
  bgpNextHopIPv4Address: 'BGP Next Hop',

  // L2
  vlanId: 'VLAN',
  postVlanId: 'Post VLAN',
  sourceMacAddress: 'Src MAC',
  destinationMacAddress: 'Dst MAC',

  // ICMP
  icmpTypeCodeIPv4: 'ICMP',
  icmpTypeCodeIPv6: 'ICMPv6',
  icmpTypeIPv4: 'ICMP Type',
  icmpCodeIPv4: 'ICMP Code',

  // NAT
  postNATSourceIPv4Address: 'Post-NAT Src',
  postNATDestinationIPv4Address: 'Post-NAT Dst',
  postNATSourceIPv6Address: 'Post-NAT Src v6',
  postNATDestinationIPv6Address: 'Post-NAT Dst v6',
  postNAPTSourceTransportPort: 'Post-NAT Src Port',
  postNAPTDestinationTransportPort: 'Post-NAT Dst Port',
  postOctetDeltaCount: 'Post-NAT Bytes',
  postPacketDeltaCount: 'Post-NAT Pkts',

  // Sampling
  samplingInterval: 'Sampling',
  samplingAlgorithm: 'Sample Algo',

  // Flow metadata
  flowEndReason: 'End Reason',
  forwardingStatus: 'Fwd Status',
  flowDirection: 'Direction',
  ipVersion: 'IP Version',
  templateId: 'Template',
  observationDomainId: 'Obs Domain',
  exportingProcessId: 'Export PID',

  // NAT
  natEvent: 'NAT Event',
  natType: 'NAT Type',
  natOriginatingAddressRealm: 'NAT Realm',
  natQuotaExceededEvent: 'NAT Quota Event',
  natThresholdEvent: 'NAT Threshold',

  // Firewall
  firewallEvent: 'FW Event',

  // MPLS
  mplsTopLabelType: 'MPLS Label Type',

  // Observation
  observationPointType: 'Obs Point Type',

  // Application
  applicationName: 'App Name',
  applicationDescription: 'App Desc',
  httpRequestHost: 'HTTP Host',
  httpStatusCode: 'HTTP Status',
  dnsQueryName: 'DNS Query',
  dnsResponseCode: 'DNS RCode',

  // TCP details
  tcpSequenceNumber: 'TCP Seq',
  tcpAcknowledgementNumber: 'TCP Ack',
  tcpWindowSize: 'TCP Win',
  tcpHeaderLength: 'TCP HdrLen',

  // Multicast
  postMCastOctetDeltaCount: 'MCast Bytes',
  postMCastPacketDeltaCount: 'MCast Pkts',

  // VRF
  ingressVRFID: 'In VRF',
  egressVRFID: 'Out VRF',
  VRFname: 'VRF Name',
};

export function getColumnLabel(name: string): string {
  return COLUMN_LABELS[name] ?? name;
}

// ──────────────────────────────────────────────
// Key columns — shown in compact table view
// ──────────────────────────────────────────────

/** Default columns for new users — shown in this exact order. */
export const DEFAULT_COLUMNS = [
  'flowcusExportTime',
  'sourceIPv4Address',
  'sourceTransportPort',
  'destinationIPv4Address',
  'destinationTransportPort',
  'protocolIdentifier',
  'octetDeltaCount',
  'flowDurationMilliseconds',
  'tcpControlBits',
];

/** Columns always shown in compact view, in display order. */
const KEY_COLUMNS_ORDERED = [
  ...DEFAULT_COLUMNS,
  'sourceIPv6Address',
  'destinationIPv6Address',
  'packetDeltaCount',
  'flowcusFlowDuration',
];


/**
 * Select which columns to show in the compact table view.
 * Timestamp always first. Aggregation queries show all columns.
 */
export function selectVisibleColumns(columns: { name: string; type: string }[]): number[] {
  const names = columns.map(c => c.name);
  const isAgg = names.some(n =>
    n.startsWith('sum(') || n.startsWith('avg(') || n.startsWith('count') ||
    n.startsWith('min(') || n.startsWith('max(') || n.startsWith('uniq(') ||
    n.startsWith('p50(') || n.startsWith('p95(') || n.startsWith('p99(') ||
    n.startsWith('rate(') || n.startsWith('stddev(')
  );
  if (isAgg) return columns.map((_, i) => i);

  // Build index map for O(1) lookup
  const nameToIndex = new Map(columns.map((c, i) => [c.name, i]));

  // Pick key columns in preferred order
  const visible: number[] = [];
  for (const name of KEY_COLUMNS_ORDERED) {
    const idx = nameToIndex.get(name);
    if (idx !== undefined) visible.push(idx);
  }

  // If we have very few key columns, show all
  return visible.length >= 3 ? visible : columns.map((_, i) => i);
}

// ──────────────────────────────────────────────
// Flow duration computation (from start/end cols)
// ──────────────────────────────────────────────

/** Time field pairs that can be used to compute duration [startCol, endCol, multiplierToMs]. */
const DURATION_PAIRS: [string, string, number][] = [
  ['flowStartMilliseconds', 'flowEndMilliseconds', 1],
  ['flowStartSeconds', 'flowEndSeconds', 1000],
  ['flowStartSysUpTime', 'flowEndSysUpTime', 1],
];

/**
 * Try to compute flow duration in ms from available start/end columns.
 * Returns null if no suitable pair found or if duration fields already exist.
 */
export function computeFlowDuration(
  columns: { name: string }[],
  row: unknown[],
): number | null {
  const nameToIdx = new Map(columns.map((c, i) => [c.name, i]));

  // Already has native duration — don't recompute
  if (nameToIdx.has('flowDurationMilliseconds') || nameToIdx.has('flowcusFlowDuration')) {
    const idx = nameToIdx.get('flowDurationMilliseconds') ?? nameToIdx.get('flowcusFlowDuration')!;
    const val = row[idx];
    if (val !== null && val !== undefined) {
      const n = Number(val);
      return Number.isFinite(n) ? n : null;
    }
  }

  // Try to compute from start/end pairs
  for (const [startCol, endCol, mult] of DURATION_PAIRS) {
    const si = nameToIdx.get(startCol);
    const ei = nameToIdx.get(endCol);
    if (si !== undefined && ei !== undefined) {
      const sv = Number(row[si]);
      const ev = Number(row[ei]);
      if (Number.isFinite(sv) && Number.isFinite(ev) && ev >= sv) {
        return (ev - sv) * mult;
      }
    }
  }

  return null;
}

// ──────────────────────────────────────────────
// Derived fields for the sidebar
// ──────────────────────────────────────────────

export interface DerivedField {
  label: string;
  value: string;
  raw?: string;
}

/**
 * Compute derived fields from raw row data for the sidebar.
 *
 * - Flow Start/End absolute time from systemInitTimeMilliseconds + sysUpTime
 * - Flow duration (if not natively present)
 * - Bits per second (bytes * 8 / duration)
 * - Packets per second
 * - Average packet size (bytes / packets)
 */
export function computeDerivedFields(
  columns: { name: string }[],
  row: unknown[],
): DerivedField[] {
  const idx = new Map(columns.map((c, i) => [c.name, i]));
  const get = (name: string): number | null => {
    const i = idx.get(name);
    if (i === undefined) return null;
    const v = Number(row[i]);
    return Number.isFinite(v) ? v : null;
  };

  const fields: DerivedField[] = [];

  // --- Absolute flow times ---
  // Try native timestamps first, then sysUpTime-derived, then fall back to
  // ingestion time (flowcusExportTime) so the user always sees *some* time
  // reference even when the exporter sends epoch-zero timestamps.

  const exportTime = get('flowcusExportTime');

  const flowStartMs = get('flowStartMilliseconds');
  const flowStartSec = get('flowStartSeconds');
  const flowEndMs = get('flowEndMilliseconds');
  const flowEndSec = get('flowEndSeconds');
  const sysInit = get('systemInitTimeMilliseconds');
  const startUp = get('flowStartSysUpTime');
  const endUp = get('flowEndSysUpTime');

  // Resolve flow start: prefer ms > sec > sysUpTime-derived > export time
  let resolvedStartMs: number | null = null;
  let startSource = '';
  if (flowStartMs !== null && flowStartMs > 0) {
    resolvedStartMs = flowStartMs;
    startSource = `flowStartMilliseconds(${flowStartMs})`;
  } else if (flowStartSec !== null && flowStartSec > 0) {
    resolvedStartMs = flowStartSec * 1000;
    startSource = `flowStartSeconds(${flowStartSec}) * 1000`;
  } else if (sysInit !== null && sysInit > 0 && startUp !== null) {
    resolvedStartMs = sysInit + startUp;
    startSource = `sysInit(${sysInit}) + startUptime(${startUp})`;
  } else if (exportTime !== null && exportTime > 0) {
    resolvedStartMs = exportTime;
    startSource = `flowcusExportTime(${exportTime}) — fallback`;
  }

  if (resolvedStartMs !== null) {
    fields.push({
      label: 'Flow Start',
      value: formatDateInTz(resolvedStartMs, _timezone, true),
      raw: `${startSource} = ${resolvedStartMs}ms`,
    });
  }

  // Resolve flow end: prefer ms > sec > sysUpTime-derived > export time
  let resolvedEndMs: number | null = null;
  let endSource = '';
  if (flowEndMs !== null && flowEndMs > 0) {
    resolvedEndMs = flowEndMs;
    endSource = `flowEndMilliseconds(${flowEndMs})`;
  } else if (flowEndSec !== null && flowEndSec > 0) {
    resolvedEndMs = flowEndSec * 1000;
    endSource = `flowEndSeconds(${flowEndSec}) * 1000`;
  } else if (sysInit !== null && sysInit > 0 && endUp !== null) {
    resolvedEndMs = sysInit + endUp;
    endSource = `sysInit(${sysInit}) + endUptime(${endUp})`;
  } else if (exportTime !== null && exportTime > 0) {
    resolvedEndMs = exportTime;
    endSource = `flowcusExportTime(${exportTime}) — fallback`;
  }

  if (resolvedEndMs !== null) {
    fields.push({
      label: 'Flow End',
      value: formatDateInTz(resolvedEndMs, _timezone, true),
      raw: `${endSource} = ${resolvedEndMs}ms`,
    });
  }

  // --- Flow duration ---
  const durationMs = computeFlowDuration(columns, row);
  const hasNativeDuration = idx.has('flowDurationMilliseconds') || idx.has('flowcusFlowDuration');
  if (!hasNativeDuration && durationMs !== null) {
    fields.push({
      label: 'Duration',
      value: formatDuration(durationMs),
      raw: `${durationMs}ms`,
    });
  }

  // --- Rate calculations (need both bytes and duration) ---
  const bytes = get('octetDeltaCount') ?? get('octetTotalCount');
  const packets = get('packetDeltaCount') ?? get('packetTotalCount');
  const durSec = durationMs !== null && durationMs > 0 ? durationMs / 1000 : null;

  if (bytes !== null && durSec !== null) {
    const bps = (bytes * 8) / durSec;
    fields.push({
      label: 'Bit Rate',
      value: formatBps(bps),
      raw: `${bytes} bytes * 8 / ${durSec.toFixed(3)}s`,
    });
  }

  if (packets !== null && durSec !== null) {
    const pps = packets / durSec;
    fields.push({
      label: 'Packet Rate',
      value: formatPps(pps),
      raw: `${packets} pkts / ${durSec.toFixed(3)}s`,
    });
  }

  // --- Average packet size ---
  if (bytes !== null && packets !== null && packets > 0) {
    const avgSize = bytes / packets;
    fields.push({
      label: 'Avg Pkt Size',
      value: `${avgSize.toFixed(0)} B`,
      raw: `${bytes} bytes / ${packets} packets`,
    });
  }

  return fields;
}
