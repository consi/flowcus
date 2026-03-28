# Flowcus Query Language (FQL) Specification

## Design Principles

1. **Keystroke-efficient**: network engineers type these all day. Every character counts.
2. **Network-native**: CIDRs, port ranges, protocol names are first-class, not functions.
3. **Pipeline model**: `time | filter | select | aggregate` — each stage is optional.
4. **Implicit defaults**: no time = last 1h, no select = all fields, no aggregate = raw records.

## Query Structure

```
[time_range] [| filter_expression] [| select fields] [| aggregate_expression]
```

Pipes separate stages. Each stage is optional. Minimal query: just a time range or filter.

---

## 1. Time Ranges

Time is always the first part of a query. If omitted, defaults to `last 1h`.

### Duration Units

```
s = seconds     m = minutes     h = hours
d = days        w = weeks       M = months
```

Combinable: `1h30m`, `2d12h`, `1w3d`

### Relative Time (most common)

```
last 5m                     # last 5 minutes
last 1h                     # last 1 hour (default)
last 24h                    # last 24 hours
last 7d                     # last 7 days
last 1M                     # last 1 month
last 1h30m                  # last 90 minutes
```

### Point-in-Time with Window

```
at 2024-03-15T14:30 +-5m    # 14:25 to 14:35 on that day
at 2024-03-15T14:30 +10m    # 14:30 to 14:40
at 2024-03-15T14:30 -1h     # 13:30 to 14:30
at 14:30 +-5m               # today at 14:30 +/- 5 min
```

### Absolute Ranges

```
2024-03-15                           # full day
2024-03-15T08:00..2024-03-15T17:00   # business hours
2024-03-15T08:00..12:00              # same day, 08:00 to 12:00
2024-03-15..2024-03-20               # 5-day range
08:00..17:00                         # today, business hours
```

### Recurring Windows (for pattern analysis)

```
last 7d daily 09:00..10:00          # 9-10 AM each of the last 7 days
last 30d weekly mon 08:00..17:00    # business hours every Monday, last 30 days
last 24h every 1h                   # 24 one-hour windows (for comparison)
```

### Combined Time Ranges

```
(last 1h, 2024-03-10T14:00..15:00)  # compare now vs. a past incident
(last 1h, last 1h offset 1d)        # compare this hour vs. same hour yesterday
(last 1h, last 1h offset 1w)        # this hour vs. same hour last week
```

### Offset (for comparisons)

```
last 1h offset 1d            # same hour, yesterday
last 1h offset 1w            # same hour, last week
```

---

## 2. Filtering

Filters go after the time range, separated by `|`. Boolean operators: `and`, `or`, `not`, parentheses.

### IP Addresses

```
src 10.0.0.1                         # exact source IP
dst 192.168.1.0/24                   # destination in CIDR
ip 10.0.0.0/8                        # either src or dst (direction-agnostic)
src 10.1.0.0/16 and dst 10.2.0.0/16  # subnet-to-subnet
src not 192.168.0.0/16               # negation
src in (10.0.0.1, 10.0.0.2, 10.0.0.3)  # explicit IP list
src 10.*.*.1                         # wildcard octets (SiLK-style)
src 10.1-5.*.*                       # octet ranges
```

**IPv6:**
```
src 2001:db8::/32
dst fe80::/10
ip ::ffff:10.0.0.0/104              # IPv4-mapped IPv6
```

### Ports

```
port 53                          # either src or dst port
dport 80                         # destination port
sport 1024-65535                 # source port range (ephemeral)
dport 80,443,8080                # port list
dport 80-90,443,8080-8090        # mixed list and ranges
dport 1024-                      # open-ended: 1024 to 65535
port not 22                      # negation
```

**Named ports (built-in aliases):**
```
dport http                       # → 80,8080,8443
dport https                      # → 443
dport dns                        # → 53
port ssh                         # → 22
dport ntp                        # → 123
dport syslog                     # → 514
dport snmp                       # → 161,162
```

### Protocols

```
proto tcp                        # protocol 6
proto udp                        # protocol 17
proto icmp                       # protocol 1
proto gre                        # protocol 47
proto 47                         # by number
proto not icmp                   # negation
proto tcp,udp                    # list
```

### TCP Flags

```
flags syn                        # SYN set
flags syn,ack                    # SYN and ACK both set
flags not rst                    # RST not set
flags syn and not ack            # SYN-only (connection initiation)
```

### Numeric Fields (bytes, packets, duration, etc.)

```
bytes > 1M                       # human-readable suffixes: K, M, G, T
bytes 1M-10M                     # range
packets > 100
duration > 30s                   # flow duration
bps > 10M                        # bits per second (computed)
pps > 1K                         # packets per second (computed)
```

### String Fields (RE2 regex support)

IPFIX has string-typed IEs: `interfaceName`, `applicationName`, `dnsQueryName`,
`httpRequestHost`, `ntopTlsServerName`, vendor-specific strings, etc.

```
applicationName = "HTTPS"            # exact match
applicationName ~ "HTTP.*"           # RE2 regex match
applicationName !~ "^(DNS|NTP)$"     # negative regex
dnsQueryName ~ ".*\.evil\.com$"      # DNS query pattern
httpRequestHost = "api.example.com"  # exact
ntopTlsServerName ~ ".*google.*"     # TLS SNI contains google
interfaceName in ("eth0", "eth1")    # list
VRFname = "CUSTOMER_A"              # VRF name
```

### IPFIX-Specific Fields

Any IPFIX IE name from the registry can be used directly:

```
flowEndReason = 1                    # idle timeout
forwardingStatus = 64                # forwarded
ingressInterface = 42
egressInterface != 0
icmpTypeIPv4 = 8                     # echo request
vlanId = 100
vlanId in (100, 200, 300)
mplsTopLabelExp > 0
```

### System Columns

```
flowcusExporterIPv4 = 10.0.0.1       # filter by exporter
flowcusObservationDomainId = 42      # filter by domain
```

### Compound Filters

```
# DDoS detection: high PPS UDP to single destination
proto udp and dport not 53 and pps > 10K and dst 203.0.113.1

# Lateral movement: internal-to-internal SSH
src 10.0.0.0/8 and dst 10.0.0.0/8 and dport ssh and bytes > 1M

# Data exfiltration: large outbound flows to non-RFC1918
src 10.0.0.0/8 and dst not (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16) and bytes > 100M

# DNS tunneling indicator: unusual DNS record sizes
dport dns and bytes > 10K

# Scanning activity: many destinations, few packets each
proto tcp and flags syn and not flags ack and packets < 4
```

---

## 3. Field Selection

Select which fields to return. Default: all fields.

```
| select src, dst, dport, bytes, packets
| select src, dst, proto, applicationName
| select *                              # explicit all (same as omitting)
| select * except (paddingOctets)       # all except specific fields
```

### Computed Fields

```
| select src, dst, bps, pps, duration
| select src, dst, bytes / packets as avg_pkt_size
```

`bps` (bits/sec) and `pps` (packets/sec) are computed from bytes, packets, and flow duration.

---

## 4. Aggregation

Aggregation functions, grouping, sorting, and limiting.

### Group By

```
| group by src                          # group by source IP
| group by src, dport                   # group by pair
| group by dst /24                      # group by /24 subnet
| group by src /16, dst /16             # subnet-to-subnet matrix
| group by proto                        # by protocol
| group by flowcusExporterIPv4          # by exporter
```

### Time Bucketing

```
| group by 5m                           # 5-minute time buckets
| group by 1h                           # hourly
| group by 1d                           # daily
| group by 5m, src                      # time series per source
| group by 1h, proto                    # protocol breakdown over time
```

### Aggregate Functions

```
sum(bytes)                  # total bytes
sum(packets)                # total packets
count()                     # flow count
avg(bytes)                  # average bytes per flow
min(bytes)                  # minimum
max(bytes)                  # maximum
p50(bytes)                  # 50th percentile (median)
p95(bytes)                  # 95th percentile
p99(bytes)                  # 99th percentile
stddev(bytes)               # standard deviation
uniq(src)                   # unique count (HyperLogLog)
uniq(dst)                   # unique destination count
first(flowStartSeconds)     # first seen
last(flowEndSeconds)        # last seen
rate(bytes)                 # bytes/sec over the time window
```

### Sorting and Limiting

```
| top 10 by sum(bytes)                  # top 10 by total bytes
| top 20 by count()                     # top 20 by flow count
| top 5 by uniq(dst)                    # top 5 sources by unique destinations (scanning)
| bottom 10 by avg(bytes)               # smallest average flow size
| sort sum(bytes) desc                  # explicit sort
| limit 100                             # raw limit
```

---

## 5. Complete Query Examples

### Operational Queries

```fql
# Top 10 talkers by bytes in last hour
last 1h | top 10 by sum(bytes)

# Top 20 destinations by flow count in last 5 minutes
last 5m | top 20 by count() | group by dst

# Traffic by protocol, 5-minute buckets, last 6 hours
last 6h | group by 5m, proto | sum(bytes)

# Bandwidth usage per exporter, hourly
last 24h | group by 1h, flowcusExporterIPv4 | rate(bytes)
```

### Security / SIEM Queries

```fql
# Potential port scan: many SYN, few packets, many destinations
last 1h | proto tcp and flags syn and not flags ack and packets < 4
         | group by src | top 20 by uniq(dst)

# DNS exfiltration: unusually large DNS flows
last 24h | dport dns and bytes > 5K
          | group by src, dnsQueryName | top 50 by sum(bytes)

# Lateral movement: internal SSH with significant data
last 1h | src 10.0.0.0/8 and dst 10.0.0.0/8 and dport ssh and bytes > 1M
         | select src, dst, sum(bytes), count()

# Beaconing: regular intervals to same destination
last 24h | dst 203.0.113.0/24 and proto tcp
          | group by 5m, src, dst | count()

# Data exfiltration: large outbound to non-RFC1918
last 1h | src 10.0.0.0/8
           and dst not (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)
           and bytes > 50M
         | top 10 by sum(bytes)

# Unusual application traffic pattern
last 1h | applicationName ~ ".*torrent.*" or applicationName ~ ".*tunnel.*"
         | group by src | top 20 by sum(bytes)

# TLS connections to suspicious domains
last 24h | ntopTlsServerName ~ ".*\.(tk|ml|ga|cf)$"
          | group by dst, ntopTlsServerName | top 50 by count()

# ICMP flood detection
last 5m | proto icmp and pps > 1K | group by dst | top 10 by sum(packets)
```

### Subnet Analysis

```fql
# Subnet-to-subnet traffic matrix
last 1h | group by src /16, dst /16 | sum(bytes) | top 20 by sum(bytes)

# Traffic from engineering to production
last 1h | src 10.1.0.0/16 and dst 10.2.0.0/16
         | group by dport | top 10 by sum(bytes)

# Per-VLAN traffic breakdown
last 24h | group by vlanId, 1h | sum(bytes)
```

### Capacity Planning

```fql
# Peak bandwidth per interface, daily
last 30d | group by 1d, ingressInterface | max(rate(bytes))

# 95th percentile link utilization
last 7d | group by 1h, ingressInterface | p95(rate(bytes))

# Growth trend: daily bytes by destination subnet
last 90d | group by 1d, dst /16 | sum(bytes) | top 20 by sum(bytes)
```

### Incident Investigation

```fql
# What happened at the incident time?
at 2024-03-15T14:30 +-10m | dst 203.0.113.42
  | select src, dst, dport, proto, bytes, packets, applicationName

# Compare incident traffic to normal baseline
(at 2024-03-15T14:30 +-10m, at 2024-03-08T14:30 +-10m) | dst 203.0.113.42
  | group by src | sum(bytes)

# Trace all flows from a compromised host
last 24h | src 10.1.2.3 | select dst, dport, proto, bytes, applicationName
          | sort sum(bytes) desc

# Find flows during a specific 30-second window
2024-03-15T14:30:00..14:30:30 | src 10.1.2.3 and dst 203.0.113.0/24

# Recurring pattern analysis: same time each day for a week
last 7d daily 14:25..14:35 | dst 203.0.113.42 | group by 1d, src | count()
```

### Application Analysis

```fql
# Top applications by bandwidth
last 1h | group by applicationName | top 20 by sum(bytes)

# HTTP response codes distribution
last 1h | httpStatusCode > 0 | group by httpStatusCode | count()

# Slow DNS queries (large response)
last 1h | dport dns and bytes > 2K
         | group by dnsQueryName | top 20 by avg(bytes)

# TLS version distribution
last 24h | ntopTlsVersion > 0
          | group by ntopTlsVersion | count() | sort count() desc
```

---

## 6. Operator Reference

### Comparison Operators
| Operator | Meaning | Example |
|----------|---------|---------|
| `=` | equals | `dport = 80` |
| `!=` | not equals | `proto != icmp` |
| `>` | greater than | `bytes > 1M` |
| `>=` | greater or equal | `packets >= 100` |
| `<` | less than | `duration < 1s` |
| `<=` | less or equal | `bps <= 10M` |
| `~` | regex match (RE2) | `dnsQueryName ~ ".*\.com$"` |
| `!~` | negative regex | `applicationName !~ "^HTTP"` |
| `in` | member of list | `dport in (80, 443)` |
| `not in` | not in list | `src not in (10.0.0.1, 10.0.0.2)` |

### Boolean Operators
| Operator | Example |
|----------|---------|
| `and` | `proto tcp and dport 80` |
| `or` | `dport 80 or dport 443` |
| `not` | `not proto icmp` |
| `()` | `(src 10.0.0.0/8 or src 172.16.0.0/12) and dport 80` |

### Byte Suffixes
| Suffix | Value |
|--------|-------|
| `K` | 1,000 |
| `M` | 1,000,000 |
| `G` | 1,000,000,000 |
| `T` | 1,000,000,000,000 |
| `Ki` | 1,024 |
| `Mi` | 1,048,576 |
| `Gi` | 1,073,741,824 |

### Named Ports (built-in)
| Name | Ports |
|------|-------|
| `http` | 80, 8080, 8443 |
| `https` | 443 |
| `dns` | 53 |
| `ssh` | 22 |
| `ftp` | 20, 21 |
| `smtp` | 25, 587 |
| `ntp` | 123 |
| `snmp` | 161, 162 |
| `syslog` | 514 |
| `rdp` | 3389 |
| `mysql` | 3306 |
| `postgres` | 5432 |
| `redis` | 6379 |
| `bgp` | 179 |

### Short Field Aliases
| Alias | Full IPFIX IE name |
|-------|-------------------|
| `src` | `sourceIPv4Address` / `sourceIPv6Address` |
| `dst` | `destinationIPv4Address` / `destinationIPv6Address` |
| `sport` | `sourceTransportPort` |
| `dport` | `destinationTransportPort` |
| `port` | either sport or dport |
| `ip` | either src or dst |
| `proto` | `protocolIdentifier` |
| `bytes` | `octetDeltaCount` |
| `packets` | `packetDeltaCount` |
| `flags` | `tcpControlBits` |
| `tos` | `ipClassOfService` |
| `ttl` | `ipTTL` |
| `vlan` | `vlanId` |
| `duration` | `flowDurationMilliseconds` (computed from start/end) |
| `bps` | computed: bytes * 8 / duration |
| `pps` | computed: packets / duration |

---

## 7. Grammar (EBNF Summary)

```ebnf
query       = [time_range] { "|" stage }
stage       = filter | select | aggregate

time_range  = relative | absolute | point | recurring | combined
relative    = "last" duration ["offset" duration]
point       = "at" datetime ["+-" duration | "+" duration | "-" duration]
absolute    = datetime ".." datetime | date
recurring   = relative ("daily" | "weekly" weekday) time ".." time
combined    = "(" time_range "," time_range {"," time_range} ")"

filter      = condition { ("and" | "or") condition }
condition   = [not] field_filter | "(" filter ")"
field_filter = ip_filter | port_filter | proto_filter | flag_filter
             | numeric_filter | string_filter | any_field_filter

ip_filter   = ("src" | "dst" | "ip") [not] (cidr | ip_addr | ip_list | ip_wildcard)
port_filter = ("sport" | "dport" | "port") [not] (port_spec | port_name)
port_spec   = port_range { "," port_range }
port_range  = NUMBER ["-" [NUMBER]]

select      = "select" (field_list | "*" ["except" "(" field_list ")"])
aggregate   = group_by | top_bottom | sort | limit
group_by    = "group" "by" group_key {"," group_key} "|" agg_func {"," agg_func}
top_bottom  = ("top" | "bottom") NUMBER "by" agg_func
sort        = "sort" agg_func ("asc" | "desc")
limit       = "limit" NUMBER

agg_func    = func_name "(" [field] ")"
func_name   = "sum" | "avg" | "min" | "max" | "count" | "uniq"
             | "p50" | "p95" | "p99" | "stddev" | "rate"
             | "first" | "last"

duration    = NUMBER unit { NUMBER unit }
unit        = "s" | "m" | "h" | "d" | "w" | "M"
```

---

## 8. Execution Model

1. **Time range** → prunes directory tree (YYYY/MM/DD/HH) + part min/max timestamps
2. **Filter** → uses column_index.bin min/max for part-level skip, bloom filters for granule-level skip, then scans matching granules only
3. **Select** → reads only the needed column files (columnar projection pushdown)
4. **Aggregate** → streaming aggregation over matching rows, using the worker pool for parallelism

This maps directly to the storage engine's scan-limiting architecture:
- Time → directory pruning + part name filtering
- Filter → column_index.bin → .bloom → .mrk → .col (minimum I/O path)
- Select → read only requested .col files
- Aggregate → parallel reduction across parts
