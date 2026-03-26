//! UUIDv7 generator for `flowcusRowId`.
//!
//! Generates RFC 9562 UUIDv7 identifiers using millisecond-precision timestamps
//! with a monotonic counter for sub-millisecond ordering. Stored as `[u64; 2]`
//! (big-endian high/low) so lexicographic comparison preserves temporal order.
//!
//! # Layout (128 bits)
//!
//! ```text
//! Bits   0..47   unix_ts_ms     48-bit millisecond timestamp
//! Bits  48..51   version        0b0111 (7)
//! Bits  52..63   rand_a         12-bit counter (high)
//! Bits  64..65   variant        0b10
//! Bits  66..127  rand_b         62-bit counter (low)
//! ```

/// Monotonic UUIDv7 generator.
///
/// Guarantees strictly increasing output for the same or increasing timestamps.
/// The counter resets when the millisecond changes and increments within the
/// same millisecond, providing 74 bits of counter space (more than enough).
pub struct Uuid7Generator {
    last_ms: u64,
    counter: u64,
}

impl Uuid7Generator {
    pub fn new() -> Self {
        Self {
            last_ms: 0,
            counter: 0,
        }
    }

    /// Generate a UUIDv7 from the given millisecond timestamp.
    ///
    /// Returns `[high_u64, low_u64]` in big-endian layout. Sequential calls
    /// produce strictly increasing output under lexicographic `(high, low)`
    /// comparison, even if `timestamp_ms` goes backward (out-of-order UDP
    /// packets). When time goes backward, the generator keeps the previous
    /// timestamp and increments the counter to preserve monotonicity.
    pub fn generate(&mut self, timestamp_ms: u64) -> [u64; 2] {
        match timestamp_ms.cmp(&self.last_ms) {
            std::cmp::Ordering::Greater => {
                // Time moved forward — reset counter for new millisecond.
                self.last_ms = timestamp_ms;
                self.counter = 0;
            }
            std::cmp::Ordering::Equal => {
                // Same millisecond — increment counter.
                self.counter += 1;
            }
            std::cmp::Ordering::Less => {
                // Time went backward (out-of-order packet). Keep the previous
                // (higher) timestamp and increment counter to stay monotonic.
                self.counter += 1;
            }
        }
        build_uuid7(self.last_ms, self.counter)
    }

    /// Generate a batch of UUIDv7s for migration.
    ///
    /// When `spread` is true, distributes UUIDs across the millisecond window
    /// within each second boundary. This is used for v1→v2 migration where
    /// export_time has only second precision: rows sharing the same second
    /// get UUIDs spread across the 1000ms window, preserving original order
    /// while giving each row a distinct sub-millisecond position.
    ///
    /// `timestamps_ms` must contain the per-row export timestamps (millisecond
    /// precision, but typically quantized to seconds in v1 parts).
    pub fn generate_batch(&mut self, timestamps_ms: &[u64], spread: bool) -> Vec<[u64; 2]> {
        let count = timestamps_ms.len();
        let mut result = Vec::with_capacity(count);

        if !spread || count == 0 {
            for &ts in timestamps_ms {
                result.push(self.generate(ts));
            }
            return result;
        }

        // Group consecutive rows by second (floor to 1000ms boundary).
        let mut i = 0;
        while i < count {
            let second_base = timestamps_ms[i] / 1000 * 1000;
            let group_start = i;

            // Find all rows in this second.
            while i < count && timestamps_ms[i] / 1000 * 1000 == second_base {
                i += 1;
            }
            let group_len = i - group_start;

            // Spread across the 1000ms window within this second.
            for j in 0..group_len {
                let spread_ms = if group_len == 1 {
                    second_base
                } else {
                    second_base + (j as u64 * 999) / (group_len as u64 - 1)
                };
                result.push(self.generate(spread_ms));
            }
        }

        result
    }
}

impl Default for Uuid7Generator {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract the millisecond timestamp from a UUIDv7.
pub fn extract_timestamp_ms(uuid: [u64; 2]) -> u64 {
    uuid[0] >> 16
}

/// Build a UUIDv7 from timestamp and counter.
fn build_uuid7(timestamp_ms: u64, counter: u64) -> [u64; 2] {
    // High 64 bits: timestamp (48) | version 0b0111 (4) | rand_a (12)
    let rand_a = (counter >> 62) & 0x0FFF; // top 12 bits of counter
    let high = (timestamp_ms << 16) | (0x7 << 12) | rand_a;

    // Low 64 bits: variant 0b10 (2) | rand_b (62)
    let rand_b = counter & 0x3FFF_FFFF_FFFF_FFFF; // bottom 62 bits
    let low = (0b10 << 62) | rand_b;

    [high, low]
}

/// Format a UUIDv7 as a standard UUID string (8-4-4-4-12).
pub fn format_uuid(uuid: [u64; 2]) -> String {
    let hex = format!("{:016x}{:016x}", uuid[0], uuid[1]);
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// Parse a UUID hex string (32 chars, no hyphens, or 36 chars with hyphens) into `[u64; 2]`.
pub fn parse_uuid_hex(s: &str) -> Option<[u64; 2]> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let high = u64::from_str_radix(&hex[..16], 16).ok()?;
    let low = u64::from_str_radix(&hex[16..], 16).ok()?;
    Some([high, low])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_within_same_ms() {
        let mut uuid_gen = Uuid7Generator::new();
        let a = uuid_gen.generate(1_000_000);
        let b = uuid_gen.generate(1_000_000);
        let c = uuid_gen.generate(1_000_000);
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn monotonic_across_ms() {
        let mut uuid_gen = Uuid7Generator::new();
        let a = uuid_gen.generate(1_000_000);
        let b = uuid_gen.generate(1_000_001);
        assert!(a < b);
    }

    #[test]
    fn counter_resets_on_new_ms() {
        let mut uuid_gen = Uuid7Generator::new();
        let _a = uuid_gen.generate(1_000_000);
        let _b = uuid_gen.generate(1_000_000);
        assert_eq!(uuid_gen.counter, 1);
        let _c = uuid_gen.generate(1_000_001);
        assert_eq!(uuid_gen.counter, 0);
    }

    #[test]
    fn monotonic_despite_backward_time() {
        let mut uuid_gen = Uuid7Generator::new();
        let a = uuid_gen.generate(1_000_000);
        let b = uuid_gen.generate(1_000_001);
        // Time goes backward (out-of-order UDP packet).
        let c = uuid_gen.generate(999_999);
        // Must still be monotonically increasing.
        assert!(a < b);
        assert!(b < c, "backward time must not break monotonicity");
        // Generator should have kept the higher timestamp.
        assert_eq!(uuid_gen.last_ms, 1_000_001);
    }

    #[test]
    fn backward_time_preserves_later_timestamp() {
        let mut uuid_gen = Uuid7Generator::new();
        let _a = uuid_gen.generate(5_000);
        let _b = uuid_gen.generate(3_000); // backward
        let _c = uuid_gen.generate(4_000); // still backward from 5000
        let d = uuid_gen.generate(6_000); // forward again
        // After going forward past 5000, timestamp should advance.
        assert_eq!(extract_timestamp_ms(d), 6_000);
    }

    #[test]
    fn extract_roundtrip() {
        let mut uuid_gen = Uuid7Generator::new();
        let ts = 1_700_000_000_123;
        let uuid = uuid_gen.generate(ts);
        assert_eq!(extract_timestamp_ms(uuid), ts);
    }

    #[test]
    fn version_and_variant_bits() {
        let mut uuid_gen = Uuid7Generator::new();
        let uuid = uuid_gen.generate(1_700_000_000_000);
        // Version nibble (bits 48-51 of high word) = 0b0111
        let version = (uuid[0] >> 12) & 0xF;
        assert_eq!(version, 7);
        // Variant (bits 62-63 of low word) = 0b10
        let variant = uuid[1] >> 62;
        assert_eq!(variant, 0b10);
    }

    #[test]
    fn format_roundtrip() {
        let mut uuid_gen = Uuid7Generator::new();
        let uuid = uuid_gen.generate(1_700_000_000_123);
        let formatted = format_uuid(uuid);
        assert_eq!(formatted.len(), 36);
        assert_eq!(&formatted[14..15], "7"); // version nibble
        let parsed = parse_uuid_hex(&formatted).unwrap();
        assert_eq!(parsed, uuid);
    }

    #[test]
    fn parse_hex_no_hyphens() {
        let mut uuid_gen = Uuid7Generator::new();
        let uuid = uuid_gen.generate(1_700_000_000_123);
        let hex = format!("{:016x}{:016x}", uuid[0], uuid[1]);
        let parsed = parse_uuid_hex(&hex).unwrap();
        assert_eq!(parsed, uuid);
    }

    #[test]
    fn batch_no_spread() {
        let mut uuid_gen = Uuid7Generator::new();
        let timestamps: Vec<u64> = vec![1000, 1000, 1000, 2000, 2000];
        let batch = uuid_gen.generate_batch(&timestamps, false);
        assert_eq!(batch.len(), 5);
        for w in batch.windows(2) {
            assert!(w[0] < w[1]);
        }
    }

    #[test]
    fn batch_spread_within_second() {
        let mut uuid_gen = Uuid7Generator::new();
        // 5 rows all at second 1700000000 (ms = 1700000000000)
        let ts = 1_700_000_000_000;
        let timestamps: Vec<u64> = vec![ts; 5];
        let batch = uuid_gen.generate_batch(&timestamps, true);
        assert_eq!(batch.len(), 5);

        // All should be strictly increasing.
        for w in batch.windows(2) {
            assert!(w[0] < w[1]);
        }

        // Timestamps should span from base to base+999.
        let first_ts = extract_timestamp_ms(batch[0]);
        let last_ts = extract_timestamp_ms(batch[4]);
        assert_eq!(first_ts, ts);
        assert_eq!(last_ts, ts + 999);
    }

    #[test]
    fn batch_spread_preserves_cross_second_order() {
        let mut uuid_gen = Uuid7Generator::new();
        let ts1 = 1_700_000_000_000;
        let ts2 = 1_700_000_001_000;
        let timestamps: Vec<u64> = vec![ts1, ts1, ts1, ts2, ts2];
        let batch = uuid_gen.generate_batch(&timestamps, true);

        // All strictly increasing.
        for w in batch.windows(2) {
            assert!(w[0] < w[1]);
        }

        // First group timestamps within [ts1, ts1+999], second within [ts2, ts2+999].
        assert!(extract_timestamp_ms(batch[2]) <= ts1 + 999);
        assert!(extract_timestamp_ms(batch[3]) >= ts2);
    }

    #[test]
    fn no_clash_millions() {
        let mut uuid_gen = Uuid7Generator::new();
        let mut seen = std::collections::HashSet::new();
        for i in 0..1_000_000 {
            let uuid = uuid_gen.generate(1_700_000_000_000 + i / 1000);
            assert!(seen.insert(uuid), "duplicate at i={i}");
        }
    }

    #[test]
    fn lexicographic_order_matches_temporal() {
        let mut uuid_gen = Uuid7Generator::new();
        let mut prev = uuid_gen.generate(0);
        for ms in 1..10_000 {
            let curr = uuid_gen.generate(ms);
            assert!(
                prev < curr,
                "ordering violated at ms={ms}: {prev:?} >= {curr:?}"
            );
            prev = curr;
        }
    }
}
