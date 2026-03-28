//! CRC32-C (Castagnoli) integrity checks for binary storage formats.
//!
//! Uses the standard lookup table approach with polynomial 0x82F6_3B78
//! (bit-reversed Castagnoli). No external dependency needed.

/// CRC32-C lookup table (Castagnoli polynomial 0x82F63B78).
const CRC32C_TABLE: [u32; 256] = {
    let poly: u32 = 0x82F6_3B78;
    let mut table = [0u32; 256];
    let mut i = 0u32;
    while i < 256 {
        let mut crc = i;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ poly;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i as usize] = crc;
        i += 1;
    }
    table
};

/// Compute CRC32-C (Castagnoli) over a byte slice.
pub fn crc32c(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        let index = ((crc ^ u32::from(byte)) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC32C_TABLE[index];
    }
    crc ^ 0xFFFF_FFFF
}

/// Verify that the CRC32-C of `data` matches `expected`.
pub fn verify_crc32c(data: &[u8], expected: u32) -> bool {
    crc32c(data) == expected
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_data() {
        // CRC32-C of empty input is 0
        assert_eq!(crc32c(b""), 0x0000_0000);
    }

    #[test]
    fn known_vectors() {
        // "123456789" -> 0xE3069283 (well-known CRC32-C test vector)
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn verify_matches() {
        let data = b"hello world";
        let checksum = crc32c(data);
        assert!(verify_crc32c(data, checksum));
        assert!(!verify_crc32c(data, checksum ^ 1));
    }

    #[test]
    fn deterministic() {
        let data = vec![0xAB; 4096];
        let a = crc32c(&data);
        let b = crc32c(&data);
        assert_eq!(a, b);
    }

    #[test]
    fn single_bit_change_detected() {
        let mut data = vec![0u8; 64];
        let original = crc32c(&data);
        data[32] = 1;
        let modified = crc32c(&data);
        assert_ne!(original, modified);
    }
}
