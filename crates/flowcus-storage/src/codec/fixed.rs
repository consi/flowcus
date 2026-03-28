//! Plain codec: raw native-endian bytes, no transformation.
//! This is the fallback codec for data that doesn't benefit from delta or GCD encoding.
//! Data is already in SIMD-friendly packed layout from the ColumnBuffer.
