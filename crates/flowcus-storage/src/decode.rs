//! Column decoder: reads encoded column files back into typed `ColumnBuffer`s.
//!
//! Decode pipeline: read raw → decompress (LZ4/zstd) → undo transform (delta/gcd) → typed buffer.

use std::io;
use std::path::Path;

use crate::codec;
use crate::column::ColumnBuffer;
use crate::part;
use crate::schema::StorageType;

/// Decode a column file from disk into a typed `ColumnBuffer`.
///
/// Steps:
/// 1. Read raw encoded bytes (past the 64-byte header)
/// 2. Read the column header for codec/compression/row_count
/// 3. Decompress LZ4 if needed
/// 4. Reverse transform codec (delta/delta-delta/gcd)
/// 5. Build typed ColumnBuffer from plain bytes
pub fn decode_column(path: &Path, storage_type: StorageType) -> io::Result<ColumnBuffer> {
    let raw_data = part::read_column_raw(path)?;
    let header = part::read_column_header(path)?;
    let rows = header.row_count as usize;

    if rows == 0 {
        return Ok(ColumnBuffer::new(storage_type));
    }

    // Step 1: Decompress based on compression type
    let compression = match header.compression {
        1 => codec::CompressionType::Lz4,
        2 => codec::CompressionType::Zstd,
        _ => codec::CompressionType::None,
    };
    let decompressed = codec::decompress(&raw_data, compression, header.raw_size as usize)?;

    // Step 2: Reverse transform codec
    let mut buf = ColumnBuffer::with_capacity(storage_type, rows);

    match storage_type {
        StorageType::VarLen => {
            append_varlen_raw(&mut buf, &decompressed, rows)?;
        }
        _ => {
            let elem_size = storage_type.element_size().unwrap_or(1);
            let plain = match header.codec {
                1 => decode_delta(&decompressed, elem_size, rows),
                2 => decode_delta_delta(&decompressed, elem_size, rows),
                3 => decode_gcd(&decompressed, elem_size, rows),
                _ => decompressed,
            };
            append_fixed_raw(&mut buf, storage_type, &plain, rows);
        }
    }

    Ok(buf)
}

// ---------------------------------------------------------------------------
// Decode functions — copied from merge.rs (pure functions, no state)
// These mirror the encode transforms in codec/delta.rs and codec/gcd.rs.
// ---------------------------------------------------------------------------

fn decode_delta(data: &[u8], elem_size: usize, rows: usize) -> Vec<u8> {
    if data.len() < elem_size || rows == 0 {
        return data.to_vec();
    }
    let mut out = Vec::with_capacity(rows * elem_size);
    match elem_size {
        4 => {
            let mut val = u32::from_le_bytes(data[..4].try_into().unwrap());
            out.extend_from_slice(&val.to_ne_bytes());
            for i in 1..rows {
                let off = i * 4;
                if off + 4 > data.len() {
                    break;
                }
                let delta = u32::from_le_bytes(data[off..off + 4].try_into().unwrap());
                val = val.wrapping_add(delta);
                out.extend_from_slice(&val.to_ne_bytes());
            }
        }
        8 => {
            let mut val = u64::from_le_bytes(data[..8].try_into().unwrap());
            out.extend_from_slice(&val.to_ne_bytes());
            for i in 1..rows {
                let off = i * 8;
                if off + 8 > data.len() {
                    break;
                }
                let delta = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
                val = val.wrapping_add(delta);
                out.extend_from_slice(&val.to_ne_bytes());
            }
        }
        2 => {
            let mut val = u16::from_le_bytes(data[..2].try_into().unwrap());
            out.extend_from_slice(&val.to_ne_bytes());
            for i in 1..rows {
                let off = i * 2;
                if off + 2 > data.len() {
                    break;
                }
                let delta = u16::from_le_bytes(data[off..off + 2].try_into().unwrap());
                val = val.wrapping_add(delta);
                out.extend_from_slice(&val.to_ne_bytes());
            }
        }
        _ => return data.to_vec(),
    }
    out
}

fn decode_delta_delta(data: &[u8], elem_size: usize, rows: usize) -> Vec<u8> {
    if data.len() < elem_size * 2 || rows < 2 {
        return decode_delta(data, elem_size, rows);
    }
    let mut out = Vec::with_capacity(rows * elem_size);
    match elem_size {
        4 => {
            let first = u32::from_le_bytes(data[..4].try_into().unwrap());
            let first_delta = u32::from_le_bytes(data[4..8].try_into().unwrap());
            out.extend_from_slice(&first.to_ne_bytes());
            let mut prev = first.wrapping_add(first_delta);
            out.extend_from_slice(&prev.to_ne_bytes());
            let mut prev_delta = first_delta;
            for i in 2..rows {
                let off = i * 4;
                if off + 4 > data.len() {
                    break;
                }
                let dd = u32::from_le_bytes(data[off..off + 4].try_into().unwrap());
                let delta = prev_delta.wrapping_add(dd);
                let val = prev.wrapping_add(delta);
                out.extend_from_slice(&val.to_ne_bytes());
                prev = val;
                prev_delta = delta;
            }
        }
        8 => {
            let first = u64::from_le_bytes(data[..8].try_into().unwrap());
            let first_delta = u64::from_le_bytes(data[8..16].try_into().unwrap());
            out.extend_from_slice(&first.to_ne_bytes());
            let mut prev = first.wrapping_add(first_delta);
            out.extend_from_slice(&prev.to_ne_bytes());
            let mut prev_delta = first_delta;
            for i in 2..rows {
                let off = i * 8;
                if off + 8 > data.len() {
                    break;
                }
                let dd = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
                let delta = prev_delta.wrapping_add(dd);
                let val = prev.wrapping_add(delta);
                out.extend_from_slice(&val.to_ne_bytes());
                prev = val;
                prev_delta = delta;
            }
        }
        _ => return data.to_vec(),
    }
    out
}

fn decode_gcd(data: &[u8], elem_size: usize, rows: usize) -> Vec<u8> {
    if data.len() < elem_size {
        return data.to_vec();
    }
    let mut out = Vec::with_capacity(rows * elem_size);
    match elem_size {
        4 => {
            let gcd = u32::from_le_bytes(data[..4].try_into().unwrap());
            for i in 0..rows {
                let off = (i + 1) * 4;
                if off + 4 > data.len() {
                    break;
                }
                let divided = u32::from_le_bytes(data[off..off + 4].try_into().unwrap());
                out.extend_from_slice(&divided.wrapping_mul(gcd).to_ne_bytes());
            }
        }
        8 => {
            let gcd = u64::from_le_bytes(data[..8].try_into().unwrap());
            for i in 0..rows {
                let off = (i + 1) * 8;
                if off + 8 > data.len() {
                    break;
                }
                let divided = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
                out.extend_from_slice(&divided.wrapping_mul(gcd).to_ne_bytes());
            }
        }
        2 => {
            let gcd = u16::from_le_bytes(data[..2].try_into().unwrap());
            for i in 0..rows {
                let off = (i + 1) * 2;
                if off + 2 > data.len() {
                    break;
                }
                let divided = u16::from_le_bytes(data[off..off + 2].try_into().unwrap());
                out.extend_from_slice(&divided.wrapping_mul(gcd).to_ne_bytes());
            }
        }
        _ => return data.to_vec(),
    }
    out
}

/// Append raw fixed-size column data to a buffer.
fn append_fixed_raw(buf: &mut ColumnBuffer, _st: StorageType, plain: &[u8], rows: usize) {
    match buf {
        ColumnBuffer::U8(v) => {
            v.extend_from_slice(&plain[..rows.min(plain.len())]);
        }
        ColumnBuffer::U16(v) => {
            for i in 0..rows {
                let off = i * 2;
                if off + 2 > plain.len() {
                    break;
                }
                v.push(u16::from_ne_bytes([plain[off], plain[off + 1]]));
            }
        }
        ColumnBuffer::U32(v) => {
            for i in 0..rows {
                let off = i * 4;
                if off + 4 > plain.len() {
                    break;
                }
                v.push(u32::from_ne_bytes(plain[off..off + 4].try_into().unwrap()));
            }
        }
        ColumnBuffer::U64(v) => {
            for i in 0..rows {
                let off = i * 8;
                if off + 8 > plain.len() {
                    break;
                }
                v.push(u64::from_ne_bytes(plain[off..off + 8].try_into().unwrap()));
            }
        }
        ColumnBuffer::U128(v) => {
            for i in 0..rows {
                let off = i * 16;
                if off + 16 > plain.len() {
                    break;
                }
                let high = u64::from_ne_bytes(plain[off..off + 8].try_into().unwrap());
                let low = u64::from_ne_bytes(plain[off + 8..off + 16].try_into().unwrap());
                v.push([high, low]);
            }
        }
        ColumnBuffer::Mac(v) => {
            for i in 0..rows {
                let off = i * 6;
                if off + 6 > plain.len() {
                    break;
                }
                let mut mac = [0u8; 6];
                mac.copy_from_slice(&plain[off..off + 6]);
                v.push(mac);
            }
        }
        ColumnBuffer::VarLen { .. } => {} // handled by append_varlen_raw
    }
}

/// Append variable-length data from encoded format to buffer.
fn append_varlen_raw(buf: &mut ColumnBuffer, data: &[u8], _rows: usize) -> io::Result<()> {
    if data.len() < 4 {
        return Ok(());
    }

    let offset_count = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
    let offsets_end = 4 + offset_count * 4;

    if offsets_end > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "VarLen offsets overflow",
        ));
    }

    let mut src_offsets = Vec::with_capacity(offset_count);
    for i in 0..offset_count {
        let off = 4 + i * 4;
        src_offsets.push(u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize);
    }

    let var_data = &data[offsets_end..];

    if let ColumnBuffer::VarLen {
        offsets,
        data: buf_data,
    } = buf
    {
        for i in 0..offset_count.saturating_sub(1) {
            let start = src_offsets[i];
            let end = src_offsets.get(i + 1).copied().unwrap_or(start);
            if end <= var_data.len() && start <= end {
                buf_data.extend_from_slice(&var_data[start..end]);
            }
            offsets.push(buf_data.len() as u32);
        }
    }

    Ok(())
}
