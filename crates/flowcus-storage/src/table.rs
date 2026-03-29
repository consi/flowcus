//! Table: a collection of time-partitioned parts on disk.
//!
//! Directory layout:
//! ```text
//! {storage_dir}/flows/
//!   {YYYY}/{MM}/{DD}/{HH}/
//!     {gen}_{mintime}_{maxtime}_{seq}/
//!       meta.bin
//!       columns/...
//! ```
//!
//! Time partitioning at the directory level enables scan-limiting:
//! - Year/month/day/hour pruning by directory traversal
//! - Part names encode generation + min/max timestamps for further filtering
//! - Readers never open files for parts outside the query time range

use std::path::{Path, PathBuf};

use tracing::info;

use crate::part;

/// A table managing time-partitioned parts on disk.
pub struct Table {
    name: String,
    base_dir: PathBuf,
}

/// A discovered part on disk with its parsed metadata from the directory name.
#[derive(Debug, Clone)]
pub struct PartEntry {
    pub path: PathBuf,
    /// Part format version (0 = legacy 4-segment dir name, 1 = current 5-segment).
    pub format_version: u32,
    pub generation: u32,
    pub time_min: u64,
    pub time_max: u64,
    pub seq: u32,
}

impl Table {
    /// Open or create a table at the given storage directory.
    pub fn open(storage_dir: &Path, name: &str) -> std::io::Result<Self> {
        let base_dir = storage_dir.join(name);
        std::fs::create_dir_all(&base_dir)?;

        info!(table = name, path = %base_dir.display(), "Table opened");

        Ok(Self {
            name: name.to_string(),
            base_dir,
        })
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// List all parts, optionally filtered by time range.
    /// Scans only the necessary year/month/day/hour directories.
    /// Returns parts sorted by (generation, time_min, seq).
    pub fn list_parts(
        &self,
        time_min: Option<u64>,
        time_max: Option<u64>,
    ) -> std::io::Result<Vec<PartEntry>> {
        let mut parts = Vec::new();
        walk_parts_recursive(&self.base_dir, 0, time_min, time_max, &mut parts)?;
        parts.sort_by_key(|p| (p.generation, p.time_min, p.seq));
        Ok(parts)
    }

    /// List all parts (no time filter).
    pub fn list_all_parts(&self) -> std::io::Result<Vec<PartEntry>> {
        self.list_parts(None, None)
    }

    /// Count parts on disk.
    pub fn part_count(&self) -> usize {
        self.list_all_parts().unwrap_or_default().len()
    }
}

fn walk_parts_recursive(
    dir: &Path,
    depth: u8,
    time_min: Option<u64>,
    time_max: Option<u64>,
    out: &mut Vec<PartEntry>,
) -> std::io::Result<()> {
    if !dir.exists() {
        return Ok(());
    }

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }

        let name = match entry.file_name().to_str() {
            Some(n) => n.to_string(),
            None => continue,
        };

        if depth < 4 {
            walk_parts_recursive(&entry.path(), depth + 1, time_min, time_max, out)?;
        } else if let Some((format_version, generation, pmin, pmax, seq)) =
            part::parse_part_dir_name_versioned(&name)
        {
            // Scan-limit: skip parts entirely outside the query range
            if let Some(qmax) = time_max {
                if pmin > qmax {
                    continue;
                }
            }
            if let Some(qmin) = time_min {
                if pmax < qmin {
                    continue;
                }
            }

            out.push(PartEntry {
                path: entry.path(),
                format_version,
                generation,
                time_min: pmin,
                time_max: pmax,
                seq,
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_creates_base_directory() {
        let dir = std::env::temp_dir().join("flowcus_test_table3");
        let _ = std::fs::remove_dir_all(&dir);

        let table = Table::open(&dir, "flows").unwrap();
        assert!(table.base_dir().exists());
        assert_eq!(table.part_count(), 0);

        std::fs::remove_dir_all(&dir).ok();
    }
}
