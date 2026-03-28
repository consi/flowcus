//! Human-readable display of decoded IPFIX messages for trace-level logging.

use std::fmt;

use crate::ie;
use crate::protocol::{
    DataField, DataRecord, FieldValue, IpfixMessage, OptionsTemplateRecord, SetContents,
    TemplateRecord,
};

/// Wrapper for trace-level pretty-printing of an IPFIX message.
pub struct DisplayMessage<'a>(pub &'a IpfixMessage);

impl fmt::Display for DisplayMessage<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = self.0;
        let hdr = &msg.header;

        writeln!(f, "IPFIX Message from {}", msg.exporter)?;
        writeln!(
            f,
            "  Version: {:#06x}  Length: {} bytes",
            hdr.version, hdr.length
        )?;
        writeln!(
            f,
            "  Export Time: {}  Seq: {}  Domain: {}",
            format_export_time(hdr.export_time),
            hdr.sequence_number,
            hdr.observation_domain_id,
        )?;
        writeln!(f, "  Sets: {}", msg.sets.len())?;

        for (i, set) in msg.sets.iter().enumerate() {
            writeln!(f)?;
            write!(f, "  Set #{} [ID={}]: ", i, set.set_id)?;
            match &set.contents {
                SetContents::Template(templates) => {
                    writeln!(f, "Template Set ({} template(s))", templates.len())?;
                    for tmpl in templates {
                        write_template(f, tmpl)?;
                    }
                }
                SetContents::OptionsTemplate(templates) => {
                    writeln!(f, "Options Template Set ({} template(s))", templates.len())?;
                    for tmpl in templates {
                        write_options_template(f, tmpl)?;
                    }
                }
                SetContents::Data(data) => {
                    writeln!(
                        f,
                        "Data Set (template={}, {} record(s))",
                        data.template_id,
                        data.records.len()
                    )?;
                    for (j, record) in data.records.iter().enumerate() {
                        write_data_record(f, j, record)?;
                    }
                }
            }
        }

        Ok(())
    }
}

fn write_template(f: &mut fmt::Formatter<'_>, tmpl: &TemplateRecord) -> fmt::Result {
    writeln!(
        f,
        "    Template ID={} ({} fields)",
        tmpl.template_id,
        tmpl.field_specifiers.len()
    )?;
    for spec in &tmpl.field_specifiers {
        let name = ie::name(spec.element_id, spec.enterprise_id);
        let len_str = if spec.is_variable_length() {
            "var".to_string()
        } else {
            format!("{}", spec.field_length)
        };
        if spec.is_enterprise() {
            writeln!(
                f,
                "      {name} (ie={}, enterprise={}, len={len_str})",
                spec.element_id, spec.enterprise_id
            )?;
        } else {
            writeln!(f, "      {name} (ie={}, len={len_str})", spec.element_id)?;
        }
    }
    Ok(())
}

fn write_options_template(f: &mut fmt::Formatter<'_>, tmpl: &OptionsTemplateRecord) -> fmt::Result {
    writeln!(
        f,
        "    Options Template ID={} ({} fields, {} scope)",
        tmpl.template_id,
        tmpl.field_specifiers.len(),
        tmpl.scope_field_count
    )?;
    for (i, spec) in tmpl.field_specifiers.iter().enumerate() {
        let name = ie::name(spec.element_id, spec.enterprise_id);
        let scope_marker = if i < tmpl.scope_field_count as usize {
            " [scope]"
        } else {
            ""
        };
        writeln!(f, "      {name} (ie={}){scope_marker}", spec.element_id)?;
    }
    Ok(())
}

fn write_data_record(f: &mut fmt::Formatter<'_>, idx: usize, record: &DataRecord) -> fmt::Result {
    writeln!(f, "    Record #{idx}")?;
    for field in &record.fields {
        write_field(f, field)?;
    }
    Ok(())
}

fn write_field(f: &mut fmt::Formatter<'_>, field: &DataField) -> fmt::Result {
    let value_str = format_value(&field.value);
    writeln!(f, "      {}: {value_str}", field.name)
}

/// Format a field value with type-appropriate presentation.
fn format_value(val: &FieldValue) -> String {
    match val {
        FieldValue::Unsigned8(v) => format!("{v}"),
        FieldValue::Unsigned16(v) => format!("{v}"),
        FieldValue::Unsigned32(v) => format!("{v}"),
        FieldValue::Unsigned64(v) => {
            // Large numbers get human-readable formatting
            if *v > 1_000_000 {
                format!("{v} ({})", human_bytes(*v))
            } else {
                format!("{v}")
            }
        }
        FieldValue::Ipv4(addr) => format!("{addr}"),
        FieldValue::Ipv6(addr) => format!("{addr}"),
        FieldValue::Mac(m) => format!(
            "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            m[0], m[1], m[2], m[3], m[4], m[5]
        ),
        FieldValue::DateTimeSeconds(ts) => format_export_time(*ts),
        FieldValue::DateTimeMilliseconds(ts) => {
            let secs = *ts / 1000;
            #[allow(clippy::cast_possible_truncation)]
            let ms = (*ts % 1000) as u32;
            format!("{}.{ms:03}", format_export_time(secs as u32))
        }
        FieldValue::String(s) => format!("\"{s}\""),
        FieldValue::Bytes(b) => {
            let mut hex = String::with_capacity(b.len() * 2 + 2);
            hex.push_str("0x");
            for byte in b {
                use std::fmt::Write;
                let _ = write!(hex, "{byte:02x}");
            }
            hex
        }
        other => other.to_string(),
    }
}

/// Format a Unix timestamp as a readable string.
fn format_export_time(ts: u32) -> String {
    // Simple UTC formatting without external dependency
    let ts = ts as u64;
    let secs_per_day: u64 = 86400;
    let days = ts / secs_per_day;
    let remaining = ts % secs_per_day;
    let hours = remaining / 3600;
    let minutes = (remaining % 3600) / 60;
    let seconds = remaining % 60;

    // Days since Unix epoch to Y-M-D (simplified)
    let (year, month, day) = days_to_ymd(days);
    format!("{year}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
}

/// Convert days since Unix epoch to (year, month, day).
const fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Civil calendar algorithm
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Format bytes as human-readable size.
fn human_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut val = bytes as f64;
    for unit in UNITS {
        if val < 1024.0 {
            return format!("{val:.1} {unit}");
        }
        val /= 1024.0;
    }
    format!("{val:.1} PB")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn human_bytes_formatting() {
        assert_eq!(human_bytes(500), "500.0 B");
        assert_eq!(human_bytes(1024), "1.0 KB");
        assert_eq!(human_bytes(1_500_000), "1.4 MB");
    }

    #[test]
    fn export_time_formatting() {
        // 2023-11-14T22:13:20Z = 1700000000
        let s = format_export_time(1_700_000_000);
        assert!(s.starts_with("2023-11-14"));
        assert!(s.contains("22:13:20"));
    }
}
