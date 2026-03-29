//! IPFIX Information Element registry.
//!
//! Provides lookup of IE metadata by (element_id, enterprise_id).
//! Includes IANA-assigned standard IEs (RFC 7012, RFC 5102) and
//! vendor-specific enterprise IEs from major vendors.

mod iana;
mod vendor;

use std::collections::HashMap;
use std::sync::LazyLock;

use crate::protocol::DataType;

/// Metadata for an Information Element.
#[derive(Debug, Clone)]
pub struct InformationElement {
    pub element_id: u16,
    pub enterprise_id: u32,
    pub name: &'static str,
    pub data_type: DataType,
    pub description: &'static str,
}

/// Key for IE lookup: (element_id, enterprise_id).
type IeKey = (u16, u32);

/// Global IE registry.
static REGISTRY: LazyLock<HashMap<IeKey, InformationElement>> = LazyLock::new(build_registry);

fn build_registry() -> HashMap<IeKey, InformationElement> {
    let mut map = HashMap::new();

    for ie in iana::iana_elements() {
        map.insert((ie.element_id, ie.enterprise_id), ie);
    }
    for ie in vendor::vendor_elements() {
        map.insert((ie.element_id, ie.enterprise_id), ie);
    }

    map
}

/// Look up an Information Element by its ID and enterprise number.
/// Returns `None` for unknown elements.
fn lookup(element_id: u16, enterprise_id: u32) -> Option<&'static InformationElement> {
    REGISTRY.get(&(element_id, enterprise_id))
}

/// Get the human-readable name for an IE, falling back to a generated name.
///
/// Unknown IEs use the naming convention from RFC/IANA terminology:
/// - IANA unknown: `ie{element_id}` (e.g., `ie999`)
/// - Enterprise unknown: `pen{enterprise_id}.ie{element_id}` (e.g., `pen9.ie12345`)
///
/// "PEN" = Private Enterprise Number, the official IANA term.
pub fn name(element_id: u16, enterprise_id: u32) -> String {
    if let Some(ie) = lookup(element_id, enterprise_id) {
        ie.name.to_string()
    } else if enterprise_id == 0 {
        format!("ie{element_id}")
    } else {
        format!("pen{enterprise_id}.ie{element_id}")
    }
}

/// Get the data type for an IE, defaulting to `OctetArray` for unknown IEs.
///
/// Per RFC 5153: unknown IEs should be decoded as opaque octet arrays.
/// The wire length from the template is trusted for parsing; no type inference
/// is attempted from field length (a 4-byte field could be uint32, float32,
/// IPv4, or an opaque token — we can't know without the IE definition).
pub fn data_type(element_id: u16, enterprise_id: u32) -> DataType {
    lookup(element_id, enterprise_id).map_or(DataType::OctetArray, |ie| ie.data_type)
}

/// Check if an IE is known in the registry.
pub fn is_known(element_id: u16, enterprise_id: u32) -> bool {
    REGISTRY.contains_key(&(element_id, enterprise_id))
}

/// Iterate over all registered Information Elements.
pub fn all() -> impl Iterator<Item = &'static InformationElement> {
    REGISTRY.values()
}

/// Total number of registered Information Elements.
#[cfg(test)]
fn registry_size() -> usize {
    REGISTRY.len()
}

/// Macro for concise IE definition.
macro_rules! ie {
    ($id:expr, $eid:expr, $name:expr, $dt:ident, $desc:expr) => {
        InformationElement {
            element_id: $id,
            enterprise_id: $eid,
            name: $name,
            data_type: $crate::protocol::DataType::$dt,
            description: $desc,
        }
    };
}
pub(crate) use ie;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_iana_element() {
        let ie = lookup(8, 0).expect("sourceIPv4Address should exist");
        assert_eq!(ie.name, "sourceIPv4Address");
        assert_eq!(ie.data_type, DataType::Ipv4Address);
    }

    #[test]
    fn lookup_unknown_returns_none() {
        assert!(lookup(65534, 0).is_none());
    }

    #[test]
    fn name_fallback_for_unknown() {
        assert_eq!(name(65534, 0), "ie65534");
        assert_eq!(name(1, 99999), "pen99999.ie1");
    }

    #[test]
    fn registry_has_entries() {
        assert!(registry_size() > 100);
    }

    #[test]
    fn lookup_vendor_cisco() {
        assert!(lookup(95, 9).is_some());
    }

    #[test]
    fn lookup_vendor_palo_alto() {
        let ie = lookup(56701, 25461).expect("panAppId should exist");
        assert_eq!(ie.name, "panAppId");
    }
}
