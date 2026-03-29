//! Per-exporter session and template management.
//!
//! IPFIX templates are scoped to (exporter address, observation domain ID).
//! Templates are cached here and used to decode data records.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::time::Instant;

use tracing::{debug, trace, warn};

use crate::protocol::{FieldSpecifier, OptionsTemplateRecord, TemplateRecord};

/// Key identifying a template scope: (exporter, observation domain, template ID).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TemplateKey {
    exporter: SocketAddr,
    observation_domain_id: u32,
    template_id: u16,
}

/// A cached template with its field specifiers.
#[derive(Debug, Clone)]
pub struct CachedTemplate {
    pub template_id: u16,
    pub field_specifiers: Vec<FieldSpecifier>,
    /// How many field specifiers are scope fields (0 for regular templates).
    pub scope_field_count: u16,
    /// When this template was last received/refreshed.
    pub last_seen: Instant,
    /// Precomputed minimum record length (sum of fixed fields; var-length = 1).
    pub min_record_length: usize,
}

impl CachedTemplate {
    fn from_template(rec: &TemplateRecord) -> Self {
        let min_len = rec
            .field_specifiers
            .iter()
            .map(|f| {
                if f.is_variable_length() {
                    1 // minimum 1 byte for variable-length encoding
                } else {
                    f.field_length as usize
                }
            })
            .sum();

        Self {
            template_id: rec.template_id,
            field_specifiers: rec.field_specifiers.clone(),
            scope_field_count: 0,
            last_seen: Instant::now(),
            min_record_length: min_len,
        }
    }

    fn from_options_template(rec: &OptionsTemplateRecord) -> Self {
        let min_len = rec
            .field_specifiers
            .iter()
            .map(|f| {
                if f.is_variable_length() {
                    1
                } else {
                    f.field_length as usize
                }
            })
            .sum();

        Self {
            template_id: rec.template_id,
            field_specifiers: rec.field_specifiers.clone(),
            scope_field_count: rec.scope_field_count,
            last_seen: Instant::now(),
            min_record_length: min_len,
        }
    }
}

/// Key for interface metadata: (exporter IP, observation domain ID, interface index).
type InterfaceKey = (IpAddr, u32, u32);

/// Thread-safe store for all IPFIX template sessions.
pub struct SessionStore {
    templates: HashMap<TemplateKey, CachedTemplate>,
    template_expiry: std::time::Duration,
    /// Interface name metadata extracted from IPFIX option data records.
    /// Maps (exporter_ip, observation_domain_id, if_index) → interface name.
    interface_names: HashMap<InterfaceKey, String>,
}

impl SessionStore {
    pub fn new(template_expiry_secs: u64) -> Self {
        Self {
            templates: HashMap::new(),
            template_expiry: std::time::Duration::from_secs(template_expiry_secs),
            interface_names: HashMap::new(),
        }
    }

    /// Register or refresh a template from a Template Set.
    pub fn update_template(
        &mut self,
        exporter: SocketAddr,
        observation_domain_id: u32,
        record: &TemplateRecord,
    ) {
        if record.field_specifiers.is_empty() {
            // Template withdrawal (RFC 7011 Section 8.1)
            if record.template_id == crate::protocol::TEMPLATE_SET_ID {
                // Withdraw ALL templates for this (exporter, domain) — RFC 7011 Section 8.1
                self.withdraw_all_templates(exporter, observation_domain_id);
                return;
            }

            let key = TemplateKey {
                exporter,
                observation_domain_id,
                template_id: record.template_id,
            };
            if self.templates.remove(&key).is_some() {
                debug!(
                    template_id = record.template_id,
                    %exporter,
                    observation_domain_id,
                    "Template withdrawn"
                );
            } else {
                debug!(
                    template_id = record.template_id,
                    %exporter,
                    observation_domain_id,
                    "Ignoring withdrawal for unknown template {}",
                    record.template_id
                );
            }
            return;
        }

        let key = TemplateKey {
            exporter,
            observation_domain_id,
            template_id: record.template_id,
        };

        // Gap 2: warn on template redefinition without prior withdrawal
        if let Some(existing) = self.templates.get(&key) {
            if existing.field_specifiers != record.field_specifiers {
                warn!(
                    template_id = record.template_id,
                    %exporter,
                    observation_domain_id,
                    "Template {} redefined without withdrawal",
                    record.template_id
                );
            }
        }

        let cached = CachedTemplate::from_template(record);
        trace!(
            template_id = record.template_id,
            fields = record.field_specifiers.len(),
            min_record_len = cached.min_record_length,
            %exporter,
            "Template registered"
        );
        self.templates.insert(key, cached);
    }

    /// Register or refresh an options template.
    pub fn update_options_template(
        &mut self,
        exporter: SocketAddr,
        observation_domain_id: u32,
        record: &OptionsTemplateRecord,
    ) {
        if record.field_specifiers.is_empty() {
            // Options template withdrawal (RFC 7011 Section 8.1)
            if record.template_id == crate::protocol::OPTIONS_TEMPLATE_SET_ID {
                // Withdraw ALL options templates for this (exporter, domain)
                self.withdraw_all_options_templates(exporter, observation_domain_id);
                return;
            }

            let key = TemplateKey {
                exporter,
                observation_domain_id,
                template_id: record.template_id,
            };
            if self.templates.remove(&key).is_some() {
                debug!(
                    template_id = record.template_id,
                    %exporter,
                    observation_domain_id,
                    "Options template withdrawn"
                );
            } else {
                debug!(
                    template_id = record.template_id,
                    %exporter,
                    observation_domain_id,
                    "Ignoring withdrawal for unknown template {}",
                    record.template_id
                );
            }
            return;
        }

        let key = TemplateKey {
            exporter,
            observation_domain_id,
            template_id: record.template_id,
        };

        // Gap 2: warn on template redefinition without prior withdrawal
        if let Some(existing) = self.templates.get(&key) {
            if existing.field_specifiers != record.field_specifiers {
                warn!(
                    template_id = record.template_id,
                    %exporter,
                    observation_domain_id,
                    "Template {} redefined without withdrawal",
                    record.template_id
                );
            }
        }

        let cached = CachedTemplate::from_options_template(record);
        trace!(
            template_id = record.template_id,
            fields = record.field_specifiers.len(),
            scope_fields = record.scope_field_count,
            %exporter,
            "Options template registered"
        );
        self.templates.insert(key, cached);
    }

    /// Withdraw all regular templates (scope_field_count == 0) for the given
    /// exporter and observation domain (RFC 7011 Section 8.1).
    fn withdraw_all_templates(&mut self, exporter: SocketAddr, observation_domain_id: u32) {
        let before = self.templates.len();
        self.templates.retain(|k, cached| {
            !(k.exporter == exporter
                && k.observation_domain_id == observation_domain_id
                && cached.scope_field_count == 0)
        });
        let removed = before - self.templates.len();
        debug!(
            %exporter,
            observation_domain_id,
            removed,
            "All templates withdrawn"
        );
    }

    /// Withdraw all options templates (scope_field_count > 0) for the given
    /// exporter and observation domain (RFC 7011 Section 8.1).
    fn withdraw_all_options_templates(&mut self, exporter: SocketAddr, observation_domain_id: u32) {
        let before = self.templates.len();
        self.templates.retain(|k, cached| {
            !(k.exporter == exporter
                && k.observation_domain_id == observation_domain_id
                && cached.scope_field_count > 0)
        });
        let removed = before - self.templates.len();
        debug!(
            %exporter,
            observation_domain_id,
            removed,
            "All options templates withdrawn"
        );
    }

    /// Look up a cached template for decoding a data set.
    pub fn get_template(
        &self,
        exporter: SocketAddr,
        observation_domain_id: u32,
        template_id: u16,
    ) -> Option<&CachedTemplate> {
        let key = TemplateKey {
            exporter,
            observation_domain_id,
            template_id,
        };
        self.templates.get(&key)
    }

    /// Remove expired templates. Call periodically.
    pub fn expire_templates(&mut self) {
        let now = Instant::now();
        let before = self.templates.len();
        self.templates
            .retain(|_, cached| now.duration_since(cached.last_seen) < self.template_expiry);
        let removed = before - self.templates.len();
        if removed > 0 {
            debug!(
                removed,
                remaining = self.templates.len(),
                "Expired templates"
            );
        }
    }

    /// Number of currently cached templates.
    pub fn template_count(&self) -> usize {
        self.templates.len()
    }

    /// Number of unique exporters (by IP:port) with at least one cached template.
    pub fn exporter_count(&self) -> usize {
        let mut seen = std::collections::HashSet::new();
        for key in self.templates.keys() {
            seen.insert(key.exporter);
        }
        seen.len()
    }

    /// Store an interface name learned from an IPFIX option data record.
    pub fn set_interface_name(
        &mut self,
        exporter: IpAddr,
        domain_id: u32,
        if_index: u32,
        name: String,
    ) {
        debug!(
            %exporter,
            domain_id,
            if_index,
            name = %name,
            "Interface name learned from option data"
        );
        self.interface_names
            .insert((exporter, domain_id, if_index), name);
    }

    /// Return a snapshot of all known interface names for API exposure.
    pub fn get_interface_names(&self) -> HashMap<(IpAddr, u32, u32), String> {
        self.interface_names.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_template(id: u16) -> TemplateRecord {
        TemplateRecord {
            template_id: id,
            field_specifiers: vec![
                FieldSpecifier {
                    element_id: 8,
                    field_length: 4,
                    enterprise_id: 0,
                },
                FieldSpecifier {
                    element_id: 12,
                    field_length: 4,
                    enterprise_id: 0,
                },
            ],
        }
    }

    #[test]
    fn register_and_lookup() {
        let mut store = SessionStore::new(1800);
        let addr: SocketAddr = "10.0.0.1:4739".parse().unwrap();
        let tmpl = test_template(256);

        store.update_template(addr, 1, &tmpl);
        let cached = store.get_template(addr, 1, 256).unwrap();
        assert_eq!(cached.field_specifiers.len(), 2);
        assert_eq!(cached.min_record_length, 8);
    }

    #[test]
    fn template_withdrawal() {
        let mut store = SessionStore::new(1800);
        let addr: SocketAddr = "10.0.0.1:4739".parse().unwrap();

        store.update_template(addr, 1, &test_template(256));
        assert_eq!(store.template_count(), 1);

        let withdrawal = TemplateRecord {
            template_id: 256,
            field_specifiers: Vec::new(),
        };
        store.update_template(addr, 1, &withdrawal);
        assert_eq!(store.template_count(), 0);
    }

    #[test]
    fn different_exporters_separate_templates() {
        let mut store = SessionStore::new(1800);
        let a1: SocketAddr = "10.0.0.1:4739".parse().unwrap();
        let a2: SocketAddr = "10.0.0.2:4739".parse().unwrap();

        store.update_template(a1, 1, &test_template(256));
        store.update_template(a2, 1, &test_template(256));
        assert_eq!(store.template_count(), 2);

        assert!(store.get_template(a1, 1, 256).is_some());
        assert!(store.get_template(a2, 1, 256).is_some());
        assert!(store.get_template(a1, 2, 256).is_none()); // different domain
    }

    #[test]
    fn interface_name_store_and_retrieve() {
        let mut store = SessionStore::new(1800);
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        store.set_interface_name(ip, 0, 1, "GigabitEthernet0/0".to_string());
        store.set_interface_name(ip, 0, 2, "GigabitEthernet0/1".to_string());

        let names = store.get_interface_names();
        assert_eq!(names.len(), 2);
        assert_eq!(names[&(ip, 0, 1)], "GigabitEthernet0/0");
        assert_eq!(names[&(ip, 0, 2)], "GigabitEthernet0/1");
    }

    #[test]
    fn interface_name_overwrite() {
        let mut store = SessionStore::new(1800);
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        store.set_interface_name(ip, 0, 1, "old-name".to_string());
        store.set_interface_name(ip, 0, 1, "new-name".to_string());

        let names = store.get_interface_names();
        assert_eq!(names.len(), 1);
        assert_eq!(names[&(ip, 0, 1)], "new-name");
    }

    #[test]
    fn interface_names_separate_by_exporter_and_domain() {
        let mut store = SessionStore::new(1800);
        let ip1: IpAddr = "10.0.0.1".parse().unwrap();
        let ip2: IpAddr = "10.0.0.2".parse().unwrap();

        store.set_interface_name(ip1, 0, 1, "router1-eth0".to_string());
        store.set_interface_name(ip2, 0, 1, "router2-eth0".to_string());
        store.set_interface_name(ip1, 1, 1, "router1-vrf1-eth0".to_string());

        let names = store.get_interface_names();
        assert_eq!(names.len(), 3);
        assert_eq!(names[&(ip1, 0, 1)], "router1-eth0");
        assert_eq!(names[&(ip2, 0, 1)], "router2-eth0");
        assert_eq!(names[&(ip1, 1, 1)], "router1-vrf1-eth0");
    }
}
