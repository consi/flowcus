//! Vendor-specific enterprise Information Elements.
//!
//! Enterprise numbers from IANA Private Enterprise Numbers registry:
//! https://www.iana.org/assignments/enterprise-numbers/

use super::{InformationElement, ie};

/// Cisco Systems enterprise number.
pub const CISCO: u32 = 9;
/// Juniper Networks enterprise number.
pub const JUNIPER: u32 = 2636;
/// VMware enterprise number.
pub const VMWARE: u32 = 6876;
/// Palo Alto Networks enterprise number.
pub const PALO_ALTO: u32 = 25461;
/// Barracuda Networks enterprise number.
pub const BARRACUDA: u32 = 10704;
/// ntop enterprise number.
pub const NTOP: u32 = 35632;
/// Fortinet enterprise number.
pub const FORTINET: u32 = 12356;
/// Nokia enterprise number.
pub const NOKIA: u32 = 637;
/// Huawei enterprise number.
pub const HUAWEI: u32 = 2011;

pub fn vendor_elements() -> Vec<InformationElement> {
    let mut elements = Vec::new();
    elements.extend(cisco_elements());
    elements.extend(juniper_elements());
    elements.extend(vmware_elements());
    elements.extend(palo_alto_elements());
    elements.extend(barracuda_elements());
    elements.extend(ntop_elements());
    elements.extend(fortinet_elements());
    elements.extend(nokia_elements());
    elements.extend(huawei_elements());
    elements
}

fn cisco_elements() -> Vec<InformationElement> {
    vec![
        // Cisco common IPFIX/NetFlow v9 enterprise IEs
        ie!(
            12232,
            CISCO,
            "ciscoAvcApplicationName",
            String,
            "Cisco AVC application name"
        ),
        ie!(
            12233,
            CISCO,
            "ciscoAvcApplicationGroupName",
            String,
            "Cisco AVC application group"
        ),
        ie!(
            12234,
            CISCO,
            "ciscoAvcApplicationAttributes",
            String,
            "Cisco AVC attributes"
        ),
        ie!(
            12235,
            CISCO,
            "ciscoAvcConnectionServer",
            String,
            "Connection server"
        ),
        ie!(
            12236,
            CISCO,
            "ciscoAvcConnectionClient",
            String,
            "Connection client"
        ),
        ie!(
            95,
            CISCO,
            "ciscoApplicationId",
            OctetArray,
            "Cisco NBar application ID"
        ),
        ie!(
            96,
            CISCO,
            "ciscoApplicationName",
            String,
            "Cisco NBar application name"
        ),
        ie!(9, CISCO, "ciscoAsa", OctetArray, "Cisco ASA extended data"),
        ie!(
            33000,
            CISCO,
            "ciscoFirewallEvent",
            Unsigned8,
            "Cisco firewall event type"
        ),
        ie!(
            33001,
            CISCO,
            "ciscoFirewallEventExtended",
            Unsigned16,
            "Extended firewall event"
        ),
        ie!(33002, CISCO, "ciscoUsername", String, "Cisco username"),
        // Cisco Stealthwatch / Cognitive Intelligence
        ie!(
            12240,
            CISCO,
            "ciscoThreatName",
            String,
            "Stealthwatch threat name"
        ),
        ie!(
            12241,
            CISCO,
            "ciscoThreatCategory",
            String,
            "Stealthwatch threat category"
        ),
        ie!(
            12242,
            CISCO,
            "ciscoThreatScore",
            Unsigned32,
            "Stealthwatch threat score"
        ),
        // Cisco performance routing
        ie!(
            12215,
            CISCO,
            "ciscoC3plPolicyName",
            String,
            "Cisco policy name"
        ),
        ie!(
            12216,
            CISCO,
            "ciscoC3plClassName",
            String,
            "Cisco class name"
        ),
        // Cisco SD-WAN
        ie!(
            12243,
            CISCO,
            "ciscoSdwanSiteId",
            Unsigned32,
            "SD-WAN site ID"
        ),
        ie!(12244, CISCO, "ciscoSdwanVpnId", Unsigned32, "SD-WAN VPN ID"),
        ie!(
            12245,
            CISCO,
            "ciscoSdwanTunnelName",
            String,
            "SD-WAN tunnel name"
        ),
    ]
}

fn juniper_elements() -> Vec<InformationElement> {
    vec![
        ie!(
            137,
            JUNIPER,
            "juniperSourceZone",
            String,
            "Juniper source security zone"
        ),
        ie!(
            138,
            JUNIPER,
            "juniperDestinationZone",
            String,
            "Juniper destination security zone"
        ),
        ie!(
            139,
            JUNIPER,
            "juniperPolicyName",
            String,
            "Juniper security policy name"
        ),
        ie!(
            140,
            JUNIPER,
            "juniperPolicyContext",
            String,
            "Juniper policy context"
        ),
        ie!(
            141,
            JUNIPER,
            "juniperPolicyType",
            Unsigned8,
            "Juniper policy type"
        ),
        ie!(142, JUNIPER, "juniperRuleName", String, "Juniper rule name"),
        ie!(
            151,
            JUNIPER,
            "juniperNatSrcAddress",
            Ipv4Address,
            "NAT translated source address"
        ),
        ie!(
            152,
            JUNIPER,
            "juniperNatDstAddress",
            Ipv4Address,
            "NAT translated destination address"
        ),
        ie!(
            153,
            JUNIPER,
            "juniperNatSrcPort",
            Unsigned16,
            "NAT translated source port"
        ),
        ie!(
            154,
            JUNIPER,
            "juniperNatDstPort",
            Unsigned16,
            "NAT translated destination port"
        ),
        ie!(155, JUNIPER, "juniperNatPoolName", String, "NAT pool name"),
        ie!(
            156,
            JUNIPER,
            "juniperApplicationName",
            String,
            "Juniper AppID application name"
        ),
        ie!(
            157,
            JUNIPER,
            "juniperNestedApplicationName",
            String,
            "Juniper nested application"
        ),
    ]
}

fn vmware_elements() -> Vec<InformationElement> {
    vec![
        ie!(
            880,
            VMWARE,
            "vmwareTenantSourceIPv4",
            Ipv4Address,
            "Tenant source IPv4"
        ),
        ie!(
            881,
            VMWARE,
            "vmwareTenantDestIPv4",
            Ipv4Address,
            "Tenant destination IPv4"
        ),
        ie!(
            882,
            VMWARE,
            "vmwareTenantSourcePort",
            Unsigned16,
            "Tenant source port"
        ),
        ie!(
            883,
            VMWARE,
            "vmwareTenantDestPort",
            Unsigned16,
            "Tenant destination port"
        ),
        ie!(
            884,
            VMWARE,
            "vmwareTenantProtocol",
            Unsigned8,
            "Tenant protocol"
        ),
        ie!(
            885,
            VMWARE,
            "vmwareVxlanId",
            Unsigned32,
            "VXLAN network identifier"
        ),
        ie!(890, VMWARE, "vmwareSegmentId", String, "NSX segment ID"),
        ie!(891, VMWARE, "vmwareRuleName", String, "NSX DFW rule name"),
        ie!(892, VMWARE, "vmwareRuleId", Unsigned32, "NSX DFW rule ID"),
    ]
}

fn palo_alto_elements() -> Vec<InformationElement> {
    vec![
        ie!(
            56701,
            PALO_ALTO,
            "panAppId",
            String,
            "Palo Alto App-ID application"
        ),
        ie!(56702, PALO_ALTO, "panUserId", String, "Palo Alto User-ID"),
        ie!(
            56703,
            PALO_ALTO,
            "panSourceZone",
            String,
            "Palo Alto source zone"
        ),
        ie!(
            56704,
            PALO_ALTO,
            "panDestinationZone",
            String,
            "Palo Alto destination zone"
        ),
        ie!(
            56705,
            PALO_ALTO,
            "panRuleName",
            String,
            "Palo Alto security policy rule"
        ),
        ie!(
            56706,
            PALO_ALTO,
            "panCategory",
            String,
            "Palo Alto URL category"
        ),
        ie!(
            56707,
            PALO_ALTO,
            "panThreatId",
            Unsigned32,
            "Palo Alto threat ID"
        ),
        ie!(
            56708,
            PALO_ALTO,
            "panThreatCategory",
            String,
            "Palo Alto threat category"
        ),
        ie!(
            56709,
            PALO_ALTO,
            "panSessionId",
            Unsigned32,
            "Palo Alto session ID"
        ),
        ie!(
            56710,
            PALO_ALTO,
            "panRepeatCount",
            Unsigned32,
            "Palo Alto repeat count"
        ),
        ie!(
            56711,
            PALO_ALTO,
            "panDeviceName",
            String,
            "Palo Alto device name"
        ),
        ie!(
            56712,
            PALO_ALTO,
            "panVsysName",
            String,
            "Palo Alto VSYS name"
        ),
    ]
}

fn barracuda_elements() -> Vec<InformationElement> {
    vec![
        ie!(
            1,
            BARRACUDA,
            "barracudaFwAction",
            Unsigned8,
            "Barracuda firewall action"
        ),
        ie!(
            2,
            BARRACUDA,
            "barracudaFwReason",
            Unsigned8,
            "Barracuda firewall reason"
        ),
        ie!(
            3,
            BARRACUDA,
            "barracudaFwRuleName",
            String,
            "Barracuda rule name"
        ),
        ie!(
            4,
            BARRACUDA,
            "barracudaServiceName",
            String,
            "Barracuda service name"
        ),
        ie!(
            8,
            BARRACUDA,
            "barracudaBindIPv4Address",
            Ipv4Address,
            "Bind IPv4 address"
        ),
    ]
}

fn ntop_elements() -> Vec<InformationElement> {
    vec![
        // ntopng / nProbe IEs
        ie!(
            57554,
            NTOP,
            "ntopL7Protocol",
            Unsigned16,
            "nDPI Layer 7 protocol ID"
        ),
        ie!(
            57555,
            NTOP,
            "ntopL7ProtocolName",
            String,
            "nDPI Layer 7 protocol name"
        ),
        ie!(57556, NTOP, "ntopDnsQueryName", String, "DNS query name"),
        ie!(
            57557,
            NTOP,
            "ntopDnsQueryType",
            Unsigned16,
            "DNS query type"
        ),
        ie!(
            57558,
            NTOP,
            "ntopDnsResponseCode",
            Unsigned16,
            "DNS response code"
        ),
        ie!(57559, NTOP, "ntopHttpUrl", String, "HTTP URL"),
        ie!(57560, NTOP, "ntopHttpMethod", String, "HTTP method"),
        ie!(
            57561,
            NTOP,
            "ntopHttpRetCode",
            Unsigned16,
            "HTTP return code"
        ),
        ie!(57562, NTOP, "ntopHttpHost", String, "HTTP host"),
        ie!(57563, NTOP, "ntopHttpUserAgent", String, "HTTP user agent"),
        ie!(
            57564,
            NTOP,
            "ntopHttpContentType",
            String,
            "HTTP content type"
        ),
        ie!(
            57580,
            NTOP,
            "ntopTlsServerName",
            String,
            "TLS Server Name Indication (SNI)"
        ),
        ie!(57581, NTOP, "ntopTlsVersion", Unsigned16, "TLS version"),
        ie!(
            57600,
            NTOP,
            "ntopSshClientSignature",
            String,
            "SSH client signature"
        ),
        ie!(
            57601,
            NTOP,
            "ntopSshServerSignature",
            String,
            "SSH server signature"
        ),
    ]
}

fn fortinet_elements() -> Vec<InformationElement> {
    vec![
        ie!(
            1,
            FORTINET,
            "fortinetPolicyId",
            Unsigned32,
            "FortiGate policy ID"
        ),
        ie!(2, FORTINET, "fortinetUserId", String, "FortiGate user ID"),
        ie!(
            3,
            FORTINET,
            "fortinetAppCategory",
            String,
            "FortiGate application category"
        ),
        ie!(
            4,
            FORTINET,
            "fortinetAppName",
            String,
            "FortiGate application name"
        ),
        ie!(
            5,
            FORTINET,
            "fortinetUrlCategory",
            String,
            "FortiGate URL category"
        ),
        ie!(
            6,
            FORTINET,
            "fortinetSourceInterface",
            String,
            "FortiGate source interface"
        ),
        ie!(
            7,
            FORTINET,
            "fortinetDestInterface",
            String,
            "FortiGate destination interface"
        ),
        ie!(8, FORTINET, "fortinetAction", Unsigned8, "FortiGate action"),
        ie!(9, FORTINET, "fortinetVdom", String, "FortiGate VDOM name"),
        ie!(
            10,
            FORTINET,
            "fortinetThreatLevel",
            Unsigned8,
            "FortiGate threat level"
        ),
    ]
}

fn nokia_elements() -> Vec<InformationElement> {
    vec![
        ie!(100, NOKIA, "nokiaServiceId", Unsigned32, "Nokia service ID"),
        ie!(101, NOKIA, "nokiaServiceName", String, "Nokia service name"),
        ie!(
            102,
            NOKIA,
            "nokiaSapIngressId",
            Unsigned32,
            "SAP ingress ID"
        ),
        ie!(103, NOKIA, "nokiaSapEgressId", Unsigned32, "SAP egress ID"),
        ie!(
            104,
            NOKIA,
            "nokiaVRouterId",
            Unsigned32,
            "Virtual router ID"
        ),
        ie!(
            105,
            NOKIA,
            "nokiaSubscriberInfo",
            String,
            "Subscriber information"
        ),
    ]
}

fn huawei_elements() -> Vec<InformationElement> {
    vec![
        ie!(
            1,
            HUAWEI,
            "huaweiNatEvent",
            Unsigned8,
            "Huawei NAT event type"
        ),
        ie!(
            2,
            HUAWEI,
            "huaweiNatSourceAddress",
            Ipv4Address,
            "Huawei NAT source address"
        ),
        ie!(
            3,
            HUAWEI,
            "huaweiNatDestAddress",
            Ipv4Address,
            "Huawei NAT destination address"
        ),
        ie!(
            4,
            HUAWEI,
            "huaweiNatSourcePort",
            Unsigned16,
            "Huawei NAT source port"
        ),
        ie!(
            5,
            HUAWEI,
            "huaweiNatDestPort",
            Unsigned16,
            "Huawei NAT destination port"
        ),
        ie!(
            6,
            HUAWEI,
            "huaweiServiceName",
            String,
            "Huawei service name"
        ),
        ie!(7, HUAWEI, "huaweiPolicyName", String, "Huawei policy name"),
    ]
}
