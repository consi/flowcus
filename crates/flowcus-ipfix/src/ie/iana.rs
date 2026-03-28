//! IANA-assigned IPFIX Information Elements.
//!
//! Source: IANA IPFIX Information Element registry
//! https://www.iana.org/assignments/ipfix/ipfix.xhtml
//!
//! Names follow the official IANA camelCase naming convention.

use super::{InformationElement, ie};

pub fn iana_elements() -> Vec<InformationElement> {
    vec![
        // ---- Flow identifiers and counters (1-40) ----
        ie!(
            1,
            0,
            "octetDeltaCount",
            Unsigned64,
            "Number of octets since last report"
        ),
        ie!(
            2,
            0,
            "packetDeltaCount",
            Unsigned64,
            "Number of packets since last report"
        ),
        ie!(
            3,
            0,
            "deltaFlowCount",
            Unsigned64,
            "Number of flows since last report"
        ),
        ie!(4, 0, "protocolIdentifier", Unsigned8, "IP protocol number"),
        ie!(
            5,
            0,
            "ipClassOfService",
            Unsigned8,
            "IPv4 TOS or IPv6 Traffic Class"
        ),
        ie!(
            6,
            0,
            "tcpControlBits",
            Unsigned16,
            "TCP control bits (flags)"
        ),
        ie!(
            7,
            0,
            "sourceTransportPort",
            Unsigned16,
            "Transport source port"
        ),
        ie!(
            8,
            0,
            "sourceIPv4Address",
            Ipv4Address,
            "IPv4 source address"
        ),
        ie!(
            9,
            0,
            "sourceIPv4PrefixLength",
            Unsigned8,
            "IPv4 source prefix length"
        ),
        ie!(
            10,
            0,
            "ingressInterface",
            Unsigned32,
            "Ingress interface index"
        ),
        ie!(
            11,
            0,
            "destinationTransportPort",
            Unsigned16,
            "Transport destination port"
        ),
        ie!(
            12,
            0,
            "destinationIPv4Address",
            Ipv4Address,
            "IPv4 destination address"
        ),
        ie!(
            13,
            0,
            "destinationIPv4PrefixLength",
            Unsigned8,
            "IPv4 destination prefix length"
        ),
        ie!(
            14,
            0,
            "egressInterface",
            Unsigned32,
            "Egress interface index"
        ),
        ie!(
            15,
            0,
            "ipNextHopIPv4Address",
            Ipv4Address,
            "IPv4 next hop address"
        ),
        ie!(
            16,
            0,
            "bgpSourceAsNumber",
            Unsigned32,
            "BGP source AS number"
        ),
        ie!(
            17,
            0,
            "bgpDestinationAsNumber",
            Unsigned32,
            "BGP destination AS number"
        ),
        ie!(
            18,
            0,
            "bgpNextHopIPv4Address",
            Ipv4Address,
            "BGP next hop IPv4 address"
        ),
        ie!(
            19,
            0,
            "postMCastPacketDeltaCount",
            Unsigned64,
            "Multicast replicated packet count"
        ),
        ie!(
            20,
            0,
            "postMCastOctetDeltaCount",
            Unsigned64,
            "Multicast replicated octet count"
        ),
        ie!(
            21,
            0,
            "flowEndSysUpTime",
            Unsigned32,
            "SysUpTime at flow end"
        ),
        ie!(
            22,
            0,
            "flowStartSysUpTime",
            Unsigned32,
            "SysUpTime at flow start"
        ),
        ie!(
            23,
            0,
            "postOctetDeltaCount",
            Unsigned64,
            "Post-NAT octet delta count"
        ),
        ie!(
            24,
            0,
            "postPacketDeltaCount",
            Unsigned64,
            "Post-NAT packet delta count"
        ),
        ie!(
            25,
            0,
            "minimumIpTotalLength",
            Unsigned64,
            "Minimum IP total length"
        ),
        ie!(
            26,
            0,
            "maximumIpTotalLength",
            Unsigned64,
            "Maximum IP total length"
        ),
        ie!(
            27,
            0,
            "sourceIPv6Address",
            Ipv6Address,
            "IPv6 source address"
        ),
        ie!(
            28,
            0,
            "destinationIPv6Address",
            Ipv6Address,
            "IPv6 destination address"
        ),
        ie!(
            29,
            0,
            "sourceIPv6PrefixLength",
            Unsigned8,
            "IPv6 source prefix length"
        ),
        ie!(
            30,
            0,
            "destinationIPv6PrefixLength",
            Unsigned8,
            "IPv6 destination prefix length"
        ),
        ie!(31, 0, "flowLabelIPv6", Unsigned32, "IPv6 flow label"),
        ie!(
            32,
            0,
            "icmpTypeCodeIPv4",
            Unsigned16,
            "ICMP type and code (IPv4)"
        ),
        ie!(33, 0, "igmpType", Unsigned8, "IGMP type"),
        ie!(34, 0, "samplingInterval", Unsigned32, "Sampling interval"),
        ie!(35, 0, "samplingAlgorithm", Unsigned8, "Sampling algorithm"),
        ie!(
            36,
            0,
            "flowActiveTimeout",
            Unsigned16,
            "Active timeout in seconds"
        ),
        ie!(
            37,
            0,
            "flowIdleTimeout",
            Unsigned16,
            "Idle timeout in seconds"
        ),
        ie!(38, 0, "engineType", Unsigned8, "Flow switching engine type"),
        ie!(39, 0, "engineId", Unsigned8, "Flow switching engine ID"),
        ie!(
            40,
            0,
            "exportedOctetTotalCount",
            Unsigned64,
            "Total exported octets"
        ),
        // ---- More counters and identifiers (41-80) ----
        ie!(
            41,
            0,
            "exportedMessageTotalCount",
            Unsigned64,
            "Total exported messages"
        ),
        ie!(
            42,
            0,
            "exportedFlowRecordTotalCount",
            Unsigned64,
            "Total exported flow records"
        ),
        ie!(44, 0, "sourceIPv4Prefix", Ipv4Address, "IPv4 source prefix"),
        ie!(
            45,
            0,
            "destinationIPv4Prefix",
            Ipv4Address,
            "IPv4 destination prefix"
        ),
        ie!(46, 0, "mplsTopLabelType", Unsigned8, "MPLS top label type"),
        ie!(
            47,
            0,
            "mplsTopLabelIPv4Address",
            Ipv4Address,
            "MPLS top label IPv4 address"
        ),
        ie!(48, 0, "samplerId", Unsigned16, "Sampler ID"),
        ie!(49, 0, "samplerMode", Unsigned8, "Sampler mode"),
        ie!(
            50,
            0,
            "samplerRandomInterval",
            Unsigned32,
            "Sampler random interval"
        ),
        ie!(52, 0, "minimumTTL", Unsigned8, "Minimum TTL"),
        ie!(53, 0, "maximumTTL", Unsigned8, "Maximum TTL"),
        ie!(
            54,
            0,
            "fragmentIdentification",
            Unsigned32,
            "IPv4 fragment identification"
        ),
        ie!(
            55,
            0,
            "postIpClassOfService",
            Unsigned8,
            "Post IP class of service"
        ),
        ie!(56, 0, "sourceMacAddress", MacAddress, "Source MAC address"),
        ie!(
            57,
            0,
            "postDestinationMacAddress",
            MacAddress,
            "Post destination MAC address"
        ),
        ie!(58, 0, "vlanId", Unsigned16, "VLAN ID"),
        ie!(59, 0, "postVlanId", Unsigned16, "Post VLAN ID"),
        ie!(60, 0, "ipVersion", Unsigned8, "IP version (4 or 6)"),
        ie!(
            61,
            0,
            "flowDirection",
            Unsigned8,
            "Flow direction (0=ingress, 1=egress)"
        ),
        ie!(
            62,
            0,
            "ipNextHopIPv6Address",
            Ipv6Address,
            "IPv6 next hop address"
        ),
        ie!(
            63,
            0,
            "bgpNextHopIPv6Address",
            Ipv6Address,
            "BGP next hop IPv6 address"
        ),
        ie!(
            64,
            0,
            "ipv6ExtensionHeaders",
            Unsigned32,
            "IPv6 extension headers bitmap"
        ),
        ie!(
            70,
            0,
            "mplsTopLabelStackSection",
            OctetArray,
            "MPLS top label stack entry"
        ),
        ie!(
            71,
            0,
            "mplsLabelStackSection2",
            OctetArray,
            "MPLS label stack entry 2"
        ),
        ie!(
            72,
            0,
            "mplsLabelStackSection3",
            OctetArray,
            "MPLS label stack entry 3"
        ),
        ie!(
            73,
            0,
            "mplsLabelStackSection4",
            OctetArray,
            "MPLS label stack entry 4"
        ),
        ie!(
            74,
            0,
            "mplsLabelStackSection5",
            OctetArray,
            "MPLS label stack entry 5"
        ),
        ie!(
            75,
            0,
            "mplsLabelStackSection6",
            OctetArray,
            "MPLS label stack entry 6"
        ),
        ie!(
            76,
            0,
            "mplsLabelStackSection7",
            OctetArray,
            "MPLS label stack entry 7"
        ),
        ie!(
            77,
            0,
            "mplsLabelStackSection8",
            OctetArray,
            "MPLS label stack entry 8"
        ),
        ie!(
            78,
            0,
            "mplsLabelStackSection9",
            OctetArray,
            "MPLS label stack entry 9"
        ),
        ie!(
            79,
            0,
            "mplsLabelStackSection10",
            OctetArray,
            "MPLS label stack entry 10"
        ),
        ie!(
            80,
            0,
            "destinationMacAddress",
            MacAddress,
            "Destination MAC address"
        ),
        // ---- More identifiers (81-100) ----
        ie!(
            81,
            0,
            "postSourceMacAddress",
            MacAddress,
            "Post source MAC address"
        ),
        ie!(82, 0, "interfaceName", String, "Interface name"),
        ie!(
            83,
            0,
            "interfaceDescription",
            String,
            "Interface description"
        ),
        ie!(
            85,
            0,
            "octetTotalCount",
            Unsigned64,
            "Total octets for this flow"
        ),
        ie!(
            86,
            0,
            "packetTotalCount",
            Unsigned64,
            "Total packets for this flow"
        ),
        ie!(88, 0, "fragmentOffset", Unsigned16, "Fragment offset"),
        ie!(89, 0, "forwardingStatus", Unsigned32, "Forwarding status"),
        ie!(
            90,
            0,
            "mplsVpnRouteDistinguisher",
            OctetArray,
            "MPLS VPN route distinguisher"
        ),
        ie!(
            91,
            0,
            "mplsTopLabelPrefixLength",
            Unsigned8,
            "MPLS top label prefix length"
        ),
        ie!(
            94,
            0,
            "applicationDescription",
            String,
            "Application description"
        ),
        ie!(95, 0, "applicationId", OctetArray, "Application ID"),
        ie!(96, 0, "applicationName", String, "Application name"),
        // ---- Timestamps (150-160) ----
        ie!(
            150,
            0,
            "flowStartSeconds",
            DateTimeSeconds,
            "Flow start time (seconds)"
        ),
        ie!(
            151,
            0,
            "flowEndSeconds",
            DateTimeSeconds,
            "Flow end time (seconds)"
        ),
        ie!(
            152,
            0,
            "flowStartMilliseconds",
            DateTimeMilliseconds,
            "Flow start time (ms)"
        ),
        ie!(
            153,
            0,
            "flowEndMilliseconds",
            DateTimeMilliseconds,
            "Flow end time (ms)"
        ),
        ie!(
            154,
            0,
            "flowStartMicroseconds",
            DateTimeMilliseconds,
            "Flow start time (us)"
        ),
        ie!(
            155,
            0,
            "flowEndMicroseconds",
            DateTimeMilliseconds,
            "Flow end time (us)"
        ),
        ie!(
            156,
            0,
            "flowStartNanoseconds",
            DateTimeMilliseconds,
            "Flow start time (ns)"
        ),
        ie!(
            157,
            0,
            "flowEndNanoseconds",
            DateTimeMilliseconds,
            "Flow end time (ns)"
        ),
        ie!(
            158,
            0,
            "flowStartDeltaMicroseconds",
            Unsigned32,
            "Flow start delta (us)"
        ),
        ie!(
            159,
            0,
            "flowEndDeltaMicroseconds",
            Unsigned32,
            "Flow end delta (us)"
        ),
        ie!(
            160,
            0,
            "systemInitTimeMilliseconds",
            DateTimeMilliseconds,
            "System init time (ms)"
        ),
        // ---- Duration (161-163) ----
        ie!(
            161,
            0,
            "flowDurationMilliseconds",
            Unsigned32,
            "Flow duration (ms)"
        ),
        ie!(
            162,
            0,
            "flowDurationMicroseconds",
            Unsigned32,
            "Flow duration (us)"
        ),
        // ---- Counters and aggregation (163-180) ----
        ie!(
            163,
            0,
            "observedFlowTotalCount",
            Unsigned64,
            "Total observed flows"
        ),
        ie!(
            164,
            0,
            "ignoredPacketTotalCount",
            Unsigned64,
            "Ignored packets"
        ),
        ie!(
            165,
            0,
            "ignoredOctetTotalCount",
            Unsigned64,
            "Ignored octets"
        ),
        ie!(
            166,
            0,
            "notSentFlowTotalCount",
            Unsigned64,
            "Not sent flows"
        ),
        ie!(
            167,
            0,
            "notSentPacketTotalCount",
            Unsigned64,
            "Not sent packets"
        ),
        ie!(
            168,
            0,
            "notSentOctetTotalCount",
            Unsigned64,
            "Not sent octets"
        ),
        ie!(176, 0, "icmpTypeIPv4", Unsigned8, "ICMP type (IPv4)"),
        ie!(177, 0, "icmpCodeIPv4", Unsigned8, "ICMP code (IPv4)"),
        ie!(178, 0, "icmpTypeIPv6", Unsigned8, "ICMP type (IPv6)"),
        ie!(179, 0, "icmpCodeIPv6", Unsigned8, "ICMP code (IPv6)"),
        // ---- More network fields (180-230) ----
        ie!(180, 0, "udpSourcePort", Unsigned16, "UDP source port"),
        ie!(
            181,
            0,
            "udpDestinationPort",
            Unsigned16,
            "UDP destination port"
        ),
        ie!(182, 0, "tcpSourcePort", Unsigned16, "TCP source port"),
        ie!(
            183,
            0,
            "tcpDestinationPort",
            Unsigned16,
            "TCP destination port"
        ),
        ie!(
            184,
            0,
            "tcpSequenceNumber",
            Unsigned32,
            "TCP sequence number"
        ),
        ie!(
            185,
            0,
            "tcpAcknowledgementNumber",
            Unsigned32,
            "TCP acknowledgement number"
        ),
        ie!(186, 0, "tcpWindowSize", Unsigned16, "TCP window size"),
        ie!(187, 0, "tcpUrgentPointer", Unsigned16, "TCP urgent pointer"),
        ie!(188, 0, "tcpHeaderLength", Unsigned8, "TCP header length"),
        ie!(189, 0, "ipHeaderLength", Unsigned8, "IP header length"),
        ie!(190, 0, "totalLengthIPv4", Unsigned16, "IPv4 total length"),
        ie!(
            191,
            0,
            "payloadLengthIPv6",
            Unsigned16,
            "IPv6 payload length"
        ),
        ie!(192, 0, "ipTTL", Unsigned8, "IP time to live"),
        ie!(193, 0, "nextHeaderIPv6", Unsigned8, "IPv6 next header"),
        ie!(
            194,
            0,
            "mplsPayloadLength",
            Unsigned32,
            "MPLS payload length"
        ),
        ie!(195, 0, "ipDiffServCodePoint", Unsigned8, "DSCP"),
        ie!(196, 0, "ipPrecedence", Unsigned8, "IP precedence"),
        ie!(197, 0, "fragmentFlags", Unsigned8, "Fragment flags"),
        ie!(
            198,
            0,
            "octetDeltaSumOfSquares",
            Unsigned64,
            "Sum of squared octet deltas"
        ),
        ie!(
            199,
            0,
            "octetTotalSumOfSquares",
            Unsigned64,
            "Sum of squared total octets"
        ),
        ie!(200, 0, "mplsTopLabelTTL", Unsigned8, "MPLS top label TTL"),
        ie!(
            201,
            0,
            "mplsLabelStackLength",
            Unsigned32,
            "MPLS label stack length"
        ),
        ie!(
            202,
            0,
            "mplsLabelStackDepth",
            Unsigned32,
            "MPLS label stack depth"
        ),
        ie!(
            203,
            0,
            "mplsTopLabelExp",
            Unsigned8,
            "MPLS top label experimental bits"
        ),
        ie!(204, 0, "ipPayloadLength", Unsigned32, "IP payload length"),
        ie!(205, 0, "udpMessageLength", Unsigned16, "UDP message length"),
        ie!(
            206,
            0,
            "isMulticast",
            Unsigned8,
            "Is multicast (0=no, 1=yes)"
        ),
        ie!(207, 0, "ipv4IHL", Unsigned8, "IPv4 IHL"),
        ie!(208, 0, "ipv4Options", Unsigned32, "IPv4 options bitmap"),
        ie!(209, 0, "tcpOptions", Unsigned64, "TCP options bitmap"),
        ie!(210, 0, "paddingOctets", OctetArray, "Padding"),
        ie!(
            211,
            0,
            "collectorIPv4Address",
            Ipv4Address,
            "Collector IPv4 address"
        ),
        ie!(
            212,
            0,
            "collectorIPv6Address",
            Ipv6Address,
            "Collector IPv6 address"
        ),
        ie!(213, 0, "exportInterface", Unsigned32, "Export interface"),
        ie!(
            214,
            0,
            "exportProtocolVersion",
            Unsigned8,
            "Export protocol version"
        ),
        ie!(
            215,
            0,
            "exportTransportProtocol",
            Unsigned8,
            "Export transport protocol"
        ),
        ie!(
            216,
            0,
            "collectorTransportPort",
            Unsigned16,
            "Collector transport port"
        ),
        ie!(
            217,
            0,
            "exporterTransportPort",
            Unsigned16,
            "Exporter transport port"
        ),
        ie!(
            218,
            0,
            "tcpSynTotalCount",
            Unsigned64,
            "TCP SYN total count"
        ),
        ie!(
            219,
            0,
            "tcpFinTotalCount",
            Unsigned64,
            "TCP FIN total count"
        ),
        ie!(
            220,
            0,
            "tcpRstTotalCount",
            Unsigned64,
            "TCP RST total count"
        ),
        ie!(
            221,
            0,
            "tcpPshTotalCount",
            Unsigned64,
            "TCP PSH total count"
        ),
        ie!(
            222,
            0,
            "tcpAckTotalCount",
            Unsigned64,
            "TCP ACK total count"
        ),
        ie!(
            223,
            0,
            "tcpUrgTotalCount",
            Unsigned64,
            "TCP URG total count"
        ),
        ie!(224, 0, "ipTotalLength", Unsigned64, "IP total length"),
        // ---- NAT fields (225-240) ----
        ie!(
            225,
            0,
            "postNATSourceIPv4Address",
            Ipv4Address,
            "Post-NAT source IPv4"
        ),
        ie!(
            226,
            0,
            "postNATDestinationIPv4Address",
            Ipv4Address,
            "Post-NAT destination IPv4"
        ),
        ie!(
            227,
            0,
            "postNAPTSourceTransportPort",
            Unsigned16,
            "Post-NAPT source port"
        ),
        ie!(
            228,
            0,
            "postNAPTDestinationTransportPort",
            Unsigned16,
            "Post-NAPT destination port"
        ),
        ie!(
            229,
            0,
            "natOriginatingAddressRealm",
            Unsigned8,
            "NAT originating address realm"
        ),
        ie!(230, 0, "natEvent", Unsigned8, "NAT event"),
        ie!(
            231,
            0,
            "initiatorOctets",
            Unsigned64,
            "Initiator direction octets"
        ),
        ie!(
            232,
            0,
            "responderOctets",
            Unsigned64,
            "Responder direction octets"
        ),
        // ---- Observation and metering (233-250) ----
        ie!(234, 0, "ingressVRFID", Unsigned32, "Ingress VRF ID"),
        ie!(235, 0, "egressVRFID", Unsigned32, "Egress VRF ID"),
        ie!(236, 0, "VRFname", String, "VRF name"),
        // ---- Post-NAT IPv6 (281-282) ----
        ie!(
            281,
            0,
            "postNATSourceIPv6Address",
            Ipv6Address,
            "Post-NAT source IPv6"
        ),
        ie!(
            282,
            0,
            "postNATDestinationIPv6Address",
            Ipv6Address,
            "Post-NAT destination IPv6"
        ),
        // ---- Layer 2 (298-300) ----
        ie!(298, 0, "layer2SegmentId", Unsigned64, "Layer 2 segment ID"),
        ie!(
            299,
            0,
            "layer2OctetDeltaCount",
            Unsigned64,
            "Layer 2 octet delta count"
        ),
        ie!(
            300,
            0,
            "layer2OctetTotalCount",
            Unsigned64,
            "Layer 2 octet total count"
        ),
        // ---- Observation point and selector (309-313) ----
        ie!(
            309,
            0,
            "selectorAlgorithm",
            Unsigned16,
            "Selector algorithm"
        ),
        ie!(
            310,
            0,
            "selectorIdTotalFlowsObserved",
            Unsigned64,
            "Total flows observed by selector"
        ),
        ie!(
            311,
            0,
            "selectorIdTotalFlowsSelected",
            Unsigned64,
            "Total flows selected"
        ),
        ie!(312, 0, "absoluteError", Float64, "Absolute error"),
        ie!(313, 0, "relativeError", Float64, "Relative error"),
        // ---- Exporter info (144-149) ----
        ie!(
            144,
            0,
            "exportingProcessId",
            Unsigned32,
            "Exporting process ID"
        ),
        ie!(145, 0, "templateId", Unsigned16, "Template ID"),
        ie!(148, 0, "flowId", Unsigned64, "Flow ID"),
        ie!(
            149,
            0,
            "observationDomainId",
            Unsigned32,
            "Observation domain ID"
        ),
        // ---- Interface (ingress/egress extended) (130-143) ----
        ie!(
            130,
            0,
            "exporterIPv4Address",
            Ipv4Address,
            "Exporter IPv4 address"
        ),
        ie!(
            131,
            0,
            "exporterIPv6Address",
            Ipv6Address,
            "Exporter IPv6 address"
        ),
        ie!(136, 0, "flowEndReason", Unsigned8, "Flow end reason"),
        ie!(
            137,
            0,
            "commonPropertiesId",
            Unsigned64,
            "Common properties ID"
        ),
        ie!(
            138,
            0,
            "observationPointId",
            Unsigned64,
            "Observation point ID"
        ),
        ie!(
            139,
            0,
            "icmpTypeCodeIPv6",
            Unsigned16,
            "ICMP type/code IPv6"
        ),
        ie!(
            140,
            0,
            "mplsTopLabelIPv6Address",
            Ipv6Address,
            "MPLS top label IPv6"
        ),
        ie!(141, 0, "lineCardId", Unsigned32, "Line card ID"),
        ie!(142, 0, "portId", Unsigned32, "Port ID"),
        ie!(
            143,
            0,
            "meteringProcessId",
            Unsigned32,
            "Metering process ID"
        ),
        // ---- Newer IANA additions (330-470) ----
        ie!(
            330,
            0,
            "observationDomainName",
            String,
            "Observation domain name"
        ),
        ie!(
            331,
            0,
            "selectionSequenceId",
            Unsigned64,
            "Selection sequence ID"
        ),
        ie!(332, 0, "selectorId", Unsigned64, "Selector ID"),
        ie!(
            333,
            0,
            "informationElementId",
            Unsigned16,
            "Information Element ID"
        ),
        ie!(
            334,
            0,
            "selectorIDTotalFlowsObserved",
            Unsigned64,
            "Total flows observed"
        ),
        ie!(
            335,
            0,
            "selectorIDTotalFlowsSelected",
            Unsigned64,
            "Total flows selected"
        ),
        ie!(
            346,
            0,
            "privateEnterpriseNumber",
            Unsigned32,
            "Private enterprise number"
        ),
        // ---- HTTP/Application (459-462) ----
        ie!(459, 0, "httpStatusCode", Unsigned16, "HTTP status code"),
        ie!(460, 0, "httpRequestMethod", String, "HTTP request method"),
        ie!(461, 0, "httpRequestHost", String, "HTTP request host"),
        ie!(462, 0, "httpRequestTarget", String, "HTTP request target"),
        // ---- DNS (468-470) ----
        ie!(468, 0, "dnsQueryName", String, "DNS query name"),
        ie!(469, 0, "dnsQueryType", Unsigned16, "DNS query type"),
        ie!(470, 0, "dnsResponseCode", Unsigned16, "DNS response code"),
    ]
}
