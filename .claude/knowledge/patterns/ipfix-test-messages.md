# Building IPFIX Test Messages
**Context:** Writing unit tests for IPFIX protocol parsing
**Decision/Pattern:** Build raw byte arrays manually. Set length fields MUST include the 4-byte set header. Message length includes the 16-byte message header. Template set: set_id=2, set_length=4+content. Data set: set_id=template_id (>=256), set_length=4+data. Fix the message header length field last after all sets are appended.
**Rationale:** Got bitten by set_length=20 when actual content was 12 bytes (should have been 16 including header). The RFC says set length includes the set header.
**Last verified:** 2026-03-23
