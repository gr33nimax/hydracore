# HydraCore Android test build

HydraCore is HydraBox's public distribution identity for this extended libbox
runtime. It is an independent derivative of **etonify-core**, which is built on
the pinned `sing-box-extended` baseline. This name does not claim upstream
authorship, affiliation, or endorsement.

## Identity and compatibility

- New bindings expose `HydraCoreCapabilities()` and
  `core_id: io.hydrabox.hydracore`.
- The capability document retains `upstream_project: etonify-core`.
- `EtonifyCapabilities()` remains a deprecated source/binary compatibility
  alias for existing mobile bindings.
- Go module paths and the `libbox.aar` artifact name remain upstream-compatible.

## WDTT subscription endpoint

- Adds the lazy `wdtt` endpoint behind the `with_wdtt` build tag.
- Uses anonymous VK Calls TURN relays and the WDTT DTLS/WRAP protocol while
  keeping external DNS, HTTPS, and UDP inside HydraCore's protected dialer.
- Exposes remote policy v2 and bounded WDTT capability metadata to HydraBox.
- Accepts WDTT only from authenticated HydraBox Subscription JWE. Direct links,
  raw sing-box imports, publisher-owned device IDs, local ports, TURN
  credentials, and dynamic WireGuard authority are deliberately unsupported.
- Preserves stable runtime device binding in the HydraCore cache and starts no
  TURN allocations until a WDTT profile is actually selected or used.

## Provenance

The release includes the AAR, generated Java sources, an attributed source
archive, checksums, and machine-readable build provenance. The historical
Etonify baseline files and notices remain intact and authoritative for the
inherited integration.

Protocol coverage and compatibility limitations are documented in
`ETONIFY_CORE.md`; HydraCore does not relabel unsupported upstream stubs as
runnable protocols.
