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

## Provenance

The release includes the AAR, generated Java sources, an attributed source
archive, checksums, and machine-readable build provenance. The historical
Etonify baseline files and notices remain intact and authoritative for the
inherited integration.

Protocol coverage and compatibility limitations are documented in
`ETONIFY_CORE.md`; HydraCore does not relabel unsupported upstream stubs as
runnable protocols.
