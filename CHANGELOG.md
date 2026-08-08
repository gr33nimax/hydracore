# Changelog

## Unreleased

- Moved the stable product line to `main` and reduced the public branch surface.
- Removed the inherited upstream documentation site and unsupported packaging
  workflows from the HydraCore repository.
- Consolidated lineage, license, and compatibility attribution in `CREDITS.md`
  and `THIRD_PARTY_NOTICES.md`.
- Established `hydracore` as the canonical repository and public distribution
  name while retaining upstream module, ABI, and capability aliases.
- Reframed HydraCore as the maintained Android runtime for the Hydra
  self-hosted VPN stack.
- Added project roadmap, contribution, security, third-party, and issue-report
  documentation.
- Moved WDTT work out of the stable line and preserved it in the
  `wdtt-archive` branch for possible future research.

## v1.13.16-extended-hydracore.1

- Updated the exact `sing-box-extended` baseline to commit
  `da4c532efb1f86a38a324909fc9b8867f811551c` from the 2.6.1 line.
- Introduced HydraCore API v2, remote policy v2, build metadata, runtime
  snapshot/events, and managed URLTest sessions.
- Added the core-owned Hydra Subscription v2 plaintext/JWE contract, strict
  validation, redacted inspection, and authenticated JWE opening.
- Enabled release-tagged Call inbound/outbound for `dion`, `telemost`, `vk`,
  and `wbstream`, plus Rmux and bounded AmneziaWG v3 validation.
- Removed active legacy capability and provenance surfaces while retaining
  explicit historical attribution and source lineage.

## v1.13.14-extended-hydracore.5

- Published the standalone HydraCore repository structure on `main`.
- Retained complete GPL/source lineage while removing inherited product-facing
  Etonify and sing-box documentation surfaces.
- Preserved the stable runtime behavior and deferred WDTT archive.

## v1.13.14-extended-hydracore.4

- Published the canonical HydraCore repository identity and project contract.
- Removed the deferred WDTT experiment from the stable source and capability
  line while preserving it in `wdtt-archive`.
- Retained the `.2` runtime behavior and compatibility identity on the pinned
  upstream baseline.

## v1.13.14-extended-hydracore.2

- Published a provenance-bound Android `libbox.aar` from the pinned
  `sing-box-extended` and Etonify mobile-integration baseline.
- Added `HydraCoreCapabilities()` and the `io.hydrabox.hydracore` distribution
  identity while retaining `EtonifyCapabilities()` for compatibility.
- Added the exact remote-safety policy consumed by HydraBox Subscription v1.
