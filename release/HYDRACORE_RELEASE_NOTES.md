# HydraCore verified Android runtime

This release contains the provenance-bound Android runtime used by HydraBox.

## Artifact contract

- `libbox.aar` and generated Java sources;
- SHA-256 checksum and machine-readable build provenance;
- attributed source archive bound to the published commit and pinned toolchain;
- `HydraCoreCapabilities()` with `core_id: io.hydrabox.hydracore`.

Compatibility identifiers required by existing bindings remain unchanged and
are documented in `CREDITS.md`. They do not alter the public HydraCore product
identity.

## HydraCore revision 5

- Uses `main` as the single stable development and release branch.
- Removes the inherited documentation site and unsupported upstream packaging
  workflows from the HydraCore product repository.
- Consolidates source lineage, licenses, and retained compatibility identifiers
  into a compact credits and third-party notice set.
- Keeps the deferred WDTT experiment outside the stable runtime in the
  `wdtt-archive` branch.
