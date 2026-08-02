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

## HydraCore revision 6

- Adds cancellable one-shot URLTest sessions for a concrete outbound without
  TUN, local inbounds, or background group probes.
- Publishes structured latency, status, timing, and error results through the
  mobile binding and advertises `supports_preconnect_url_test`.
- Guarantees session cleanup across success, failure, timeout, cancellation,
  network changes, and application lifecycle transitions.
