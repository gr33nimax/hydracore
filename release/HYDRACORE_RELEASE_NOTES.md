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

## WDTT subscription endpoint

- Adds the lazy `wdtt` endpoint behind the `with_wdtt` build tag.
- Uses anonymous VK Calls TURN relays and the WDTT DTLS/WRAP protocol while
  keeping external DNS, HTTPS, and UDP inside HydraCore's protected dialer.
- Exposes remote policy v2 and bounded WDTT capability metadata to HydraBox.
- Accepts WDTT only from a Hydra subscription endpoint with an opaque
  `credential_ref`; direct links and raw sing-box imports are unsupported.
- Uses persistent device grants for offline cold start and 15-minute leases
  refreshed after 10 minutes through the established VK-relayed transport.
- Warms a parallel lease generation, waits for 9 server-acknowledged workers,
  switches traffic atomically, and drains the previous generation without a
  VPN restart.
- Uses anonymous VK Calls authentication first and accepts process-local,
  short-lived account TURN credentials only for the captcha fallback.
- Requires at least 9 workers and recommends 18 workers per user.
- Receives the stable subscription device identity from HydraBox; legacy WDTT
  password profiles retain the existing HydraCore cache behavior.
- Starts no TURN allocations until a WDTT profile is actually selected or used.

## HydraCore revision 6

- Adds cancellable one-shot URLTest sessions for a concrete outbound without
  TUN, local inbounds, or background group probes.
- Publishes structured latency, status, timing, and error results through the
  mobile binding and advertises `supports_preconnect_url_test`.
- Guarantees session cleanup across success, failure, timeout, cancellation,
  network changes, and application lifecycle transitions.
