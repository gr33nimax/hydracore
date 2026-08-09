# HydraCore v1.13.16-extended-hydracore.3

This prerelease fixes VK Call connectivity on Android VPN mode. The VK SFU
WebTransport socket is now created through the configured sing-box outbound
dialer, so Android can protect it from HydraBox's own TUN. The protected UDP
socket is also closed with its QUIC connection.

The release retains the HydraCore API v2 and Subscription v2 contracts from
`.2` and the same `sing-box-extended` baseline.

## Artifacts

The release contains the AAR, generated Java sources, attributed source,
subscription contract files, SHA-256 files, and schema-v3 provenance.

<!-- Previous release notes retained below for contract history. -->

# HydraCore v1.13.16-extended-hydracore.2

This prerelease updates the Android runtime to the exact
`sing-box-extended` commit
`da4c532efb1f86a38a324909fc9b8867f811551c` from the 2.6.1 line.

## Contract changes

- Introduces HydraCore API v2 and removes the former active capability alias
  and product-specific provenance fields while preserving historical credits.
- Ships Hydra Subscription v2 plaintext and flattened JWE schemas as embedded,
  checksummed release artifacts.
- Adds strict remote-policy v2 validation, independent resource graphs,
  permissions, profiles, versioned requirements, redacted inspection, and
  authenticated `dir`/`A256GCM` JWE opening.
- Replaces fire-and-forget group URL tests with start/get/cancel sessions and a
  bounded event stream. Adds coherent runtime snapshots and coalesced typed
  runtime events.

## Protocol and safety changes

- VK Call joiners now prefer the anonymous VK Calls API flow used by current
  clients, with the former `calls.getAnonymousToken` path retained as a
  Smart Captcha-capable fallback.
- Release builds include Call inbound and outbound for `dion`, `telemost`,
  `vk`, and `wbstream`, together with Rmux and AmneziaWG v3.
- Adds Amnezia key, padding, timing, handshake-attempt, and range guards.
- Carries forward upstream AnyTLS, XHTTP, QUIC, VLESS, and other 2.6.1 changes
  while preserving HydraCore-specific synchronization and safety fixes.

## Artifacts

The release contains the AAR, generated Java sources, attributed source,
subscription contract files, SHA-256 files, and schema-v3 provenance. HydraBox
is intentionally not modified by this core release; existing client releases
must not be assumed to understand Subscription v2 or the API-v2 binding break.
