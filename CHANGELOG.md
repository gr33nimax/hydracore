# Changelog

## v1.13.16-extended-hydracore.11-debug.11

- Replaced the shared VK KCP conversation with four independent wire-v4 lanes.
- Added per-flow sequence bonding and bounded reordering above the lanes.
- Consolidated the implementation under `transport/call/vk-parasite` and
  removed the obsolete scheduler profile and wire-v3 contract.

## Unreleased

- Added independent delivery windows for adaptive VK/TURN workers. Clean paths
  grow from a 40-segment starting window, while sustained KCP retry or local
  output-queue pressure backs off only the affected path. KCP admission and
  pending backpressure now follow the bounded aggregate path window, preventing
  all four Calls workers from filling a shared 2048-segment standing queue.
  Added delivery-rate, window, in-flight and backoff telemetry per worker.
- Replaced the obsolete shared wire-v3 VK scheduler with wire-v4 four-lane KCP
  affinity, alternate-path retransmission, ACK/control priority, per-worker
  feedback and explicit legacy A/B fallback. The scheduler uses bounded KCP
  local/remote windows without a single dynamic congestion window spanning all
  TURN paths and without a post-KCP pacer. Added exact per-path attempt/retry
  counters and the `call_vk_four_lane_kcp` capability gate; raw mode is
  unchanged.
- Made same-path physical feedback mandatory for adaptive traffic and removed
  transitional worker-auth compatibility. Mixed old/new APK and VPS cores now
  fail at the v3 handshake instead of silently accepting incompatible data.
- Established a separately versioned `debug` release channel. A push to the
  `debug` branch publishes verified Android and VPS artifacts as a GitHub
  prerelease only after all test and build jobs pass.
- Added operator-gated native VK multi-user server/client telemetry compatible
  with Hydra Ultimate's schema-v1 JSONL ingestion contract.
- Instrumented VK/TURN/DTLS/inner-auth stages, authenticated outer loss and
  jitter, KCP RTT/retransmission/backpressure, worker/peer/relay queues,
  network handover and runtime pressure.
- Client records travel through bounded, rate-limited control frames inside
  the existing authenticated KCP/DTLS/TURN path; no new listener or
  client-controlled identity is introduced.

## v1.13.16-extended-hydracore.8

- Exposed the authenticated inbound user in Clash `/connections` metadata so
  HYDRA Ultimate can attribute Hydra VK Tunnel traffic per user.

## v1.13.16-extended-hydracore.7

- Split release capabilities and artifacts into a client Android runtime and
  a VPS Linux runtime while keeping one versioned source tree. Release roles
  expose only VK Calls `vk_parasite`; the combined developer tag retains legacy
  compatibility outside the product release surface.
- Added Calls wire v2 worker epochs and immediate network rebind so Wi-Fi/mobile
  handover replaces stale DTLS/TURN workers without discarding the logical KCP
  session. The VPS accepts wire v1 and v2 for one transition release.
- Reused the obfuscation read buffer instead of allocating a maximum-size
  packet buffer for every datagram.
- Made stable publication an explicit verified workflow action. Ordinary main
  pushes now produce CI artifacts without silently replacing `latest`.

## v1.13.16-extended-hydracore.6

- Fixed Hydra Subscription v2 plaintext/JWE validation so a `with_call` build
  accepts the advertised `call_vk_parasite` required feature alongside
  `call`; builds without Calls and unknown requirements remain fail-closed.

## v1.13.16-extended-hydracore.5

- Fixed targeted managed URLTest sessions so an outbound group probes its
  concrete `Now()` leaf while result events retain the requested group tag.
  Direct targets and concrete-leaf URLTest history remain unchanged.

## v1.13.16-extended-hydracore.4

- Updated the exact `sing-box-extended` baseline to 2.6.2 commit
  `545424b86bc4513f90580ebeab2e2d1514089718`.
- Added native VK Calls `vk_parasite` with O(1) per-user authentication,
  bounded sessions/handshakes, cached VK TURN credentials, one KCP session
  striped across reconnecting DTLS workers, and up to four distinct room
  links.
- Added `sing-box hydra capabilities --json` and the canonical
  `features.call_vk_parasite` / `protocols.call_modes` contract.
- Added reproducible, checksummed Linux `amd64` and `arm64` release archives
  for VPS deployment alongside the Android AAR.

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
