# HydraCore 1.14.1 release notes

HydraCore is the network runtime behind Hydra: the `vk_parasite` transport that carries
QUIC inside VK call datagrams, plus the Android runtime the client application loads. The
VPS side is a single `sing-box` binary; the client side is a libbox AAR. This release moves
the core onto sing-box-extended `v1.14.0-extended-2.7.1` and is the first stable release
published under the readable tag contract.

## Compatibility — read before upgrading

- **Transport is wire v10.** The DTLS layer that sat under the RTP-shaped wrapper is gone:
  every byte of it travelled inside an already-sealed payload, while costing 37 bytes and
  an AES-GCM pass per packet in each direction. Worker authentication is now the first
  stream of each QUIC connection, and the VPS serves every worker from one QUIC listener
  on the shared UDP socket, telling connections apart by QUIC connection ID.
  A client older than protocol 10 cannot talk to a protocol 10 VPS at all.
- **Client and VPS come from one release.** Take both sides from the same release manifest
  and the same source commit; mixed wire versions are refused during worker authentication.
- **Client ABI is 2.** The core accepts the complete AmneziaWG 3.1 configuration, including
  `random_trailers` and `disable_cookies`. An application built against ABI 1 refuses to run
  with this core instead of failing to parse a 3.1 profile at tunnel start.
- **Runtime identity is unchanged:** `io.hydrabox.hydracore`, contract version 1, `vps` role,
  `vk_parasite` mode. The core answers both the product contract and the capability document
  an older HYDRA reads before switching kernels, so a server whose core and updater sit a
  release apart can move in either direction.

## What this release carries

- `vk_parasite` over four required VK/TURN paths, four workers per path by default and up to
  twenty in multiples of four, with generation-scoped recovery across network changes.
- Transport health as part of the typed runtime stream: reports carry the outbound tag and
  runtime generation, and the TURN edge a transport last reached is readable as an
  attribution instead of whichever server was announced last.
- Client-facing behaviour behind capability flags, so an older client paired with this core
  keeps its own defaults: a DoH resolver keeps its query string, the automatic `urltest`
  group honours the client's probe timeout and concurrency, and a failed probe is retried
  sooner than the general interval instead of being reported unreachable for the whole wait.
- A switchable log factory: the active level can be turned off and built again at runtime.

## Tag contract

Release names now name the sing-box-extended baseline and a counter inside a channel:
`hydracore-sbe-<sbe-version>` for a stable release, `-debug-<n>` for an ordinary debug
prerelease, and `-rc-<n>` for a frozen release candidate. The counters start at `1` in this
contract. Older `v1.14.0-extended-2.7.1-hydracore.<cycle>-debug.<n>` tags are neither
renumbered nor renamed, and only ordinary `-debug-<n>` prereleases take part in the debug
channel's ten-release rolling window, so a release candidate or a legacy tag is never
trimmed as new debug builds arrive.

## Assets

CI verifies every release before publication. Each release carries the Android AAR and its
sources, three Android shared libraries, the Linux `amd64` and `arm64` archives, and a signed
bundle manifest. Install artifacts from GitHub Releases only: a VPS takes the `sing-box`
binary, a client takes the AAR and the shared libraries.

## Upgrade and rollback

Upgrading means replacing the core binary or the client artifacts; no configuration change is
required for a deployment that already runs `vk_parasite`. The named rollback target for this
release is `v1.14.0-extended-2.7.1-hydracore.12-debug.11`, which stays published and carries
the same runtime code as the release candidate this stable release was frozen from.

History for earlier builds lives in `CHANGELOG.md`; this file describes the current release only.

## Fixes since the previous build

- **WireGuard no longer takes the core down with it.** An element the encryption routine
  dropped — the buffer it asked for was larger than the pool could hand out, and the
  random trailer decides when that happens — kept its place in the batch with no packet
  left. The sender then sliced it for a transport header and panicked with
  `slice bounds out of range [8:0]`, which ended the whole core process: a tunnel that
  starts and immediately dies, once in a while, more often on the first attempt after the
  service is created. The sender now asks a single helper whether there is a body to send,
  and that decision has its own test.
