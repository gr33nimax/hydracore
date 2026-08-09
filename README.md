# HydraCore

[![HydraCore checks](https://github.com/gr33nimax/hydracore/actions/workflows/hydracore.yml/badge.svg?branch=main)](https://github.com/gr33nimax/hydracore/actions/workflows/hydracore.yml)
[![License](https://img.shields.io/badge/license-GPL--3.0--or--later-blue.svg)](LICENSE)

**Maintained Android and Linux networking runtime for the Hydra self-hosted VPN stack.**

HydraCore validates and executes native networking configurations accepted by
[HydraBox](https://github.com/gr33nimax/hydrabox) and the native Calls endpoint
managed by HYDRA Ultimate. Supported releases contain a provenance-bound
Android `libbox.aar` and Linux `sing-box` runtimes; HydraCore is not a VPN
service, subscription server, or control panel.

```text
HYDRA Ultimate  ->  encrypted subscription  ->  HydraBox  ->  HydraCore AAR
       |                                                client runtime
       +---------------- native Calls ----------------> HydraCore Linux
```

## What HydraCore ships

- Android `libbox.aar` and generated Java bindings.
- Reproducible Linux `amd64` and `arm64` archives whose root executable is
  named `sing-box`.
- SHA-256 checksums, generated sources, attributed source archive, and
  machine-readable build provenance for every release.
- HydraCore API v2: capability/build manifests, strict local and remote
  validation, runtime snapshots/events, and managed URLTest sessions.
- Hydra Subscription v2 plaintext and flattened JWE schemas, validation,
  redacted inspection, authenticated opening, and checksummed contract files.
- WireGuard/AmneziaWG, VLESS, VMess, Trojan, Hysteria 2, TUIC, AnyTLS,
  ShadowTLS, XHTTP, OpenVPN, TrustTunnel, MASQUE, MTProxy, Snell, Naive, Call
  inbound/outbound (`dion`, `telemost`, `vk`, `wbstream`), and
  the inherited routing/DNS runtime enabled by the published build tags.
- Native VK Calls `multi_user`: O(1) user authentication, a bounded pool of up
  to four VK room links, and one reliable KCP session striped across dynamic
  TURN/DTLS workers.

Other build targets present in the source tree are not HydraCore release
targets unless a HydraCore release explicitly includes them.

## Releases

Install only artifacts from [HydraCore Releases](https://github.com/gr33nimax/hydracore/releases).
HydraBox and HYDRA Ultimate pin the release/source identity and verify artifact
digests before build or activation. The Linux capability probe is
`sing-box hydra capabilities --json`.

The public distribution identity is `io.hydrabox.hydracore`. Compatibility
identifiers required by existing native bindings remain stable; they are not
public product names. The runtime and subscription contracts are documented in
[HYDRACORE.md](HYDRACORE.md) and
[contract/subscription/HYDRA_SUBSCRIPTION_V2.md](contract/subscription/HYDRA_SUBSCRIPTION_V2.md).

## Development

The authoritative checks run in GitHub Actions: the complete Go suite,
race/resource gates, WireGuard configuration checks, pinned Android AAR and
Linux builds, and checksum/provenance validation.

Contributions target `main`. Read [CONTRIBUTING.md](CONTRIBUTING.md) and report
security issues according to [SECURITY.md](SECURITY.md).

## Credits and license

HydraCore evolved from
[`yamixdev/etonify-core`](https://github.com/yamixdev/etonify-core), the mobile
runtime integration associated with
[Etonify](https://github.com/yamixdev/Etonify) by MeowTeam. We are grateful to
the team and all upstream contributors whose work provided this foundation.
HydraCore preserves the complete source history, copyright notices, and
corresponding source required by its GPL-3.0-or-later lineage. Project lineage,
pinned baselines, retained compatibility identifiers, and non-affiliation
notices are recorded in [CREDITS.md](CREDITS.md),
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md), and [LICENSE](LICENSE).
