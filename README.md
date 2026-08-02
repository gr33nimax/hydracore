# HydraCore

[![HydraCore checks](https://github.com/gr33nimax/hydracore/actions/workflows/hydracore.yml/badge.svg?branch=main)](https://github.com/gr33nimax/hydracore/actions/workflows/hydracore.yml)
[![License](https://img.shields.io/badge/license-GPL--3.0--or--later-blue.svg)](LICENSE)

**Maintained Android networking runtime for the Hydra self-hosted VPN stack.**

HydraCore validates and executes native networking configurations accepted by
[HydraBox](https://github.com/gr33nimax/hydrabox). The supported distribution
is a versioned, provenance-bound Android `libbox.aar`; HydraCore is not a VPN
service, subscription server, or control panel.

```text
HYDRA Ultimate  ->  encrypted subscription  ->  HydraBox  ->  HydraCore
server/control                                  client        runtime
```

## What HydraCore ships

- Android `libbox.aar` and generated Java bindings.
- SHA-256 checksums, generated sources, attributed source archive, and
  machine-readable build provenance for every release.
- `HydraCoreCapabilities()` with an exact versioned remote-safety contract for
  HydraBox Subscription v1.
- WireGuard/AmneziaWG, VLESS, VMess, Trojan, Hysteria 2, TUIC, AnyTLS,
  ShadowTLS, XHTTP, OpenVPN, TrustTunnel, MASQUE, MTProxy, Snell, Naive, and
  the inherited routing/DNS runtime enabled by the published build tags.

Other build targets present in the source tree are not HydraCore release
targets unless a HydraCore release explicitly includes them.

## Releases

Install only artifacts from [HydraCore Releases](https://github.com/gr33nimax/hydracore/releases).
HydraBox pins the release tag, source commit, download URL, and digest and
rejects a mismatched runtime before build or activation.

The public distribution identity is `io.hydrabox.hydracore`. Compatibility
identifiers required by existing native bindings remain stable; they are not
public product names. The exact contract is documented in
[HYDRACORE.md](HYDRACORE.md).

## Development

The authoritative checks run in GitHub Actions: the complete Go suite,
race/resource gates, WireGuard configuration checks, pinned Android AAR build,
and checksum/provenance validation.

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
