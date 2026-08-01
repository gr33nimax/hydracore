# HydraCore

[![Android libbox](https://github.com/gr33nimax/hydracore/actions/workflows/etonify-libbox.yml/badge.svg?branch=extended-integration)](https://github.com/gr33nimax/hydracore/actions/workflows/etonify-libbox.yml)
[![Static checks](https://github.com/gr33nimax/hydracore/actions/workflows/lint.yml/badge.svg?branch=extended-integration)](https://github.com/gr33nimax/hydracore/actions/workflows/lint.yml)
[![License](https://img.shields.io/badge/license-GPL--3.0--or--later-blue.svg)](LICENSE)

**Maintained networking runtime for the Hydra self-hosted VPN stack.**

HydraCore is the core distribution used by
[HydraBox](https://github.com/gr33nimax/hydrabox). It produces a verified
Android `libbox.aar`, publishes a versioned mobile capability contract, and
keeps the native sing-box configuration as the protocol-schema authority.

HydraCore is not a VPN service and does not manage customers or subscriptions.
In the complete stack, [HYDRA Ultimate](https://github.com/gr33nimax/HYDRA-ULTIMATE)
owns the self-hosted server and subscription layer, HydraBox owns the client
experience, and HydraCore executes the accepted native configuration.

```text
HYDRA Ultimate  ->  encrypted subscription  ->  HydraBox  ->  HydraCore
server/control                                  client        runtime
```

## Supported distribution

- Android-first `libbox.aar` and generated Java bindings.
- Reproducible source archive, checksums, and machine-readable provenance for
  every HydraCore prerelease.
- Versioned `HydraCoreCapabilities()` identity and remote-safety manifest for
  HydraBox Subscription v1.
- Extended sing-box client surface, including WireGuard/AmneziaWG, VLESS,
  VMess, Trojan, Hysteria 2, TUIC, AnyTLS, ShadowTLS, XHTTP, OpenVPN,
  TrustTunnel, MASQUE, MTProxy, Snell, Naive, and the inherited routing/DNS
  stack when their documented build tags are enabled.

The Android AAR is the supported HydraCore deliverable today. Other inherited
sing-box build and packaging targets remain in the source tree, but are not a
Hydra release promise unless they appear in a HydraCore release.

## Releases

Use assets from [HydraCore Releases](https://github.com/gr33nimax/hydracore/releases),
not an arbitrary branch build. A release contains:

- `libbox.aar` and generated sources;
- SHA-256 checksums;
- an attributed source archive;
- `provenance.json` with the source commit and pinned toolchain.

The current compatibility line is recorded in
[`release/HYDRACORE_VERSION`](release/HYDRACORE_VERSION) and the exact upstream
baseline in [`release/ETONIFY_BASELINE`](release/ETONIFY_BASELINE).

## Compatibility policy

The public distribution identity is `io.hydrabox.hydracore`.
`HydraCoreCapabilities()` is the preferred binding. The inherited
`EtonifyCapabilities()` entry point, Go module path, native package names, and
`libbox.aar` filename intentionally remain compatible so existing bindings and
clients are not broken by the public rename.

Remote subscriptions are fail-closed: HydraBox activates a native document only
when the runtime exposes an exact supported capability and remote-safety
contract. See [HYDRACORE.md](HYDRACORE.md) for the contract and
[ROADMAP.md](ROADMAP.md) for planned work.

## Development

The authoritative gates run in GitHub Actions:

- complete Go test suite on Linux;
- targeted race and resource gates;
- WireGuard/libbox configuration checks;
- Android AAR build with pinned Go, gomobile, JDK, NDK, and build tags;
- artifact checksum and provenance verification.

Contributions should target `extended-integration`. Read
[CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request and use
[SECURITY.md](SECURITY.md) for vulnerability reports.

## Lineage and license

HydraCore is an independent derivative of
[Etonify's `etonify-core`](https://github.com/yamixdev/etonify-core/tree/etonify-dev),
which is maintained on top of
[`sing-box-extended`](https://github.com/shtorm-7/sing-box-extended) and
ultimately [`sing-box`](https://github.com/SagerNet/sing-box). HydraCore is not
affiliated with or endorsed by those projects.

The source history and existing notices are retained. See
[ETONIFY_CORE.md](ETONIFY_CORE.md), [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md),
and [LICENSE](LICENSE).
