# Credits and upstream lineage

HydraCore is an independent distribution maintained for the Hydra self-hosted
VPN stack. It is not affiliated with or endorsed by any upstream project.

## Source lineage

The retained Git history and source derive from:

1. [`SagerNet/sing-box`](https://github.com/SagerNet/sing-box), licensed under
   GPL-3.0-or-later;
2. [`shtorm-7/sing-box-extended`](https://github.com/shtorm-7/sing-box-extended),
   which provides the pinned extended runtime baseline;
3. [`yamixdev/etonify-core`](https://github.com/yamixdev/etonify-core), which
   contributed the mobile integration from which HydraCore evolved.

VK anonymous-call request sequencing and the RTP/HKDF wrapper design were
studied from
[`SpaceNeuroX/proxy-turn-vk-android`](https://github.com/SpaceNeuroX/proxy-turn-vk-android)
at commit `40117047d71f0303504e276b18372c0626b94a35`. The authentication flow is
GPL-3.0-compatible; the two wrapper source files used for the native
multi-user transport carry explicit MIT SPDX notices. Exact boundaries are
listed in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

Original copyright, license, and dependency notice files remain in the source
tree. HydraCore releases include corresponding source and machine-readable
provenance. The exact baseline repository, tag, commit, and toolchain are
recorded in [`release/UPSTREAM_BASELINE`](release/UPSTREAM_BASELINE). Current
build metadata names `sing-box-extended` as the active upstream and retains
Etonify only in the historical lineage.

## Compatibility identifiers

The Go module path, native package names, and `libbox.aar` artifact name remain
unchanged where renaming would break inherited source or binary compatibility.
The active Etonify capability alias and provenance fields were removed in the
v2 contract. HydraCore's public identity is `io.hydrabox.hydracore`, exposed
through `HydraCoreCapabilities()`.

These retained technical identifiers are attribution and compatibility data,
not public Etonify branding and not a claim of upstream authorship or approval.

## Redistribution

Redistributors must comply with [LICENSE](LICENSE), provide corresponding
source, and preserve notices required by applicable dependency licenses. See
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for the concise notice index.
