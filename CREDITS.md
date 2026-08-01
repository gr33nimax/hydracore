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

Original copyright, license, and dependency notice files remain in the source
tree. HydraCore releases include corresponding source and machine-readable
provenance. The exact baseline repository, tag, commit, and toolchain are
recorded in [`release/ETONIFY_BASELINE`](release/ETONIFY_BASELINE); the filename
is retained as historical build provenance.

## Compatibility identifiers

The Go module path, native package names, `libbox.aar` artifact name,
`upstream_project: "etonify-core"`, and deprecated `EtonifyCapabilities()`
binding remain unchanged where renaming would break source, schema, or binary
compatibility. HydraCore's public identity is `io.hydrabox.hydracore`, exposed
through `HydraCoreCapabilities()`.

These retained technical identifiers are attribution and compatibility data,
not public Etonify branding and not a claim of upstream authorship or approval.

## Redistribution

Redistributors must comply with [LICENSE](LICENSE), provide corresponding
source, and preserve notices required by applicable dependency licenses. See
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for the concise notice index.
