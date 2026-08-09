# Third-party notices

HydraCore is an independent derivative distribution. This file summarizes
major lineage; dependency-specific licenses and copyright notices in the source
tree remain authoritative.

## Mobile integration lineage

HydraCore retains mobile integration derived from
[`yamixdev/etonify-core`](https://github.com/yamixdev/etonify-core). The exact
lineage and compatibility identifiers are documented in
[CREDITS.md](CREDITS.md). HydraCore is not an official release of that project.

## sing-box-extended

The pinned extended baseline comes from
[`shtorm-7/sing-box-extended`](https://github.com/shtorm-7/sing-box-extended).
Extended protocol implementations and their dependency notices remain subject
to their respective licenses and copyrights. HydraCore is not an official
sing-box-extended release.

## sing-box

The underlying project is [`SagerNet/sing-box`](https://github.com/SagerNet/sing-box),
licensed under GPL-3.0-or-later. Original copyright and license files are
retained. HydraCore is not affiliated with or endorsed by SagerNet.

## qWDTT VK Calls flow

The anonymous VK Calls authentication request sequence in
`transport/call/vk/vk_calls_auth.go` is adapted from
[`SpaceNeuroX/proxy-turn-vk-android`](https://github.com/SpaceNeuroX/proxy-turn-vk-android),
licensed under GPL-3.0. HydraCore's implementation keeps its existing dialer
and joiner architecture and retains the former VK authentication path as a
fallback.

The RTP-shaped ChaCha20-Poly1305 wrapper and HKDF construction in
`transport/call/multiuser/obfs.go` are adapted from the separately MIT-licensed
`go_client/obfs.go` and `go_client/wrap.go` files at exact qWDTT commit
`40117047d71f0303504e276b18372c0626b94a35`. Their SPDX copyright and MIT
license notice are retained in the adapted source. The surrounding multi-user
session/authentication/KCP implementation is HydraCore code and does not copy
qWDTT's O(N) password-scanning server design.

## Redistribution

Redistributors must provide the corresponding source and preserve all license
and copyright notices required by the GPL and applicable third-party licenses.
HydraCore release provenance identifies the exact source commit and toolchain
used for the Android and Linux artifacts.
