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

## Redistribution

Redistributors must provide the corresponding source and preserve all license
and copyright notices required by the GPL and applicable third-party licenses.
HydraCore release provenance identifies the exact source commit and toolchain
used for the Android artifact.

## SpaceNeuroX/proxy-turn-vk-android

HydraCore's `protocol/wdtt` package adapts protocol behavior from:

- Project: `SpaceNeuroX/proxy-turn-vk-android`
- Source: https://github.com/SpaceNeuroX/proxy-turn-vk-android
- Reviewed commit: `2dd5d37f18a0475a786a90d69feb7c503e33bdf3`
- Project license: GNU General Public License version 3

The shared Hydra access, control, worker-policy, and WRAP implementation is
consumed from `github.com/gr33nimax/hydra-wdtt`, the Hydra-maintained fork of
that project. This revision pins module commit
`69ca964cfa2f6dd81d86d45b2159ffdb04469c8c`.

The adapted areas include the WDTT application handshake, anonymous VK Calls
TURN credential flow, Pion TURN/DTLS relay layout, packet dispatcher behavior,
and dynamic WireGuard configuration exchange. HydraCore substantially changes
lifecycle, networking, validation, storage, error handling, and product
integration so the transport is a lazy endpoint controlled by encrypted
HydraBox Subscription.

The RTP-style WRAP/obfuscation implementation is derived from upstream files
marked `SPDX-License-Identifier: MIT`; HydraCore retains that SPDX identifier
in `protocol/wdtt/obfs.go`. The remaining adapted WDTT package files are
distributed under HydraCore's GPL-3.0-compatible repository license. The full
GPL license text accompanies this source tree in `LICENSE`.
