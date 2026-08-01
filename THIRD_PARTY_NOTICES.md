# Third-party notices

## SpaceNeuroX/proxy-turn-vk-android

HydraCore's `protocol/wdtt` package adapts protocol behavior from:

- Project: `SpaceNeuroX/proxy-turn-vk-android`
- Source: https://github.com/SpaceNeuroX/proxy-turn-vk-android
- Reviewed commit: `2dd5d37f18a0475a786a90d69feb7c503e33bdf3`
- Project license: GNU General Public License version 3

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
