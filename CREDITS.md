# Credits and upstream lineage

HydraCore is an independent derivative distribution for the Hydra self-hosted
VPN stack. It is not affiliated with or endorsed by its upstream projects.

## GPL lineage

The repository retains source derived from:

1. [SagerNet/sing-box](https://github.com/SagerNet/sing-box), GPL-3.0-or-later;
2. [shtorm-7/sing-box-extended](https://github.com/shtorm-7/sing-box-extended),
   the active pinned baseline;
3. [yamixdev/etonify-core](https://github.com/yamixdev/etonify-core), the mobile
   integration lineage.

The exact baseline, commit, toolchain, and release build tags are in
[release/UPSTREAM_BASELINE](release/UPSTREAM_BASELINE). Retained module paths,
native package names, and artifact names are compatibility data, not branding
or a claim of upstream approval.

## MIT wrapper code

The VK anonymous-call request sequence in `transport/call/vk/vk_calls_auth.go`
is adapted from the GPL-3.0 implementation in the same SpaceNeuroX repository.

`transport/call/vk-parasite/obfs.go` is adapted from
[SpaceNeuroX/proxy-turn-vk-android](https://github.com/SpaceNeuroX/proxy-turn-vk-android)
at commit `40117047d71f0303504e276b18372c0626b94a35`. That file retains its
SPDX copyright and MIT identifier. Its complete license text is in
[LICENSES/MIT.txt](LICENSES/MIT.txt).

## Redistribution

Redistributors must preserve applicable copyright and license notices and
provide corresponding source as required by GPL-3.0-or-later. See
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for the notice index.
