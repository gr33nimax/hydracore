# HydraCore

HydraCore is the Android and Linux runtime for the Hydra self-hosted VPN stack.
It is a GPL-3.0-or-later derivative of sing-box-extended and ships a verified
Android `libbox.aar` plus Linux `sing-box` archives. It is not a VPN service or
control plane.

## Release artifacts

Releases contain the Android AAR and sources, three Android shared libraries,
two Linux archives (`amd64`, `arm64`), and a signed bundle manifest. Install
only assets from [GitHub Releases](https://github.com/gr33nimax/hydracore/releases).

The `debug` branch publishes prereleases; `main` publishes stable candidates.
Client and VPS artifacts must come from the same release and source commit.

```bash
sing-box hydra capabilities --json
```

The public distribution identity is `io.hydrabox.hydracore`. Compatibility
module and binary names retained from upstream are not product branding.

## Main components

- `vk_parasite` Call transport over VK/TURN/DTLS with an inner QUIC relay;
- HydraCore API v2 capabilities, validation, runtime state, and URL-test APIs;
- Hydra Subscription v2 schemas and validation;
- the upstream networking runtime enabled by the release build tags.

`vk_parasite` is documented in [HYDRACORE.md](HYDRACORE.md). The Subscription
contract is in [contract/subscription/HYDRA_SUBSCRIPTION_V2.md](contract/subscription/HYDRA_SUBSCRIPTION_V2.md).

## Development

CI in [`.github/workflows/hydracore.yml`](.github/workflows/hydracore.yml) is
the release authority. It runs the Go, role, race, baseline, Android, Linux,
checksum, and provenance checks. Do not build release artifacts locally.

Contributions target `main`; see [CONTRIBUTING.md](CONTRIBUTING.md). Security
reports follow [SECURITY.md](SECURITY.md).

## License and notices

HydraCore is GPL-3.0-or-later. Source lineage, copyright notices, and the
MIT-licensed wrapper files are recorded in [CREDITS.md](CREDITS.md) and
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md). See [LICENSE](LICENSE) and
[LICENSES/MIT.txt](LICENSES/MIT.txt).
