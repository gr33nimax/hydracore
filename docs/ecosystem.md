# HydraCore in the Hydra ecosystem

HydraCore is one of three independently released products. It is the engine; it never runs
as a service or ships as an app on its own.

```text
   sing-box-extended (upstream)
            │ merge-based baseline (release/UPSTREAM_BASELINE)
            ▼
        HydraCore ──────────────┬───────────────────────────┐
   io.hydrabox.hydracore        │                           │
            │ libbox.aar (client)│      sing-box binary (vps)│
            ▼                    ▼                           ▼
        HydraBox (Android)   HYDRA-ULTIMATE (VPS orchestrator)
```

Each product releases on its own cadence; a deployment must use client and VPS artifacts
from **one** HydraCore release (mixed wire versions are refused at worker authentication).

## How HYDRA-ULTIMATE consumes the VPS core

HYDRA-ULTIMATE installs the Linux `sing-box` binary from a HydraCore release and, before
switching to it, verifies the runtime contract:

```bash
sing-box hydra contract --json
# {"contract_version":1,"core_id":"io.hydrabox.hydracore","role":"vps","calls_mode":"vk_parasite"}
```

A HYDRA older than the product contract instead reads the legacy capability document
(`sing-box hydra capabilities --json`), which the core still answers so an update can reach
a machine whose orchestrator has not been updated yet. Both are defined in
`common/hydracore/` (`contract.go`, `legacy_capabilities.go`) and surfaced by
`cmd/sing-box/cmd_hydra.go`.

The core's identity is fixed: `io.hydrabox.hydracore`, contract version 1, role `vps`,
mode `vk_parasite`.

## How HydraBox consumes the client core

HydraBox does not build the core. It pins a published HydraCore release and hydrates the
verified AAR:

- the pinned core commit is a git submodule gitlink;
- `platform/android/libs/libbox.provenance.json` records the release tag, source commit,
  upstream commit and the AAR's SHA-256;
- `hydrate_libbox.py` downloads the exact AAR named by that provenance and refuses a
  mismatched digest;
- `verifyLibboxProvenance` (Gradle) checks the submodule HEAD, the provenance and the AAR
  agree before a build.

The client core is built with `with_call_client`; the VPS core with `with_call_server`
(`include/call*.go`). The client is always the joiner (see
[architecture.md](architecture.md#client--server-roles)).

## Release channels and the supply chain

HydraCore publishes from two branches (`.github/workflows/hydracore.yml`):

| Branch | Tag form | GitHub release |
| --- | --- | --- |
| `debug` | `hydracore-sbe-<sbe>-debug-<n>` or `-rc-<n>` | prerelease |
| `main` | `hydracore-sbe-<sbe>` | latest, non-prerelease |

Publishing is manual (`workflow_dispatch` with `publish=true`); the tag contract and the
Ed25519-signed bundle manifest gate it. A published core then drives the matching HydraBox
channel automatically:

- core `debug` (`-debug-<n>` / `-rc-<n>`) → HydraBox `canary`
- core `main` (`hydracore-sbe-<sbe>`) → HydraBox `stable`

New upstream sing-box-extended releases are picked up by cron and turned into a reviewable
merge request into `debug`. The full mechanism, the one-time `HYDRABOX_DISPATCH_PAT`
secret, and what stays manual are documented in
[../release/AUTOMATION.md](../release/AUTOMATION.md). What upstream layers the fork owns
outright (and an SBE merge never touches) is listed in
[../release/FORK_OWNED_PATHS](../release/FORK_OWNED_PATHS).

## Where the truth lives

| Fact | Source |
| --- | --- |
| Runtime identity, role, mode | `common/hydracore/contract.go` |
| Pinned upstream commit, Go/NDK/JDK, libbox build tags | `release/UPSTREAM_BASELINE` |
| Core version in the native string | `release/HYDRACORE_VERSION` |
| Build-info stamped into the AAR | `experimental/libbox/hydracore_build_info.go` |
| Fork-owned layers (never merged from upstream) | `release/FORK_OWNED_PATHS` |
