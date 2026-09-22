# Building and releasing HydraCore

## Local checks

```bash
go build ./...
go test ./...
go vet ./...
bash release/verify_upstream_baseline.sh   # pins: Go/NDK/JDK/gomobile + libbox build tags
make lint
```

Role-specific builds use tags (`include/call*.go`):

```bash
go build -tags with_call_client ./...   # client core (HydraBox AAR path)
go build -tags with_call_server ./...   # VPS core
```

The AAR itself (gomobile), the Linux archives and the signed manifest are produced **only
by CI** (`.github/workflows/hydracore.yml`) — a reproducible local AAR build is not part of
the workflow.

## Pinned toolchain and baseline

Everything the build depends on is pinned in `release/UPSTREAM_BASELINE` and checked as the
first step of every CI job (`release/verify_upstream_baseline.sh`):

| Key | Meaning |
| --- | --- |
| `UPSTREAM_REPOSITORY` / `UPSTREAM_BRANCH` | the sing-box-extended source and branch |
| `UPSTREAM_COMMIT` / `UPSTREAM_TAG` | the exact baseline commit (authoritative) and its descriptive tag |
| `GO_VERSION`, `GOMOBILE_VERSION`, `ANDROID_NDK_VERSION`, `JAVA_VERSION` | toolchain pins |
| `LIBBOX_ANDROID_API`, `LIBBOX_BUILD_TAGS` | libbox surface |

`experimental/libbox/hydracore_build_info.go` stamps the same baseline into the AAR;
`TestHydraCoreBuildInfoMatchesReleaseBaseline` fails the build if the two drift apart, so
an upgrade must move both together.

## The tag contract

`release/verify_release_version.sh` enforces exactly three forms, tied to their branch:

| Source | Tag | Release kind |
| --- | --- | --- |
| `debug` ordinary prerelease | `hydracore-sbe-<sbe>-debug-<n>` | prerelease |
| `debug` frozen candidate | `hydracore-sbe-<sbe>-rc-<n>` | prerelease |
| `main` stable | `hydracore-sbe-<sbe>` | latest, non-prerelease |

`<sbe>` is the selected sing-box-extended version; `<n>` is a monotonic counter that
restarts at 1 on each new SBE baseline. Debug pruning keeps a rolling window of the ten
most recent current-contract debug prereleases and never deletes stable releases or
release candidates.

## Publishing (manual gate)

A release is cut through `workflow_dispatch` with `publish=true` on `main` or `debug`
(`.github/workflows/hydracore.yml`). The job:

1. re-verifies the version against the source ref;
2. downloads the CI-built libbox AAR and Linux runtimes;
3. checks identity/role/capability and runs the config smoke test;
4. builds and Ed25519-signs the bundle manifest (`HYDRACORE_ED25519_PRIVATE_KEY`,
   `HYDRACORE_BUNDLE_KEY_ID`);
5. publishes the exact expected asset inventory — a `debug` cut as prerelease, a `main` cut
   as latest.

A red build never publishes and never advances the baseline.

## Automated SBE upgrades

New upstream releases are detected by cron and prepared as a reviewable merge request into
`debug`; a published core then triggers the matching HydraBox channel build. The mechanism,
the `HYDRABOX_DISPATCH_PAT` secret, and what stays manual are in
[../release/AUTOMATION.md](../release/AUTOMATION.md).

The upgrade merge never rewrites fork-owned layers: `release/upgrade_sbe.sh` restores every
path listed in [../release/FORK_OWNED_PATHS](../release/FORK_OWNED_PATHS) to the fork's
pre-merge state and keeps retired files deleted, so only genuinely shared files ever reach a
manual conflict.

Manual dry runs:

```bash
bash release/sbe_watch.sh                       # detect a newer SBE (read-only)
bash release/sbe_watch_test.sh                  # test the tag-selection rule
bash release/upgrade_sbe.sh v1.14.1-extended-2.7.2   # merge + commit locally (no push)
```

## Release artifacts

Install only from
[GitHub Releases](https://github.com/gr33nimax/hydracore/releases): the client AAR (+
sources jar), three Android shared libraries, `amd64`/`arm64` Linux archives, and the
signed bundle manifest. Take client and VPS artifacts from the **same** release.
