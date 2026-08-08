# HydraCore distribution contract

HydraCore is the maintained networking runtime used by HydraBox. The public
native contract is API v2 and identifies the runtime as
`io.hydrabox.hydracore`. Source lineage, licenses, and retained compatibility
identifiers are documented separately in [CREDITS.md](CREDITS.md).

## Release baseline

Release `v1.13.16-extended-hydracore.1` is based on the exact
`sing-box-extended` commit
`da4c532efb1f86a38a324909fc9b8867f811551c` (descriptive upstream tag
`v1.13.16-extended-2.6.1`). The full commit, rather than a movable tag, is the
authority. `release/UPSTREAM_BASELINE` pins the toolchain and Android build
tags, including `with_call`.

`HydraCoreBuildInfo()` exposes the distribution, source commit, exact upstream
baseline, toolchain, build tags, and historical lineage. The release workflow
binds the same values and every artifact digest into `provenance.json`.

## Capability and validation APIs

- `HydraCoreCapabilities()` returns the versioned feature, protocol, runtime,
  subscription, and remote-policy manifest.
- `HydraCoreBuildInfo()` returns build provenance available at runtime.
- `HydraCoreValidateConfig(content, profile)` validates either trusted local
  configuration (`local`) or untrusted remote configuration (`remote_v2`).

Remote policy v2 permits only `$schema`, `inbounds`, `outbounds`, and
`endpoints` at the resource root. It applies strict object typing, unique and
closed references, cycle detection, reserved-tag checks, nesting and size
limits, and native HydraCore validation. Local listeners, DNS, providers,
rule-sets, files, and other host authority are not remotely grantable.

Release builds implement Call inbound and outbound with the `dion`,
`telemost`, `vk`, and `wbstream` platforms. Call objects are accepted only by
a core built with `with_call`; their complete native schema is validated rather
than a second field allowlist. Diagnostics and subscription inspection never
echo credentials, cookies, join links, or resource documents. Rmux and
AmneziaWG v3 are also release capabilities; Amnezia resource limits are
enforced before startup.

## Hydra Subscription v2

The authoritative, client-independent subscription contract lives in
`contract/subscription/`:

- `HYDRA_SUBSCRIPTION_V2.md` defines ownership and processing rules;
- `schema/hydra-subscription-v2.schema.json` defines plaintext v2;
- `schema/hydra-subscription-jwe-v2.schema.json` defines the encrypted
  flattened JWE envelope.

The same files are embedded into the core and published as checksummed release
artifacts. `HydraCoreSubscriptionSchema()`,
`HydraCoreSubscriptionJWESchema()`, and `HydraCoreSubscriptionJWEPolicy()`
expose them to bindings. `HydraCoreValidateSubscription()` and
`HydraCoreInspectSubscription()` perform strict validation and redacted
inspection. The corresponding `...SubscriptionJWE` APIs authenticate and open
`dir`/`A256GCM` envelopes using a 32-byte base64url `hydra-key` value.

Each subscription resource is an independent sing-box graph. Cross-resource
references, undeclared authority, unknown required extensions, incompatible
core requirements, and missing profile entrypoints fail closed. HydraCore does
not fetch subscriptions, store keys, persist profiles, request user consent,
or activate a profile. Those remain client responsibilities. Existing
HydraBox releases are not claimed to support v2 and are intentionally not
changed by this repository refactor.

## Runtime API v1

`CommandClient.GetRuntimeSnapshot()` returns one coherent view of service
lifecycle, process/traffic counters, outbound groups, Clash mode, and managed
URLTest sessions. `CommandRuntimeEvents` provides a typed delta stream whose
first envelope is always a complete `reset` snapshot. Event intervals are
clamped to 250 ms through 30 seconds and updates are coalesced.

Group URL tests use `StartURLTest`, `GetURLTestSession`, `CancelURLTest`, and
`CommandURLTestEvents`. A session has a stable ID, explicit state, progress,
structured per-outbound results, cancellation, and a maximum of 64 retained
completed sessions. Reloading or stopping an instance cancels its active
sessions. The old fire-and-forget command is not part of API v2. Isolated
pre-connect `StandaloneURLTestSession` remains available.

## Artifact and stability boundary

The supported distribution is the Android AAR. A release contains
`libbox.aar`, generated Java sources, the attributed source archive,
subscription contract files, SHA-256 files, and machine-readable provenance.
A client should pin the release tag, source commit, download URL, and digest
and reject disagreement before compilation or startup.

HydraCore owns native parsing, cryptographic envelope verification, runtime
validation, protocol execution, capability reporting, and runtime telemetry.
Clients own network fetching, trust and rollback policy, user consent, local
overlays and inbounds, key storage, persistence, and activation. Server-side
products own subscription generation and deployment.
