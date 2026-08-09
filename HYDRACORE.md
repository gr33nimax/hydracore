# HydraCore distribution contract

HydraCore is the maintained networking runtime used by HydraBox. The public
native contract is API v2 and identifies the runtime as
`io.hydrabox.hydracore`. Source lineage, licenses, and retained compatibility
identifiers are documented separately in [CREDITS.md](CREDITS.md).

## Release baseline

Release `v1.13.16-extended-hydracore.4` is based on the exact
`sing-box-extended` commit
`545424b86bc4513f90580ebeab2e2d1514089718` (descriptive upstream tag
`v1.13.16-extended-2.6.2`). The full commit, rather than a movable tag, is the
authority. `release/UPSTREAM_BASELINE` pins the toolchain and release build
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
- `sing-box hydra capabilities --json` exposes the same manifest to VPS
  orchestration. Builds with `with_call` report
  `features.call_vk_multi_user=true` and
  `protocols.call_modes=["p2p","multi_user"]`.

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

### Native VK multi-user Calls

Missing `mode` and `mode: "p2p"` retain the inherited one-room/one-peer path.
The new VK-only `mode: "multi_user"` makes the VPS a plain UDP/DTLS endpoint;
it never joins VK and stores no VK account cookies or room-creator credentials.

```json
{
  "type": "call",
  "tag": "vk-call-server",
  "platform": "vk",
  "mode": "multi_user",
  "listen": "0.0.0.0",
  "listen_port": 2443,
  "obfs_password": "group-secret",
  "users": [{"name": "alice", "password": "user-secret", "max_sessions": 1}],
  "max_sessions": 64,
  "max_workers_per_session": 4,
  "max_pending_handshakes": 256,
  "handshake_timeout": "15s",
  "session_idle_timeout": "5m"
}
```

```json
{
  "type": "call",
  "tag": "proxy",
  "platform": "vk",
  "mode": "multi_user",
  "server": "vpn.example.com",
  "server_port": 2443,
  "join_links": ["https://vk.com/call/join/room-a", "https://vk.com/call/join/room-b"],
  "user": "alice",
  "password": "user-secret",
  "obfs_password": "group-secret",
  "workers": 2,
  "worker_connect_timeout": "30s"
}
```

`join_links` contains one through four distinct rooms. `workers` is the total
per-user worker count, defaults to the number of links, and is bounded by both
108 globally and 27 allocations per link. Workers are distributed round-robin
across links. A typical four-link profile uses four workers (one per link); if
VK permits 27 allocations on every room, that topology has room for roughly 27
simultaneous four-worker sessions. This is an operational estimate, not a VK
service guarantee.

The shared outer secret permits O(1) RTP-shaped packet unwrap. Each worker then
performs one bounded user attach inside DTLS; the server uses a username map and
constant-time password-hash comparison. Passwords are not carried in data
packets. One KCP conversation is striped across all live workers, so losing one
allocation does not reset application streams. Hard limits cover users,
sessions, workers, pending handshakes, frame lengths, duplicate active workers,
and timeouts.

The current 512-segment KCP window represents about 512 KiB of data in flight:
the theoretical ceiling is roughly 41 Mbit/s at 100 ms RTT or 20 Mbit/s at
200 ms RTT before TURN, DTLS, obfuscation, and retransmission overhead. Real
throughput is workload- and VK-dependent. The self-signed DTLS identity is
protected by the shared `obfs_password`; treat it as a trusted group secret and
rotate it when membership changes.

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

The supported distribution contains the Android AAR plus
`hydracore-linux-amd64.tar.gz` and `hydracore-linux-arm64.tar.gz`. Each Linux
archive contains a root `sing-box` executable and has a `.sha256` sidecar and
machine-readable provenance. A client or server should pin the trusted
repository/release identity and reject digest disagreement before compilation
or startup.

HydraCore owns native parsing, cryptographic envelope verification, runtime
validation, protocol execution, capability reporting, and runtime telemetry.
Clients own network fetching, trust and rollback policy, user consent, local
overlays and inbounds, key storage, persistence, and activation. Server-side
products own subscription generation and deployment.
