# HydraCore v1.13.16-extended-hydracore.10-debug.6

This is the verified `debug` channel build. It is published automatically only
after the full Go, Android and Linux workflow succeeds and is intended for
HYDRA ULTIMATE installations that explicitly select the Hydracore `debug`
channel. It is not promoted to the stable `latest` release.

This revision fixes the remaining adaptive VK throughput bottleneck found by a
full 1440p field run. Debug.5 removed the post-KCP worker pacer, but still
enabled the standard dynamic KCP congestion window. One logical KCP stream is
carried by four independent TURN paths, so ordinary cross-path reordering or a
loss on one path repeatedly reduced the aggregate congestion window and filled
`WaitSnd` even while VPS CPU, UDP ingress and socket buffers were idle.

Adaptive now uses KCP's bounded local and advertised remote windows without a
single dynamic congestion window spanning every TURN path. It retains the
adaptive-only 16-packet/16-ms chunk affinity, control priority, fast-resend=4,
actual socket-write RTT, and retransmission path switching. No packet is paced
after KCP starts its timer. This does not turn adaptive into packet-striped
legacy: the exact legacy scheduler and raw mode are unchanged.

Native telemetry distinguishes authenticated outer network loss from KCP retry
pressure and records output-queue delay/late writes. It now also publishes an
exact per-worker attempt counter so analysis can report cumulative failed-path
attempts instead of treating a short-lived retry EWMA as a loss ratio. Path RTT
begins at the socket write attempt rather than scheduler enqueue. The old
`worker_path_loss_ratio` is retained only as a compatibility alias for retry
pressure. `features.call_vk_adaptive_multipath=true` remains the explicit
client/VPS/subscription compatibility gate.

This revision makes the telemetry dataset safe for multi-tester protocol
analysis: process, authenticated session, and individual worker snapshots are
separated; TURN choice is exposed only as a non-secret ordinal; control and
record loss are counted; and a 120-second lease protects RTT/loss baselines
from ordinary control delays. Empty sessions no longer receive control
traffic that can keep them alive. The transient `/run` source uses atomic,
session-scoped handoff parts and reports its rotations; Hydra Ultimate drains
each old inode before deletion and retains the complete compressed timeline.

This release adds native, operator-gated VK Calls telemetry compatible with
Hydra Ultimate. It retains the `.9` XHTTP handover and RuntimeEvents fixes,
authenticated per-user traffic attribution, role-specific client/VPS
artifacts, and the exact `sing-box-extended` 2.6.2 baseline
(`545424b86bc4513f90580ebeab2e2d1514089718`).

## Native VK Calls telemetry

- The VPS follows Hydra Ultimate's active telemetry session and appends the
  schema-v1 native JSONL stream only while recording is enabled; Hydracore has
  no experiment-duration timer. A process restart resumes the same session
  stream, while a genuinely new session rotates it atomically.
- Authenticated clients receive short fail-safe collection leases and return
  bounded records through reserved KCP control frames inside the existing
  TURN/DTLS path. No extra listener or client-controlled identity exists.
- Metrics cover VK API stages, TURN, DTLS and inner auth, authenticated outer
  loss/jitter, KCP RTT/retransmission/backpressure, workers, peer/relay queues,
  network handover, CPU, RSS, Go runtime and best-effort thermal pressure.
- `sing-box hydra capabilities --json` reports
  `features.call_vk_telemetry=true` in both client and VPS roles.

## XHTTP network handover

- An interface update now resets XHTTP's active streams and physical Xmux
  clients without permanently closing the reusable transport object.
- Dials racing with a network reset are rejected by generation, while the next
  dial lazily creates a transport bound to the new interface.
- Terminal service shutdown still closes XHTTP permanently. Other V2Ray
  transports keep their existing interface-update behavior.

## Runtime traffic telemetry

- RuntimeEvents now derives upload and download bytes per second from both
  cumulative counters and the actual observation interval instead of emitting
  permanent zero rates.

## Per-user traffic attribution

- Clash `/connections` metadata now includes the authenticated inbound user.
- HYDRA Ultimate can therefore attribute Hydra VK Tunnel upload and download
  counters to each managed user instead of leaving this protocol unassigned.

## Subscription feature contract

- A JWE or plaintext Hydra Subscription v2 document may require `call`,
  `call_vk_multi_user`, and `call_vk_adaptive_multipath` when the release
  advertises those capabilities.
- Builds without a Calls role continue to reject both requirements. Unknown
  feature names remain fail-closed.
- Regression coverage follows the same encrypted JWE validation path used by
  HydraBox subscription import.

## Managed URLTest

- A targeted group probes the concrete leaf selected by `Now()` while emitting
  the managed result under the originally requested group tag.
- Direct targets keep their existing result tag. Concrete URLTest history stays
  attached to the probed leaf so group health and selection remain accurate.

## Native VK Calls

- `mode: "multi_user"` hosts many independent authenticated users on one
  native UDP Calls inbound. Release artifacts do not expose legacy P2P mode.
- A shared RTP-shaped ChaCha20-Poly1305 layer makes packet unwrap O(1). User
  lookup is O(1), the password hash comparison is constant-time, and attach
  credentials are sent once inside DTLS instead of in every data packet.
- Clients can use one through four distinct VK join links and a total bounded
  worker pool distributed round-robin across them. VK TURN credentials are
  cached/singleflighted and all usable UDP relay URLs are rotated.
- One KCP conversation uses either the exact legacy packet striping or the
  adaptive chunk-affine scheduler across live workers. Authenticated heartbeat
  records evict dead TURN/DTLS paths without consuming user quota forever;
  worker loss/reconnect preserves the session. If server KCP state was reset,
  generation checks rebuild the native session behind the persistent relay.
- Users, sessions, per-user sessions, workers, pending handshakes, frame
  lengths, duplicate active workers, handshakes, reconnects, and idle state
  all have explicit hard bounds.
- Wire v2 gives every reconnecting worker a monotonic epoch. Network changes
  immediately replace stale TURN/DTLS transports while keeping the logical KCP
  session and RelayBridge alive. The VPS accepts wire v1 and v2 for one
  transition release; the client emits v2.
- Obfuscation reads reuse a bounded buffer instead of allocating the maximum
  packet size for every UDP datagram.

The exact runtime probe is:

```console
sing-box hydra capabilities --json
```

The client reports role `client`, the client feature, wire v2, and only
`multi_user`; the VPS reports role `vps`, the server feature, wire v1..2, and
only `multi_user`. The legacy combined build is not a release artifact.

## Role-specific artifacts

- `hydracore-client-libbox.aar`
- `hydracore-client-libbox-sources.jar`
- `hydracore-vps-linux-amd64.tar.gz`
- `hydracore-vps-linux-arm64.tar.gz`

Each archive contains a root executable named `sing-box` and ships with a
SHA-256 sidecar plus per-architecture provenance. The release also retains the
client Android bindings, a release manifest, the attributed source archive,
subscription contracts, checksums, and schema-v3 Android provenance.

Stable publication is explicit: ordinary pushes build and verify artifacts,
while a maintainer must dispatch the workflow with `publish=true` to update
the stable release.

## Security and capacity boundary

The VPS never joins VK and receives no VK cookies or room-creator credentials.
`obfs_password` is a trusted group secret protecting the self-signed DTLS
identity and should be rotated when group membership changes. With four rooms
and four workers per user, a 27-allocation-per-room VK limit corresponds to an
estimated 27 concurrent sessions; actual limits and throughput depend on VK,
RTT, loss, and the VPS.
