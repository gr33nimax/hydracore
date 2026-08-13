# HydraCore v1.13.16-extended-hydracore.10-debug.10

This is the verified paired client/VPS `debug` build for the four-call adaptive
VK tunnel. Adaptive mode now has an explicit per-worker transport envelope and
mandatory physical-path feedback; a debug.10 endpoint must be deployed
on both the client and VPS. The legacy profile remains available and retains
its existing KCP wire behavior.

The worker authentication protocol is intentionally bumped to v3 and accepts
no older wire version. Mixed old/new deployments now fail during the worker
handshake instead of appearing connected and silently discarding adaptive
traffic. This debug build therefore requires a coordinated VPS and APK update.

The previous controller inferred the health of a VK/TURN call from ACKs and
retransmissions belonging to one KCP conversation striped across all four
calls. That attribution was fundamentally ambiguous: an ACK could return over
a different call, and a retransmission could be charged to the path that did
not lose the packet. In the field this produced the observed inverse feedback,
collapsed healthy path windows, and held aggregate goodput near 3-6 Mbit/s even
when the four physical calls carried materially more wire traffic.

Debug.10 gives every adaptive data datagram a per-call physical packet sequence.
The receiver returns a 64-packet selective delivery map through that same
worker every 10 ms while traffic is active. Only this direct signal can now
change per-path RTT, loss, delivered rate, or congestion window; shared KCP
ACKs only release the common flight map. This cleanly separates physical TURN
loss from end-to-end KCP retries, including repeated attempts of the same KCP
segment.

KCP ACK/control segments are separated from PUSH data before dispatch. ACKs
retain affinity to the worker that actually delivered the corresponding PUSH
and are copied to one additional best live path, while data packets are never
duplicated. Control traffic has a reserved 64-segment admission margin. The
scheduler uses 4-packet/4-ms affinity chunks to reduce the 16-ms microbursts
seen in debug.8, incorporates directly measured delivery and physical loss,
and keeps all four configured calls active.

Each of the four paths starts with a 48-segment window, has a 24-segment
minimum, and may grow independently to 192; the shared KCP window remains
bounded at 512 and the pending cap follows twice the current aggregate window.
Backoff is path-local, loss-driven, RTT-limited, and cannot shrink below the
already in-flight data plus a safety margin. This removes the self-imposed
five-megabit ceiling without claiming that VK/TURN capacity or the tester's
access network can always supply a particular bitrate.

Native telemetry now publishes physical feedback age, delivery/loss counters,
control-copy counts, and separate KCP retry pressure for every worker. The
default server peer-read queue is raised to 512 packets. Relay queue gauges
discard buffered bytes atomically on close, and authenticated network jitter
resets after an idle gap, preventing historical queue and idle artifacts from
being reported as current bottlenecks.

## Previous debug.8 changes

This is the verified `debug` channel build. It is published automatically only
after the full Go, Android and Linux workflow succeeds and is intended for
HYDRA ULTIMATE installations that explicitly select the Hydracore `debug`
channel. It is not promoted to the stable `latest` release.

Debug.8 fixes the adaptive flight-accounting regression observed in the first
debug.7 field run. KCP cumulatively acknowledges every sequence below the UNA
field of every incoming segment and separately processes exact ACK sequence
numbers. The first path-window controller mirrored only exact ACKs, so its
per-path flight map retained packets that KCP had already removed. This made
healthy paths appear hundreds of segments over their windows and eventually
held every path at its minimum window.

The scheduler and its native telemetry tracker now mirror both KCP acknowledgement
forms with wrap-safe sequence comparison. Cumulative acknowledgements release
the original path's flight, delivery bytes and retry pressure; exact clean ACKs
continue to provide the path RTT sample. Regression coverage includes a peer
PUSH that advances UNA without carrying the missing exact ACKs. UNA processing
is incremental and bounded, including sequence wrap and anomalous jumps, so it
does not add a per-packet scan of the flight map. Wire format,
subscription schema, legacy mode and raw mode remain unchanged.

This revision addresses the remaining adaptive VK overload found by the v3
1440p field run. Debug.6 removed the inappropriate single KCP congestion window
and raised loaded throughput, but KCP could still admit up to 512 segments and
queue 2048 more before the four independent TURN paths supplied any absolute
capacity feedback. In the captured downstream run that produced a standing
`WaitSnd` queue, excessive retransmission traffic and poor wire efficiency.

Adaptive now maintains an independent delivery window for every live VK/TURN
worker. Clean acknowledgements grow only that path; sustained retry pressure or
a late local output queue backs off only that path. The KCP send window is the
bounded sum of live path windows, and its pending limit follows the same sum.
Four paths start at 160 in-flight and 640 pending segments, can grow toward the
existing 512-segment ceiling when healthy, and no longer begin every loaded run
with the 2048-segment standing queue. This is designed around the measured
50-60 ms RTT and a 20 Mbit/s target, not a guarantee imposed on VK TURN.

The controller runs before KCP emits a segment. It retains 16-packet/16-ms
chunk affinity, control priority, fast-resend=4, socket-write RTT and
alternate-path retransmission, with no post-KCP pacer. The exact legacy
scheduler, wire format and raw mode are unchanged.

Native telemetry distinguishes authenticated outer network loss from KCP retry
pressure and records output-queue delay/late writes. It now also publishes an
exact per-worker attempt counter so analysis can report cumulative failed-path
attempts instead of treating a short-lived retry EWMA as a loss ratio. Path RTT
begins at the socket write attempt rather than scheduler enqueue. The old
`worker_path_loss_ratio` is retained only as a compatibility alias for retry
pressure. Per-worker delivery rate, adaptive window, in-flight occupancy and
window-backoff totals make the new control loop directly observable.
`features.call_vk_adaptive_multipath=true` remains the explicit client/VPS/
subscription compatibility gate.

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
- Wire v3 gives every reconnecting worker a monotonic epoch and requires the
  adaptive physical-feedback envelope. Network changes
  immediately replace stale TURN/DTLS transports while keeping the logical KCP
  session and RelayBridge alive. Older wire versions are rejected by both roles.
- Obfuscation reads reuse a bounded buffer instead of allocating the maximum
  packet size for every UDP datagram.

The exact runtime probe is:

```console
sing-box hydra capabilities --json
```

The client reports role `client`, the client feature, wire v3, and only
`multi_user`; the VPS reports role `vps`, the server feature, wire v3, and
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
