# HydraCore v1.13.16-extended-hydracore.11-debug.17

This debug release keeps VK parasite wire v5 but returns its physical topology
to the intended model: four VK calls, four TURN/DTLS workers and four independent
KCP lanes. Each lane retains its own conversation, RTT/RTO estimator, windows,
retransmission state, admission controller and reconnect lifecycle. Raw mode is
unchanged.

Existing client subscriptions and VPS configurations that contain
`workers=8` or `max_workers_per_session=8` are normalized to four inside
HydraCore. This lets both endpoints receive a kernel-only update without a new
HydraBox APK or an immediate subscription rewrite. The legacy-named
`call_vk_eight_lane_kcp` capability is retained only because current signed
subscription documents require it; it no longer describes the physical count.

Reconnect recovery now has three explicit repairs. A disconnected or
not-fully-attached worker invalidates its cached VK/TURN credential, so its next
attempt requests a fresh allocation instead of repeating a dead path for the
eight-minute cache lifetime. The VPS recognizes a new DTLS ClientHello arriving
from an established relay endpoint and replaces the stale peer; while a
handshake is pending it also distinguishes a retransmission from a genuinely
new handshake by the ClientHello random. Finally, the latest fully authenticated
session for a user immediately supersedes that user's previous ready session,
even when the old workers still look active.

Four-lane KCP keeps an aggregate send window of 512 segments and an aggregate
pending ceiling of 1024 segments. The pre-KCP admission controller now starts at
320 kB/s per physical call (about 10.2 Mbit/s aggregate) and may grow to
800 kB/s per call when that call's own retry and queue pressure permit it. This
does not promise a particular VK throughput; it removes the eight-lane ramp
penalty while preserving per-path backoff.

The non-raw RTP wrapper continues to emit dynamic video payload type 96 with a
90 kHz timestamp clock. Relay TCP streams retain end-to-end byte credit, 16 KiB
data frames and a 256 KiB receive window per flow. Telemetry continues to expose
per-lane throughput, loss, retries, RTT, WaitSnd, output queues and reconnects.

Both client and VPS must run this exact signed HydraCore release. The exact
runtime contract remains `call_vk_parasite_wire={min:5,max:5}` with
`call_vk_eight_lane_kcp`, `call_vk_pre_kcp_admission` and
`call_vk_relay_flow_control` advertised.
