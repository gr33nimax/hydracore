# HydraCore v1.13.16-extended-hydracore.11-debug.18

This debug release fixes the terminal VK parasite reconnect failure observed in
session `20260815T121142Z-bedb92f2` and removes the fixed-rate controller that
artificially limited four-lane throughput. Wire v5 remains four VK calls, four
TURN/DTLS workers and four independent KCP lanes. Raw mode is unchanged.

Existing client subscriptions and VPS configurations that contain
`workers=8` or `max_workers_per_session=8` are normalized to four inside
HydraCore. This lets both endpoints receive a kernel-only update without a new
HydraBox APK or an immediate subscription rewrite. The legacy-named
`call_vk_eight_lane_kcp` capability is retained only because current signed
subscription documents require it; it no longer describes the physical count.

The reconnect path no longer closes a logical tunnel inline while `SendData`
owns a flow mutex. Relay teardown caused by a dead tunnel closes local TCP/UDP
state without sending `MsgClose` back through the same tunnel. Client recovery
starts a replacement session independently of stale cleanup, and the VPS admits
an authenticated replacement independently of closing the superseded session.
Initial transient VK/TURN/DTLS setup failures are retried with bounded backoff,
so the call outbound no longer remains permanently `tunnel not initialized`.

Four-lane KCP keeps an aggregate send window of 512 segments and an aggregate
pending ceiling of 1024 segments. The debug.17 per-lane token bucket, whose
320 kB/s starting rate imposed about 10.2 Mbit/s aggregate and whose maximum
imposed 25.6 Mbit/s aggregate, has been removed. New data is admitted while the
selected lane has KCP pending and physical output-queue capacity. This removes
the configured speed ceiling; actual throughput remains determined by the four
VK/TURN paths, loss, RTT and retransmission pressure.

The non-raw RTP wrapper continues to emit dynamic video payload type 96 with a
90 kHz timestamp clock. Relay TCP streams retain end-to-end byte credit, 16 KiB
data frames and a 256 KiB receive window per flow. Telemetry continues to expose
per-lane throughput, loss, retries, RTT, WaitSnd, output queues and reconnects.

Both client and VPS must run this exact signed HydraCore release. The exact
runtime contract remains `call_vk_parasite_wire={min:5,max:5}` with
`call_vk_eight_lane_kcp`, `call_vk_pre_kcp_admission` and
`call_vk_relay_flow_control` advertised. The legacy
`call_vk_pre_kcp_admission` name now represents queue-headroom admission and no
longer means a fixed bitrate limiter.
