# HydraCore v1.13.16-extended-hydracore.11-debug.21

This build keeps wire v6 and exactly four VK calls. It closes the debug.20
reconnect cascade by coordinating recovery across the complete logical
session: only one affected TURN/DTLS worker is recycled at a time, and blocked
flows coalesce behind that recovery instead of removing the remaining three
workers. A bounded 1024-packet per-peer burst queue and ClientHello
deduplication prevent a speed-test burst from exhausting ingress and stale DTLS
handshake slots.

Each of the four independent KCP lanes now enables its own congestion control
and uses fast-resend 4. Loss on one VK/TURN path therefore reduces pressure on
that path instead of feeding retransmissions back into the same bottleneck.
Telemetry retains aggregate KCP retransmissions and adds explicitly estimated
fast-resend versus RTO counters for the next tuning run. Both roles remain
wire-v6 compatible, but client and VPS should use debug.21 together.

## Previous debug.20 notes

Debug.20 kept wire v6 and exactly four VK calls. It added pressure spillover
for an ordered flow when its preferred KCP lane was saturated, recycled the
affected TURN/DTLS worker on a send stall, and kept physical output blocking
out of RelayBridge's synchronous send path.

## Previous debug.19 notes

This debug release addresses the remaining failure modes measured in
`vk-debug18.tar.gz`. It introduces incompatible wire v6: both the VPS and the
client must run debug.19. The transport remains exactly four VK calls, four
TURN/DTLS workers and four independent KCP lanes. Raw mode is unchanged.

KCP output is no longer discarded when a physical worker queue is full. The
output callback waits for queue capacity or for that worker to be replaced, so
KCP cannot start a retransmission timer for a packet silently dropped inside
HydraCore. Every lane now has an independent update loop; backpressure on one
TURN path does not stop timers or acknowledgements on the other three.

Ordered TCP relay flows are assigned to one lane for their lifetime. This
removes cross-call sequence gaps and the whole-session head-of-line failure
that previously produced `lane_reorder_timeout`. UDP and QUIC datagrams remain
striped over all four calls and are delivered without global in-order blocking;
their sequence tracking suppresses duplicates and keeps terminal flow cleanup
bounded without killing the logical tunnel.

Android network handover replaces workers sequentially. The client drops one
old TURN/DTLS transport, waits for its higher-epoch replacement, then proceeds
to the next worker. If a replacement misses its bounded deadline the remaining
three calls stay alive and the failed worker's normal retry loop continues.
Complete zero-path failure is detected in 5-10 seconds instead of 15-30 seconds,
allowing the bridge manager to create a fresh logical session sooner.

The exact runtime contract is
`call_vk_parasite_wire={min:6,max:6}` with
`call_vk_four_lane_kcp`, `call_vk_pre_kcp_admission` and
`call_vk_relay_flow_control` advertised. `workers` and
`max_workers_per_session` must both be four. The obsolete
`call_vk_eight_lane_kcp` capability is false.

The non-raw RTP wrapper continues to use video payload type 96. Four-lane KCP
keeps the aggregate 512-segment send window and 1024-segment pending ceiling;
there is no fixed bitrate token bucket. Actual throughput is therefore limited
by the VK/TURN paths, physical loss, KCP retransmission pressure and relay
workload rather than a configured per-lane speed cap.
