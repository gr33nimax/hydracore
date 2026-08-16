# HydraCore v1.13.16-extended-hydracore.11-debug.29

This build corrects the debug.28 CI failure and one real lifecycle boundary it
exposed. The fast three-worker detach escalation is armed only after a tunnel
has reached all four physical workers. A single-lane recycle during initial
attach or staged network rebind therefore remains lane-local, while an
established four-line session still closes immediately after three transports
disappear. Verified runtime capability gates and emulator selection now use
wire v8.

## Previous debug.28 notes

This build closes the failure chain captured in the debug.27 speed-test and
streaming run and introduces incompatible wire v8. Client and VPS must be
updated together. Lane recovery now has one cross-endpoint coordinator: the VPS
suggests the affected lane and the client serializes generation resets, so the
two ends cannot independently quarantine different calls. Deferred lanes are
revisited after cooldown instead of being lost.

Reset handshakes use the measured KCP RTO with a bounded three-to-ten-second
deadline. Complete-session no-progress replacement now also requires recent
application demand, which leaves an idle residual KCP tail alone while still
recovering a genuinely blocked active flow. Reconnect attempts are capped at
ten seconds and retry backoff at five seconds to prevent minute-scale outages.

Client telemetry retains its original event timestamp alongside server receive
time and carries the authoritative server-session generation. Snapshot
coalescing has its own counter, so sequence gaps caused by latest-wins buffering
are no longer indistinguishable from actual record loss.

## Previous debug.27 notes

This build hardens wire-v7 behavior observed across speed tests, video and
YouTube. Soft recovery is reserved before reset begins and remains
single-flight through a bounded cooldown, so several near-simultaneous lane
stalls cannot launch competing generation resets or unnecessarily replace the
logical session.

The ACK-clocked pacer now distinguishes demand-limited congestion from an
application-limited stream. Idle or low-rate media intervals preserve the last
safe pacing rate, delivery estimate and inflight window, while real queue,
retry and RTT pressure under demand still backs the affected lane down.

Client telemetry now retains events in a bounded FIFO and coalesces snapshots
to their latest value until a healthy lane can carry them. New gauges expose
the pending telemetry backlog and application-limited state; a counter records
soft recoveries deferred by the active recovery or cooldown.

## Previous debug.26 notes

This build introduces incompatible VK parasite wire v7. Client and VPS must be
updated together; wire v6 is rejected. TCP data is fragmented to at most four
KCP MSS before admission, while UDP datagrams remain whole. Every lane starts
at 1.5 Mbit/s and uses ACK-clocked delivered-rate sampling, bounded startup,
loss/RTT/queue backoff, periodic probes and an 8-64 segment BDP inflight limit.

Lane recovery is now generation-scoped. `RESET_PREPARE`, `RESET_ACK` and
`RESET_COMMIT` are retried over every healthy call. Commit creates a new KCP,
clears staged output and transport estimators, and rejects old-generation KCP
or lane frames. A replacement is declared healthy only after both endpoints
exchange KCP probes and observe fresh ACK progress. TCP flows owned by the
reset lane close locally; unrelated flows and the other calls stay alive.

Aggregate ACK/relay progress now protects the complete session. Pending data
without progress for `max(5s, 3 x median RTO)`, capped at ten seconds, replaces
the logical session; three quarantined lanes escalate immediately. Telemetry
adds lane generation/state, pacing and delivered rates, minRTT, inflight limit,
token starvation, ACK age, reset lifecycle, stale-generation drops, probe
result, aggregate progress age and quarantined-lane count.

## Previous debug.24 notes

This build fixes the failure chain measured after debug.23. A stalled lane now
sends an authenticated recycle marker to its DTLS peer, so the client and VPS
detach the same physical worker immediately and the client's existing worker
maintainer can establish its replacement without waiting for UDP liveness
expiry.

HydraCore also detects sustained output/WaitSnd pressure with no ACK progress.
Recovery remains single-flight to preserve the other three calls, but timeout
is flow-local: only ordered TCP flows pinned to the missing lane are closed so
applications can reconnect them through a healthy lane. The logical tunnel is
not closed, and a missing preferred lane cannot cause an unrelated healthy
worker to be recycled.

The per-peer DTLS ingress burst queue is now bounded at 4096 packets. This
targets the internal queue drops seen during speed tests while keeping KCP,
worker and lane counts unchanged: exactly four VK calls and four independent
KCP lanes.

## Previous debug.23 notes

This build keeps wire v6 and exactly four VK calls. Each KCP lane now stages
its generated segments in a bounded queue and performs the potentially
blocking TURN/DTLS write outside the KCP mutex. ACKs and inbound data therefore
continue to advance while a physical writer is congested, instead of creating
the self-amplifying WaitSnd and retransmission collapse seen in the latest run.

Admission is bounded but ACK-clocked, with reserved room for heartbeats and
flow-control records. Ordered TCP flows remain on one lane for their lifetime;
a terminal reorder gap closes only the affected flow. Tunnel replacement also
uses generation-aware callbacks, preventing a late close from an old tunnel
from tearing down its replacement, and the shorter liveness interval detects
dead VK workers sooner.

Telemetry now exposes the admission window, staged KCP output depth and
capacity, update backpressure, KCP mutex blocking, physical worker write
latency and flow-local reorder aborts. These counters distinguish path
congestion from an internal output or locking bottleneck in the next test.

## Previous debug.22 notes

This build keeps wire v6 and exactly four VK calls. It removes the speed clamp
seen in debug.21: KCP's Reno controller treated delayed or duplicated TURN
delivery as physical congestion and repeatedly collapsed a lane after RTO.
The four lanes again use their bounded 128-segment windows and 256-segment
pending ceilings for independent backpressure, with fast-resend 2 to recover
missing segments before a full RTO where ACK progress allows it.

RTT telemetry now matches every ACK to the exact KCP sequence and echoed send
timestamp, including retransmitted attempts. Per-lane records add RTT variance,
RTT sample count, ACK count, acknowledged progress and tracked in-flight
segments. Recovery timeout and session-close escalation are explicit events,
so a lane recycle followed by a complete session replacement no longer appears
as an unexplained unfinished recovery.

## Previous debug.21 notes

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
