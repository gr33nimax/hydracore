# HydraCore v1.13.16-extended-hydracore.11-debug.11

This is an intentionally incompatible paired client/VPS debug release of the
VK parasite transport. Both endpoints must run this exact release.

The transport now maps the four VK calls to four independent KCP lanes. Every
lane owns its conversation, RTT/RTO estimator, send/receive window, pending
queue, retransmission state, output queue and reconnecting TURN/DTLS worker.
A degraded call can therefore no longer inflate the retransmission timer or
block the KCP send window of the other three calls.

Wire v4 adds a bounded relay-frame sequence envelope above KCP. Consecutive
frames of one TCP or UDP flow may use different healthy lanes, while the peer
restores per-flow order before passing frames to RelayBridge. This retains
four-call aggregation for heavy flows without sharing KCP recovery state.
The reorder buffer is bounded to 4096 frames per flow and closes a corrupt or
unbounded session rather than silently damaging the byte stream.

New flows and frames are scheduled using lane activity, KCP WaitSnd, output
queue depth, smoothed RTT and the number of active flows using each lane.
Reconnects replace only the physical TURN/DTLS transport; lane KCP state and
existing relay sessions remain alive.

The implementation now lives under `transport/call/vk-parasite`. The former
mode/profile implementation and profile selector are removed. The only native
VK mode is `vk_parasite`, it requires exactly four lanes, and capabilities now
advertise `call_vk_parasite`, `call_vk_four_lane_kcp`, and
`call_vk_parasite_wire={min:4,max:4}`.

Telemetry reports aggregate and per-lane KCP WaitSnd, RTT/RTO, segment/byte
retransmissions, output queue delay/drops, active state and flow count. This is
the data needed to tune each lane independently in subsequent debug releases.
