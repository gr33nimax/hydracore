# HydraCore v1.13.16-extended-hydracore.11-debug.16

This paired debug release corrects the Android native build identity used by
the signed HydraCore bundle contract. The gomobile build now receives the exact
version stored in `HYDRACORE_VERSION`, including its leading `v`, instead of a
normalized Git tag. The native capability bytes and the signed manifest digest
therefore describe the same runtime identity in the isolated candidate process.

The previous debug release added the signed Android HydraCore bundle contract.
The client AAR now routes generated gomobile loading through
`HydraNativeLoader`, while the release also publishes detached per-ABI native
artifacts, a capability digest, manifest, signature, provenance and checksums.
HydraBox can therefore validate a candidate in an isolated process, activate
it manually, and fall back to the previous or APK-embedded core after an
unhealthy launch.

This is an intentionally incompatible paired client/VPS debug release of the
VK parasite transport. Both endpoints must run this exact release.

Wire v5 expands the transport from four to eight independent KCP lanes. The
change follows the measured VK/TURN media-path ceiling of approximately
2.07 Mbit/s per lane: adding independent allocations raises aggregate headroom
without coupling congestion recovery between calls. Each lane keeps its own
conversation, window, RTT/RTO estimator, retransmission state, output queue and
reconnecting TURN/DTLS worker.

New bulk bytes are adaptively admitted before KCP starts retransmission timing.
The per-lane controller begins below the measured media-path ceiling, increases
only while retransmission pressure stays low, and reduces only the affected
lane under loss. KCP ACK/recovery traffic and relay control frames bypass this
gate. There is no post-KCP rate limiter or timer-distorting output queue.

The non-raw RTP wrapper now emits dynamic video payload type 96 with a 90 kHz
timestamp clock. Receivers continue to accept legacy payload type 111 only for
diagnostics. Raw mode is unchanged.

Relay TCP streams now use end-to-end byte credit, 16 KiB data frames and a
256 KiB receive window per flow. This bounds the previously unbounded relay
backlog and propagates slow consumers to the originating socket instead of
letting memory and KCP WaitSnd grow. Remote terminal frames also release local
lane-flow accounting, removing stale flows observed after speed tests.

Telemetry adds each lane's admission rate and the emitted RTP payload type.
The exact runtime capability contract is now `call_vk_eight_lane_kcp`,
`call_vk_pre_kcp_admission`, `call_vk_relay_flow_control`, and
`call_vk_parasite_wire={min:5,max:5}`.

For an atomic VPS rollout only, the server normalizes an existing persisted
`max_workers_per_session=4` configuration to eight. Worker authentication and
all client traffic remain strict wire v5; the next HYDRA apply writes eight.
