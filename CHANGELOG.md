# Changelog

## Unreleased

- Removed the retired VK parasite telemetry collectors, reports, wire hooks, and capability flag.
- `vk_parasite` protocol 10: removed the DTLS layer from the Call path. Worker
  authentication and QUIC now travel directly inside the RTP-shaped wrapper,
  which frees 37 bytes and one AES-GCM pass per packet in each direction. A
  typical 1250-byte inner UDP datagram now travels as a single unreliable
  fragment instead of two. Client and VPS artifacts must come from the same
  release, as before.
- Bounded the Go heap with a soft memory limit instead of `GOGC=10` when the
  memory limit is enabled.
- Stopped allocating per packet on the receive path, per outbound TURN frame and
  per outbound UDP datagram.
- Path selection now skips a path whose RTT or loss is far worse than the best
  active one; keep-alive moved from 15 s to 25 s per path and transport health
  is published on events instead of once a second.
- Telemost, wbstream, VK P2P and Dion Call platforms moved behind
  `with_call_legacy` and are absent from release builds.
- The runtime event stream no longer stops the world once a second to read
  process memory, and no longer rebuilds the outbound group list — with its
  per-item url-test lookups and per-group cache-file reads — on every tick.
- The `vk_parasite` server serves every worker from one QUIC listener on the
  shared UDP socket instead of a per-peer packet queue and a per-peer listener,
  so an inbound packet is no longer copied twice on its way to a connection and
  connections are told apart by their QUIC connection ID. Worker authentication
  moved to the first stream of the connection, which makes it reliable and
  removes the frame retransmission it needed as a datagram. `ingress_workers`,
  `ingress_queue_packets` and `peer_read_queue_packets` are now ignored.

## v1.13.16-extended-hydracore.11-debug.57

- Stop an XHTTP outbound from handing an `http2.StreamError` to whatever dialled
  through it, which wedged an HTTP/2 client above the transport in a read loop that
  held a full CPU core for the lifetime of the process.

## v1.13.16-extended-hydracore.11-debug.52

- Fail fast when a `vk_parasite` relay has no active QUIC path.

## v1.13.16-extended-hydracore.11-debug.51

- Keep transport health snapshots consistent and apply network rebinds only to
  a newer network generation.
- Stop reconnecting paths after a terminal authentication failure.

## v1.13.16-extended-hydracore.11-debug.50

- Allow worker counts of 4, 8, 12, 16, or 20 across four required VK links.
- Preserve terminal transport failures and report safe dial-error events.

## v1.13.16-extended-hydracore.11-debug.49

- Publish `vk_parasite` transport health and support fragmented UDP datagrams.

## v1.13.16-extended-hydracore.11-debug.48

- Replace the KCP relay with the protocol-v9 QUIC relay over VK/TURN/DTLS paths.

## v1.13.16-extended-hydracore.11-debug.44

- Cleaned up internal comments and finalized verified DoT RFC 7858 bootstrap transport.

## v1.13.16-extended-hydracore.11-debug.43

- Replaced DoH with native RFC 7858 DNS-over-TLS (DoT) on port 853 (`77.88.8.8:853` / `77.88.8.1:853` with SNI `common.dot.dns.yandex.net`) and direct UDP/TCP DNS port 53 fallback, eliminating HTTP framing incompatibilities and ensuring instant resolution of VK signaling and TURN hosts in cellular whitelist environments.

## v1.13.16-extended-hydracore.11-debug.42

- Integrated autonomous RFC 8484 Bootstrap DoH resolver via direct TLS SNI `common.dot.dns.yandex.net` (77.88.8.8 / 77.88.8.1) with fallback to Google and Cloudflare for resolving VK signaling and TURN endpoints in cellular whitelist environments.
- Added automatic fallback to Bootstrap DoH in `HttpClient` and `resolveUDPAddresses` when system or tunnel DNS is uninitialized or blocked.
- Implemented automatic TCP fallback endpoint generation for UDP TURN allocations and added a 4-second fast-fail candidate timeout to switch to TCP STUN (`turn.NewSTUNConn`) under DPI UDP blackholing.

## v1.13.16-extended-hydracore.11-debug.41

- Deterministically sorted resolved TURN relay IPv4 addresses (`netip.Addr.Compare`) and distributed physical workers across distinct relay IPs via `preferred % len(addrs)` candidate rotation and fallback retry loop.
- Halved per-lane cold start pacing rate to 2 Mbit/s (`lanePacingInitialBPS = 2_000_000 / 8`), providing an aggregate 8 Mbit/s start across four lanes closer to typical TURN policer knees.
- Lowered absolute pacing floor to 512 Kbit/s (`lanePacingMinimumBPS = 512_000 / 8`) and clamped the pacing rate to an adaptive floor following 25% of measured delivered capacity.
- Desynchronized per-lane pacing probing by offsetting probe deadlines by `laneID * 1s`, ensuring non-overlapping probe windows across all four lanes.
- Implemented adaptive KCP ticking: extended ticker period to 40ms on fully quiesced lanes while preserving instant wake-up upon new input or application data.

## v1.13.16-extended-hydracore.11-debug.40

- Gated pacing probe initiation on baseline application demand (`windowDemandBits == 0b11`),
  preventing spurious probes and reducing inconclusive telemetry noise on application-limited
  traffic.
- Accelerated stalled session recovery: when all physical workers are detached with
  pending data, the watchdog triggers session replacement at the 30s threshold without
  waiting for the 70s cascade.
- Optimized lane reset handshake: added a zero-flow fast path to flow migration and halved
  the control retry interval to 125ms, reducing reset latency to < 2s.
- Added TURN endpoint quality scoring to bias selection toward reliable endpoints and
  introduced early lane quarantine when a lane's smoothed loss exceeds 3x the median of other
  active lanes.
- Tuned DTLS flight retransmission interval to 500ms for resilient multi-roundtrip handshakes
  on lossy networks, and configured 2 MiB UDP socket buffers on client TURN packet sockets.
- Removed unused server decoder state, removed dead medianRTO threshold parameter, and
  documented wire-v9 worker constraints, host sysctl recommendations, and architectural boundaries.

## v1.13.16-extended-hydracore.11-debug.39

- Evaluated pacing probes by marginal goodput: a probe now compares the
  byte-measured delivered gain against the byte-measured offered gain and
  falls back to a three-way verdict (useful / harmful / inconclusive).
  Harmful probes roll back the offered rate and lower the compensation
  ceiling after two consecutive verdicts; useful probes raise the ceiling
  toward 1.5x. The wire format is unchanged.
- Subtracted the smoothed KCP retransmit rate from the new-data admission
  budget as retransmission debt. Fresh data backs off while retransmits
  consume the measured path, instead of competing with them and sustaining
  a loss plateau on policer links.
- Added byte-granular lane counters (admitted and uniquely-acked KCP bytes)
  and recovery state gauges (`lane_recovery_attempt_id`,
  `lane_recovery_last_outcome`) so analyzers can reconcile recovery attempts
  from snapshots without relying on terminal events.

## v1.13.16-extended-hydracore.11-debug.38

- Replaced retransmission-only pacing backoff with a composite congestion
  signal. A lossy VK media path now backs off only when loss or RTT inflation
  coincides with a pressured transport window and collapsing delivery.
- Added bounded loss compensation to steady pacing and allowed startup probing
  to continue across stable double-digit loss. Four healthy calls can use their
  available capacity instead of converging near the former 15% retry threshold.
- Overlapped local ordered-flow migration with the peer PREPARE migration during
  lane reset, while retaining a barrier before generation commit. This removes
  the serial two-sided migration delay and reduces the interval in which TCP
  flows can time out during a real lane stall.

## v1.13.16-extended-hydracore.11-debug.37

- Revalidated serialized lane recoveries against fresh ACK progress, current
  backlog, and the adaptive stall deadline before starting a deferred reset.
  A lane that recovered while another lane was being repaired is no longer
  reset from a stale pending bit.
- Applied Karn's algorithm to the per-lane RTT estimator. ACKs for retransmitted
  KCP segments no longer inflate RTT/RTO and postpone or trigger recovery using
  an ambiguous timing sample.
- Classified material KCP and writer backlogs as network demand even when the
  caller did not hit the pacing limiter during that sample. Sustained overload
  can now engage congestion backoff instead of remaining falsely marked as
  application-limited.

## v1.13.16-extended-hydracore.11-debug.36

- Corrected the debug.35 deterministic-load regression: the capacity-based
  controller now starts at 4 Mbit/s per lane, confirms overload across two
  samples, and uses bounded 1.10 steady and 1.15 probe gains. This preserves
  the anti-collapse admission changes without spending prolonged intervals
  above a 2 Mbit/s path and turning the excess into retransmissions.

## v1.13.16-extended-hydracore.11-debug.35

- Replaced a permanently quarantined VK lane with a bounded full-session
  reconnect when reset acknowledgement, commit, or bidirectional probing does
  not complete. Both reset peers now have a watchdog, so a lost control frame
  cannot leave a physically attached but unusable lane indefinitely.
- Migrated affected ordered flows concurrently with eight bounded slots and a
  flow-count-aware reset deadline. A reset no longer serializes every TCP flow
  behind one four-second budget or announces readiness before failed flows are
  closed.
- Raised the initial KCP admission window and per-lane pacing baseline, retained
  a decaying delivery-capacity estimate, and require persistent congestion
  evidence before reducing rate. Admission is now derived from pacing capacity
  and RTT instead of the already self-limited delivered rate.
- Reported only active-generation lanes with an attached worker as usable
  transport lanes; quarantined, resetting, and probing lanes no longer make a
  degraded tunnel appear healthy.

## v1.13.16-extended-hydracore.11-debug.34

- Kept a successfully connected VK parasite client on the bridge manager's
  lifetime context instead of cancelling it when the connection attempt
  returned. This prevents an authenticated DTLS session from closing within
  milliseconds and entering an endless reconnect loop.

## v1.13.16-extended-hydracore.11-debug.32

- Kept VK API flood-control state process-wide so restarting the VPN runtime
  cannot immediately repeat a blocked control-plane join.
- Preserved the last worker failure at reconnect boundaries and labeled the
  failing VK credentials, TURN, DTLS or inner-auth stage instead of replacing
  it with an uninformative context deadline.
- Derived the outer reconnect deadline from the worker connection budget so a
  valid TURN/DTLS setup is no longer cut off by the old ten-second wrapper.

## v1.13.16-extended-hydracore.11-debug.31

- Prevented Android network handover from invalidating reusable VK TURN
  credentials or restarting an already active staged rebind.
- Serialized VK credential acquisition across all four join links and added a
  shared two-minute circuit breaker for VK API error 9. Flood control no
  longer falls through to the legacy auth path or creates an auth retry storm.

## v1.13.16-extended-hydracore.11-debug.30

- Start each planned network-rebind generation reset before detaching its
  worker, so staged handover does not inherit the four-second cooldown reserved
  for repeated automatic stall recovery.

## v1.13.16-extended-hydracore.11-debug.29

- Limited three-worker detach escalation to sessions which previously reached
  all four physical workers, so initial attach, focused lane tests and staged
  network rebind cannot be mistaken for a collapsed established session.
- Updated verified runtime capability gates and the deterministic emulator job
  selector from wire v7 to wire v8.

## v1.13.16-extended-hydracore.11-debug.28

- Introduced incompatible VK parasite wire v8 so the coordinator-only recovery
  control cannot be silently mixed with a debug.27 wire-v7 peer.
- Assigned lane-reset initiation to the client endpoint; the VPS now sends a
  reset suggestion, eliminating independent cross-endpoint recovery of
  different lanes and draining deferred lanes serially after cooldown.
- Made reset handshakes adaptive to measured KCP RTO with a bounded 3-10 second
  deadline, so high-latency TURN paths are not declared dead before an ACK can
  physically return.
- Required fresh application demand before aggregate no-progress can replace a
  session, preventing an idle residual KCP tail from destroying a healthy
  video session.
- Bounded reconnect attempts to ten seconds with a five-second maximum retry
  backoff, avoiding the measured minute-scale recovery gap.
- Preserved the client event timestamp, stamped every received client record
  with the authoritative server-session generation, and counted coalesced
  snapshots separately from telemetry record drops.

## v1.13.16-extended-hydracore.11-debug.27

- Serialized soft lane recovery before reset initiation and added a cooldown,
  preventing simultaneous ACK stalls from starting multiple generation resets
  and replacing the complete logical session.
- Made the ACK-clocked controller application-limited aware, preserving the
  last safe pacing rate and admission window during video and YouTube idle
  intervals instead of learning a false low path capacity.
- Buffered client telemetry across transient lane outages with bounded FIFO
  event retention and latest-wins snapshots; added pending-record visibility.
- Added per-lane application-limited and deferred-recovery telemetry plus
  deterministic coverage for concurrent recovery, cooldown and burst resume.

## v1.13.16-extended-hydracore.11-debug.26

- Introduced incompatible VK parasite wire v7 with generation-scoped worker
  authentication, KCP conversations and lane frames; wire v6 is rejected.
- Added an ACK-clocked 1.5 Mbit/s startup pacer per VK lane, 500 ms delivered
  rate sampling, bounded probing/backoff and an 8-64 segment BDP inflight limit.
- Replaced one-shot worker recycle with retrying `RESET_PREPARE`, `RESET_ACK`
  and `RESET_COMMIT`; reset now discards the poisoned KCP and staged output.
- Require a replacement lane to complete bidirectional KCP probe/ACK and fresh
  ACK progress before it returns to active service.
- Added aggregate no-progress session replacement, three-lane quarantine
  escalation and serialized TURN/DTLS reconnects while healthy lanes continue.
- Added wire-v7 lane generation/state/pacing/reset/probe telemetry and a
  deterministic four-lane CI emulator for capacity, loss, delay and blackhole.
- Route lane reset controls over healthy calls without waiting on a blocked
  target writer, and require staged network rebind to finish generation probe
  before the next TURN reconnect starts.

## v1.13.16-extended-hydracore.11-debug.24

- Added an authenticated in-band recycle signal so both DTLS endpoints detach
  a failed lane immediately instead of waiting up to the liveness timeout.
- Detect sustained KCP output pressure without ACK progress and recycle only
  that lane while the remaining calls continue carrying traffic.
- Abort only TCP flows pinned to a lane that fails to return; a lane recovery
  timeout no longer closes the complete VK parasite session.
- Prevent a missing preferred lane from recycling an unrelated healthy call
  and allow control traffic to reselect a lane before its current path stalls.
- Raised the bounded per-peer DTLS burst queue to 4096 packets to absorb the
  measured speed-test bursts without converting local drops into KCP RTOs.

## v1.13.16-extended-hydracore.11-debug.23

- Moved physical worker writes out of the KCP mutex and staged generated
  segments in a bounded per-lane queue, so a saturated TURN writer cannot
  block ACK/input processing and make its own retransmission backlog grow.
- Replaced the fixed per-lane admission ceiling with bounded ACK-clocked
  admission while reserving capacity for flow-control and heartbeat records.
- Kept every ordered TCP flow on one KCP lane for its complete lifetime and
  made a reorder timeout abort only that flow instead of the whole tunnel.
- Ignored stale bridge callbacks after tunnel replacement and shortened worker
  heartbeat/liveness intervals so dead transports are detected and replaced
  without an old tunnel closing the new bridge.
- Added output-backlog, admission-window, KCP-lock, worker-write and local
  reorder-abort telemetry for the next throughput and reconnect comparison.

## v1.13.16-extended-hydracore.11-debug.22

- Removed the debug.21 throughput clamp caused by applying KCP Reno congestion
  collapse to delayed or duplicated VK TURN delivery; each of the four lanes
  remains bounded by its own send, pending and physical output queues.
- Restored fast-resend 2 so ACK progress can repair a missing segment before a
  full RTO instead of leaving timeout retransmission as the dominant path.
- Matched RTT samples to the exact KCP sequence and echoed transmission
  timestamp, including retransmitted attempts, and added per-lane RTT variance,
  sample, ACK-progress and in-flight telemetry.
- Added explicit failed and session-escalated lane recovery events so a full
  session replacement is distinguishable from a permanently unfinished lane
  recovery.

## v1.13.16-extended-hydracore.11-debug.21

- Coalesced simultaneous send stalls into one session-wide lane recovery, so
  concurrent relay flows can no longer recycle all four VK workers in a
  cascade and leave the tunnel permanently disconnected.
- Enabled independent KCP congestion control on every VK/TURN line and raised
  fast-resend to four, preventing retransmission pressure on one lossy call
  from creating the measured loss-amplification loop across the tunnel.
- Raised the effective, bounded per-peer DTLS ingress burst capacity to 1024
  packets and deduplicated pending ClientHello state across TURN endpoint
  changes, reducing internal queue drops and stale handshake-slot exhaustion.
- Split KCP retransmission telemetry into estimated fast-resend and RTO segment
  and byte counters, while retaining the aggregate counters.

## v1.13.16-extended-hydracore.11-debug.20

- Kept ordered relay flows on their preferred KCP lane while it has headroom,
  but allowed pressure spillover to the other three independent VK calls.
- Replaced the affected TURN/DTLS worker on a send stall before escalating to a
  full logical-session reconnect, preserving KCP state and the other calls.
- Removed synchronous KCP flushing from RelayBridge's send path so a blocked
  physical writer cannot hide the stall timer behind the lane mutex.

## v1.13.16-extended-hydracore.11-debug.19

- Introduced incompatible VK parasite wire v6 with exactly four physical
  VK/TURN calls and four independent KCP lanes. Ordered TCP flows stay on one
  lane; unordered UDP/QUIC datagrams retain aggregate four-lane scheduling.
- Replaced post-KCP non-blocking worker-queue drops with physical backpressure.
  Each lane now owns its update loop, so a blocked TURN writer cannot stall
  KCP timers on the other three calls.
- Rebound Android network transports one lane at a time and shortened zero-path
  failure detection. A failed replacement leaves the remaining calls alive
  while its own maintenance loop continues retrying.
- Added regression coverage for queue saturation, TCP affinity, UDP striping,
  non-fatal UDP reorder gaps and staged four-worker handover.

## v1.13.16-extended-hydracore.11-debug.18

- Removed the per-lane 320-800 kB/s token bucket that imposed an artificial
  aggregate throughput ceiling. New relay data is now admitted from actual KCP
  pending and physical worker-queue headroom.
- Prevented a terminal lane send from synchronously re-entering the same relay
  flow during close, eliminating the `lane_send_stalled` self-deadlock.
- Made client reconnect and authenticated server takeover independent from
  cleanup of a superseded session, and retry initial transient setup failures
  instead of leaving the call outbound permanently uninitialized.
- Added regression coverage for reentrant relay close, blocked old-session
  cleanup, non-blocking client replacement and initial connection retry.

## v1.13.16-extended-hydracore.11-debug.17

- Returned wire v5 to four physical VK/TURN calls and four independent KCP
  lanes; existing `workers=8` and `max_workers_per_session=8` settings are
  normalized by HydraCore so the client and VPS can update independently.
- Replaced a stale server-side DTLS peer when a new ClientHello arrives from a
  reused TURN relay address, including rapid reconnects with a new handshake
  identity.
- Invalidated cached VK/TURN credentials after a worker disconnect or failed
  setup so retries do not reuse a dead allocation for the eight-minute cache
  lifetime.
- Made the latest fully authenticated session replace the previous session for
  the same user immediately, instead of rejecting reconnects while old workers
  remained attached.
- Restored four-lane aggregate KCP headroom and raised the per-call admission
  starting point to the operating range measured in telemetry.

## v1.13.16-extended-hydracore.11-debug.16

- Passed the canonical `HYDRACORE_VERSION` directly into the gomobile linker so
  native capabilities retain the same leading `v` as the signed manifest.
- Added an Android bundle gate that rejects every ABI whose native library does
  not contain the exact release identity.

## v1.13.16-extended-hydracore.11-debug.15

- Generated the signed client capability document with the exact version and
  byte framing returned by the Android native runtime, so isolated candidate
  verification compares identical bytes instead of rejecting every valid
  update.
- Restricted automatic debug publication to the permanent `debug` branch.

## v1.13.16-extended-hydracore.11-debug.14

- Added the signed, independently updateable Android bundle contract with
  per-ABI native artifacts, capability digest, provenance and rollback slots.
- Patched generated gomobile loading through HydraNativeLoader so a verified
  read-only native bundle can be selected before the embedded APK fallback.
- Isolated capability generation from the libbox role build and verified the
  client protocol contract in GitHub Actions.

## v1.13.16-extended-hydracore.11-debug.13

- Introduced incompatible VK parasite wire v5 with eight independent KCP
  lanes, per-lane pre-KCP admission control and video-class RTP payload type
  96. Raw mode remains unchanged.
- Added end-to-end TCP relay byte credit and 16 KiB frame chunking, bounding
  each receive backlog at 256 KiB and propagating slow consumers to the source.
- Released send-flow lane accounting when the remote endpoint closes a flow;
  telemetry now includes lane admission rate and RTP payload type.

## v1.13.16-extended-hydracore.11-debug.12

- Prevented one saturated wire-v4 KCP lane or logical flow from blocking every
  relay flow and the native telemetry producer.
- Re-evaluate all live lanes while a flow is backpressured, using prospective
  KCP fragmentation and physical output-queue capacity for admission.
- Close and reconnect a logical client session after a bounded send stall or
  per-flow reorder gap instead of leaving a dead tunnel alive indefinitely.
- Permit a newly authenticated client to replace an old session whose workers
  are still attached but which has made no relay progress during the takeover
  grace period.

## v1.13.16-extended-hydracore.11-debug.11

- Replaced the shared VK KCP conversation with four independent wire-v4 lanes.
- Added per-flow sequence bonding and bounded reordering above the lanes.
- Consolidated the implementation under `transport/call/vk-parasite` and
  removed the obsolete scheduler profile and wire-v3 contract.

## Unreleased

- Added independent delivery windows for adaptive VK/TURN workers. Clean paths
  grow from a 40-segment starting window, while sustained KCP retry or local
  output-queue pressure backs off only the affected path. KCP admission and
  pending backpressure now follow the bounded aggregate path window, preventing
  all four Calls workers from filling a shared 2048-segment standing queue.
  Added delivery-rate, window, in-flight and backoff telemetry per worker.
- Replaced the obsolete shared wire-v3 VK scheduler with wire-v4 four-lane KCP
  affinity, alternate-path retransmission, ACK/control priority, per-worker
  feedback and explicit legacy A/B fallback. The scheduler uses bounded KCP
  local/remote windows without a single dynamic congestion window spanning all
  TURN paths and without a post-KCP pacer. Added exact per-path attempt/retry
  counters and the `call_vk_four_lane_kcp` capability gate; raw mode is
  unchanged.
- Made same-path physical feedback mandatory for adaptive traffic and removed
  transitional worker-auth compatibility. Mixed old/new APK and VPS cores now
  fail at the v3 handshake instead of silently accepting incompatible data.
- Established a separately versioned `debug` release channel. A push to the
  `debug` branch publishes verified Android and VPS artifacts as a GitHub
  prerelease only after all test and build jobs pass.
- Added operator-gated native VK multi-user server/client telemetry compatible
  with Hydra Ultimate's schema-v1 JSONL ingestion contract.
- Instrumented VK/TURN/DTLS/inner-auth stages, authenticated outer loss and
  jitter, KCP RTT/retransmission/backpressure, worker/peer/relay queues,
  network handover and runtime pressure.
- Client records travel through bounded, rate-limited control frames inside
  the existing authenticated KCP/DTLS/TURN path; no new listener or
  client-controlled identity is introduced.

## v1.13.16-extended-hydracore.8

- Exposed the authenticated inbound user in Clash `/connections` metadata so
  HYDRA Ultimate can attribute Hydra VK Tunnel traffic per user.

## v1.13.16-extended-hydracore.7

- Split release capabilities and artifacts into a client Android runtime and
  a VPS Linux runtime while keeping one versioned source tree. Release roles
  expose only VK Calls `vk_parasite`; the combined developer tag retains legacy
  compatibility outside the product release surface.
- Added Calls wire v2 worker epochs and immediate network rebind so Wi-Fi/mobile
  handover replaces stale DTLS/TURN workers without discarding the logical KCP
  session. The VPS accepts wire v1 and v2 for one transition release.
- Reused the obfuscation read buffer instead of allocating a maximum-size
  packet buffer for every datagram.
- Made stable publication an explicit verified workflow action. Ordinary main
  pushes now produce CI artifacts without silently replacing `latest`.

## v1.13.16-extended-hydracore.6

- Fixed Hydra Subscription v2 plaintext/JWE validation so a `with_call` build
  accepts the advertised `call_vk_parasite` required feature alongside
  `call`; builds without Calls and unknown requirements remain fail-closed.

## v1.13.16-extended-hydracore.5

- Fixed targeted managed URLTest sessions so an outbound group probes its
  concrete `Now()` leaf while result events retain the requested group tag.
  Direct targets and concrete-leaf URLTest history remain unchanged.

## v1.13.16-extended-hydracore.4

- Updated the exact `sing-box-extended` baseline to 2.6.2 commit
  `545424b86bc4513f90580ebeab2e2d1514089718`.
- Added native VK Calls `vk_parasite` with O(1) per-user authentication,
  bounded sessions/handshakes, cached VK TURN credentials, one KCP session
  striped across reconnecting DTLS workers, and up to four distinct room
  links.
- Added `sing-box hydra capabilities --json` and the canonical
  `features.call_vk_parasite` / `protocols.call_modes` contract.
- Added reproducible, checksummed Linux `amd64` and `arm64` release archives
  for VPS deployment alongside the Android AAR.

- Moved the stable product line to `main` and reduced the public branch surface.
- Removed the inherited upstream documentation site and unsupported packaging
  workflows from the HydraCore repository.
- Consolidated lineage, license, and compatibility attribution in `CREDITS.md`
  and `THIRD_PARTY_NOTICES.md`.
- Established `hydracore` as the canonical repository and public distribution
  name while retaining upstream module, ABI, and capability aliases.
- Reframed HydraCore as the maintained Android runtime for the Hydra
  self-hosted VPN stack.
- Added project roadmap, contribution, security, third-party, and issue-report
  documentation.
- Moved WDTT work out of the stable line and preserved it in the
  `wdtt-archive` branch for possible future research.

## v1.13.16-extended-hydracore.1

- Updated the exact `sing-box-extended` baseline to commit
  `da4c532efb1f86a38a324909fc9b8867f811551c` from the 2.6.1 line.
- Introduced HydraCore API v2, remote policy v2, build metadata, runtime
  snapshot/events, and managed URLTest sessions.
- Added the core-owned Hydra Subscription v2 plaintext/JWE contract, strict
  validation, redacted inspection, and authenticated JWE opening.
- Enabled release-tagged Call inbound/outbound for `dion`, `telemost`, `vk`,
  and `wbstream`, plus Rmux and bounded AmneziaWG v3 validation.
- Removed active legacy capability and provenance surfaces while retaining
  explicit historical attribution and source lineage.

## v1.13.14-extended-hydracore.5

- Published the standalone HydraCore repository structure on `main`.
- Retained complete GPL/source lineage while removing inherited product-facing
  Etonify and sing-box documentation surfaces.
- Preserved the stable runtime behavior and deferred WDTT archive.

## v1.13.14-extended-hydracore.4

- Published the canonical HydraCore repository identity and project contract.
- Removed the deferred WDTT experiment from the stable source and capability
  line while preserving it in `wdtt-archive`.
- Retained the `.2` runtime behavior and compatibility identity on the pinned
  upstream baseline.

## v1.13.14-extended-hydracore.2

- Published a provenance-bound Android `libbox.aar` from the pinned
  `sing-box-extended` and Etonify mobile-integration baseline.
- Added `HydraCoreCapabilities()` and the `io.hydrabox.hydracore` distribution
  identity while retaining `EtonifyCapabilities()` for compatibility.
- Added the exact remote-safety policy consumed by HydraBox Subscription v1.
