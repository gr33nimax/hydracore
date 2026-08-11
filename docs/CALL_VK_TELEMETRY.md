# Native VK Calls telemetry

HydraCore `with_call` builds expose temporary, operator-gated telemetry for the
VK `multi_user` transport. There is no duration configured in the core. Hydra
Ultimate starts and stops an experiment through its own telemetry session;
HydraCore follows that state and is otherwise idle.

## Activation and transport

The VPS runtime checks
`/var/lib/hydra/calls/vk/telemetry/active.json` and the referenced session
document. A session is active only while its `stopped_at` value is zero. On a
new session, the runtime atomically replaces
`/run/hydra/calls-telemetry.jsonl`, creates it with mode `0600`, and appends
schema-v1 records accepted by Hydra Ultimate. A protected adjacent session
marker lets a restarted Hydracore process resume the same active stream
without truncating records; a different Ultimate session still rotates it.

While recording is active, the server periodically grants every authenticated
client a short telemetry lease. Client records use reserved KCP control frames
inside the existing RTP-shaped, TURN, DTLS and inner-authenticated path. No new
listener, HTTP endpoint or client credential is introduced. The VPS validates
record size, schema, metric names and scalar values, rate-limits each session,
discards client-supplied identities and timestamps, validates worker IDs, and
assigns the authenticated user, native session and VPS receipt time itself.
Legacy peers ignore the reserved control messages.

## Measurements

Server snapshots cover:

- outer authenticated RTP packet/byte totals and authentication/wrap failures;
- pending/rejected/timed-out handshakes, DTLS latency and auth outcomes;
- active/created/closed sessions and worker attach/liveness outcomes;
- peer and worker queue depth, queue drops and no-available-worker drops;
- per-tunnel KCP pending data, output/retransmitted segments, ACK-derived RTT,
  derived RTO and accumulated send-backpressure time;
- active TCP/UDP relay flows, relayed payload bytes, buffered relay bytes,
  relay queue drops and destination-connect failures;
- Go goroutine, heap and cumulative GC-pause values plus process CPU, RSS and
  best-effort thermal pressure.

Client snapshots add:

- total VK authentication latency and the anonymous-token, call-preview,
  anonymous-call-token, anonymous-login and join-conversation stages;
- TURN endpoints attempted, allocation outcomes and allocation latency;
- DTLS and inner user-auth outcomes and latency;
- desired/active workers, reconnects, current reconnect backoff and queue loss;
- authenticated outer packet loss from per-SSRC RTP sequence windows, packet
  reordering/duplication and inter-arrival jitter;
- default-interface changes and changes occurring with live workers;
- process CPU time from `/proc/self/schedstat`, RSS from `/proc/self/statm`, and
  best-effort Linux/Android thermal state.

`runtime_thermal_state` uses `0=unknown`, `1=nominal`, `2=fair`, `3=serious`,
`4=critical`. `runtime_thermal_available` distinguishes a real zero/unknown
sample from an accessible thermal reading. Thermal files are not readable on
all Android devices; the field remains explicitly unknown instead of being
fabricated.

KCP RTT is sampled only for segments that were not retransmitted (Karn's
rule). Network loss is based on unique authenticated outer RTP sequence
numbers with wrap, reordering and duplicate handling. Jitter is outer-packet
inter-arrival variation; it includes scheduling/burst effects and is therefore
interpreted together with KCP retransmission and queue pressure.

A client must complete at least one TURN/DTLS/inner-auth worker before it can
receive a lease or deliver buffered setup events to the VPS. A client that
never establishes any authenticated worker is therefore visible only through
the VPS-side handshake/auth counters; it cannot report its own VK/TURN failure
path without adding a separate endpoint, which this design intentionally does
not do. When a later worker succeeds, bounded recent setup events are delivered
through the authenticated tunnel.

## Privacy and overhead

Records contain bounded numeric/boolean metrics and fixed slug events. They do
not contain join links, VK/OK tokens, TURN credentials, passwords, cookies,
destinations or packet payloads. Hydra Ultimate pseudonymizes the user and
native session while ingesting the local JSONL file.

Hot packet paths use atomic counters. Sequence/jitter tracking and client
runtime sampling are enabled only during a live telemetry lease. Client
snapshots are emitted every two seconds, and the server lease expires
automatically if the VPS stops refreshing it.
