# Native VK Calls telemetry

HydraCore exposes operator-gated telemetry for the `vk_parasite` QUIC transport.
Hydra Ultimate owns experiment start/stop/export; the core has no duration
timer and records only while Ultimate's active session marker is present.

## Collection path

The VPS watches `/var/lib/hydra/calls/vk/telemetry/active.json`, writes schema-v1
JSONL to `/run/hydra/calls-telemetry.jsonl`, and rotates 64 MiB handoff parts.
Ultimate drains them into its compressed bounded timeline.

The VPS overwrites client identity and the primary timestamp with the
authenticated user, native session and receipt time. It also preserves the
client event time as `origin_timestamp` and stamps the authoritative
`session_generation`.

## Record scopes

- `server_process`: VPS runtime, listener, ingress, handshake and lifecycle;
- `server_session`: aggregate QUIC paths, streams and relay state for one user;
- `client_session`: aggregate client runtime, network and QUICRelay state.

## Transport measurements

Telemetry captures:

- QUIC connection count, active streams, RTT, RTT variance, congestion window, lost packets and retransmitted bytes;
- QUIC datagram counters for UDP traffic;
- DTLS/TURN/VK setup latency, failure stage and path replacement counts;
- Outer packets, payload/wrapper bytes, physical loss and jitter;
- Process runtime metrics (CPU %, RSS bytes, goroutines, GC pause).

No tokens, passwords, join links, TURN URLs, IP addresses, domains or payload
contents are emitted.
