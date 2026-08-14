# Native VK Calls telemetry

HydraCore exposes operator-gated telemetry for the `vk_parasite` transport.
Hydra Ultimate owns experiment start/stop/export; the core has no duration
timer and records only while Ultimate's active session marker is present.

## Collection path

The VPS watches `/var/lib/hydra/calls/vk/telemetry/active.json`, writes schema-v1
JSONL to `/run/hydra/calls-telemetry.jsonl`, and rotates 64 MiB handoff parts.
Ultimate drains them into its compressed bounded timeline. Client snapshots
travel inside authenticated wire-v4 control frames; no extra listener or
credential is exposed.

The VPS overwrites client identity and timestamp with the authenticated user,
native session and receipt time. It validates record size, schema, scalar
values and lane IDs, rate-limits records, and records continuity counters.

## Record scopes

- `server_process`: VPS runtime, listener, ingress, handshake and lifecycle;
- `server_session`: aggregate four-lane KCP and relay state for one user;
- `server_worker`: one server-side lane and its DTLS/TURN/outer transport;
- `client_session`: aggregate client runtime, network and four-lane state;
- `client_worker`: one client-side lane including VK/TURN/DTLS setup.

`worker_id` is the stable lane ID `0..3`. Counters roll up from a lane to its
session and then to the process; analysis must not add parent and child records
together.

## Transport measurements

Every lane reports independently:

- active/reconnect/liveness state and TURN endpoint ordinal;
- KCP WaitSnd, smoothed RTT, derived RTO, output and retransmitted segments;
- KCP output and retransmitted bytes;
- assigned active flow count;
- DTLS/TURN/VK setup latency and failure stage;
- outer packets, payload/wrapper bytes, physical loss and jitter;
- output and peer-read queue depth, delay, late packets and drops.

Session records report aggregate KCP pending depth and retransmissions, active
lane count, relay goodput, connection counts and backpressure. Process records
add UDP ingress/socket buffers, listener drops, CPU, RSS, thermal state and
handshake capacity.

The eight independent KCP records are the critical difference in wire v5: RTT,
RTO and retransmission pressure can be attributed to the exact VK call instead
of inferred from a shared reliable session. Comparing lane goodput, WaitSnd,
RTT/RTO, physical loss and queue delay reveals whether the next optimization
belongs in TURN selection, lane scheduling, KCP windows, reconnect policy,
socket/CPU handling or the relay-frame reorder layer.

No tokens, passwords, join links, TURN URLs, IP addresses, domains or payload
contents are emitted.
