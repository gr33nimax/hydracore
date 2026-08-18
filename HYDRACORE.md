# HydraCore distribution contract

Current debug release:
`v1.13.16-extended-hydracore.11-debug.42`.

HydraCore publishes separate client and VPS artifacts. A VK parasite deployment
must use artifacts from the same release manifest and commit; mixed wire
versions are rejected during worker authentication.

## VK parasite transport

The native mode is `vk_parasite` and uses exactly four VK calls. Each call is
one independent KCP lane with its own conversation, RTT/RTO, windows, queues,
retransmission state and TURN/DTLS lifecycle. Incompatible wire v9 pins each
ordered TCP flow to one reliable lane, stripes unordered UDP/QUIC datagrams
over all four, fragments TCP data to at most four MSS before KCP, and admits
user data through an ACK-clocked 1-25 Mbit/s pacer per lane. The pacer probes
for headroom and keeps the raised rate only when delivered goodput follows
the byte-measured offered gain; harmful probes roll back with a doubling
cooldown, the loss-compensation ceiling starts at 1.25x and rises toward 1.5x
only after proven probes, and the smoothed KCP retransmit rate is subtracted
from the new-data admission budget as retransmission debt. Each reset advances
a lane
generation carried by worker auth, KCP conversation IDs and lane frames. The
VPS suggests recovery to the client-side coordinator; the two endpoints then
exchange `RESET_PREPARE`, `RESET_ACK` and `RESET_COMMIT`, discard the old KCP
and activate the new one only after a bidirectional probe
and fresh ACK progress. A line that does not return aborts only its pinned TCP
flows; the other calls and the logical tunnel remain available.
Generation-aware bridge callbacks prevent a retired tunnel from closing its
replacement. Per-lane send, pending and physical output bounds provide
backpressure without applying Reno congestion collapse to VK TURN delivery.
Soft lane recovery is single-flight across both endpoints through reset and
cooldown, while the ACK-clocked controller preserves its last safe capacity
estimate when the
application is not supplying enough traffic to measure the path.

The complete implementation is in `transport/call/vk-parasite`. Files outside
that directory only register the sing-box inbound/outbound and map configuration
into the package.

The role contract is:

- client: `identity.role="client"`, `call_vk_parasite_client=true`;
- VPS: `identity.role="vps"`, `call_vk_parasite_server=true`;
- both: `call_vk_four_lane_kcp=true`, `call_vk_pre_kcp_admission=true`,
  `call_vk_relay_flow_control=true`, `call_vk_telemetry=true`,
  `call_vk_parasite_wire={"min":9,"max":9}` and
  `call_modes=["vk_parasite"]`.

Example VPS inbound:

```json
{
  "type": "call",
  "tag": "call-vk-server",
  "platform": "vk",
  "mode": "vk_parasite",
  "listen": "0.0.0.0",
  "listen_port": 8443,
  "obfs_password": "outer-secret",
  "max_workers_per_session": 4,
  "users": [
    {"name": "tester-1", "password": "per-user-secret"}
  ]
}
```

Example client outbound:

```json
{
  "type": "call",
  "tag": "proxy-main",
  "platform": "vk",
  "mode": "vk_parasite",
  "server": "203.0.113.10",
  "server_port": 8443,
  "join_links": ["https://vk.com/call/join/..."],
  "user": "tester-1",
  "password": "per-user-secret",
  "obfs_password": "outer-secret",
  "workers": 4
}
```

`workers` and `max_workers_per_session` default to four and every other value is
rejected (frozen at 4 for the wire-v9 contract). One to four distinct join links are accepted; links are reused to
create the fixed four calls when fewer than four are supplied. The legacy-named
`call_vk_pre_kcp_admission` capability now covers the wire-v9 ACK-clocked lane
pacer and its BDP-derived 32-512 segment inflight limit.

## Host OS tuning and client socket allocation

High-throughput multi-lane operation requires adequate OS network buffer limits
and ephemeral port space to avoid client-side socket starvation (`no_ports`) and
UDP packet loss (`RcvbufErrors`):

- **Linux Sysctl Guidelines**:
  ```sysctl
  net.ipv4.ip_local_port_range = 10240 65535
  net.core.rmem_max = 16777216
  net.core.wmem_max = 16777216
  net.core.rmem_default = 2097152
  net.core.wmem_default = 2097152
  ```
- **Android VPN Protection**: Under Android VPN mode, client UDP socket allocations
  must be protected from looping back into the VPN interface, and socket buffers
  are explicitly sized to 2 MiB during TURN endpoint allocation.

## TURN worker distribution and relay endpoint allocation

To prevent all four physical workers from converging on the same TURN relay IP behind DNS round-robin and sharing a single per-IP policer, endpoint resolution deterministically sorts all resolved IPv4 addresses (`netip.Addr.Compare`) and rotates candidates by `workerID % len(addrs)`. Each candidate is attempted with full dial, STUN/TURN allocation and fallback on failure before escalating to other transports.

## Policer validation and performance criteria

When traversing token-bucket rate limiters and policers (e.g. 8 Mbit/s cap with
1-2% baseline loss), the ACK-clocked controller maintains:
- Offered-to-delivered ratio < 1.3x (preventing retransmission plateau collapse);
- Retransmission share < 15% of transmitted bytes;
- Goodput >= 90% of available bottleneck capacity without flow abort cascades;
- Demand-gated pacing probes with low inconclusive noise.

On heavy / lossy paths (policer <= 5.5 Mbit/s, loss >= 25%):
- Offered-to-delivered ratio <= 1.7x;
- Retransmission share <= 25% of transmitted bytes;
- Goodput >= 90% of available bottleneck capacity without flow abort cascades.

## Known limitations

- **IPv4-only TURN**: TURN endpoint resolution and allocation require IPv4 addresses
  (`requireIPv4`); IPv6 TURN allocation is deferred to future wire versions.
- **Platform dependency**: The transport relies completely on VK call infrastructure
  and TURN availability.
- **Fixed RTP imitation**: RTP headers use fixed Payload Type (PT=96), padding ±24B,
  and static packet distribution.
- **Flag-day wire versioning**: Wire v9 enforces strict symmetric version matching
  between client and VPS; mixed wire versions are rejected during worker auth.
- **Severe loss behavior**: At extreme loss rates (~50%), idle lanes may experience
  intermittent liveness expiration (~5%/window).
- **Asymmetric flow abort**: Pinned flow termination is unidirectional; peer state
  recovers via timeout-driven garbage collection.
- **Global singleton supervisor**: Concurrent transport gating shares a global
  singleton supervisor.
- **Tunnel state machine boundary**: `lane_tunnel.go` (~3k LOC) remains a unified
  high-performance concurrency boundary.

## Verification and publication

GitHub Actions runs Go tests, role/capability checks, Android AAR verification,
Linux runtime integration and reproducible packaging. The debug workflow then
updates the prerelease named by `release/HYDRACORE_VERSION` and publishes the
manifest, checksums, client AARs and VPS archives.
