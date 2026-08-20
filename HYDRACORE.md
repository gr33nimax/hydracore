# HydraCore distribution contract

HydraCore publishes separate client and VPS artifacts. A VK parasite deployment
must use artifacts from the same release manifest and commit; mixed wire
versions are rejected during worker authentication.

## VK parasite transport

The native mode is `vk_parasite` using a 4x4 topology: 16 independent KCP lanes
distributed evenly across up to 4 distinct VK calls (4 workers per call). Each
worker maintains its own conversation, RTT/RTO, windows, queues, retransmission
state, and TURN/DTLS lifecycle. Wire v9 pins each ordered TCP flow to one reliable
lane, stripes unordered UDP/QUIC datagrams across active lanes, fragments TCP data
before KCP, and admits user data through an ACK-clocked pacer per lane.

Each lane reset advances a generation carried by worker auth and lane frames.
The VPS suggests recovery to the client-side coordinator; the two endpoints
exchange `RESET_PREPARE`, `RESET_ACK` and `RESET_COMMIT`, discard old KCP state,
and activate the new lane upon bidirectional probe completion. A lane that does
not return aborts only its pinned flows; remaining lanes and the logical tunnel
remain operational. Quorum-based session replacement triggers when 75% of lanes
are quarantined.

The complete implementation is in `transport/call/vk-parasite`. Files outside
that directory only register the sing-box inbound/outbound and map configuration
into the package.

The role contract is:

- client: `identity.role="client"`, `call_vk_parasite=true`, `call_vk_parasite_client=true`;
- VPS: `identity.role="vps"`, `call_vk_parasite=true`, `call_vk_parasite_server=true`;
- both: `call_vk_telemetry=true`, `protocols.call_modes=["vk_parasite"]`.

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
  "max_workers_per_session": 16,
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
  "join_links": [
    "https://vk.com/call/join/call-0",
    "https://vk.com/call/join/call-1",
    "https://vk.com/call/join/call-2",
    "https://vk.com/call/join/call-3"
  ],
  "user": "tester-1",
  "password": "per-user-secret",
  "obfs_password": "outer-secret",
  "workers": 16
}
```

`workers` defaults to 16 on the client. `max_workers_per_session` on the VPS supports
both legacy 4-lane and modern 16-lane clients. Between 1 and 4 join links are accepted;
links are rotated round-robin across the 16 workers.

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
  are explicitly sized during TURN endpoint allocation.

## TURN worker distribution and relay endpoint allocation

To prevent physical workers from converging on the same TURN relay IP behind DNS round-robin, endpoint resolution deterministically sorts all resolved IPv4 addresses (`netip.Addr.Compare`) and rotates candidates by `workerID % len(addrs)`. Each candidate is attempted with full dial, STUN/TURN allocation and fallback on failure before escalating.

## Verification and publication

GitHub Actions runs Go tests, role/capability checks, Android AAR verification,
Linux runtime integration and reproducible packaging. Publication is an explicit
manual action and creates an immutable release containing exactly the signed bundle
manifest, its signature, three Android shared libraries, the client AAR and sources,
and two VPS archives.
