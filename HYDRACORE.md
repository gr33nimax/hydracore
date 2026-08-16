# HydraCore distribution contract

Current debug release:
`v1.13.16-extended-hydracore.11-debug.29`.

HydraCore publishes separate client and VPS artifacts. A VK parasite deployment
must use artifacts from the same release manifest and commit; mixed wire
versions are rejected during worker authentication.

## VK parasite transport

The native mode is `vk_parasite` and uses exactly four VK calls. Each call is
one independent KCP lane with its own conversation, RTT/RTO, windows, queues,
retransmission state and TURN/DTLS lifecycle. Incompatible wire v8 pins each
ordered TCP flow to one reliable lane, stripes unordered UDP/QUIC datagrams over all four,
fragments TCP data to at most four MSS before KCP, and admits user data through
an ACK-clocked 256 Kbit/s-4 Mbit/s pacer per lane. Each reset advances a lane
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
  `call_vk_parasite_wire={"min":8,"max":8}` and
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
rejected. One to four distinct join links are accepted; links are reused to
create the fixed four calls when fewer than four are supplied. The legacy-named
`call_vk_pre_kcp_admission` capability now covers the wire-v8 ACK-clocked lane
pacer and its BDP-derived 8-64 segment inflight limit.

## Verification and publication

GitHub Actions runs Go tests, role/capability checks, Android AAR verification,
Linux runtime integration and reproducible packaging. The debug workflow then
updates the prerelease named by `release/HYDRACORE_VERSION` and publishes the
manifest, checksums, client AARs and VPS archives.
