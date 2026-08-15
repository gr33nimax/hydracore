# HydraCore distribution contract

Current debug release:
`v1.13.16-extended-hydracore.11-debug.18`.

HydraCore publishes separate client and VPS artifacts. A VK parasite deployment
must use artifacts from the same release manifest and commit; mixed wire
versions are rejected during worker authentication.

## VK parasite transport

The native mode is `vk_parasite` and uses exactly four VK calls. Each call is
one independent KCP lane with its own conversation, RTT/RTO, windows, queues,
retransmission state and TURN/DTLS lifecycle. Wire v5 bonds relay frames across
the four lanes, restores order per proxied connection, controls bulk admission
before KCP from actual KCP/output-queue headroom without a fixed-rate pacer,
and bounds each TCP flow with end-to-end byte credit.

The complete implementation is in `transport/call/vk-parasite`. Files outside
that directory only register the sing-box inbound/outbound and map configuration
into the package.

The role contract is:

- client: `identity.role="client"`, `call_vk_parasite_client=true`;
- VPS: `identity.role="vps"`, `call_vk_parasite_server=true`;
- both: `call_vk_eight_lane_kcp=true`, `call_vk_pre_kcp_admission=true`,
  `call_vk_relay_flow_control=true`, `call_vk_telemetry=true`,
  `call_vk_parasite_wire={"min":5,"max":5}` and
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

`workers` and `max_workers_per_session` default to four. The legacy value eight
is normalized to four at the HydraCore boundary so already-issued subscriptions
continue to work after a kernel-only update; every other value is rejected. The
legacy-named `call_vk_eight_lane_kcp` capability is retained for those current
subscription documents and no longer describes the physical lane count. One to
four distinct join links are accepted; links are reused to create the fixed
four calls when fewer than four are supplied. The legacy-named
`call_vk_pre_kcp_admission` capability now means queue-headroom admission; it no
longer installs the debug.17 token bucket or imposes a configured bitrate.

## Verification and publication

GitHub Actions runs Go tests, role/capability checks, Android AAR verification,
Linux runtime integration and reproducible packaging. The debug workflow then
updates the prerelease named by `release/HYDRACORE_VERSION` and publishes the
manifest, checksums, client AARs and VPS archives.
