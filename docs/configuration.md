# HydraCore configuration reference — `type: call`

HydraCore adds one config type to sing-box: `"type": "call"`. Everything else in the
config document is upstream sing-box (routing, DNS, TLS, other inbounds/outbounds). Fields
below are from `option/call.go`; validation from `transport/call/vk-parasite/`.

Only `mode: vk_parasite` is a HydraCore product mode. The client is always the **joiner**;
the creator role is hosted by the native inbound (`transport/call/config.go`).

> **Secrets.** `user`/`password`, `obfs_password`, `join_links` and cookies are secrets.
> Never commit real values. The examples use placeholders.

## Common fields (`CallCommonOptions`)

Shared by inbound and outbound:

| Field | Type | Meaning |
| --- | --- | --- |
| `platform` | string | call platform; `vk` for `vk_parasite` |
| `mode` | string | transport mode; only `vk_parasite` is a HydraCore product mode |
| `read_buffer` | int | read buffer size (bytes) |
| `max_buffered_amount` | int | max buffered bytes before backpressure |
| `memory_limit` | int64 | soft memory ceiling |

## VPS inbound (`CallInboundOptions`)

```json
{
  "type": "call",
  "tag": "call-vk-server",
  "platform": "vk",
  "mode": "vk_parasite",
  "listen": "0.0.0.0",
  "listen_port": 8443,
  "obfs_password": "OUTER_SECRET",
  "max_workers_per_session": 4,
  "users": [
    { "name": "tester-1", "password": "PER_USER_SECRET" }
  ]
}
```

| Field | Type | Meaning |
| --- | --- | --- |
| `listen` | addr | bind address |
| `listen_port` | uint16 | UDP port (one shared socket; QUIC demuxes by connection ID) |
| `obfs_password` | string | outer RTP-obfs secret; must match the client |
| `users[]` | list | `{name, password, max_sessions?}` — per-user credentials |
| `max_sessions` | int | server-wide session cap |
| `max_workers_per_session` | int | must be **4, 8, 12, 16, or 20** (`server.go:161`) |
| `max_pending_handshakes` | int | in-flight handshake cap |
| `handshake_timeout` | duration | per-handshake deadline |
| `session_idle_timeout` | duration | idle session eviction |
| `udp_receive_buffer_bytes` / `udp_send_buffer_bytes` | int | socket buffer tuning |
| `cookies[]` | list | inline `{name, value}` cookies for the call platform |
| `join_link` | string | creator-side join link (platform-hosted role) |

Deprecated and ignored (QUIC now demultiplexes on the shared listener, so no per-peer
queueing): `ingress_workers`, `ingress_queue_packets`, `peer_read_queue_packets`.

`email`/`password` on the inbound are only for re-authenticating with the `dion.vc`
platform when a refresh cookie is missing — not part of the `vk_parasite` path.

## Client outbound (`CallOutboundOptions`)

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
  "password": "PER_USER_SECRET",
  "obfs_password": "OUTER_SECRET",
  "workers": 4
}
```

| Field | Type | Meaning |
| --- | --- | --- |
| `server` / `server_port` | via `ServerOptions` | VPS address |
| `join_links[]` | list | one join link **per lane**; count should match `workers` |
| `join_link` | string | single-link form (use `join_links` for multi-lane) |
| `user` / `password` | string | credentials matching a server `users[]` entry |
| `obfs_password` | string | outer RTP-obfs secret; must match the server |
| `workers` | int | number of lanes; must be **4, 8, 12, 16, or 20** (`client.go:270`) |
| `worker_connect_timeout` | duration | per-lane connect deadline |
| `cookies[]` | list | inline `{name, value}` cookies |

The outbound also carries the standard sing-box `DialerOptions` (bind interface, detour,
etc.) since it embeds `DialerOptions`.

## Rules that fail closed

- `workers` / `max_workers_per_session` outside `{4, 8, 12, 16, 20}` → config rejected
  (`MaximumWorkerCount = 20`, `auth.go`).
- `mode` other than `vk_parasite` on the VPS inbound → rejected (`vk_parasite` is joiner-
  only from the client; creator is the native inbound).
- Client and VPS built from **different releases** → rejected at worker authentication
  (mixed wire versions, `auth_handshake.go`).

## Verifying the runtime contract

The VPS core reports what HYDRA-ULTIMATE must check before starting it:

```bash
sing-box hydra contract --json
# {"contract_version":1,"core_id":"io.hydrabox.hydracore","role":"vps","calls_mode":"vk_parasite"}

sing-box hydra capabilities --json   # legacy capability document for older HYDRA
```

Both subcommands require `--json` (`cmd/sing-box/cmd_hydra.go`). See
[ecosystem.md](ecosystem.md) for how consumers use these.
