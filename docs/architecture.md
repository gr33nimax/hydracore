# HydraCore architecture — the `vk_parasite` transport

This is the deep reference for HydraCore's own transport. Everything below is grounded in
`transport/call/vk-parasite/`, `protocol/call/`, `option/call.go`, and `common/hydracore/`.
The rest of the runtime is upstream sing-box and is documented upstream.

## The idea in one paragraph

`vk_parasite` carries a QUIC connection through the media path of a VK voice call. To a
network observer the traffic looks like an ongoing WebRTC/TURN call, not a proxy. The
client opens several **independent lanes** to the server, each allocated through a TURN
relay reached via a real VK call join link; a QUIC connection rides across those lanes as
UDP datagrams wrapped to look like SRTP. The server terminates QUIC on a single UDP
socket and relays the tunnelled streams to their real destinations.

```text
app socket ──┐
             │  QUIC streams (MsgConnect/MsgData/…)  — transport/call/tunnel/protocol.go
        QUIC connection (quic-go, conn-id len 4)     — quic_conn.go / quic_relay.go
             │  QUIC packets as DATAGRAM frames
   RTP-obfuscated datagrams (ChaCha20-Poly1305)      — obfs.go
             │  one per TURN channel
        N independent lanes over TURN relays          — turn.go
             │  allocated via VK call join links
        VK call media path  ── looks like a voice call to the network
```

## Lanes (paths), not one tunnel

The client builds `Workers` **independent lanes** — a QUIC path each — and spreads traffic
across them; a lane that drops reconnects on its own without taking the others down. The
count is fixed to one of **4, 8, 12, 16, 20** (`MaximumWorkerCount = 20`,
`transport/call/vk-parasite/auth.go`; validated in `client.go:270` and `server.go:161`).
Each lane allocates through its own `join_links` entry, so a client with four lanes needs
four join links.

The supervised pool is in `client.go`; per-path quality (`PathQuality`) comes from
quic-go's own counters (`quic_relay.go`). The default path count equals the default worker
count (`quic_relay.go`, `defaultQUICPathCount = DefaultWorkerCount`).

## Wire protocol (tunnel framing)

Streams inside the QUIC connection are framed by a one-byte message kind
(`transport/call/tunnel/protocol.go`):

| Byte | Message | Meaning |
| --- | --- | --- |
| `0x01` | `MsgConnect` | open a stream to a destination |
| `0x02` | `MsgConnectOK` | destination reached |
| `0x03` | `MsgConnectErr` | connect failed |
| `0x04` | `MsgData` | stream payload |
| `0x05` | `MsgClose` | close stream |
| `0x06` / `0x07` | `MsgUDP` / `MsgUDPReply` | UDP associate |
| `0x08` / `0x09` | `MsgConfig` / `MsgConfigAck` | config exchange |
| `0x0c` | `MsgFlowCredit` | bytes consumed, returned to sender (flow control) |
| `0x0d`–`0x12` | `MsgFlow*` | wire-v10 ordered-flow migration controls (consumed by the parasite, never forwarded to the relay bridge) |

The stream header (`stream_header.go`, `writeStreamHeader`/`readStreamHeader`) carries the
message kind and a `M.Socksaddr` destination.

## Obfuscation layer

`obfs.go` (MIT, adapted from SpaceNeuroX/proxy-turn-vk-android — see
[CREDITS.md](../CREDITS.md)) wraps each datagram to resemble SRTP: a 12-byte RTP header
plus an RFC-8285 one-byte extension (`0xBEDE`), then the payload sealed with
**ChaCha20-Poly1305** (key derived with label `rtp-obfs/chacha20poly1305`). There is no
DTLS layer — it was removed in wire v10 because it lived *inside* the sealed RTP payload
and was invisible to any observer while costing 37 bytes and an AES-GCM per packet
(`mtu.go` header comment).

## MTU budget

The path budget is computed in `mtu.go`, not guessed. From a conservative mobile path MTU:

| Constant | Value | What it accounts for |
| --- | --- | --- |
| `conservativePathMTU` | 1400 | assumed mobile path MTU |
| `overheadIPUDP` | 28 | outer IP + UDP |
| `overheadTURNChannel` | 4 | TURN ChannelData |
| `overheadRTPAEAD` | 16 | RTP-obfs ChaCha20-Poly1305 tag |
| `quicConnectionIDLength` | 4 | short QUIC conn-id |
| `overheadQUICAEAD` | 16 | QUIC packet AEAD |
| `overheadDatagramFrame` | 3 | QUIC DATAGRAM frame |
| `quicPacketSize` | 1320 | resulting QUIC packet size |

`TestWrappedPacketFitsPathMTU` measures the real wrapped packet rather than re-deriving
these constants, so the numbers can't silently drift from `rtpCodec.wrap`.

## TURN edge

Lanes allocate through TURN relays (`turn.go`). `TURNCredentials` are fetched per join link
by a `CredentialProvider` (`vk.NewTURNCredentialProvider`, wired in `bridge.go`). Endpoint
health is tracked with success/failure penalties
(`recordTURNEndpointSuccess`/`recordTURNEndpointFailure`, `getTURNEndpointPenalty`) so the
pool prefers TURN edges that actually answered. The client also runs a workerless edge
probe (STUN Binding to the TURN edge) surfaced to the app as the server "ping"
(`common/hydracore/turn_edge.go`, and `TurnEdgeProbe` on the client side).

## Health, failure, recovery (supervisor)

`common/hydracore` defines the health contract the Android app reads; the client fills it
in `client.go healthSnapshot`. States (`TransportState*`):

| State | When (`client.go:195-214`) |
| --- | --- |
| `Healthy` | at least one active path |
| `Recovering` | paths were up and are being rebuilt, and the failure is retryable |
| `Failed` | a terminal failure, **or** a failure before the first path ever came up (`!sawPath`) |

The distinction is deliberate: `sawPath` (not just `Terminal`) is the key. Reporting
`Failed` while the pool is merely rebuilding retryable paths would trip the app's fail-fast
and stop the tunnel; reporting `Recovering` before the very first path came up would hide a
genuine startup failure. (This is the fix recorded in the connectivity bugfix work — a
retryable failure with `sawPath` true stays `Recovering`.)

Health is published under a **network generation** (`PublishTransportHealth`,
`common/hydracore/network_generation.go`): when the underlying network changes, the
generation is raised in the core *before* the tunnel rebinds, so a snapshot measured on the
old network is never applied to the new one.

## Integration with sing-box

- `option/call.go` — the `"type": "call"` config surface: `CallInboundOptions` (VPS),
  `CallOutboundOptions` (client), shared `CallCommonOptions`. See
  [configuration.md](configuration.md) for every field.
- `protocol/call/` — registers the `call` inbound/outbound so routing, DNS, TLS and the
  rest of sing-box treat it like any other protocol.
- `transport/call/config.go` — dispatches `mode: vk_parasite` to
  `vkparasite.ConnectBridge`; other platforms (telemost/vk/wbstream/…) are upstream call
  transports and are not part of the HydraCore product surface.
- `experimental/libbox/` — the Android runtime (gomobile AAR): runtime commands,
  snapshots, URL-test, and `HydraCoreBuildInfo()` which stamps the pinned upstream
  baseline (verified against `release/UPSTREAM_BASELINE`).

## Client / server roles

- **Client (joiner)** — `mode: vk_parasite` outbound, built with `with_call_client`. Opens
  the lanes, runs the QUIC connection, tunnels app traffic. This is what HydraBox ships in
  its AAR.
- **Server (VPS)** — `mode: vk_parasite` inbound, built with `with_call_server`. One UDP
  socket (`server_socket*.go`), terminates QUIC, relays streams. The creator role is
  hosted by the native inbound, so the client is always the joiner
  (`transport/call/config.go`).

Client and VPS artifacts must come from **one release** — mixed wire versions are rejected
at worker authentication (`auth_handshake.go`, `exchangeAuth`).
