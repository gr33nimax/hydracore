# HydraCore distribution contract

HydraCore releases separate Android client and Linux VPS runtimes. Deployments
must use artifacts from one release manifest and source commit. The client role
exposes the Call outbound; the VPS role exposes the Call inbound.

## `vk_parasite`

`vk_parasite` carries authenticated QUIC streams and datagrams through VK Call
paths. Each path uses VK signalling, TURN, an RTP-shaped encrypted wrapper, and
QUIC. The protocol version is 10; mixed protocol versions are rejected during
worker authentication.

Version 10 removed the DTLS layer. It ran underneath the RTP wrapper, so every
byte of it — handshake included — travelled inside the sealed
ChaCha20-Poly1305 payload and was never visible to an observer on the path,
while costing 37 bytes and one AES-GCM pass per packet in each direction. The
worker authentication frame and QUIC now travel directly in the wrapper.

The client requires exactly four distinct `join_links`. It starts four paths by
default. `workers` and `max_workers_per_session` accept only `4`, `8`, `12`,
`16`, or `20`; workers are distributed round-robin across the four links. A
failed non-terminal path reconnects independently. A network change replaces
the current path generation.

Required capability fields are:

```json
{
  "features": {
    "call_vk_parasite": true,
    "call_vk_parasite_quic": true
  },
  "protocols": {"call_modes": ["vk_parasite"]}
}
```

The client build also has `call_vk_parasite_client`; the VPS build has
`call_vk_parasite_server`.

### VPS inbound

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
  "users": [{"name": "tester-1", "password": "per-user-secret"}]
}
```

### Client outbound

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
  "workers": 4
}
```

Credentials and join links are secrets. Do not commit real values.

## Build and verification

The release baseline, Go version, Android API, NDK, JDK, gomobile version, and
build tags are pinned in [release/UPSTREAM_BASELINE](release/UPSTREAM_BASELINE).

`mode` accepts only `vk_parasite`, so the other Call platforms — Telemost,
wbstream, VK P2P and Dion — are behind `with_call_legacy` and are absent from
release builds. They carry the whole `pion/webrtc` stack, which nothing in
`vk_parasite` uses.
CI builds all distributable artifacts. Local checks may use `go test`, `go vet`,
`go build`, and the baseline verifier, but must not produce release artifacts.

## Related contracts

- [Hydra Subscription v2](contract/subscription/HYDRA_SUBSCRIPTION_V2.md)
- [Release notes](release/HYDRACORE_RELEASE_NOTES.md)
