# HydraCore v1.13.16-extended-hydracore.8

This release fixes reusable VLESS/VMess/Trojan XHTTP transport handover,
separates the Android client and Linux VPS runtime roles, adds wire-v2 network
handover for native VK Calls multi-user, retains the managed URLTest and
subscription validation fixes from `.5`/`.6`, and uses the exact
`sing-box-extended` 2.6.2 baseline
(`545424b86bc4513f90580ebeab2e2d1514089718`).

## XHTTP network handover

- An interface update now resets XHTTP's active streams and physical Xmux
  clients without permanently closing the reusable transport object.
- Dials racing with a network reset are rejected by generation, while the next
  dial lazily creates a transport bound to the new interface.
- Terminal service shutdown still closes XHTTP permanently. Other V2Ray
  transports keep their existing interface-update behavior.

## Runtime traffic telemetry

- RuntimeEvents now derives upload and download bytes per second from both
  cumulative counters and the actual observation interval instead of emitting
  permanent zero rates.

## Subscription feature contract

- A JWE or plaintext Hydra Subscription v2 document may require both `call`
  and `call_vk_multi_user` when the release advertises those capabilities.
- Builds without a Calls role continue to reject both requirements. Unknown
  feature names remain fail-closed.
- Regression coverage follows the same encrypted JWE validation path used by
  HydraBox subscription import.

## Managed URLTest

- A targeted group probes the concrete leaf selected by `Now()` while emitting
  the managed result under the originally requested group tag.
- Direct targets keep their existing result tag. Concrete URLTest history stays
  attached to the probed leaf so group health and selection remain accurate.

## Native VK Calls

- `mode: "multi_user"` hosts many independent authenticated users on one
  native UDP Calls inbound. Release artifacts do not expose legacy P2P mode.
- A shared RTP-shaped ChaCha20-Poly1305 layer makes packet unwrap O(1). User
  lookup is O(1), the password hash comparison is constant-time, and attach
  credentials are sent once inside DTLS instead of in every data packet.
- Clients can use one through four distinct VK join links and a total bounded
  worker pool distributed round-robin across them. VK TURN credentials are
  cached/singleflighted and all usable UDP relay URLs are rotated.
- One KCP conversation is striped across live workers. Authenticated heartbeat
  records evict dead TURN/DTLS paths without consuming user quota forever;
  worker loss/reconnect preserves the session. If server KCP state was reset,
  generation checks rebuild the native session behind the persistent relay.
- Users, sessions, per-user sessions, workers, pending handshakes, frame
  lengths, duplicate active workers, handshakes, reconnects, and idle state
  all have explicit hard bounds.
- Wire v2 gives every reconnecting worker a monotonic epoch. Network changes
  immediately replace stale TURN/DTLS transports while keeping the logical KCP
  session and RelayBridge alive. The VPS accepts wire v1 and v2 for one
  transition release; the client emits v2.
- Obfuscation reads reuse a bounded buffer instead of allocating the maximum
  packet size for every UDP datagram.

The exact runtime probe is:

```console
sing-box hydra capabilities --json
```

The client reports role `client`, the client feature, wire v2, and only
`multi_user`; the VPS reports role `vps`, the server feature, wire v1..2, and
only `multi_user`. The legacy combined build is not a release artifact.

## Role-specific artifacts

- `hydracore-client-libbox.aar`
- `hydracore-client-libbox-sources.jar`
- `hydracore-vps-linux-amd64.tar.gz`
- `hydracore-vps-linux-arm64.tar.gz`

Each archive contains a root executable named `sing-box` and ships with a
SHA-256 sidecar plus per-architecture provenance. The release also retains the
client Android bindings, a release manifest, the attributed source archive,
subscription contracts, checksums, and schema-v3 Android provenance.

Stable publication is explicit: ordinary pushes build and verify artifacts,
while a maintainer must dispatch the workflow with `publish=true` to update
the stable release.

## Security and capacity boundary

The VPS never joins VK and receives no VK cookies or room-creator credentials.
`obfs_password` is a trusted group secret protecting the self-signed DTLS
identity and should be rotated when group membership changes. With four rooms
and four workers per user, a 27-allocation-per-room VK limit corresponds to an
estimated 27 concurrent sessions; actual limits and throughput depend on VK,
RTT, loss, and the VPS.
