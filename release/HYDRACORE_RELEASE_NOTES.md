# HydraCore v1.13.16-extended-hydracore.6

This stable patch release aligns Hydra Subscription validation with the
advertised native VK Calls multi-user capability and retains the managed
URLTest identity fix from `.5`, the Linux/VPS distribution, and the exact
`sing-box-extended` 2.6.2 baseline
(`545424b86bc4513f90580ebeab2e2d1514089718`).

## Subscription feature contract

- A JWE or plaintext Hydra Subscription v2 document may require both `call`
  and `call_vk_multi_user` when the release advertises those capabilities.
- Builds without `with_call` continue to reject both requirements. Unknown
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
  native UDP Calls inbound. Legacy missing/`p2p` mode remains unchanged.
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

The exact runtime probe is:

```console
sing-box hydra capabilities --json
```

It reports `features.call_vk_multi_user=true` and
`protocols.call_modes=["p2p","multi_user"]` in release builds.

## VPS artifacts

- `hydracore-linux-amd64.tar.gz`
- `hydracore-linux-arm64.tar.gz`

Each archive contains a root executable named `sing-box` and ships with a
SHA-256 sidecar plus per-architecture provenance. The release also retains the
Android `libbox.aar`, generated bindings, attributed source archive,
subscription contracts, checksums, and schema-v3 Android provenance.

## Security and capacity boundary

The VPS never joins VK and receives no VK cookies or room-creator credentials.
`obfs_password` is a trusted group secret protecting the self-signed DTLS
identity and should be rotated when group membership changes. With four rooms
and four workers per user, a 27-allocation-per-room VK limit corresponds to an
estimated 27 concurrent sessions; actual limits and throughput depend on VK,
RTT, loss, and the VPS.
