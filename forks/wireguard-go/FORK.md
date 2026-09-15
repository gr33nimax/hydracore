# wireguard-go fork

Vendored copy of the WireGuard userspace implementation HydraCore's WireGuard endpoint runs on.

## Origin

- Upstream project: `github.com/sagernet/wireguard-go`
- Fork this copy came from: `github.com/shtorm-7/wireguard-go@v0.0.5-extended-1.6.1`
- Vendored here so HydraCore builds from its own tree instead of a third-party module tag, and so a
  protocol fix can be committed next to the code that depends on it.

## Why vendored

The AmneziaWG transport depends on behaviour that lives in this fork (junk packets, `S1`–`S4` padding,
`H1`–`H4` headers, header protection, random trailers). When one of those paths is wrong, the fix has to
land where HydraCore's release pipeline can pick it up; a module tag owned by someone else cannot be that
place.

## Our patches

### 1. Random trailers no longer break the handshake

`device/send.go` marshalled the handshake messages into the padded buffer, which is longer than the
message whenever `random_trailers` is on. Two consequences, both silent:

- `msg.marshal(packet)` / `response.marshal(packet)` / `reply.marshal(packet)` return
  `errMessageLengthMismatch` for a longer buffer, and the callers ignored the error with `_ =`, so the
  packet went out with an all-zero message — the peer could not classify it (`received message with
  unknown type`).
- `cookieGenerator.AddMacs(packet)` writes the MACs into the last 32 bytes of the slice it is given and
  computes them over that slice, so the MACs landed in the trailer and the peer rejected the packet with
  `received packet with invalid mac1`.

The fix gives each call exactly its own message (`packet[:MessageInitiationSize]`,
`packet[:MessageResponseSize]`, `packet[:MessageCookieReplySize]`), matching what the receive side already
does when it trims a packet to its message size.

Evidence: two HydraCore instances on one host, traffic through the tunnel. Before the fix 3.1 (random
trailers on) never completed a handshake; after it, 2.0, 3.0 and 3.1 all pass with traffic.

## Known gap

The Windows ring-I/O receive path (`conn/bind_windows.go`) delivers payload bytes 1–3 of every datagram as
zeroes, which corrupts the header-protection nonce and makes any protected generation unusable on Windows.
Linux and Android use other receive paths and are unaffected. Found while debugging on Windows; not fixed
here because the product does not ship a Windows runtime.

## Updating this copy

Re-copy the upstream/fork tree over this directory, then re-apply the patches above and re-run the
interoperability stand (`/root/.awg-spike/` on the test host) before releasing a core that carries it.
