# Design: AWG transport padding safety

## Scope

Two local changes in `forks/wireguard-go/device` repair independent failures at the protocol boundary. No UAPI format, dependency, or architecture changes.

## A. Encryption-buffer invariant

`RoutineEncryption` will calculate content padding first, then compute the complete required layout:

```text
encapsulation + S4 + transport header + plaintext + content padding + AEAD tag
```

Before creating any S4-derived slice it will:

1. Drop an element if that layout exceeds `MaxMessageSize`.
2. If its buffer capacity is too small, acquire a correctly sized outbound buffer, copy plaintext to the normal post-header offset, return the old buffer, and replace `elem.buffer`/`elem.packet`.
3. Otherwise extend the buffer length to the required layout so the existing Seal path retains its backing array.

This repairs a malformed/legacy allocation at the last shared boundary. Normal `NewOutboundElement`, `InputPacket`, and `InputPackets` allocations stay unchanged.

## B. Ambiguous random-trailer packet classification

`DeterminePacketTypeAndPadding` will expose the existing H4/S4 transport check as a small internal helper. `RoutineReceiveIncoming` will use it only when a random-trailer packet first looks like H1–H3 *and* also looks like H4:

- Initiation/response: copy the fixed-size candidate, undo header protection in that copy, and accept its existing priority only when MAC1 validates. A failed MAC1 selects H4 transport without mutating the received datagram.
- Cookie reply: it has no MAC1. For an H3/H4 ambiguity, prefer H4 transport; a genuine cookie that randomly collides with H4 can be retried, while silently dropping established traffic is worse.
- No H4 match or `random_trailers=false`: retain the existing path exactly.

This preserves a valid handshake, avoids decrypting raw data under the wrong header offset, and leaves a malformed/unknown datagram on the existing drop path.

## Error handling

| Condition | Action |
| --- | --- |
| Required outgoing layout > `MaxMessageSize` or allocator returns no buffer | Drop this element; log only size/context; continue worker. |
| H1/H2 candidate has valid MAC1 | Keep handshake classification. |
| H1/H2 candidate fails MAC1 and H4 matches | Classify as transport. |
| H3/H4 ambiguity | Classify as transport. |
| No valid classification | Existing unknown-message drop. |

## Tests

1. Unit test the buffer-repair helper/layout using capacity 256 and S4=278; assert replacement capacity and byte-identical plaintext.
2. Unit test oversized layout is rejected without panic.
3. Loopback-UDP regression with `random_trailers=true`, `S1=16`, `S4=0`, broad H1 and a disjoint single-value H4. Its encrypted transport byte at S1 almost certainly collides with H1; assert data reaches the peer. The old ordering deterministically misclassifies it.
4. Control: a valid initiation/response still completes under random trailers.

## Deliberate limits

- The Android binary's 256-byte allocator path is not in this checkout, so this fix hardens the observed shared failure boundary rather than pretending to locate a source unavailable here.
- The selected cookie tie-break may cause a rare handshake retry for an H3/H4 byte collision; it never corrupts data and avoids the established-session data loss.
