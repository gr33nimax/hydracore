# Quick bugfix spec: AWG transport padding safety

## Status

Quick Spec approved by the user's directive «расширяй и исправляй».

## Current behavior

### A. Outbound encryption panic

HydraBox on Android reproduced a native-core crash while starting an AWG profile:

```text
panic: runtime error: slice bounds out of range [:278] with capacity 256
.../device.(*Device).RoutineEncryption
```

`forks/wireguard-go/device/send.go` derives slice bounds directly from `elem.padding` (`S4`) in `RoutineEncryption`. The checkout's normal allocation paths reserve S4 space, but this production trace proves that an element with a smaller buffer reached encryption. The exact allocation provenance in the shipped Android binary cannot be proven from this detached HydraCore checkout; therefore the fix must preserve the buffer-layout invariant at the encryption boundary rather than assert an unproven single cause.

### B. Silent loss with random trailers

With `random_trailers=true`, `DeterminePacketTypeAndPadding` tests H1–H3 handshake headers before H4 transport. A transport datagram whose padded bytes collide with an H1–H3 range is classified as a handshake, later fails MAC1 validation, and is discarded. The current checkout contains this ordering. Upstream report: <https://github.com/amnezia-vpn/amneziawg-go/issues/186>.

## Expected behavior

- WHEN an outbound element has a valid plaintext packet and S4 exceeds its current buffer capacity, THEN encryption SHALL allocate a correctly sized replacement buffer, preserve the plaintext, and continue without panic.
- WHEN the required encrypted packet cannot fit the protocol maximum or allocator capacity, THEN the element SHALL be dropped safely and the worker SHALL continue; it SHALL neither panic nor emit a malformed packet.
- WHEN `random_trailers=true` and an H1–H3-looking datagram fails handshake MAC1, THEN the receiver SHALL retry H4 transport classification before dropping it.
- WHEN a genuine handshake has a valid MAC1, THEN it SHALL retain the existing handshake path and trailer semantics.

## Unchanged behavior

- Valid WireGuard/AWG wire format, S1–S4, H1–H4, header protection, random trailers, and packet encryption remain compatible.
- Header ranges remain validated as non-overlapping by UAPI.
- The fix does not log private keys, profile contents, or user traffic.

## Acceptance criteria

- [ ] A 256-byte outbound buffer with S4=278 is repaired before encryption; the supplied plaintext survives in the replacement buffer and no slice panic occurs.
- [ ] A required size above `MaxMessageSize` is dropped without panic or malformed output.
- [ ] A crafted random-trailer transport candidate that collides with H1 is classified as H4 transport after failing MAC1.
- [ ] A valid MAC1 handshake candidate remains a handshake.
- [ ] Existing targeted WireGuard/AWG tests plus new focused regression tests pass.
- [ ] HydraCore builds successfully for its Go target.

## Evidence

- ADB reproduction on `io.hydrabox.client` v`2.0.0-alpha4`: `slice bounds out of range [:278] with capacity 256` in `RoutineEncryption`.
- `send.go`: unguarded `buf[:elem.padding]`, `buf[elem.padding:elem.padding+MessageTransportHeaderSize]`, and Seal destination slices.
- `pools.go`: normal callers use `buf.Get(size)`; `NewOutboundElement` asks for `MaxMessageSize`, while direct injection reserves S4. This does not explain the device trace, hence boundary hardening.
- `receive.go`: H1, H2, H3 are tested before H4 whenever random trailers permit a variable-length handshake.
