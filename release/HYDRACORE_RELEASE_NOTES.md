# HydraCore 1.14.3 release notes

This build carries the stable runtime fixes into the sing-box-extended
`v1.14.3-extended-2.7.3` baseline. The debug build is
`hydracore-sbe-1.14.3-debug-2`; the Call transport and local TURN, DTLS and
AmneziaWG patches are unchanged.

## Automatic route and runtime events

- Every new connection publishes its initial automatic route before background latency
  probes start, including reconnects and configurations with background checks disabled.
- Selection changes notify runtime observers after the choice is committed.
- Runtime event subscriptions start before the initial snapshot is read and sent. Changes
  made while that snapshot is being delivered remain queued for the client.
- Probe budgets, tolerance and scoped resource cleanup retain the SBE 1.14.3 behaviour.

## Release validation

The release verifier accepts independent stable revision counters (`-stable-<n>`) and
checks that the release version matches the pinned SBE baseline. Pending upstream merges
are checked before committing, with committed ancestry verified afterwards.

## Compatibility and assets

Transport wire version 10, client ABI 2 and the runtime identity `io.hydrabox.hydracore`
are unchanged. Use client and VPS artifacts from the same release manifest; clients older
than wire version 10 remain incompatible with wire version 10 servers.

Published releases include the Android AAR and sources, Android shared libraries,
Linux amd64 and arm64 archives, and a signed bundle manifest.

## Rollback

The previous debug build is `hydracore-sbe-1.14.3-debug-1` on the same SBE baseline.
Earlier release history remains in `CHANGELOG.md`.
