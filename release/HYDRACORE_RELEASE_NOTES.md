# HydraCore 1.14.1 stable revision 1 release notes

This patch fixes reporting of the automatic route to HydraBox. The sing-box-extended
baseline remains `v1.14.1-extended-2.7.2`. This release is
`hydracore-sbe-1.14.1-stable-1`: the stable revision increases independently of the
upstream SBE version.

## Automatic route

- Each new connection publishes the actual initial outbound before background latency
  probes start. Reconnecting no longer leaves the route name empty while a long probe
  batch runs. This also works when background checks are disabled.
- After a selection changes, the runtime stream is notified after the choice is committed.
  A cached group snapshot can no longer retain the previous choice just because its
  history notification arrived before the selection update.
- Probe budgets, selection tolerance and interruption of existing connections are unchanged.
  Regression tests cover two successive connections without probe results, initial choice
  publication, selection changes and suppression of redundant selection notifications.
- Runtime event subscriptions are established before the initial snapshot is read and sent.
  Changes made while the first snapshot is being delivered remain queued for the stream;
  a regression test covers this on two successive subscriptions.

## Compatibility and assets

Transport wire version 10, client ABI 2 and the runtime identity `io.hydrabox.hydracore`
are unchanged from 1.14.1. No configuration or server upgrade is required for this fix.
Clients older than wire version 10 remain incompatible with wire version 10 servers.

Every published release is verified in CI and includes the Android AAR and sources,
Android shared libraries, Linux amd64 and arm64 archives, and a signed bundle manifest.
Use the published artifacts from one release manifest.

## Rollback

The rollback target is `hydracore-sbe-1.14.1`, which uses the same wire version and client
ABI. Earlier release history remains in `CHANGELOG.md`.
