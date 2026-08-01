# Contributing to HydraCore

HydraCore accepts focused fixes that improve the networking runtime used by
HydraBox while preserving the pinned upstream lineage.

## Pull requests

- Target the `extended-integration` branch.
- Keep upstream syncs, mobile integration, and behavior changes in separate
  commits when practical.
- Add regression coverage for runtime behavior and capability changes.
- Do not add a remotely activatable field without matching HydraBox validation
  and an explicit remote-safety policy revision.
- Do not include real server addresses, subscription URLs, credentials, UUIDs,
  or customer data in tests, logs, or issues.
- Do not change the Go module path, native package names, inherited capability
  alias, or artifact name as part of a cosmetic rename.

GitHub Actions is authoritative. A change is not ready to merge until the Go,
race/resource, baseline, and Android AAR jobs required for its scope pass.

## Upstream changes

When a problem reproduces in an unmodified upstream project, link the upstream
issue or fix and keep attribution intact. HydraCore-specific support requests
belong in this repository rather than upstream Etonify or sing-box channels.
