# HydraCore release notes

This server-first release introduces the VK Parasite 4x4 topology without
changing the client default:

- **VK Parasite 4x4 Topology**: Expands transport capacity to 16 independent KCP lanes
  mapped across up to 4 concurrent VK calls (4 workers per call).
- **Dual Server Compatibility**: VPS listener dynamically admits both legacy 4-lane
  and modern 16-lane client sessions with quorum-based session replacement.
- **Clean CI / Release Pipeline**: Consolidated runtime allowlist, eliminated intermediate
  sidecars, legacy Android API 21 output and obsolete performance benchmarks. Releases are
  immutable and contain exactly nine runtime assets.
- **Staged rollout**: The client remains on four lanes in this release. A later client
  release may select 16 lanes after the VPS rollout is observed.
