# HydraCore release notes

This client release completes the staged VK Parasite 4x4 rollout:

- **VK Parasite 4x4 Topology**: Expands transport capacity to 16 independent KCP lanes
  mapped across up to 4 concurrent VK calls (4 workers per call).
- **Dual Server Compatibility**: VPS listener dynamically admits both legacy 4-lane
  and modern 16-lane client sessions with quorum-based session replacement.
- **Clean CI / Release Pipeline**: Consolidated runtime allowlist, eliminated intermediate
  sidecars, legacy Android API 21 output and obsolete performance benchmarks. Releases are
  immutable and contain exactly nine runtime assets.
- **Client 4x4 activation**: The client now creates 16 independent KCP lanes mapped
  across up to four VK calls.
- **Reduced capability contract**: Internal milestone and wire-version fields are
  removed. Signed bundle manifests advertise core API major 2 so old clients reject
  the new shape before activation.
- **Outbound external info**: Lookup now uses one bounded primary/fallback request
  path without process-level cache or singleflight state.
