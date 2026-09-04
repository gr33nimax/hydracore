# HydraCore release notes

This client release makes transport health part of the typed runtime stream:

- **Scoped health events**: Every VK transport report carries its outbound tag and runtime
  generation, so a stopped or previously selected transport cannot affect the active route.
- **Event-driven delivery**: Material lane, state, challenge and failure changes wake the
  existing runtime event stream without JSON polling across JNI.
- **Stable Android binding**: Runtime snapshots and deltas expose typed transport health and
  failures through the existing gomobile command client.
