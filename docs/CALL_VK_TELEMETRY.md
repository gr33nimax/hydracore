# VK Calls telemetry

Telemetry is enabled only while the operator-managed active session marker is
present. The Linux VPS reads:

```text
/var/lib/hydra/calls/vk/telemetry/active.json
```

It writes schema-1 JSON Lines to:

```text
/run/hydra/calls-telemetry.jsonl
```

Files are mode `0600`, rotate at 64 MiB, and carry a sibling `.session` marker.
The VPS accepts client records only through the authenticated transport. It
replaces client-supplied identity and timestamp with the authenticated user,
session ID, server receipt time, and session generation; the original client
time is retained as `origin_timestamp`.

## Record format

Each record has `schema`, `timestamp`, `scope`, `kind`, and `metrics`.

- `scope`: `server` or `client`;
- `kind`: `snapshot` or `event`;
- identity fields: `user`, `session_id`, `session_generation`;
- event fields: `worker_id`, `event`, `stage`, `reason`.

Metric names are a fixed allowlist. The current set covers authentication,
TURN/DTLS, UDP ingress, QUIC paths, streams, congestion, datagrams, path
replacement, queue pressure, runtime CPU/RSS/GC, and telemetry delivery.

Records never include passwords, join links, TURN URLs, payloads, IP addresses,
or DNS names.

## Operational notes

The core creates no experiment timer and does not export records. The operator
owns session lifecycle, draining, retention, and analysis. Invalid, oversized,
symlinked, or mismatched marker files disable collection.
