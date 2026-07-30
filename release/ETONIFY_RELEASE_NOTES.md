## Etonify Extended Core Android test build

This test build integrates Etonify's mobile libbox API with
`sing-box-extended` `v1.13.14-extended-2.5.3`.

### Included

- Every runnable inbound, outbound, endpoint, DNS transport, V2Ray transport,
  and optional service registered by the extended core.
- Etonify targeted/group URLTest, resilient external-IP lookup, capability
  discovery, bounded XHTTP client, VLESS post-quantum encryption, Reality
  `spider_x`, and mobile resource safeguards.
- Reproducible Android AAR inputs and provenance pinned in
  `release/ETONIFY_BASELINE`.

### Compatibility notes

- ShadowsocksR inbound/outbound and the legacy WireGuard outbound remain
  intentional upstream stubs. ShadowsocksR is not a runnable extended
  protocol; WireGuard is available through the registered WireGuard endpoint.
- The pinned extended WireGuard implementation does not expose the old
  three-byte `reserved` bind override. Zero bytes are equivalent to no
  override; non-zero legacy values must use a compatible endpoint such as the
  dedicated WARP endpoint and are rejected by the app rather than ignored.
- Device validation is still required before replacing a production AAR.
