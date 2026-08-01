# HydraCore

HydraCore is the public distribution and mobile-integration identity used by
HydraBox. It is an independent derivative of **etonify-core**, which in turn is
maintained on top of the pinned `sing-box-extended` baseline documented in
[ETONIFY_CORE.md](ETONIFY_CORE.md).

HydraCore does not claim authorship of Etonify, etonify-core, sing-box, or any
third-party dependency, and is not presented as an official Etonify or
MeowTeam release. Existing copyright notices, source history, and license
files remain authoritative and must accompany redistributed source and
binaries.

## Compatibility identity

New bindings expose `libbox.HydraCoreCapabilities()` and identify the runtime
as `io.hydrabox.hydracore`. The former `libbox.EtonifyCapabilities()` entry
point remains as a deprecated alias so already-generated bindings continue to
work. The capability document includes `upstream_project: "etonify-core"` to
keep the provenance machine-readable.

The Go module path and native `libbox` artifact names intentionally remain
unchanged: they are upstream schema and ABI compatibility surfaces, not public
product branding.

## Remote subscription policy

The capability document publishes remote policy v2 as an exact allowlist, not
as a claim that every field of every HydraCore schema type is safe for an
untrusted remote publisher. v2 permits only `$schema`, `outbounds`, and
`endpoints`; its outbound list contains leaf client protocols and its endpoint
list contains userspace WireGuard plus the subscription-only `wdtt` endpoint.
Policy v2 inherits every v1 graph, authority, and resource restriction. WDTT
additionally requires authenticated HydraBox JWE and a closed endpoint schema;
the stable device identity, local bridge, TURN credentials, and dynamic
WireGuard authority remain owned by HydraCore. Classic AmneziaWG parameters
are covered. DNS servers, providers, rule sets, composite/reverse types, local
paths and service-capable objects require a later policy and explicit recursive
validation. Older AARs without this manifest must fail closed.

The WDTT transport is built with `with_wdtt`, starts lazily on first endpoint
use, and routes WDTT DNS, VK HTTPS, and TURN UDP through HydraCore's
dialer/resolver/protect boundary. HydraBox rejects direct WDTT links and raw
sing-box imports; the only supported product path is a profile in encrypted
HydraBox Subscription v1 whose entrypoint names a policy-v2 WDTT endpoint.
