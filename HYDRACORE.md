# HydraCore distribution contract

HydraCore is the maintained networking runtime used by HydraBox. It is an
independent derivative of `etonify-core`, built from the pinned
`sing-box-extended` baseline documented in [ETONIFY_CORE.md](ETONIFY_CORE.md).
It does not claim upstream authorship, affiliation, or endorsement.

## Public identity and compatibility

New bindings expose `libbox.HydraCoreCapabilities()` and identify the runtime
as `io.hydrabox.hydracore`. The inherited
`libbox.EtonifyCapabilities()` entry point remains a deprecated alias so
existing generated bindings continue to work.

The capability document keeps `upstream_project: "etonify-core"`. The Go module
path, native package names, and `libbox.aar` artifact name also remain unchanged.
These values are ABI, schema, and source-compatibility surfaces rather than
public product branding.

## Supported artifact

The Android AAR is the supported HydraCore distribution. A publishable build
must include the binary, generated Java source archive, SHA-256, attributed
source archive, and machine-readable provenance bound to one source commit and
one pinned toolchain.

HydraBox must pin the release tag, source commit, download URL, and digest. It
must reject a mismatched artifact before Android compilation or runtime startup.

## Remote subscription policy

The capability document publishes remote policy v1 as an exact allowlist, not
as permission to pass every native schema field from an untrusted publisher.
Policy v1 permits only `$schema`, `outbounds`, and `endpoints` at the native
document root. The outbound list contains leaf client protocols. The endpoint
list permits userspace WireGuard only after HydraBox applies the field policy.

Classic AmneziaWG fields are covered. Additional or future obfuscation fields
remain outside policy v1 until HydraBox and HydraCore both implement explicit
validation. DNS servers, providers, rule sets, composite or reverse types,
local paths, listeners, and service-capable objects require a later policy and
recursive validation. Older AARs without an exact manifest fail closed.

## Versioning

HydraCore versions use the form `v<sing-box-base>-hydracore.<revision>`. A
revision is immutable after publication. A client may move to a newer revision
only after the complete HydraCore workflow and the HydraBox compatibility suite
pass for the exact artifact digest.

Experimental transports do not enter the stable capability manifest merely
because their native schema exists. They require an explicit security model,
resource limits, compatibility tests, and an independent release decision.

## Stability boundary

HydraCore owns native parsing, runtime validation, protocol execution, and
capability reporting. HydraBox owns subscription trust, user consent, local
inbounds, persistent client data, and activation policy. HYDRA Ultimate owns
server deployment and subscription generation. Keeping these boundaries
separate lets the three projects evolve without turning a transport experiment
into a mandatory ecosystem dependency.
