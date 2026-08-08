# Hydra Subscription v2

Hydra Subscription v2 is the client-independent contract for distributing one
or more independently validated sing-box resource graphs. HydraCore owns the
portable schemas, executable remote policy, diagnostics, and test vectors.
Clients own fetching, user consent, local overlays, key storage, persistence,
and activation.

The plaintext media type is `application/vnd.hydra.subscription+json` and its
discriminator is `hydra.io/subscription/v2` with kind `Subscription`. The
encrypted form is flattened JWE JSON using only `alg=dir`, `enc=A256GCM`,
`typ=hydra-subscription+jwe`, and the plaintext media type as `cty`. A 32-byte
key may be carried in the `hydra-key` URL fragment; the fragment must never be
sent to a server or written to diagnostics.

The protected header is the complete JWE additional authenticated data.
`encrypted_key` is required and empty, the IV is exactly 12 bytes, and the tag
is exactly 16 bytes. Compression, shared unprotected headers, per-recipient
headers, and external AAD are not accepted. HydraCore provides schema/policy
accessors plus open, validate, and redacted-inspect APIs. It receives a
base64url key value from the client but does not fetch or persist it.

Each `resources` entry contains one native sing-box document. Resources cannot
reference or merge with one another. A profile names exactly one resource and
one outbound or endpoint tag. `requested_permissions` declares the authority a
future client must obtain from its local policy; it never grants authority by
itself.

Every resource is checked structurally and then passed to HydraCore
`remote_v2` validation. Unknown main-document fields fail closed. Optional
future data is permitted only inside namespaced `extensions`; an unknown
required extension rejects the subscription.

Core `version_range` uses a bounded whitespace- or comma-separated conjunction
of semantic-version comparators (`=`, `>`, `>=`, `<`, `<=`); a bare version is
exact. Core feature requirements are enforced by HydraCore. Client
`min_version`, client features, and profile `required_features` are validated
for shape and returned by inspection, but the client must evaluate them before
consent or activation because the core cannot identify the embedding client.

`HydraCoreValidateSubscription()` establishes structural, core compatibility,
remote-policy, reference, permission, and native-config validity. It does not
mean that a client has granted the requested permissions. Inspection returns
only identities, requirements, resource IDs, permission/protocol summaries,
and profile metadata; native documents, addresses, tags, and secrets are
omitted.

HydraBox Subscription v1 remains a separate historical client contract. This
repository does not claim that existing HydraBox releases support v2.
