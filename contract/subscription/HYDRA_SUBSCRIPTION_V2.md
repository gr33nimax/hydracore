# Hydra Subscription v2

The client-independent contract for distributing one or more independently validated
sing-box resource graphs. HydraCore owns the portable schemas, remote policy, diagnostics
and validation; clients own fetching, consent, key storage, persistence and activation.

The authoritative schema is JSON Schema (draft 2020-12), embedded in the core:

- plaintext: [`schema/hydra-subscription-v2.schema.json`](schema/hydra-subscription-v2.schema.json)
- encrypted (JWE): [`schema/hydra-subscription-jwe-v2.schema.json`](schema/hydra-subscription-jwe-v2.schema.json)

Retrieve them at runtime via `HydraCoreSubscriptionSchema()` /
`HydraCoreSubscriptionJWESchema()` (`experimental/libbox/hydracore_subscription.go`). The
tables below describe those schemas field by field — the schema file wins on any conflict.

## How an outbound reaches the client

This is the part that matters in practice. An outbound is **a native sing-box object,
delivered verbatim** — the subscription does not invent its own outbound format.

- Each `resources[]` entry carries a `document`: one real sing-box config fragment. Its
  proxies live in `document.outbounds[]` (or `document.endpoints[]` for WireGuard/AWG),
  exactly as sing-box expects them — same `type`, `tag`, `server`, `password`, etc.
- A `profiles[]` entry points at one of them by `entrypoint.{section, tag}`. That tag is
  what the client activates as the route.
- Tags resolve **only within their own resource**. An outbound whose `detour` names a tag
  in another resource is rejected (`missing_reference`) — a resource is a closed graph.

A minimal subscription that hands the client one SOCKS outbound:

```json
{
  "resources": [{
    "id": "res-main",
    "format": "sing-box-json",
    "requested_permissions": ["network.outbound"],
    "document": {
      "outbounds": [
        {"type": "socks", "tag": "proxy-main", "server": "origin.example", "server_port": 1080, "password": "SECRET"}
      ]
    }
  }],
  "profiles": [{
    "id": "prof-main",
    "resource": "res-main",
    "name": {"default": "Main"},
    "entrypoint": {"section": "outbounds", "tag": "proxy-main"}
  }]
}
```

The client fetches the subscription, and for the chosen profile takes
`resources[resource].document`, finds `entrypoint.tag` in `entrypoint.section`, and runs
that document through sing-box. Any outbound type sing-box supports works — `socks`,
`trojan`, `vless`, the HydraCore `call` (`vk_parasite`), etc. WireGuard/AmneziaWG comes as
an **endpoint**:

```json
{
  "resources": [{
    "id": "res-wg", "format": "sing-box-json",
    "requested_permissions": ["network.endpoint.wireguard"],
    "document": { "endpoints": [ {"type": "wireguard", "tag": "proxy-main", "address": ["10.0.0.2/32"]} ] }
  }],
  "profiles": [{
    "id": "prof-wg", "resource": "res-wg", "name": {"default": "WG"},
    "entrypoint": {"section": "endpoints", "tag": "proxy-main"}
  }]
}
```

`requested_permissions` on the resource is derived from what its document actually contains
(`outbounds` → `network.outbound`, `endpoints` → `network.endpoint.wireguard`,
`inbounds` → `network.inbound.call`) and only **declares** authority the client must obtain
from its own policy — it never grants it.

The rest of this document is the full field reference for the wrapper around those
documents.

## Media types and discriminator

| | Value |
| --- | --- |
| Plaintext media type | `application/vnd.hydra.subscription+json` |
| `api_version` (discriminator) | `hydra.io/subscription/v2` |
| `kind` | `Subscription` |

## Document shape (plaintext)

Top-level required: `api_version`, `kind`, `identity`, `validity`, `requirements`,
`resources`, `profiles`. `additionalProperties` is **false** at every documented level — an
unknown field fails closed. Optional future data goes only under namespaced `extensions`.

```json
{
  "api_version": "hydra.io/subscription/v2",
  "kind": "Subscription",
  "identity": {
    "issuer": "https://issuer.example/",
    "id": "acme-prod",
    "channel": "stable",
    "sequence": 42
  },
  "validity": {
    "issued_at": "2026-09-22T00:00:00Z",
    "not_before": "2026-09-22T00:00:00Z",
    "expires_at": "2026-12-22T00:00:00Z"
  },
  "display": {
    "name": {"default": "Acme VPN", "ru": "Acme ВПН"},
    "homepage": "https://acme.example/app",
    "support_url": "https://acme.example/help"
  },
  "requirements": {
    "core": {
      "id": "io.hydrabox.hydracore",
      "api_version": 2,
      "version_range": ">=1.14.0",
      "remote_policy": 2,
      "features": ["vk_parasite"]
    },
    "client": {
      "subscription_contract": 2,
      "min_version": "2.1.0",
      "features": []
    }
  },
  "update": {
    "url": "https://issuer.example/sub/acme-prod",
    "minimum_interval_seconds": 3600
  },
  "resources": [
    {
      "id": "res-main",
      "format": "sing-box-json",
      "requested_permissions": ["network.outbound"],
      "document": { "outbounds": [ { "type": "call", "tag": "proxy-main" } ] }
    }
  ],
  "profiles": [
    {
      "id": "prof-main",
      "resource": "res-main",
      "name": {"default": "Main"},
      "entrypoint": {"section": "outbounds", "tag": "proxy-main"},
      "enabled": true,
      "required_features": []
    }
  ],
  "default_profile": "prof-main"
}
```

### `identity`

| Field | Req | Type | Notes |
| --- | --- | --- | --- |
| `issuer` | ✓ | https origin | scheme+host only, no userinfo, no path beyond `/` |
| `id` | ✓ | id | `^[A-Za-z0-9][A-Za-z0-9._:-]*$`, ≤128 |
| `channel` | | id | default `stable` |
| `sequence` | ✓ | integer | ≥0; monotonic issue counter |

### `validity`

| Field | Req | Type |
| --- | --- | --- |
| `issued_at` | ✓ | RFC 3339 date-time |
| `not_before` | | RFC 3339 date-time |
| `expires_at` | | RFC 3339 date-time |

### `display` (optional)

`name` (localized text), `homepage`, `support_url` (https URLs). Localized text is either a
string or an object with a required `default` and per-locale overrides.

### `requirements`

`core` (required `id` = `io.hydrabox.hydracore`, `api_version` ≥1, `remote_policy` ≥1;
optional `version_range`, `features`) and `client` (required `subscription_contract` = `2`;
optional `min_version`, `features`).

- `version_range` — bounded whitespace/comma conjunction of semver comparators
  (`=`,`>`,`>=`,`<`,`<=`); a bare version is exact. **Enforced by the core.**
- `core.features` — **enforced by the core**.
- `client.min_version` / `client.features` / profile `required_features` — validated for
  shape and returned by inspection, but the **client** must evaluate them: the core cannot
  identify the embedding client.

### `update` (optional)

`url` (https) and `minimum_interval_seconds` (300…2 592 000).

### `resources` (1…64)

| Field | Req | Notes |
| --- | --- | --- |
| `id` | ✓ | unique within the document |
| `format` | ✓ | const `sing-box-json` |
| `document` | ✓ | exactly one native sing-box document |
| `requested_permissions` | | subset of `network.outbound`, `network.endpoint.wireguard`, `network.inbound.call` |

Resources cannot reference or merge with one another. `requested_permissions` only
**declares** authority a future client must obtain from its local policy — it never grants
it.

### `profiles` (1…4096)

| Field | Req | Notes |
| --- | --- | --- |
| `id` | ✓ | unique |
| `resource` | ✓ | names exactly one `resources[].id` |
| `name` | ✓ | localized text |
| `entrypoint.section` | ✓ | `outbounds` or `endpoints` |
| `entrypoint.tag` | ✓ | a tag inside that resource's document |
| `enabled` | | default `true` |
| `required_features` | | client-evaluated |

Top-level `default_profile` names one profile id; `required_extensions` /`extensions` carry
namespaced future data (`^[a-z0-9]+(?:[.-][a-z0-9]+)+/v[1-9][0-9]*$`). An unknown **required**
extension rejects the subscription.

## Encrypted form (JWE)

Flattened JWE JSON, `alg=dir`, `enc=A256GCM`, `typ=hydra-subscription+jwe`,
`cty=application/vnd.hydra.subscription+json`. Exact policy from
`HydraCoreSubscriptionJWEPolicy()`:

```json
{"schema_version":1,"serialization":"flattened-json","alg":"dir","enc":"A256GCM",
 "typ":"hydra-subscription+jwe","cty":"application/vnd.hydra.subscription+json",
 "key_bytes":32,"encrypted_key_bytes":0,"iv_bytes":12,"tag_bytes":16,
 "external_aad":false,"compression":false,"key_fragment":"hydra-key"}
```

Object shape: `protected` (base64url, the complete AAD), `encrypted_key` (required and
**empty**), `iv` (exactly 12 bytes → 16 base64url chars), `ciphertext`, `tag` (exactly 16
bytes → 22 base64url chars). Compression, shared/per-recipient headers and external AAD are
rejected. A 32-byte key may travel in the `#hydra-key` URL fragment — the client passes it
to the core as base64url, and it must **never** be sent to a server or written to
diagnostics. The core neither fetches nor persists the key.

## Runtime API (libbox)

| Function | Returns |
| --- | --- |
| `HydraCoreSubscriptionSchema()` / `…JWESchema()` | the embedded JSON Schemas |
| `HydraCoreSubscriptionJWEPolicy()` | the JWE policy JSON above |
| `HydraCoreValidateSubscription(content)` | validation result |
| `HydraCoreInspectSubscription(content)` | redacted inspection |

`HydraCoreValidateSubscription` establishes structural, core-compatibility, remote-policy,
reference, permission and native-config validity. Result:

```json
{"schema_version":1,"profile":"subscription_v2","valid":true,"diagnostics":[]}
```

A failure carries `diagnostics[]` of `{severity, code, path, message}`. Validity does **not**
mean a client has granted the requested permissions.

`HydraCoreInspectSubscription` returns only non-secret metadata — identities, validity,
requirements, resource ids with permission/protocol summaries, and profile
`{id, resource, section, enabled}`. Native documents, addresses, tags and secrets are
omitted:

```json
{
  "schema_version": 1,
  "valid": true,
  "identity": { "issuer": "https://issuer.example/", "id": "acme-prod", "sequence": 42 },
  "validity": { "issued_at": "2026-09-22T00:00:00Z" },
  "resources": [ { "id": "res-main", "requested_permissions": ["network.outbound"], "protocols": ["call"] } ],
  "profiles": [ { "id": "prof-main", "resource": "res-main", "section": "outbounds", "enabled": true } ],
  "diagnostics": []
}
```

## Validation pipeline

Every resource is checked structurally, then passed to HydraCore `remote_v2` validation
(`requirements.core.remote_policy`). Unknown main-document fields fail closed; optional
future data is allowed only inside namespaced `extensions`.

## Compatibility note

HydraBox Subscription v1 is a separate historical client contract. This repository does not
claim existing HydraBox releases support v2.
