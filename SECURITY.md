# Security policy

HydraCore processes untrusted network traffic and may validate remotely supplied
client configuration, so parser, policy, memory-safety, secret-handling, and
resource-exhaustion issues are security relevant.

## Reporting

Use GitHub private vulnerability reporting in this repository when available.
If it is unavailable, open an issue containing only a non-exploitable summary
and request a private channel. Do not publish working exploits, credentials,
private subscriptions, server addresses, or sensitive logs.

Include the affected HydraCore tag or commit, platform, build tags, minimal
reproduction conditions, and whether the issue also reproduces in an unmodified
upstream build.

## Supported line

Security fixes target the current HydraCore prerelease line used by HydraBox.
Inherited or experimental branches are not automatically supported unless a
HydraCore release explicitly names them.
