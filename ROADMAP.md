# HydraCore roadmap

HydraCore follows the needs of the Hydra self-hosted VPN stack. Dates are not
promised; release gates and compatibility come before feature count.

## Now: stable Hydra runtime

- Keep the Android AAR reproducible and provenance-bound.
- Maintain the exact capability and remote-safety contract used by HydraBox.
- Track the pinned `sing-box-extended` baseline without losing Hydra mobile
  integration fixes.
- Improve lifecycle, network-change, URL-test, memory, and cancellation
  behavior with regression coverage.

## Next: maintainability

- Publish a clear compatibility matrix for HydraBox and HydraCore releases.
- Reduce inherited workflow and packaging ambiguity around unsupported targets.
- Document the supported extended protocol surface from generated capability
  data.
- Formalize update and rollback procedures for the pinned Android AAR.

## Research: optional transports

WDTT is deliberately outside the stable HydraCore line. The preserved research
history lives in the `wdtt-archive` branch. It has no release timeline and no
stable compatibility promise.

It may return only as an optional experimental mode after its upstream
constraints, relay dependency, multi-user behavior, operational stability, and
abuse model have been independently validated. HydraBox subscriptions and the
core product must remain useful without it.
