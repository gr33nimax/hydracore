#!/usr/bin/env bash
#
# The watch rule is the one piece of the automation that runs unattended on every schedule tick,
# so its two decisions — which tag is newest, and whether the baseline already has it — are pinned
# here against injected listings instead of a live upstream.

set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
watch="${repository_root}/release/sbe_watch.sh"

# shellcheck disable=SC1091
source "${repository_root}/release/UPSTREAM_BASELINE"

baseline_sha="${UPSTREAM_COMMIT}"
baseline_tag="${UPSTREAM_TAG}"

fail() {
    printf 'sbe_watch: %s\n' "$*" >&2
    exit 1
}

# Runs the watch script with an injected listing and returns its single output line.
run() {
    SBE_WATCH_LISTING="$1" bash "${watch}"
}

expect_line() {
    local listing="$1" expected="$2"
    local actual
    actual="$(run "${listing}")" || fail "watch script failed for listing: ${listing}"
    if [[ "${actual}" != "${expected}" ]]; then
        fail "expected '${expected}', got '${actual}'"
    fi
}

expect_error() {
    local listing="$1"
    if run "${listing}" >/dev/null 2>&1; then
        fail "expected a non-zero exit for listing: ${listing}"
    fi
}

# The baseline's own tag is current: nothing to do.
expect_line \
    "$(printf '%s\trefs/tags/%s' "${baseline_sha}" "${baseline_tag}")" \
    "up-to-date ${baseline_tag}"

# A newer tag is reported with its commit, and the newest wins regardless of listing order.
newer_sha="1111111111111111111111111111111111111111"
expect_line \
    "$(printf '%s\trefs/tags/v1.13.16-extended-2.6.0\n%s\trefs/tags/%s\n%s\trefs/tags/v9.9.9-extended-9.9.9' \
        "2222222222222222222222222222222222222222" "${baseline_sha}" "${baseline_tag}" "${newer_sha}")" \
    "update v9.9.9-extended-9.9.9 ${newer_sha}"

# An annotated tag appears twice; the peeled line is the commit the baseline records.
peeled_sha="3333333333333333333333333333333333333333"
tag_object_sha="4444444444444444444444444444444444444444"
expect_line \
    "$(printf '%s\trefs/tags/v9.9.9-extended-9.9.9\n%s\trefs/tags/v9.9.9-extended-9.9.9^{}' \
        "${tag_object_sha}" "${peeled_sha}")" \
    "update v9.9.9-extended-9.9.9 ${peeled_sha}"

# The retired legacy naming is not a release under the contract and never drives an upgrade.
expect_line \
    "$(printf '%s\trefs/tags/%s\n%s\trefs/tags/v1.14.0-extended-2.7.1-hydracore.12-debug.11' \
        "${baseline_sha}" "${baseline_tag}" "5555555555555555555555555555555555555555")" \
    "up-to-date ${baseline_tag}"

# A listing with no contract tags is an unusable upstream, not an "up to date".
expect_error "$(printf '%s\trefs/tags/v1.14.0-extended-2.7.1-hydracore.12-debug.11' \
    "5555555555555555555555555555555555555555")"

expect_error ""

printf 'sbe watch contract: ok\n'
