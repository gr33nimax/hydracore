#!/usr/bin/env bash
#
# Notice a new sing-box-extended release without touching it.
#
# Reads the upstream tag list read-only (`git ls-remote`), keeps the newest tag under the
# `v<sing-box>-extended-<extended>` contract, and compares its commit with the one recorded in
# `release/UPSTREAM_BASELINE`. It prints one machine-readable line and exits 0 either way, so the
# caller decides what a new tag means; only an unusable upstream or a missing baseline is an error.
#
# `SBE_WATCH_LISTING` injects a listing instead of reaching the network, which is what makes the
# selection rule testable without an upstream checkout.

set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repository_root}"

# shellcheck disable=SC1091
source release/UPSTREAM_BASELINE

# A listing that is set — even empty — is an injected fixture and is used as-is; only an unset
# variable reaches the network. An empty injected listing therefore means "upstream answered with
# nothing", which is an error and not a quiet fallback to a live query.
if [[ -n "${SBE_WATCH_LISTING+x}" ]]; then
  listing="${SBE_WATCH_LISTING}"
else
  listing="$(git ls-remote --tags "${UPSTREAM_REPOSITORY}" 'v*-extended-*')"
fi

# A tag can appear twice: once as the tag object and once peeled to the commit it points at. The
# peeled line wins, because that commit is the one the baseline records.
declare -A commit_of=()
while IFS=$'\t' read -r sha ref; do
  [[ -n "${sha:-}" && -n "${ref:-}" ]] || continue
  name="${ref#refs/tags/}"
  peeled=0
  if [[ "${name}" == *'^{}' ]]; then
    name="${name%'^{}'}"
    peeled=1
  fi
  [[ "${name}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+-extended-[0-9]+\.[0-9]+\.[0-9]+$ ]] || continue
  if [[ -z "${commit_of[${name}]:-}" || ${peeled} -eq 1 ]]; then
    commit_of["${name}"]="${sha}"
  fi
done <<<"${listing}"

if [[ ${#commit_of[@]} -eq 0 ]]; then
  printf 'no sing-box-extended release tags found upstream\n' >&2
  exit 1
fi

# Newest by the sing-box version first, then by the extended version, with every numeric field
# zero-padded so a plain string comparison orders them the way a reader expects.
newest_name=""
newest_key=""
for name in "${!commit_of[@]}"; do
  if [[ "${name}" =~ ^v([0-9]+)\.([0-9]+)\.([0-9]+)-extended-([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    key="$(printf '%05d.%05d.%05d.%05d.%05d.%05d' \
      "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" "${BASH_REMATCH[3]}" \
      "${BASH_REMATCH[4]}" "${BASH_REMATCH[5]}" "${BASH_REMATCH[6]}")"
  else
    continue
  fi
  if [[ -z "${newest_key}" || "${key}" > "${newest_key}" ]]; then
    newest_key="${key}"
    newest_name="${name}"
  fi
done

newest_commit="${commit_of[${newest_name}]}"

if [[ "${newest_commit}" == "${UPSTREAM_COMMIT}" ]]; then
  printf 'up-to-date %s\n' "${UPSTREAM_TAG}"
else
  printf 'update %s %s\n' "${newest_name}" "${newest_commit}"
fi
