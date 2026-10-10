#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 || $# -gt 3 ]]; then
  printf 'usage: %s <version> [git-ref] [sbe-version]\n' "$0" >&2
  exit 2
fi

version="$1"
ref="${2:-}"
baseline="${3:-}"
# Bare stable tags remain valid for published releases. New revisions carry their
# own counter rather than pretending to upgrade the pinned SBE baseline.
stable_pattern='^hydracore-sbe-[0-9]+\.[0-9]+\.[0-9]+(-stable-[1-9][0-9]*)?$'
debug_pattern='^hydracore-sbe-[0-9]+\.[0-9]+\.[0-9]+-debug-[1-9][0-9]*$'
rc_pattern='^hydracore-sbe-[0-9]+\.[0-9]+\.[0-9]+-rc-[1-9][0-9]*$'

if ! [[ "$version" =~ $stable_pattern || "$version" =~ $debug_pattern || "$version" =~ $rc_pattern ]]; then
  printf 'invalid Hydracore release version: %s\n' "$version" >&2
  exit 1
fi

if [[ -n "$baseline" ]]; then
  if ! [[ "$baseline" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf 'invalid SBE baseline version: %s\n' "$baseline" >&2
    exit 1
  fi
  release_baseline="${version#hydracore-sbe-}"
  release_baseline="${release_baseline%%-*}"
  if [[ "$release_baseline" != "$baseline" ]]; then
    printf 'release SBE version %s differs from pinned baseline %s\n' "$release_baseline" "$baseline" >&2
    exit 1
  fi
fi

case "$ref" in
'')
  ;;
refs/heads/debug)
  if ! [[ "$version" =~ $debug_pattern || "$version" =~ $rc_pattern ]]; then
    printf 'debug publication requires a debug or RC version: %s\n' "$version" >&2
    exit 1
  fi
  ;;
refs/heads/main)
  if ! [[ "$version" =~ $stable_pattern ]]; then
    printf 'main publication requires a stable version: %s\n' "$version" >&2
    exit 1
  fi
  ;;
*)
  printf 'unsupported release ref: %s\n' "$ref" >&2
  exit 1
  ;;
esac
