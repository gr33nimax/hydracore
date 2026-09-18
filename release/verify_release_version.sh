#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  printf 'usage: %s <version> [git-ref]\n' "$0" >&2
  exit 2
fi

version="$1"
ref="${2:-}"
stable_pattern='^hydracore-sbe-[0-9]+\.[0-9]+\.[0-9]+$'
debug_pattern='^hydracore-sbe-[0-9]+\.[0-9]+\.[0-9]+-debug-[1-9][0-9]*$'
rc_pattern='^hydracore-sbe-[0-9]+\.[0-9]+\.[0-9]+-rc-[1-9][0-9]*$'

if ! [[ "$version" =~ $stable_pattern || "$version" =~ $debug_pattern || "$version" =~ $rc_pattern ]]; then
  printf 'invalid Hydracore release version: %s\n' "$version" >&2
  exit 1
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
