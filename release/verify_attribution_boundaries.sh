#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repository_root}"

allowed_files=(
  .github/workflows/hydracore.yml
  CHANGELOG.md
  CREDITS.md
  experimental/libbox/hydracore_build_info.go
  experimental/libbox/hydracore_build_info_test.go
  HYDRACORE.md
  README.md
  release/verify_attribution_boundaries.sh
  THIRD_PARTY_NOTICES.md
)

is_allowed() {
  local candidate="$1"
  local allowed
  for allowed in "${allowed_files[@]}"; do
    if [[ "${candidate}" == "${allowed}" ]]; then
      return 0
    fi
  done
  return 1
}

failed=0
while IFS= read -r match; do
  [[ -n "${match}" ]] || continue
  file="${match%%:*}"
  if ! is_allowed "${file}"; then
    printf 'active Etonify reference outside attribution allowlist: %s\n' "${match}" >&2
    failed=1
  fi
done < <(git grep -I -n -i etonify -- . || true)

exit "${failed}"
