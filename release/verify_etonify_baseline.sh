#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repository_root}"

# shellcheck disable=SC1091
source release/ETONIFY_BASELINE

git cat-file -e "${UPSTREAM_COMMIT}^{commit}"
git merge-base --is-ancestor "${UPSTREAM_COMMIT}" HEAD
git cat-file -e "${UPSTREAM_TAG}^{commit}"
test "$(git rev-parse "${UPSTREAM_TAG}^{commit}")" = "${UPSTREAM_COMMIT}"

grep -Fq "github.com/sagernet/gomobile/cmd/gomobile@${GOMOBILE_VERSION}" Makefile
grep -Fq "github.com/sagernet/gomobile/cmd/gobind@${GOMOBILE_VERSION}" Makefile
grep -Fq "AndroidAPI: ${LIBBOX_ANDROID_API}" cmd/internal/build_libbox/main.go

IFS=',' read -ra required_tags <<< "${LIBBOX_BUILD_TAGS}"
for tag in "${required_tags[@]}"; do
  grep -Fq "\"${tag}\"" cmd/internal/build_libbox/main.go
done

printf 'upstream_repository=%s\n' "${UPSTREAM_REPOSITORY}"
printf 'upstream_branch=%s\n' "${UPSTREAM_BRANCH}"
printf 'upstream_commit=%s\n' "${UPSTREAM_COMMIT}"
printf 'go=%s\n' "${GO_VERSION}"
printf 'gomobile=%s\n' "${GOMOBILE_VERSION}"
printf 'ndk=%s\n' "${ANDROID_NDK_VERSION}"
printf 'java=%s\n' "${JAVA_VERSION}"
printf 'android_api=%s\n' "${LIBBOX_ANDROID_API}"
printf 'build_tags=%s\n' "${LIBBOX_BUILD_TAGS}"
