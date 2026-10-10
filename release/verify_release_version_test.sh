#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
validator="${repository_root}/release/verify_release_version.sh"

expect_ok() {
  if ! bash "${validator}" "$@"; then
    printf 'expected success: %q\n' "$*" >&2
    exit 1
  fi
}

expect_fail() {
  if bash "${validator}" "$@"; then
    printf 'expected failure: %q\n' "$*" >&2
    exit 1
  fi
}

expect_ok hydracore-sbe-1.14.0-debug-1 refs/heads/debug
expect_ok hydracore-sbe-1.14.0-rc-1 refs/heads/debug
expect_ok hydracore-sbe-1.14.0 refs/heads/main
expect_ok hydracore-sbe-1.14.0
expect_ok hydracore-sbe-1.14.1-stable-1 refs/heads/main
expect_ok hydracore-sbe-1.14.1-stable-2 refs/heads/main 1.14.1
expect_ok hydracore-sbe-1.14.1 refs/heads/main 1.14.1
expect_ok hydracore-sbe-1.14.1-debug-2 refs/heads/debug 1.14.1

expect_fail hydracore-sbe-1.14.0 refs/heads/debug
expect_fail hydracore-sbe-1.14.0-debug-1 refs/heads/main
expect_fail hydracore-sbe-1.14.0-rc-1 refs/heads/main
expect_fail v1.14.0-extended-2.7.1-hydracore.12-debug.11 refs/heads/debug
expect_fail hydracore-sbe-1.14.0-debug- refs/heads/debug
expect_fail hydracore-sbe--debug-1 refs/heads/debug
expect_fail hydracore-sbe-1.14.0-debug-0 refs/heads/debug
expect_fail hydracore-sbe-1.14.0-preview-1 refs/heads/debug
expect_fail hydracore-sbe-1.14.0 refs/heads/release
expect_fail hydracore-sbe-1.14.1-stable-1 refs/heads/debug
expect_fail hydracore-sbe-1.14.1-stable-0 refs/heads/main
expect_fail hydracore-sbe-1.14.1-stable-01 refs/heads/main
expect_fail hydracore-sbe-1.14.1-stable- refs/heads/main
expect_fail hydracore-sbe-1.14.1-stable-1-debug-1 refs/heads/main
expect_fail hydracore-sbe-1.14.2 refs/heads/main 1.14.1
expect_fail hydracore-sbe-1.14.2-stable-1 refs/heads/main 1.14.1
expect_fail hydracore-sbe-1.14.1-debug-2 refs/heads/debug 1.14.0
expect_fail hydracore-sbe-1.14.1 refs/heads/main malformed

printf 'release version contract: ok\n'
