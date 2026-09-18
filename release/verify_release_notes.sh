#!/usr/bin/env bash

# The shared release notes file is published as the GitHub release body for every
# HydraCore release, so it must describe the current release only. Per-build history
# belongs in CHANGELOG.md: it used to accumulate here as a ladder of `## <tag>`
# sections and buried the current release under every debug build that came before.

set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
notes="${repository_root}/release/HYDRACORE_RELEASE_NOTES.md"
max_chars=7000

if [[ ! -f "${notes}" ]]; then
  printf 'release notes are missing: %s\n' "${notes}" >&2
  exit 1
fi

chars="$(wc -c <"${notes}" | tr -d '[:space:]')"
if ((chars > max_chars)); then
  printf 'release notes are %s characters; the limit is %s.\n' "${chars}" "${max_chars}" >&2
  printf 'Describe the current release only and move build history to CHANGELOG.md.\n' >&2
  exit 1
fi

# A heading that names a version is a section for another release: the old ladder of
# `## v1.14.0-extended-2.7.1-hydracore.12-debug.N` headings, or a readable-contract tag.
if version_headings="$(grep -nE '^#+[[:space:]].*(hydracore[-.][0-9]|hydracore-sbe-|debug\.[0-9]+)' "${notes}")"; then
  printf 'release notes carry a versioned section heading:\n%s\n' "${version_headings}" >&2
  printf 'Keep one document for the current release; put per-build history in CHANGELOG.md.\n' >&2
  exit 1
fi

# The file heads a published release, so its title must not tell the reader they are
# looking at a debug-only build.
title="$(head -n 1 "${notes}")"
if ! [[ "${title}" == '# HydraCore'* ]]; then
  printf 'release notes title must start with "# HydraCore": %s\n' "${title}" >&2
  exit 1
fi
if [[ "${title}" == *debug* ]]; then
  printf 'release notes title must not describe a debug-only build: %s\n' "${title}" >&2
  exit 1
fi
