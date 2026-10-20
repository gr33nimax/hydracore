#!/usr/bin/env bash

# Exercise the real upgrade and baseline checks against a local upstream, including
# a conflict in a fork-owned path. No network, Go compiler or published refs needed.
set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
test_root="$(mktemp -d)"
trap 'rm -rf -- "${test_root}"' EXIT
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null
unset GIT_AUTHOR_NAME GIT_AUTHOR_EMAIL GIT_COMMITTER_NAME GIT_COMMITTER_EMAIL EMAIL || true

fail() {
  printf 'upgrade_sbe: %s\n' "$*" >&2
  exit 1
}

fixture="${test_root}/fixture"
git init -q -b debug "${fixture}"
cd "${fixture}"
git config user.name 'github-actions[bot]'
git config user.email '41898282+github-actions[bot]@users.noreply.github.com'
mkdir -p release cmd/internal/build_libbox transport/call experimental/libbox
cp "${repository_root}"/release/{upgrade_sbe.sh,verify_upstream_baseline.sh,verify_release_version.sh} release/
printf 'module example.invalid/upgrade-test\n\ngo 1.26.4\n' > go.mod
printf 'base\n' > shared.txt
printf 'base\n' > transport/call/owned.txt
printf 'transport/call/\n' > release/FORK_OWNED_PATHS
printf 'github.com/sagernet/gomobile/cmd/gomobile@fixture\ngithub.com/sagernet/gomobile/cmd/gobind@fixture\n' > Makefile
printf 'AndroidAPI: 23\n"with_call"\n' > cmd/internal/build_libbox/main.go
git add .
git commit -qm base
baseline="$(git rev-parse HEAD)"
git tag v1.14.1-extended-2.7.2
git switch -qc upstream
printf 'upstream\n' > shared.txt
printf 'upstream\n' > transport/call/owned.txt
git commit -qam upstream
git tag v1.14.3-extended-2.7.3
upstream_commit="$(git rev-parse HEAD)"
git clone -q --bare . "${test_root}/upstream.git"
git switch -q debug
printf 'preserved fork transport\n' > transport/call/owned.txt
cat > release/UPSTREAM_BASELINE <<EOF
UPSTREAM_REPOSITORY=${test_root}/upstream.git
UPSTREAM_BRANCH=upstream
UPSTREAM_COMMIT=${baseline}
UPSTREAM_TAG=v1.14.1-extended-2.7.2
GO_VERSION=1.26.4
GOMOBILE_VERSION=fixture
LIBBOX_ANDROID_API=23
LIBBOX_BUILD_TAGS=with_call
ANDROID_NDK_VERSION=r28
JAVA_VERSION=17
EOF
printf 'hydracore-sbe-1.14.1-debug-1' > release/HYDRACORE_VERSION
printf 'hydraCoreUpstreamTag = "v1.14.1-extended-2.7.2"\nhydraCoreUpstreamCommit = "%s"\n' "${baseline}" > experimental/libbox/hydracore_build_info.go
git add .
git commit -qm fork
fork_head="$(git rev-parse HEAD)"

# A missing identity must expose Git's actual error and leave the checkout clean.
git config --unset user.name
git config --unset user.email
if bash release/upgrade_sbe.sh v1.14.3-extended-2.7.3 > "${test_root}/identity.log" 2>&1; then
  fail 'upgrade unexpectedly succeeded without a committer identity'
fi
grep -Eq 'identity unknown|unable to auto-detect email' "${test_root}/identity.log" || fail 'Git identity diagnostic was hidden'
[[ "$(git rev-parse HEAD)" == "${fork_head}" ]] || fail 'failed upgrade changed HEAD'
[[ -z "$(git status --porcelain)" ]] || fail 'failed upgrade left changes behind'

git config user.name 'github-actions[bot]'
git config user.email '41898282+github-actions[bot]@users.noreply.github.com'
bash release/upgrade_sbe.sh v1.14.3-extended-2.7.3 > "${test_root}/upgrade.log" 2>&1 || {
  cat "${test_root}/upgrade.log" >&2
  fail 'configured upgrade failed'
}
git merge-base --is-ancestor "${fork_head}" HEAD || fail 'fork history was lost'
git merge-base --is-ancestor "${upstream_commit}" HEAD || fail 'upstream is absent from merge history'
[[ "$(cat transport/call/owned.txt)" == 'preserved fork transport' ]] || fail 'fork-owned transport changed'
[[ "$(cat shared.txt)" == upstream ]] || fail 'shared upstream change was lost'
[[ "$(cat release/HYDRACORE_VERSION)" == hydracore-sbe-1.14.3-debug-1 ]] || fail 'incorrect upgraded version'
[[ -z "$(git status --porcelain)" ]] || fail 'successful upgrade left uncommitted changes'
grep -Fq "${upstream_commit}" experimental/libbox/hydracore_build_info.go || fail 'client provenance was not updated'
bash release/verify_upstream_baseline.sh > /dev/null
if bash release/verify_upstream_baseline.sh --pending-merge > /dev/null 2>&1; then
  fail 'pending-merge verification accepted a checkout with no merge'
fi
printf 'sbe upgrade contract: ok\n'
