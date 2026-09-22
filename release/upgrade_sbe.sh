#!/usr/bin/env bash
#
# Move the HydraCore fork onto a new sing-box-extended baseline.
#
# The fork is merge-based: `release/UPSTREAM_BASELINE` pins a commit that is an ancestor of the
# release branches, so an upgrade is an upstream merge, never a rebase of the fork's own history.
# The script fetches the upstream tag, merges it into the current checkout, records the new
# baseline, and moves the debug version counter to its first value on that baseline.
#
# It commits the result and stops there. It never pushes, tags or publishes, so a run that turns
# red cannot reach a channel from here, and publishing stays behind the existing manual gate.
#
# A merge conflict is the one outcome this script refuses to guess at: it aborts the merge, leaves
# the tree as it found it, and exits non-zero so the run fails loudly and the owner resolves it by
# hand ("не мержим вслепую").

set -euo pipefail

if [[ $# -ne 1 ]]; then
  printf 'usage: %s <upstream-tag>   e.g. %s v1.14.1-extended-2.7.2\n' "$0" "$0" >&2
  exit 2
fi

tag="$1"

if [[ ! "$tag" =~ ^v([0-9]+\.[0-9]+\.[0-9]+)-extended-([0-9]+\.[0-9]+\.[0-9]+)$ ]]; then
  printf 'not a sing-box-extended release tag: %s\n' "$tag" >&2
  exit 1
fi
sbe_version="${BASH_REMATCH[1]}"

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repository_root}"

# shellcheck disable=SC1091
source release/UPSTREAM_BASELINE

if ! git remote get-url upstream >/dev/null 2>&1; then
  git remote add upstream "${UPSTREAM_REPOSITORY}"
fi
# Fetch only the tag being merged, never `--tags`: upstream carries tags that collide with the
# fork's own history (e.g. `v1.0.1`), and a bulk tag fetch aborts on "would clobber existing tag"
# before the merge is ever reached. A single explicit tag refspec brings this release and its
# objects without touching any other tag.
git fetch --no-tags upstream "refs/tags/${tag}:refs/tags/${tag}"

if ! git rev-parse -q --verify "refs/tags/${tag}^{commit}" >/dev/null; then
  printf 'upstream tag is not available locally after fetch: %s\n' "$tag" >&2
  exit 1
fi
upstream_commit="$(git rev-parse "${tag}^{commit}")"

if [[ "${upstream_commit}" == "${UPSTREAM_COMMIT}" ]]; then
  printf 'baseline is already %s (%s); nothing to do\n' "${tag}" "${upstream_commit}"
  exit 0
fi

current_branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "${current_branch}" == "HEAD" ]]; then
  printf 'refusing to merge onto a detached HEAD\n' >&2
  exit 1
fi
if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
  printf 'the working tree has uncommitted changes; refusing to merge into it\n' >&2
  exit 1
fi

printf 'merging %s (%s) into %s\n' "${tag}" "${upstream_commit}" "${current_branch}"
premerge_head="$(git rev-parse HEAD)"
merge_status=0
git merge --no-commit --no-ff "${tag}" >/dev/null 2>&1 || merge_status=$?

# Fork-owned layers never track upstream. Before any conflict is judged, restore each owned
# path to the fork's pre-merge state and drop anything upstream added under it. This runs
# whether or not the merge reported a conflict, so an owned path can neither be rewritten nor
# extended by an upstream merge, and a conflict inside one is resolved in the fork's favour
# rather than raised for the owner. The pre-merge tree is premerge_head, captured above.
owned_file="release/FORK_OWNED_PATHS"
if [[ -f "${owned_file}" ]]; then
  while IFS= read -r owned || [[ -n "${owned}" ]]; do
    owned="${owned%%#*}"
    owned="$(printf '%s' "${owned}" | sed -e 's/[[:space:]]*$//' -e 's/^[[:space:]]*//')"
    [[ -z "${owned}" ]] && continue
    # Restore the owned path to its pre-merge content (staged + working tree).
    git checkout "${premerge_head}" -- "${owned}" 2>/dev/null || true
    git add -A -- "${owned}" 2>/dev/null || true
    # Drop files upstream added under an owned subtree that the fork never had.
    if [[ "${owned}" == */ ]]; then
      while IFS= read -r tracked; do
        [[ -z "${tracked}" ]] && continue
        if ! git cat-file -e "${premerge_head}:${tracked}" 2>/dev/null; then
          git rm -q --force -- "${tracked}" 2>/dev/null || true
        fi
      done < <(git ls-files -- "${owned}")
    fi
  done < "${owned_file}"
fi

# A file the fork deleted, that upstream only modified, is never a real conflict: the fork's
# intent is to keep it gone. Resolve every such modify/delete by keeping it deleted, so an
# upstream merge cannot resurrect a CI workflow or doc the fork retired.
while IFS= read -r deleted; do
  [[ -z "${deleted}" ]] && continue
  git rm -q --force -- "${deleted}" 2>/dev/null || true
done < <(git status --porcelain | awk '/^DU /{print substr($0,4)}')

# After owned paths are settled, any remaining conflict is in a genuinely shared file and must
# be resolved by hand. Abort loudly, leaving nothing committed.
if git diff --name-only --diff-filter=U | grep -q .; then
  printf 'merge conflict in shared paths while bringing in %s:\n' "${tag}" >&2
  git diff --name-only --diff-filter=U >&2 || true
  git merge --abort || true
  printf 'resolve the merge by hand and re-run; nothing was committed\n' >&2
  exit 1
fi
if [[ "${merge_status}" -ne 0 ]] && ! git rev-parse -q --verify MERGE_HEAD >/dev/null 2>&1; then
  # The merge failed for a reason other than a path conflict (e.g. unrelated histories).
  git merge --abort >/dev/null 2>&1 || true
  printf 'merge of %s failed and left no resolvable state; nothing was committed\n' "${tag}" >&2
  exit 1
fi

# Read the Go version from the merged go.mod: the baseline records the toolchain upstream builds
# with, and that is exactly what upstream may have moved in this merge.
go_version="$(awk '/^go /{print $2; exit}' go.mod)"
if [[ -z "${go_version}" ]]; then
  printf 'could not read the Go version from go.mod\n' >&2
  git merge --abort || true
  exit 1
fi

sed -i -E "s|^UPSTREAM_COMMIT=.*|UPSTREAM_COMMIT=${upstream_commit}|" release/UPSTREAM_BASELINE
sed -i -E "s|^UPSTREAM_TAG=.*|UPSTREAM_TAG=${tag}|" release/UPSTREAM_BASELINE
sed -i -E "s|^GO_VERSION=.*|GO_VERSION=${go_version}|" release/UPSTREAM_BASELINE

# The debug counter restarts at 1 on a new SBE baseline, per the tag contract. The version file is
# compared byte for byte against the native artifact string, so it carries no trailing newline.
new_version="hydracore-sbe-${sbe_version}-debug-1"
printf '%s' "${new_version}" >release/HYDRACORE_VERSION

# Fail closed before committing: the baseline must be a real ancestor of the merge, and the new
# version must satisfy the same tag contract the release workflow enforces.
bash release/verify_release_version.sh "${new_version}" refs/heads/debug
bash release/verify_upstream_baseline.sh

git add release/UPSTREAM_BASELINE release/HYDRACORE_VERSION
git commit -m "release(sbe): merge ${tag} (${new_version})" >/dev/null

printf 'upgraded to %s; version %s\n' "${tag}" "${new_version}"
printf 'sbe_tag=%s\n' "${tag}"
printf 'sbe_commit=%s\n' "${upstream_commit}"
printf 'hydracore_version=%s\n' "${new_version}"
