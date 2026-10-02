#!/usr/bin/env bash
# check-version-surfaces.sh: assert that every file which states the
# offshoot binary's version agrees with the tag being released.
#
# The binary's version is stamped at build time from the git tag, but four
# files in the tree repeat the number by hand and have drifted before
# (server.json sat at 0.2.9 while v0.2.13 shipped). This script is the
# release gate for those: given a tag `vX.Y.Z` it checks
#
#   server.json            "version": "X.Y.Z"
#   docs/ci-recipes.md     every OFFSHOOT_VERSION: pin names vX.Y.Z
#   CHANGELOG.md           a "## [X.Y.Z] - YYYY-MM-DD" header exists
#   Formula/offshoot.rb    url names .../tags/vX.Y.Z.tar.gz  (--with-formula only)
#
# and exits non-zero naming every surface that disagrees. Run it before
# pushing a tag (CONTRIBUTING.md, "Binary releases") and from release.yml
# once the tag is resolved:
#
#   scripts/check-version-surfaces.sh v0.2.13
#
# The Formula is checked only with --with-formula: its sha256 is of the
# source tarball GitHub serves for the tag, so by design it is bumped in a
# commit AFTER the tag exists (CONTRIBUTING.md), and a tag-time check of it
# would always fail. Use --with-formula after that bump to confirm it.
#
set -euo pipefail

with_formula=0
if [ "${1:-}" = "--with-formula" ]; then
  with_formula=1
  shift
fi
if [ $# -ne 1 ]; then
  echo "usage: $0 [--with-formula] vX.Y.Z" >&2
  exit 2
fi
tag="$1"
case "$tag" in
  v[0-9]*.[0-9]*.[0-9]*) ;;
  *) echo "tag ${tag@Q} is not of the form vX.Y.Z" >&2; exit 2 ;;
esac
ver="${tag#v}"

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

fail=0
bad() { echo "version-surfaces: $*" >&2; fail=1; }

# server.json: the MCP registry manifest.
have="$(sed -n 's/^[[:space:]]*"version":[[:space:]]*"\([^"]*\)".*/\1/p' server.json | head -n1)"
if [ "$have" != "$ver" ]; then
  bad "server.json \"version\" is ${have:-missing}, want $ver"
fi

# docs/ci-recipes.md: every OFFSHOOT_VERSION pin (there is more than one).
pins="$(grep -o 'OFFSHOOT_VERSION: v[0-9][0-9.]*' docs/ci-recipes.md | awk '{print $2}' | sort -u || true)"
if [ -z "$pins" ]; then
  bad "docs/ci-recipes.md has no OFFSHOOT_VERSION pin"
elif [ "$pins" != "$tag" ]; then
  bad "docs/ci-recipes.md pins OFFSHOOT_VERSION $(echo "$pins" | tr '\n' ' ')— want $tag only"
fi

# Formula/offshoot.rb: the url names the release tarball (post-tag bump).
if [ "$with_formula" -eq 1 ] && ! grep -q "url \"https://github.com/[^\"]*/archive/refs/tags/${tag}.tar.gz\"" Formula/offshoot.rb; then
  bad "Formula/offshoot.rb url does not name refs/tags/${tag}.tar.gz: $(grep -m1 '^  url ' Formula/offshoot.rb || echo missing)"
fi

# CHANGELOG.md: a released header for this version.
if ! grep -q "^## \[${ver}\] - [0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}" CHANGELOG.md; then
  bad "CHANGELOG.md has no \"## [${ver}] - YYYY-MM-DD\" header"
fi

if [ "$fail" -ne 0 ]; then
  echo "version-surfaces: $tag disagrees with the tree; fix the surfaces above and retag" >&2
  exit 1
fi
if [ "$with_formula" -eq 1 ]; then
  echo "version-surfaces: server.json, docs/ci-recipes.md, CHANGELOG.md and Formula/offshoot.rb all name $tag"
else
  echo "version-surfaces: server.json, docs/ci-recipes.md and CHANGELOG.md all name $tag (Formula not checked: bumped after the tag)"
fi
