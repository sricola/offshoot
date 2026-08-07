#!/usr/bin/env python3
"""Verify a pushed `sdk-v<version>` tag matches sdk/VERSION.

Used by .github/workflows/publish.yml before it does anything else on a tag
push: both SDKs publish in lockstep from one tag (see CONTRIBUTING.md's
"Release process"), so the tag itself is a third place the version number
has to agree with sdk/VERSION, sdk/python/pyproject.toml, and
sdk/typescript/package.json (the latter two checked by
scripts/check_sdk_versions.py). A tag cut against a stale sdk/VERSION would
otherwise publish artifacts whose own version metadata disagrees with the
release name that shipped them.

Usage: check_sdk_tag_version.py <tag>   e.g. check_sdk_tag_version.py sdk-v0.1.0
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
TAG_PREFIX = "sdk-v"


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} <tag>", file=sys.stderr)
        return 2
    tag = argv[1]
    if not tag.startswith(TAG_PREFIX):
        print(f"tag {tag!r} does not start with {TAG_PREFIX!r}", file=sys.stderr)
        return 1

    tag_version = tag[len(TAG_PREFIX):]
    truth = (REPO_ROOT / "sdk" / "VERSION").read_text().strip()
    if tag_version != truth:
        print(
            f"tag {tag!r} implies version {tag_version!r}, but sdk/VERSION is "
            f"{truth!r} — bump sdk/VERSION (and the two manifests) and retag.",
            file=sys.stderr,
        )
        return 1

    print(f"tag {tag!r} matches sdk/VERSION ({truth!r})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
