#!/usr/bin/env python3
"""Verify the two SDKs' versions agree with the single source of truth.

Version discipline (see CONTRIBUTING.md's "Release process"): `sdk/VERSION`
is the one file a maintainer edits to bump the SDK version. Everything else
that spells the version out literally (`sdk/python/pyproject.toml`'s
`[project].version`, `sdk/typescript/package.json`'s `version`) must be kept
in lockstep by hand and is checked against `sdk/VERSION` here, in CI, and
again by `.github/workflows/publish.yml` against the pushed tag. This script
is stdlib-only, matching the base SDKs' own dependency policy, and exits
non-zero with every mismatch listed (not just the first) so a release bump
that missed a spot fails loudly instead of silently drifting.

Both SDKs are published in lockstep from one `sdk-v<version>` tag (see
CONTRIBUTING.md) — there is deliberately one version number, not two.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def read_source_of_truth() -> str:
    return (REPO_ROOT / "sdk" / "VERSION").read_text().strip()


def read_pyproject_version() -> str:
    text = (REPO_ROOT / "sdk" / "python" / "pyproject.toml").read_text()
    # A plain, non-dynamic `version = "X"` under [project]. Deliberately not
    # a full TOML parse (tomllib is 3.11+; this repo's SDK floor is 3.10) —
    # this is a build-time/CI check script, not shipped SDK code, so stdlib
    # simplicity wins over TOML rigor for a single well-known key.
    m = re.search(r'(?m)^version\s*=\s*"([^"]+)"', text)
    if not m:
        raise SystemExit("sdk/python/pyproject.toml: no top-level version = \"...\" line found")
    return m.group(1)


def read_package_json_version() -> str:
    data = json.loads((REPO_ROOT / "sdk" / "typescript" / "package.json").read_text())
    return data["version"]


def main() -> int:
    truth = read_source_of_truth()
    checks = {
        "sdk/python/pyproject.toml": read_pyproject_version(),
        "sdk/typescript/package.json": read_package_json_version(),
    }

    mismatches = [
        f"  {path}: {version!r} != sdk/VERSION's {truth!r}"
        for path, version in checks.items()
        if version != truth
    ]
    if mismatches:
        print(f"SDK version mismatch (sdk/VERSION = {truth!r}):", file=sys.stderr)
        for line in mismatches:
            print(line, file=sys.stderr)
        print(
            "Bump sdk/VERSION, sdk/python/pyproject.toml's version, and "
            "sdk/typescript/package.json's version together — see "
            "CONTRIBUTING.md's Release process.",
            file=sys.stderr,
        )
        return 1

    print(f"sdk versions agree: {truth}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
