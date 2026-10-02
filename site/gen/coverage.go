package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// unlistedDocs are the markdown files under docs/ that are deliberately
// NOT docs-site pages. Everything else under docs/ must appear in Nav —
// a new doc that is written but never wired into nav.go would otherwise
// be invisible on the site (and never link-checked) with nothing failing.
// Adding a doc that should stay off the site is a one-line edit here.
var unlistedDocs = map[string]string{
	"docs/demo/README.md":          "asciinema recording notes; the recording itself is linked from the README",
	"docs/demo/mcp-walkthrough.md": "transcript backing the MCP demo, linked from docs/agents.md",
}

// skippedDocDirs are subtrees of docs/ the coverage check does not look at:
// design records and agent working notes are repo history, not site pages.
var skippedDocDirs = []string{"docs/design", "docs/superpowers"}

// checkDocsCoverage walks docs/**/*.md and returns the files that are in
// neither Nav nor unlistedDocs (and allowlist entries that no longer exist,
// so the allowlist cannot rot either). Paths are repo-relative, slash-separated.
func checkDocsCoverage(root string, bySrc map[string]Page) []string {
	var missing []string
	err := filepath.WalkDir(filepath.Join(root, "docs"), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel := filepath.ToSlash(strings.TrimPrefix(p, root+string(filepath.Separator)))
		if d.IsDir() {
			for _, skip := range skippedDocDirs {
				if rel == skip {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if filepath.Ext(rel) != ".md" {
			return nil
		}
		if _, ok := bySrc[rel]; ok {
			return nil
		}
		if _, ok := unlistedDocs[rel]; ok {
			return nil
		}
		missing = append(missing, rel+" (add it to site/gen/nav.go's Nav, or to unlistedDocs with a reason)")
		return nil
	})
	check(err)
	for rel := range unlistedDocs {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel))); err != nil {
			missing = append(missing, rel+" (listed in unlistedDocs but does not exist; drop the entry)")
		}
	}
	sort.Strings(missing)
	return missing
}

// reportDocsCoverage prints every unaccounted-for doc and exits non-zero
// if there are any.
func reportDocsCoverage(root string, bySrc map[string]Page) {
	missing := checkDocsCoverage(root, bySrc)
	if len(missing) == 0 {
		return
	}
	for _, m := range missing {
		fmt.Fprintf(os.Stderr, "gen: docs coverage: %s\n", m)
	}
	fmt.Fprintf(os.Stderr, "gen: %d doc(s) under docs/ are neither in Nav nor allowlisted\n", len(missing))
	os.Exit(1)
}
