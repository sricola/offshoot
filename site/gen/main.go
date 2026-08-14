// Command gen renders the repo's canonical docs/*.md (plus the sdk READMEs
// and root project files listed in nav.go) into site/docs/<slug>/index.html
// with a shared documentation shell, and emits site/sitemap.xml.
//
// Run from the repo root:  go -C site/gen run .
// Output is deterministic: same inputs, byte-identical outputs.
package main

import (
	"bytes"
	"fmt"
	"html/template"
	"os"
	"path"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	chromahtml "github.com/alecthomas/chroma/v2/formatters/html"
	"github.com/yuin/goldmark"
	highlighting "github.com/yuin/goldmark-highlighting/v2"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/parser"
	htmlr "github.com/yuin/goldmark/renderer/html"
	"github.com/yuin/goldmark/text"
)

const (
	siteURL  = "https://sricola.github.io/offshoot" // no trailing slash
	basePath = "/offshoot"                          // GitHub Pages project path
	repoBlob = "https://github.com/sricola/offshoot/blob/main/"
	repoTree = "https://github.com/sricola/offshoot/tree/main/"
	favicon  = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Cpath d='M10 28V14M10 14v-4a4 4 0 0 1 4-4h0M10 14c0-4 3-7 8-7h4' stroke='%23a9cd7c' stroke-width='3.4' fill='none' stroke-linecap='round'/%3E%3Ccircle cx='10' cy='28' r='3' fill='%23a9cd7c'/%3E%3Ccircle cx='25' cy='7' r='3' fill='%23d9a05b'/%3E%3C/svg%3E"
)

// ghIDs implements parser.IDs with GitHub-style heading slugs so the
// #fragment anchors already written in the corpus keep working.
type ghIDs struct{ used map[string]int }

func newGHIDs() *ghIDs { return &ghIDs{used: map[string]int{}} }

func ghSlug(value []byte) string {
	var b strings.Builder
	for _, r := range string(value) {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			b.WriteRune(unicode.ToLower(r))
		case r == ' ':
			b.WriteByte('-')
		case r == '-' || r == '_':
			b.WriteRune(r)
		}
	}
	return b.String()
}

func (s *ghIDs) Generate(value []byte, kind ast.NodeKind) []byte {
	slug := ghSlug(value)
	if slug == "" {
		slug = "section"
	}
	n := s.used[slug]
	s.used[slug] = n + 1
	if n > 0 {
		slug = fmt.Sprintf("%s-%d", slug, n)
	}
	return []byte(slug)
}

func (s *ghIDs) Put(value []byte) { s.used[string(value)]++ }

// tocEntry is one h2/h3 heading for the right-hand "on this page" list.
type tocEntry struct {
	Level int
	ID    string
	Text  string
}

type pageData struct {
	Title       string
	Description string
	Canonical   string
	Base        string // basePath, e.g. /offshoot
	Slug        string
	MDPath      string // repo-relative markdown source path
	Content     template.HTML
	TOC         []tocEntry
	Nav         []Group
	Prev, Next  *Page
	IsIndex     bool
}

func main() {
	root, err := findRoot()
	check(err)

	bySrc := map[string]Page{}
	var flat []Page
	for _, g := range Nav {
		for _, p := range g.Pages {
			if _, err := os.Stat(filepath.Join(root, p.MD)); err != nil {
				// Fail loud: a renamed or deleted source must break the build,
				// not deploy a site silently missing the page.
				fmt.Fprintf(os.Stderr, "gen: manifest entry %s does not exist\n", p.MD)
				os.Exit(1)
			}
			bySrc[p.MD] = p
			flat = append(flat, p)
		}
	}

	md := goldmark.New(
		goldmark.WithExtensions(
			extension.GFM,
			highlighting.NewHighlighting(
				highlighting.WithFormatOptions(chromahtml.WithClasses(true)),
			),
		),
		goldmark.WithParserOptions(parser.WithAutoHeadingID()),
		goldmark.WithRendererOptions(htmlr.WithUnsafe()),
	)

	tpl := template.Must(template.New("shell").Parse(shellTemplate))

	order := map[string]int{}
	for i, p := range flat {
		order[p.MD] = i
	}

	for i := range flat {
		p := flat[i]
		src, err := os.ReadFile(filepath.Join(root, p.MD))
		check(err)

		ctx := parser.NewContext(parser.WithIDs(newGHIDs()))
		doc := md.Parser().Parse(text.NewReader(src), parser.WithContext(ctx))

		rewriteLinks(doc, root, path.Dir(p.MD), bySrc)
		toc := collectTOC(doc, src)
		desc := firstParagraph(doc, src)

		var buf bytes.Buffer
		check(md.Renderer().Render(&buf, src, doc))
		content := wrapTables(buf.String())

		data := pageData{
			Title:       p.Title,
			Description: desc,
			Canonical:   siteURL + "/docs/" + p.Slug + "/",
			Base:        basePath,
			Slug:        p.Slug,
			MDPath:      p.MD,
			Content:     template.HTML(content),
			TOC:         toc,
			Nav:         Nav,
		}
		if i > 0 {
			data.Prev = &flat[i-1]
		}
		if i < len(flat)-1 {
			data.Next = &flat[i+1]
		}
		writePage(tpl, root, p.Slug, data)
	}

	// Docs landing page: a clean grouped index.
	writePage(tpl, root, "", pageData{
		Title:       "Documentation",
		Description: "offshoot documentation — guides, architecture, operations, and reference for branchable SQLite: fork, checkpoint, rollback, promote.",
		Canonical:   siteURL + "/docs/",
		Base:        basePath,
		Content:     template.HTML(docsIndexHTML(bySrc)),
		Nav:         Nav,
		IsIndex:     true,
	})

	writeSitemap(root, flat)
	fmt.Printf("gen: %d pages + index + sitemap -> site/docs/\n", len(flat))
}

func findRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "docs", "reference.md")); err == nil {
			if _, err := os.Stat(filepath.Join(dir, "site")); err == nil {
				return dir, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("repo root not found (run from within the offshoot repo)")
		}
		dir = parent
	}
}

// rewriteLinks rewrites relative link destinations in place:
//   - manifest pages -> /offshoot/docs/<slug>/ (keeping #fragments)
//   - anything else in the repo -> github.com blob/tree URL
//
// Absolute URLs and pure #fragments pass through untouched.
func rewriteLinks(doc ast.Node, root, srcDir string, bySrc map[string]Page) {
	ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}
		switch v := n.(type) {
		case *ast.Link:
			v.Destination = []byte(rewrite(string(v.Destination), root, srcDir, bySrc))
		case *ast.Image:
			v.Destination = []byte(rewrite(string(v.Destination), root, srcDir, bySrc))
		}
		return ast.WalkContinue, nil
	})
}

func rewrite(dest, root, srcDir string, bySrc map[string]Page) string {
	if dest == "" || strings.HasPrefix(dest, "#") ||
		strings.Contains(dest, "://") || strings.HasPrefix(dest, "mailto:") {
		return dest
	}
	frag := ""
	if i := strings.IndexByte(dest, '#'); i >= 0 {
		frag = dest[i:]
		dest = dest[:i]
	}
	rel := path.Clean(path.Join(srcDir, dest))
	if p, ok := bySrc[rel]; ok {
		return basePath + "/docs/" + p.Slug + "/" + frag
	}
	if strings.HasSuffix(dest, "/") || isDir(filepath.Join(root, filepath.FromSlash(rel))) {
		return repoTree + rel + "/" + frag
	}
	return repoBlob + rel + frag
}

func isDir(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.IsDir()
}

func collectTOC(doc ast.Node, src []byte) []tocEntry {
	var toc []tocEntry
	ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}
		h, ok := n.(*ast.Heading)
		if !ok || (h.Level != 2 && h.Level != 3) {
			return ast.WalkContinue, nil
		}
		id, _ := h.AttributeString("id")
		idb, _ := id.([]byte)
		toc = append(toc, tocEntry{Level: h.Level, ID: string(idb), Text: nodeText(h, src)})
		return ast.WalkSkipChildren, nil
	})
	return toc
}

// nodeText extracts the plain text of a node's inline children.
func nodeText(n ast.Node, src []byte) string {
	var b strings.Builder
	ast.Walk(n, func(c ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}
		switch v := c.(type) {
		case *ast.Text:
			b.Write(v.Segment.Value(src))
			if v.SoftLineBreak() || v.HardLineBreak() {
				b.WriteByte(' ')
			}
		case *ast.CodeSpan:
			for gc := v.FirstChild(); gc != nil; gc = gc.NextSibling() {
				if t, ok := gc.(*ast.Text); ok {
					b.Write(t.Segment.Value(src))
				}
			}
			return ast.WalkSkipChildren, nil
		}
		return ast.WalkContinue, nil
	})
	return b.String()
}

// firstParagraph returns the page's first paragraph as plain text,
// truncated to ~160 chars for the meta description.
func firstParagraph(doc ast.Node, src []byte) string {
	var out string
	ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering || out != "" {
			return ast.WalkContinue, nil
		}
		if p, ok := n.(*ast.Paragraph); ok {
			out = strings.Join(strings.Fields(nodeText(p, src)), " ")
			return ast.WalkStop, nil
		}
		return ast.WalkContinue, nil
	})
	if len(out) > 160 {
		cut := out[:160]
		for len(cut) > 0 && !utf8.ValidString(cut) {
			cut = cut[:len(cut)-1]
		}
		if i := strings.LastIndexByte(cut, ' '); i > 100 {
			cut = cut[:i]
		}
		// Don't end a SERP snippet on a dangling article/conjunction.
		for {
			i := strings.LastIndexByte(cut, ' ')
			if i <= 100 {
				break
			}
			switch strings.ToLower(strings.TrimRight(cut[i+1:], ",;:.")) {
			case "a", "an", "the", "or", "and", "of", "to", "its", "it's", "their":
				cut = cut[:i]
				continue
			}
			break
		}
		out = strings.TrimRight(cut, " ,;:.'’s") + "…"
	}
	return out
}

// wrapTables wraps every rendered <table> in an overflow-x container so wide
// tables scroll inside their own box, never the page.
func wrapTables(s string) string {
	s = strings.ReplaceAll(s, "<table>", `<div class="tablewrap"><table>`)
	s = strings.ReplaceAll(s, "</table>", "</table></div>")
	return s
}

func docsIndexHTML(bySrc map[string]Page) string {
	var b strings.Builder
	b.WriteString("<h1>Documentation</h1>\n")
	b.WriteString("<p>Everything here is generated from the repo's canonical markdown — " +
		"the same files you can read on GitHub. New here? Start with the " +
		`<a href="` + basePath + `/docs/introduction/">introduction</a> and the ` +
		`<a href="` + basePath + `/docs/quickstart/">quickstart</a>; building a test suite, start with the ` +
		`<a href="` + basePath + `/docs/eval-harness/">eval-harness tutorial</a>; or jump straight to the ` +
		`<a href="` + basePath + `/docs/reference/">CLI &amp; API reference</a>.</p>` + "\n")
	for _, g := range Nav {
		fmt.Fprintf(&b, "<h2 id=%q>%s</h2>\n<ul>\n", ghSlug([]byte(g.Name)), template.HTMLEscapeString(g.Name))
		for _, p := range g.Pages {
			if _, ok := bySrc[p.MD]; !ok {
				continue
			}
			fmt.Fprintf(&b, "<li><a href=\"%s/docs/%s/\">%s</a></li>\n",
				basePath, p.Slug, template.HTMLEscapeString(p.Title))
		}
		b.WriteString("</ul>\n")
	}
	return b.String()
}

func writePage(tpl *template.Template, root, slug string, data pageData) {
	dir := filepath.Join(root, "site", "docs", slug)
	check(os.MkdirAll(dir, 0o755))
	var buf bytes.Buffer
	check(tpl.Execute(&buf, data))
	check(os.WriteFile(filepath.Join(dir, "index.html"), buf.Bytes(), 0o644))
}

func writeSitemap(root string, flat []Page) {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">` + "\n")
	url := func(u string) { fmt.Fprintf(&b, "  <url><loc>%s</loc></url>\n", u) }
	url(siteURL + "/")
	url(siteURL + "/docs/")
	for _, p := range flat {
		url(siteURL + "/docs/" + p.Slug + "/")
	}
	b.WriteString("</urlset>\n")
	check(os.WriteFile(filepath.Join(root, "site", "sitemap.xml"), []byte(b.String()), 0o644))
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "gen:", err)
		os.Exit(1)
	}
}
