package main

// The nav manifest is the single place that defines which markdown files
// become docs pages, their slugs, their titles, and their order. Adding a
// page is a one-line edit here.

// Page maps one markdown source file to one generated docs page.
type Page struct {
	MD    string // repo-relative path to the markdown source
	Slug  string // output slug: site/docs/<slug>/index.html
	Title string // sidebar + <title> text
}

// Group is an ordered set of pages under one sidebar label.
type Group struct {
	Name  string
	Pages []Page
}

// Nav is the ordered manifest: groups -> pages.
var Nav = []Group{
	{"Getting started", []Page{
		{"docs/introduction.md", "introduction", "Introduction"},
		{"docs/installation.md", "installation", "Installation"},
		{"docs/quickstart.md", "quickstart", "Quickstart"},
		{"docs/concepts.md", "concepts", "Core concepts"},
	}},
	{"Guides", []Page{
		{"docs/agents.md", "agents", "Agents & MCP"},
		{"docs/eval-harness.md", "eval-harness", "Eval harness tutorial"},
		{"docs/ci-recipes.md", "ci-recipes", "CI recipes"},
		{"docs/recipes/frameworks.md", "frameworks", "Framework recipes"},
		{"docs/recipes/claude-agent-sdk.md", "claude-agent-sdk", "Claude Agent SDK"},
		{"docs/recipes/openai-agents.md", "openai-agents", "OpenAI Agents SDK"},
		{"docs/recipes/kubernetes.md", "kubernetes", "Kubernetes sidecar"},
		{"docs/diff.md", "diff", "Branch diff"},
		{"sdk/python/README.md", "sdk-python", "Python SDK"},
		{"sdk/typescript/README.md", "sdk-typescript", "TypeScript SDK"},
		{"sdk/python-langgraph/README.md", "sdk-langgraph", "LangGraph adapter"},
	}},
	{"Architecture", []Page{
		{"docs/architecture.md", "architecture", "Architecture"},
		{"docs/testing.md", "testing", "How it's tested"},
		{"docs/benchmarks.md", "benchmarks", "Benchmarks"},
		{"docs/stability.md", "stability", "Stability contract"},
	}},
	{"Operations", []Page{
		{"docs/operations.md", "operations", "Operations"},
	}},
	{"Reference", []Page{
		{"docs/reference.md", "reference", "CLI & API reference"},
		{"docs/status.md", "status", "Status: shipped & deferred"},
		{"docs/faq.md", "faq", "FAQ: why not X?"},
		{"docs/limitations.md", "limitations", "Limitations"},
	}},
	{"Project", []Page{
		{"CHANGELOG.md", "changelog", "Changelog"},
		{"CONTRIBUTING.md", "contributing", "Contributing"},
		{"SECURITY.md", "security", "Security"},
		{"ROADMAP.md", "roadmap", "Roadmap"},
	}},
}
