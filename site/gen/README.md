# site/gen

Static docs-site generator: renders the repo's canonical markdown (per the
manifest in `nav.go`) into `site/docs/<slug>/index.html` plus `site/sitemap.xml`.
Run from the repo root: `go -C site/gen run .` (output is gitignored; CI builds
it in `.github/workflows/pages.yml`).
