package main

// shellTemplate is the one shared documentation shell. Palette variables are
// copied verbatim from site/index.html (light and dark) for identity
// continuity with the landing page.
const shellTemplate = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{.Title}} — offshoot docs</title>
{{if .Description}}<meta name="description" content="{{.Description}}">
{{end}}<link rel="canonical" href="{{.Canonical}}">
<meta property="og:title" content="{{.Title}} — offshoot docs">
{{if .Description}}<meta property="og:description" content="{{.Description}}">
{{end}}<meta property="og:type" content="article">
<meta property="og:url" content="{{.Canonical}}">
<meta property="og:image" content="https://sricola.github.io/offshoot/og.png">
<meta name="twitter:card" content="summary_large_image">
<link rel="icon" href="` + favicon + `">
<script>(function(){var t=localStorage.getItem("theme");if(t==="dark"||t==="light")document.documentElement.dataset.theme=t})()</script>
<style>
:root{
  --bg:#fcfcfa; --ink:#18150f; --dim:#6b675d; --faint:#706c63;
  --accent:#3c7a1a; --code-bg:#f4f3ee; --border:#e4e2da; --rule:#ecebe4;
  --hl-s:#8a5a00; --hl-m:#145f8a;
  --mono:ui-monospace,"SF Mono","Cascadia Code",Menlo,Consolas,"Liberation Mono",monospace;
  --sans:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;
}
:root[data-theme=dark]{
  --bg:#131211; --ink:#e6e3da; --dim:#97927f; --faint:#868278;
  --accent:#a9cd7c; --code-bg:#1b1a17; --border:#2b2925; --rule:#232119;
  --hl-s:#d9a05b; --hl-m:#7ab8d9;
}
@media(prefers-color-scheme:dark){
  :root:not([data-theme=light]){
    --bg:#131211; --ink:#e6e3da; --dim:#97927f; --faint:#868278;
    --accent:#a9cd7c; --code-bg:#1b1a17; --border:#2b2925; --rule:#232119;
    --hl-s:#d9a05b; --hl-m:#7ab8d9;
  }
}
*{box-sizing:border-box;margin:0;padding:0}
html{-webkit-text-size-adjust:100%;scroll-behavior:smooth}
@media(prefers-reduced-motion:reduce){html{scroll-behavior:auto}*{transition:none!important}}
body{background:var(--bg);color:var(--ink);font-family:var(--sans);
  font-size:15.5px;line-height:1.7;-webkit-font-smoothing:antialiased}
::selection{background:var(--accent);color:var(--bg)}
a{color:var(--accent);text-decoration:none}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px;border-radius:2px}

.skip{position:absolute;left:-9999px;top:0;z-index:20;background:var(--bg);
  color:var(--accent);padding:10px 16px;border:1px solid var(--border);border-radius:6px}
.skip:focus{left:12px;top:12px}

.top{position:sticky;top:0;z-index:10;background:var(--bg);
  border-bottom:1px solid var(--border);height:56px;
  display:flex;align-items:center;gap:8px;padding:0 20px}
.top .wordmark{font-family:var(--mono);font-size:16px;font-weight:700;
  color:var(--ink);letter-spacing:-.01em;padding:10px 4px}
.top .wordmark .g{color:var(--accent);font-weight:400}
.top nav{margin-left:auto;display:flex;align-items:center;gap:4px;
  font-family:var(--mono);font-size:13px}
.top nav a{color:var(--dim);padding:12px 10px;min-height:44px;display:inline-flex;align-items:center}
.top nav a:hover,.top nav a[aria-current]{color:var(--accent)}
.top button{background:none;border:none;color:var(--dim);cursor:pointer;
  font-family:var(--mono);font-size:16px;min-width:44px;min-height:44px;border-radius:6px}
.top button:hover{color:var(--accent)}
.menu{display:none}

.layout{display:grid;grid-template-columns:230px minmax(0,1fr) 210px;
  gap:0 40px;max-width:1280px;margin:0 auto;padding:0 24px}

.sidebar{position:sticky;top:56px;height:calc(100vh - 56px);overflow-y:auto;
  padding:28px 16px 40px 0;border-right:1px solid var(--rule);font-size:13.5px}
.sidebar .glabel{font-family:var(--mono);font-size:11px;letter-spacing:.12em;
  text-transform:uppercase;color:var(--faint);margin:22px 0 6px}
.sidebar .group:first-child .glabel{margin-top:0}
.sidebar ul{list-style:none}
.sidebar a{display:block;color:var(--dim);padding:5px 10px;
  border-left:2px solid transparent;border-radius:0 4px 4px 0}
.sidebar a:hover{color:var(--ink)}
.sidebar a[aria-current=page]{color:var(--accent);border-left-color:var(--accent);
  background:var(--code-bg);font-weight:600}
.scrim{position:fixed;inset:0;z-index:11;background:rgba(0,0,0,.4)}

main{padding:36px 0 64px;min-width:0}
.prose{max-width:46rem}
.prose h1{font-size:28px;line-height:1.25;letter-spacing:-.015em;margin:0 0 18px}
.prose h2{font-size:20px;line-height:1.35;letter-spacing:-.01em;
  border-top:1px solid var(--rule);padding-top:22px;margin:36px 0 12px}
.prose h3{font-size:16.5px;margin:26px 0 10px}
.prose h4{font-size:15px;margin:20px 0 8px}
.prose p,.prose ul,.prose ol{margin:0 0 14px;color:var(--ink)}
.prose ul,.prose ol{padding-left:1.4em}
.prose li{margin:4px 0}
.prose li>ul,.prose li>ol{margin-bottom:0}
.prose a{text-decoration:underline;text-decoration-color:color-mix(in srgb,var(--accent) 45%,transparent);
  text-underline-offset:3px}
.prose a:hover{text-decoration-color:var(--accent)}
.prose blockquote{border-left:2px solid var(--border);padding:2px 0 2px 16px;
  color:var(--dim);margin:0 0 14px}
.prose hr{border:none;border-top:1px solid var(--border);margin:28px 0}
.prose img{max-width:100%}
.prose code{font-family:var(--mono);background:var(--code-bg);border:1px solid var(--border);
  border-radius:3px;padding:.08em .35em;font-size:13px;overflow-wrap:anywhere}
.prose pre{background:var(--code-bg);border:1px solid var(--border);border-radius:6px;
  padding:14px 16px;font-size:13px;line-height:1.75;overflow-x:auto;margin:0 0 14px;
  font-family:var(--mono)}
.prose pre code{background:none;border:none;padding:0;font-size:inherit;overflow-wrap:normal}
.tablewrap{overflow-x:auto;border:1px solid var(--border);border-radius:6px;margin:0 0 14px}
.prose table{border-collapse:collapse;font-size:13.5px;width:100%}
.prose th,.prose td{padding:8px 14px;border-top:1px solid var(--rule);
  text-align:left;vertical-align:top}
.prose th{border-top:none;font-family:var(--mono);font-size:12px;
  letter-spacing:.06em;text-transform:uppercase;color:var(--dim)}
.prose td code,.prose th code{white-space:nowrap}

.chroma .c,.chroma .c1,.chroma .cm,.chroma .ch,.chroma .cs,.chroma .cp,.chroma .cpf{color:var(--dim);font-style:italic}
.chroma .k,.chroma .kc,.chroma .kd,.chroma .kn,.chroma .kp,.chroma .kr,.chroma .kt,.chroma .nt,.chroma .ow{color:var(--accent)}
.chroma .s,.chroma .s1,.chroma .s2,.chroma .sa,.chroma .sb,.chroma .sc,.chroma .sd,.chroma .se,.chroma .sh,.chroma .si,.chroma .sx,.chroma .sr,.chroma .ss{color:var(--hl-s)}
.chroma .m,.chroma .mb,.chroma .mf,.chroma .mh,.chroma .mi,.chroma .il,.chroma .mo,.chroma .no,.chroma .l{color:var(--hl-m)}
.chroma .nf,.chroma .fm,.chroma .nc,.chroma .nd{color:var(--ink);font-weight:600}
.chroma .gp{color:var(--accent);user-select:none}
.chroma .ge{font-style:italic}.chroma .gs{font-weight:600}

.pagefoot{max-width:46rem;margin-top:44px;border-top:1px solid var(--border);
  padding-top:14px;font-size:13px;font-family:var(--mono)}
.pagefoot .src a{color:var(--faint)}
.pagefoot .src a:hover{color:var(--accent)}
.pagenav{display:flex;justify-content:space-between;gap:16px;margin-top:10px}
.pagenav a{display:inline-flex;align-items:center;min-height:44px;color:var(--accent)}
.pagenav .next{margin-left:auto;text-align:right}

.toc{position:sticky;top:56px;height:calc(100vh - 56px);overflow-y:auto;
  padding:32px 0 40px;font-size:12.5px}
.toc .tlabel{font-family:var(--mono);font-size:11px;letter-spacing:.12em;
  text-transform:uppercase;color:var(--faint);margin-bottom:8px}
.toc ul{list-style:none}
.toc a{display:block;color:var(--dim);padding:3px 0}
.toc a:hover{color:var(--accent)}
.toc li.l3 a{padding-left:14px;color:var(--faint)}
.toc li.l3 a:hover{color:var(--accent)}

@media(max-width:1100px){
  .layout{grid-template-columns:230px minmax(0,1fr)}
  .toc{display:none}
}
@media(max-width:900px){
  .layout{grid-template-columns:minmax(0,1fr)}
  .menu{display:inline-flex;align-items:center;justify-content:center}
  .sidebar{position:fixed;left:0;top:56px;z-index:12;width:min(280px,85vw);
    background:var(--bg);border-right:1px solid var(--border);
    transform:translateX(-105%);transition:transform .18s ease;padding:20px 16px 40px}
  .sidebar.open{transform:none}
  .sidebar a{padding:10px 12px;min-height:44px}
  main{padding-top:24px}
}
@media(max-width:480px){
  body{font-size:15px}
  .layout{padding:0 16px}
  .prose h1{font-size:24px}
}
</style>
</head>
<body>
<a class="skip" href="#content">Skip to content</a>
<header class="top">
  <button class="menu" aria-label="Toggle navigation" aria-expanded="false" aria-controls="sidebar">☰</button>
  <a class="wordmark" href="{{.Base}}/"><span class="g">⑂</span> offshoot</a>
  <nav aria-label="Site">
    <a href="{{.Base}}/docs/"{{if .IsIndex}} aria-current="page"{{end}}>docs</a>
    <a href="https://github.com/sricola/offshoot">github</a>
    <button class="theme" aria-label="Toggle color theme">◐</button>
  </nav>
</header>
<div class="layout">
<nav id="sidebar" class="sidebar" aria-label="Documentation">
{{$cur := .Slug}}{{range .Nav}}  <div class="group">
    <div class="glabel">{{.Name}}</div>
    <ul>
{{range .Pages}}      <li><a href="{{$.Base}}/docs/{{.Slug}}/"{{if eq .Slug $cur}} aria-current="page"{{end}}>{{.Title}}</a></li>
{{end}}    </ul>
  </div>
{{end}}</nav>
<main id="content">
  <article class="prose">
{{.Content}}
  </article>
  <footer class="pagefoot">
{{if .MDPath}}    <div class="src"><a href="` + repoBlob + `{{.MDPath}}">Markdown source</a></div>
{{end}}{{if or .Prev .Next}}    <nav class="pagenav" aria-label="Previous and next page">
{{if .Prev}}      <a class="prev" href="{{.Base}}/docs/{{.Prev.Slug}}/" rel="prev">← {{.Prev.Title}}</a>
{{end}}{{if .Next}}      <a class="next" href="{{.Base}}/docs/{{.Next.Slug}}/" rel="next">{{.Next.Title}} →</a>
{{end}}    </nav>
{{end}}  </footer>
</main>
{{if .TOC}}<aside class="toc" aria-label="On this page">
  <div class="tlabel">On this page</div>
  <ul>
{{range .TOC}}    <li class="l{{.Level}}"><a href="#{{.ID}}">{{.Text}}</a></li>
{{end}}  </ul>
</aside>
{{end}}</div>
<div class="scrim" hidden></div>
<script>
(function(){
  var mb=document.querySelector(".menu"),sb=document.getElementById("sidebar"),
      sc=document.querySelector(".scrim"),tb=document.querySelector(".theme");
  function nav(open){
    sb.classList.toggle("open",open);
    sc.hidden=!open;
    mb.setAttribute("aria-expanded",String(open));
  }
  mb.addEventListener("click",function(){nav(!sb.classList.contains("open"))});
  sc.addEventListener("click",function(){nav(false)});
  document.addEventListener("keydown",function(e){if(e.key==="Escape")nav(false)});
  tb.addEventListener("click",function(){
    var cur=document.documentElement.dataset.theme||
      (matchMedia("(prefers-color-scheme: dark)").matches?"dark":"light");
    var next=cur==="dark"?"light":"dark";
    document.documentElement.dataset.theme=next;
    localStorage.setItem("theme",next);
  });
})();
</script>
</body>
</html>
`
