#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Robert McDermott
"""Build the fs-inventory documentation site from the repository's Markdown.

    uv run --no-project --with markdown-it-py==4.2.0 site/build.py [options]

Reads README.md, docs/user-guide.md, docs/schema.md, docs/architecture.md, and
CHANGELOG.md from --source (default: this repository) and writes a static site
to --out (default: _site/) for GitHub Pages. Links between those files become
links between pages; links to other files point to them on GitHub at --ref.
Fails if a link to a page, or to a section of a page, is broken.
"""

import argparse
import html
import json
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from markdown_it import MarkdownIt

HERE = Path(__file__).resolve().parent
TEMPLATES = HERE / "templates"
REPOSITORY = "robert-mcdermott/fs-inventory"
GITHUB = f"https://github.com/{REPOSITORY}"


@dataclass
class Page:
    file: str  # output file name
    source: str  # Markdown source, relative to the repository root
    title: str  # tab and <title>
    description: str
    # Drop the Markdown before this heading: the page has its own header.
    start: str = ""
    wide: bool = False  # for pages that are mostly tables


PAGES = [
    Page(
        "index.html",
        "README.md",
        "Overview",
        "fs-inventory crawls file systems in parallel and records the metadata of every file "
        "as Parquet, CSV, or JSON Lines for DuckDB.",
        start="## Features",
    ),
    Page(
        "guide.html",
        "docs/user-guide.md",
        "User guide",
        "Installing fs-inventory, common scenarios, every command and option, the output, "
        "and querying scans with DuckDB.",
    ),
    Page(
        "schema.html",
        "docs/schema.md",
        "Schema",
        "Every column of every table fs-inventory writes.",
        wide=True,
    ),
    Page(
        "architecture.html",
        "docs/architecture.md",
        "Architecture",
        "How fs-inventory works: the crawler, platform backends, output pipeline, "
        "robustness, and checkpoints.",
    ),
    Page(
        "changelog.html",
        "CHANGELOG.md",
        "Changelog",
        "What each fs-inventory release contains.",
    ),
]
BY_SOURCE = {p.source: p for p in PAGES}

# "4. " or "4.12 ", but not a version such as "0.4.0".
SECTION_NUMBER = r"^(\d+)\.(?:\d+)?\s+"

# Code block labels and highlight.js languages.
LABELS = {"bash": "shell", "sql": "sql", "json": "json", "": "text"}
HIGHLIGHT = {"": "plaintext"}


def slugify(text: str, seen: dict) -> str:
    """A heading's anchor, the way GitHub makes it, so links work in both."""
    s = html.unescape(re.sub(r"<[^>]+>", "", text)).strip().lower()
    s = re.sub(r"[^\w\- ]", "", s).replace(" ", "-")
    n = seen.get(s, 0)
    seen[s] = n + 1
    return s if n == 0 else f"{s}-{n}"


def strip_tags(s: str) -> str:
    # The pieces of inline code join up again without spaces.
    s = re.sub(r'<span class="nobr">([^<]*)</span>|<wbr>', r"\1", s)
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", s))).strip()


# Where inline code may wrap besides its spaces: after a slash, comma, or
# opening parenthesis.
CODE_BREAK = re.compile(r"(?<=[/,(])")


def code_html(text: str) -> str:
    """Inline code that wraps only at those places, never inside a name such
    as `--max-depth` or `FS_INVENTORY_INSTALL_DIR`. Browsers would otherwise
    break it at a hyphen, and narrow table columns to break it anywhere."""
    words = []
    for word in text.split(" "):
        pieces: list[str] = []
        for piece in CODE_BREAK.split(word):
            # No piece as short as the "/" that starts a path or the ")"
            # that ends a call.
            if pieces and (len(piece) < 3 or len(pieces[-1]) < 3):
                pieces[-1] += piece
            elif piece:
                pieces.append(piece)
        words.append("<wbr>".join(f'<span class="nobr">{html.escape(p)}</span>' for p in pieces))
    return " ".join(words)


def markdown_body(page: Page, source_dir: Path) -> str:
    text = (source_dir / page.source).read_text()
    if page.start:
        text = text[text.index(page.start) :]
    else:
        # The page header shows the title.
        text = re.sub(r"\A# .*\n", "", text)
    # The site's navigation replaces a Markdown table of contents.
    text = re.sub(r"^## Contents\n.*?(?=^## )", "", text, flags=re.S | re.M)
    return text


class Renderer:
    def __init__(self, ref: str):
        self.ref = ref
        self.md = MarkdownIt("commonmark", {"html": True}).enable(["table", "strikethrough"])
        self.md.add_render_rule(
            "code_inline", lambda _, tokens, idx, *rest: f"<code>{code_html(tokens[idx].content)}</code>"
        )
        self.ids: dict[str, set[str]] = {}
        self.links: list[tuple[str, str]] = []  # (page, href) to check

    def link(self, page: Page, href: str) -> str:
        """Points a link in `page`'s source at a page of the site or at GitHub."""
        if re.match(r"^[a-z]+:", href):
            return href
        if href.startswith("#"):
            self.links.append((page.file, page.file + href))
            return href
        path, _, fragment = href.partition("#")
        base = PurePosixPath(page.source).parent
        target = str(PurePosixPath(base, path))
        # Normalize "docs/../CHANGELOG.md" and "./x".
        parts: list[str] = []
        for part in target.split("/"):
            if part == "..":
                if parts:
                    parts.pop()
            elif part not in ("", "."):
                parts.append(part)
        target = "/".join(parts)
        if target in BY_SOURCE:
            local = BY_SOURCE[target].file + (f"#{fragment}" if fragment else "")
            self.links.append((page.file, local))
            return local
        kind = "tree" if path.endswith("/") or target in ("sql", "bench", "docs") else "blob"
        return f"{GITHUB}/{kind}/{self.ref}/{target}" + (f"#{fragment}" if fragment else "")

    def render(self, page: Page, source_dir: Path) -> tuple[str, list, list]:
        body = self.md.render(markdown_body(page, source_dir))

        seen: dict = {}
        headings = []

        def heading(m):
            level, inner = int(m.group(1)), m.group(2)
            hid = slugify(inner, seen)
            headings.append((level, hid, inner))
            return (
                f'<h{level} id="{hid}">{inner}'
                f'<a class="anchor" href="#{hid}" aria-label="Link to this section">#</a></h{level}>'
            )

        body = re.sub(r"<h([234])>(.*?)</h\1>", heading, body, flags=re.S)
        self.ids[page.file] = {h[1] for h in headings}

        def anchor(m):
            href = html.unescape(m.group(1))
            new = self.link(page, href)
            attrs = ' target="_blank" rel="noopener"' if re.match(r"^https?:", new) else ""
            return f'<a href="{html.escape(new, quote=True)}"{attrs}>'

        body = re.sub(r'<a href="([^"]*)">', anchor, body)

        def codeblock(m):
            lang = m.group(1) or ""
            code = m.group(2)
            if lang == "mermaid":
                return f'<figure class="diagram"><pre class="mermaid">{code}</pre></figure>'
            return (
                f'<div class="code" data-lang="{lang or "text"}">'
                f'<div class="code-bar"><span class="code-lang">{LABELS.get(lang, lang)}</span>'
                f'<button class="copy" type="button" aria-label="Copy code">Copy</button></div>'
                f'<pre><code class="language-{HIGHLIGHT.get(lang, lang)}">{code}</code></pre></div>'
            )

        body = re.sub(
            r'<pre><code(?: class="language-([\w-]+)")?>(.*?)</code></pre>', codeblock, body, flags=re.S
        )
        body = body.replace("<table>", '<div class="table-wrap"><table>').replace(
            "</table>", "</table></div>"
        )
        body = re.sub(
            r"<blockquote>(.*?)</blockquote>",
            r'<aside class="callout callout-note">\1</aside>',
            body,
            flags=re.S,
        )
        body = re.sub(r"<hr\s*/?>\n?", "", body)

        # One <section> per h2, for the "on this page" list. Sections with
        # nothing under their heading (an empty "Unreleased") are dropped.
        parts = re.split(r'(?=<h2 id=")', body)
        sections = []
        for part in parts:
            part = part.strip()
            if not part:
                continue
            m = re.match(r'<h2 id="([^"]+)">.*?</h2>', part, flags=re.S)
            if m and not part[m.end() :].strip():
                headings = [h for h in headings if h[1] != m.group(1)]
                continue
            sid = m.group(1) if m else "intro"
            sections.append(f'<section class="doc-section" data-section="{sid}">\n{part}\n</section>')
        return "\n".join(sections), headings, self.search_entries(page, body)

    def search_entries(self, page: Page, body: str) -> list:
        entries = []
        h2_title = ""
        for chunk in re.split(r'(?=<h[23] id=")', body):
            m = re.match(r'<h([23]) id="([^"]+)">(.*?)<a class="anchor"', chunk, flags=re.S)
            if not m:
                continue
            level, hid, title = int(m.group(1)), m.group(2), strip_tags(m.group(3))
            if level == 2:
                h2_title = clean_title(title)
            text = re.sub(r"<h[23][^>]*>.*?</h[23]>", "", chunk, count=1, flags=re.S)
            text = strip_tags(re.sub(r'<div class="code-bar">.*?</div>', "", text, flags=re.S))
            terms = re.findall(r"--[a-z][a-z0-9-]+|fs-inventory [a-z]+|\bfs_[a-z_]+", text)
            entries.append(
                {
                    "p": page.file,
                    "l": level,
                    "id": hid,
                    "t": clean_title(title),
                    "s": page.title + (f" · {h2_title}" if level == 3 else ""),
                    "x": text[:900],
                    "c": sorted(set(terms))[:16],
                }
            )
        return entries


def clean_title(title: str) -> str:
    """A heading without its section number ("4." or "4.12")."""
    return re.sub(SECTION_NUMBER, "", title)


def nav_html(headings: list) -> str:
    sections: list[dict] = []
    for level, hid, inner in headings:
        # The entry is a link itself, so links in the heading become plain text.
        inner = re.sub(r"</?a\b[^>]*>", "", inner)
        if level == 2:
            sections.append({"id": hid, "title": inner, "children": []})
        elif level == 3 and sections:
            sections[-1]["children"].append({"id": hid, "title": inner})
    items = []
    for s in sections:
        number = re.match(SECTION_NUMBER, strip_tags(s["title"]))
        label = re.sub(SECTION_NUMBER, "", s["title"])
        num = f'<span class="nav-num">{int(number.group(1)):02d}</span>' if number else ""
        kids = "".join(
            f'<li><a href="#{c["id"]}" data-target="{c["id"]}">{c["title"]}</a></li>'
            for c in s["children"]
        )
        items.append(
            f'<li class="nav-sec{"" if number else " nav-sec-plain"}">'
            f'<a href="#{s["id"]}" data-target="{s["id"]}">{num}<span>{label}</span></a>'
            + (f'<ul class="nav-sub">{kids}</ul>' if kids else "")
            + "</li>"
        )
    return f'<div class="nav-group"><p class="nav-label">On this page</p><ul>{"".join(items)}</ul></div>'


def tabs_html(current: Page, cls: str) -> str:
    current_attr = ' aria-current="page"'
    links = "".join(
        f'<a href="{p.file}"{current_attr if p is current else ""}>{p.title}</a>' for p in PAGES
    )
    return f'<nav class="{cls}" aria-label="Pages">{links}</nav>'


def version(source_dir: Path) -> str:
    cargo = (source_dir / "Cargo.toml").read_text()
    m = re.search(r'^version = "([^"]+)"', cargo, flags=re.M)
    return m.group(1) if m else "unknown"


def default_ref(source_dir: Path) -> str:
    try:
        out = subprocess.run(
            ["git", "-C", str(source_dir), "describe", "--tags", "--exact-match"],
            capture_output=True,
            text=True,
            check=True,
        )
        return out.stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "main"


def fill(template: str, values: dict) -> str:
    for key, value in values.items():
        template = template.replace("{{" + key + "}}", value)
    leftover = re.findall(r"\{\{[A-Z_]+\}\}", template)
    if leftover:
        sys.exit(f"unfilled placeholders: {sorted(set(leftover))}")
    return template


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--source", type=Path, default=HERE.parent, help="repository to read")
    parser.add_argument("--out", type=Path, default=HERE.parent / "_site", help="output directory")
    parser.add_argument("--ref", help="tag or branch for links to GitHub [default: the tag checked out, or main]")
    args = parser.parse_args()
    source_dir = args.source.resolve()
    ref = args.ref or default_ref(source_dir)
    ver = version(source_dir)

    renderer = Renderer(ref)
    head = (TEMPLATES / "head.html").read_text()
    shell = (TEMPLATES / "page.html").read_text()
    hero = re.sub(
        r"<code>(.*?)</code>",
        lambda m: f"<code>{code_html(html.unescape(m.group(1)))}</code>",
        (TEMPLATES / "hero.html").read_text(),
    )
    search_index = []
    pages_html = {}
    for page in PAGES:
        content, headings, entries = renderer.render(page, source_dir)
        search_index.extend(entries)
        if page.file == "index.html":
            header = fill(hero, {"VERSION": ver})
        else:
            header = (
                f'<header class="page-header"><p class="eyebrow">fs-inventory {ver}</p>'
                f"<h1>{page.title}</h1><p class=\"lede\">{html.escape(page.description)}</p></header>"
            )
        diagrams = '<pre class="mermaid">' in content
        values = {
            "TITLE": "fs-inventory" if page.file == "index.html" else f"{page.title} · fs-inventory",
            "DESCRIPTION": html.escape(page.description, quote=True),
            "VERSION": ver,
            "GITHUB": GITHUB,
            "TABS": tabs_html(page, "tabs"),
            "SIDE_TABS": tabs_html(page, "side-tabs"),
            "NAV": nav_html(headings),
            "HEADER": header,
            "CONTENT": content,
            "DOC_CLASS": "doc doc-wide" if page.wide else "doc",
            "EDIT_URL": f"{GITHUB}/blob/main/{page.source}",
            "SOURCE_URL": f"{GITHUB}/blob/{ref}/{page.source}",
            "SCRIPTS": (
                '<script src="https://cdn.jsdelivr.net/npm/mermaid@11.12.0/dist/mermaid.min.js"></script>\n'
                if diagrams
                else ""
            ),
        }
        page_head = fill(head, {k: values[k] for k in ("TITLE", "DESCRIPTION")})
        pages_html[page.file] = (
            "<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n"
            '<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n'
            + page_head
            + '<link rel="icon" href="favicon.svg" type="image/svg+xml">\n'
            '<link rel="stylesheet" href="assets/css/site.css">\n'
            "</head>\n<body>\n"
            + fill(shell, values)
            + '\n<script src="assets/js/search-index.js"></script>\n'
            + values["SCRIPTS"]
            + '<script src="assets/js/site.js"></script>\n</body>\n</html>\n'
        )
        # Links in the hero and page shell are checked too.
        for href in re.findall(r'href="([^"#:]*\.html)?(#[^"]*)?"', header):
            target = (href[0] or page.file) + (href[1] or "")
            renderer.links.append((page.file, target))

    broken = []
    for origin, target in renderer.links:
        file, _, fragment = target.partition("#")
        file = file or origin
        if file not in renderer.ids:
            broken.append(f"{origin}: {target} (no such page)")
        elif fragment and fragment not in renderer.ids[file]:
            broken.append(f"{origin}: {target} (no such section)")
    if broken:
        sys.exit("broken links:\n  " + "\n  ".join(sorted(set(broken))))

    out = args.out.resolve()
    if out.exists():
        shutil.rmtree(out)
    (out / "assets" / "css").mkdir(parents=True)
    (out / "assets" / "js").mkdir(parents=True)
    shutil.copy(TEMPLATES / "site.css", out / "assets" / "css" / "site.css")
    shutil.copy(TEMPLATES / "site.js", out / "assets" / "js" / "site.js")
    shutil.copy(TEMPLATES / "favicon.svg", out / "favicon.svg")
    (out / "assets" / "js" / "search-index.js").write_text(
        "window.FSINV_SEARCH_INDEX = " + json.dumps(search_index, ensure_ascii=False) + ";\n"
    )
    (out / ".nojekyll").write_text("")
    for name, text in pages_html.items():
        (out / name).write_text(text)
    print(
        f"built {len(PAGES)} pages for fs-inventory {ver} ({ref}): "
        f"{len(search_index)} search entries, {len(renderer.links)} links checked, into {out}"
    )


if __name__ == "__main__":
    main()
