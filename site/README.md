# Documentation site

`build.py` turns the repository's Markdown into the site at <https://robert-mcdermott.github.io/fs-inventory/>:

| Page | Source |
|---|---|
| `index.html` | `README.md`, from "Features" on, below a hero from `templates/hero.html` |
| `guide.html` | `docs/user-guide.md` |
| `schema.html` | `docs/schema.md` |
| `architecture.html` | `docs/architecture.md` |
| `changelog.html` | `CHANGELOG.md` |

The Markdown is the only source: never edit the generated HTML. Links between those files become links between pages, and links to other files in the repository point to them on GitHub. The build fails if a link to a page, or to a section of a page, is broken, so CI catches broken links in pull requests.

## Build and preview

```bash
uv run --no-project --with markdown-it-py==4.2.0 site/build.py
```

```bash
python3 -m http.server 8000 --directory _site
```

Then open <http://localhost:8000>. The site goes to `_site/`, which git ignores.

## Publishing

GitHub Pages serves the `gh-pages` branch (**Settings → Pages → Deploy from a branch → `gh-pages`, `/ (root)`**). That branch holds only the built site, as a single commit, and nobody edits it by hand.

`.github/workflows/pages.yml` builds the site from a release's Markdown and commits it to `gh-pages` whenever a release is published, so the site always describes the version `install.sh` installs. To republish between releases, such as after fixing a typo in the docs or changing a template, run the workflow by hand:

```bash
gh workflow run pages.yml
```

That publishes the latest release's docs with the current builder. Add `-f ref=main` to publish the docs on `main` instead.

## Templates

`templates/` holds the page shell, the home page hero, the stylesheet, the script (navigation, search, copy buttons, diagrams, dark mode), and the favicon. They're adapted from the uv Field Guide. External assets are pinned: Google Fonts, highlight.js 11.12.0 from cdnjs, and, on pages with diagrams, Mermaid 11.12.0 from jsDelivr.
