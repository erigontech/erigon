"""Unit tests for generate-llms.py.

Run from repo root:
  python3 -m unittest discover docs/site/scripts -v
  python3 docs/site/scripts/test_generate_llms.py    # direct invocation also works
"""

import importlib.util
import re
import unittest
from collections import Counter
from pathlib import Path

_HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("generate_llms", _HERE / "generate-llms.py")
g = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(g)

class FrontmatterTests(unittest.TestCase):
    def test_simple_kv(self):
        meta, body = g.parse_frontmatter("---\ntitle: Hello\nposition: 1\n---\nbody")
        self.assertEqual(meta["title"], "Hello")
        self.assertEqual(meta["position"], "1")
        self.assertEqual(body.strip(), "body")

    def test_quoted_value_with_colon(self):
        meta, _ = g.parse_frontmatter('---\ntitle: "Hello: World"\n---\n')
        self.assertEqual(meta["title"], "Hello: World")

    def test_indented_continuation_skipped(self):
        meta, _ = g.parse_frontmatter("---\ntags:\n  - one\n  - two\ntitle: Foo\n---\n")
        self.assertEqual(meta["title"], "Foo")
        self.assertNotIn("- one", meta)

    def test_no_frontmatter(self):
        meta, body = g.parse_frontmatter("# Heading\n\nBody only.")
        self.assertEqual(meta, {})
        self.assertIn("# Heading", body)


class SafeIntTests(unittest.TestCase):
    def test_valid(self):
        self.assertEqual(g._safe_int("42", 0), 42)

    def test_invalid_falls_back(self):
        self.assertEqual(g._safe_int("not-a-number", 99), 99)

    def test_none_falls_back(self):
        self.assertEqual(g._safe_int(None, 7), 7)


class LeadingH1StripTests(unittest.TestCase):
    """The build() body-prep step strips a leading H1 to avoid duplicate headings."""

    def test_leading_h1_stripped(self):
        import re
        body = "# Title\n\nContent here."
        out = re.sub(r"^#\s+[^\n]+\n?", "", body.lstrip("\n"), count=1)
        self.assertNotIn("# Title", out)
        self.assertIn("Content here.", out)

    def test_no_h1_unchanged(self):
        import re
        body = "## Subheading\n\nContent."
        out = re.sub(r"^#\s+[^\n]+\n?", "", body.lstrip("\n"), count=1)
        self.assertIn("## Subheading", out)


class ArticleTextTests(unittest.TestCase):
    """Rendering a built <article> subtree as Markdown-ish text."""

    def render(self, html):
        p = g._ArticleText()
        p.feed(html)
        p.close()
        return p.text()

    def test_bullet_list_one_item_per_line(self):
        self.assertEqual(
            "- A\n- B",
            self.render('<ul><li class="">A</li>\n<li class="">B</li></ul>'),
        )

    def test_ordered_list_keeps_its_numbers(self):
        self.assertEqual(
            "1. First\n2. Second",
            self.render("<ol><li>First</li><li>Second</li></ol>"),
        )

    def test_ordered_list_honours_start(self):
        self.assertEqual(
            "3. Third\n4. Fourth",
            self.render('<ol start="3"><li>Third</li><li>Fourth</li></ol>'),
        )

    def test_nested_list_is_indented(self):
        self.assertEqual(
            "- Top\n  - Inner\n\n- Next",
            self.render("<ul><li>Top<ul><li>Inner</li></ul></li><li>Next</li></ul>"),
        )

    def test_table_becomes_markdown_with_header_rule(self):
        self.assertEqual(
            "| H1 | H2 |\n| --- | --- |\n| a | b |",
            self.render(
                "<table><thead><tr><th>H1</th><th>H2</th></tr></thead>"
                "<tbody><tr><td>a</td><td>b</td></tr></tbody></table>"
            ),
        )

    def test_code_block_keeps_column_alignment(self):
        # CLI help and directory trees are aligned; reflowing them destroys the
        # only structure they have. This uses the markup Docusaurus actually
        # emits — one <div class="token-line"> per line, each ending in <br> —
        # because a synthetic <pre><code> with raw newlines cannot catch the
        # double-spacing that shape causes, and --check compares generated output
        # against output from the same code so it never would either.
        out = self.render(
            '<pre class="prism-code"><code>'
            '<div class="token-line"><span class="token plain">'
            "--webseed string      URLs</span><br></div>"
            '<div class="token-line"><span class="token plain">'
            "--datadir  string     Dir</span><br></div>"
            "</code></pre>"
        )
        self.assertEqual("```\n--webseed string      URLs\n--datadir  string     Dir\n```", out)

    def test_synthetic_pre_with_raw_newlines_also_works(self):
        out = self.render("<pre><code>line1\nline2\n</code></pre>")
        self.assertEqual("```\nline1\nline2\n```", out)

    def test_inline_emphasis_and_code(self):
        self.assertEqual(
            "A **bold** and `tok`.",
            self.render("<p>A <strong>bold</strong> and <code>tok</code>.</p>"),
        )

    def test_link_keeps_resolved_href(self):
        self.assertEqual(
            "See [that page](https://docs.erigon.tech/x/y) now.",
            self.render('<p>See <a href="/x/y">that page</a> now.</p>'),
        )

    def test_card_link_splits_label_from_blurb(self):
        # A landing-grid card is one <a> wrapping a title div and a description
        # div; fusing them would produce "[TitleDescription](url)".
        out = self.render(
            '<a href="/get-started/"><div class="lp-card-title">Get Started</div>'
            '<div class="lp-card-desc">Hardware and install.</div></a>'
        )
        self.assertEqual(
            "- [Get Started](https://docs.erigon.tech/get-started/): Hardware and install.",
            out,
        )

    def test_heading_anchor_is_dropped(self):
        out = self.render(
            '<h2 id="x">Title<a href="#x" class="hash-link">​</a></h2>'
        )
        self.assertEqual("## Title", out)

    def test_chrome_subtree_is_skipped(self):
        out = self.render(
            '<div class="tocCollapsible_x theme-doc-toc-mobile">'
            "<ul><li>On this page</li></ul></div><p>Body.</p>"
        )
        self.assertEqual("Body.", out)


class MermaidSpliceTests(unittest.TestCase):
    """Mermaid renders client-side, so its source is absent from built HTML."""

    def test_fence_is_extracted_from_source(self):
        src = 'intro\n\n```mermaid\ngraph TD\n  A --> B\n```\n\ntail\n'
        self.assertEqual([("", 0, "```mermaid\ngraph TD\n  A --> B\n```")],
                         g.mermaid_blocks(src))

    def test_no_mermaid_yields_nothing(self):
        self.assertEqual([], g.mermaid_blocks("prose\n\n```bash\nls\n```\n"))


class BuiltPathTests(unittest.TestCase):
    def test_flat_page_and_index_forms(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "get-started").mkdir()
            (root / "get-started" / "intro.html").write_text("x", encoding="utf-8")
            (root / "section").mkdir()
            (root / "section" / "index.html").write_text("x", encoding="utf-8")
            (root / "index.html").write_text("x", encoding="utf-8")
            orig = g.BUILD_DIR
            g.BUILD_DIR = root
            try:
                b = g.BASE_URL
                self.assertEqual(root / "get-started" / "intro.html",
                                 g.built_path_for(b + "/get-started/intro"))
                self.assertEqual(root / "section" / "index.html",
                                 g.built_path_for(b + "/section"))
                self.assertEqual(root / "index.html", g.built_path_for(b + "/"))
                self.assertIsNone(g.built_path_for(b + "/nope"))
            finally:
                g.BUILD_DIR = orig


class ArticleTextEdgeTests(unittest.TestCase):
    """Constructs the happy-path tests miss."""

    def render(self, html):
        p = g._ArticleText()
        p.feed(html)
        p.close()
        return p.text()

    def test_definition_list_is_not_fused(self):
        self.assertEqual(
            "Term\n\nDefinition",
            self.render("<dl><dt>Term</dt><dd>Definition</dd></dl>"),
        )

    def test_image_alt_text_is_kept(self):
        self.assertEqual(
            "![Meaningful diagram](https://docs.erigon.tech/x.png)",
            self.render('<p><img alt="Meaningful diagram" src="/x.png"></p>'),
        )

    def test_table_without_thead_gets_no_header_rule(self):
        # Promoting the first data row to a header would misattribute every cell.
        self.assertEqual(
            "| a | b |\n| c | d |",
            self.render(
                "<table><tbody><tr><td>a</td><td>b</td></tr>"
                "<tr><td>c</td><td>d</td></tr></tbody></table>"
            ),
        )

    def test_th_row_without_thead_is_a_header(self):
        self.assertEqual(
            "| H |\n| --- |\n| a |",
            self.render("<table><tr><th>H</th></tr><tr><td>a</td></tr></table>"),
        )

    def test_code_block_containing_a_fence_gets_a_longer_fence(self):
        self.assertEqual(
            "````\ntext\n```\nmore\n````",
            self.render("<pre><code>text\n```\nmore\n</code></pre>"),
        )

    def test_class_substring_is_not_treated_as_chrome(self):
        # `theme-doc-breadcrumbs-note` is page content, not the breadcrumb nav;
        # only whole class tokens count.
        self.assertEqual(
            "Real content.",
            self.render('<div class="theme-doc-breadcrumbs-note"><p>Real content.</p></div>'),
        )

    def test_real_chrome_class_is_still_skipped(self):
        self.assertEqual(
            "Body.",
            self.render(
                '<nav class="theme-doc-breadcrumbs breadcrumbsContainer_Z_bl">'
                "<p>nav</p></nav><p>Body.</p>"
            ),
        )

    def test_void_element_with_chrome_class_keeps_following_text(self):
        # A void tag has no end tag, so entering skip mode on one would swallow
        # the rest of its parent.
        self.assertEqual(
            "Before after.\n\nNext.",
            self.render(
                '<p>Before <img class="hash-link" src="/x"> after.</p><p>Next.</p>'
            ),
        )


class MermaidFenceSafetyTests(unittest.TestCase):
    def test_inner_fence_inside_a_longer_fence_is_not_a_diagram(self):
        # A page showing a mermaid example inside a ```` block is documentation
        # about mermaid, not a diagram to extract.
        self.assertEqual([], g.mermaid_blocks("````\n```mermaid\ngraph TD\n```\n````\n"))

    def test_shorter_closer_does_not_end_the_block(self):
        out = g.mermaid_blocks("````mermaid\ngraph TD\n```\n")
        self.assertEqual(1, len(out))
        self.assertTrue(out[0][2].startswith("````mermaid"))

    def test_diagram_is_extracted(self):
        self.assertEqual(
            [("", 0, "```mermaid\ngraph TD\n A-->B\n```")],
            g.mermaid_blocks("x\n\n```mermaid\ngraph TD\n A-->B\n```\n\ny\n"),
        )


# Captured verbatim from a real `npm run build` of this site. The synthetic
# fixtures above cannot catch a class-name mismatch or a wrapper shape change,
# and `--check` compares generated output against output from the same code, so
# it cannot either. These strings are the only guard against both.
_REAL_BREADCRUMBS = (
    '<nav class="theme-doc-breadcrumbs breadcrumbsContainer_Z_bl" aria-label="Breadcrumbs">'
    '<ul class="breadcrumbs"><li class="breadcrumbs__item">'
    '<a class="breadcrumbs__link" href="/get-started/">Get Started</a></li>'
    '<li class="breadcrumbs__item breadcrumbs__item--active">'
    '<span class="breadcrumbs__link">Why using Erigon?</span></li></ul></nav>'
)
_REAL_MOBILE_TOC = (
    '<div class="tocCollapsible_ETCw theme-doc-toc-mobile tocMobile_ITEo">'
    '<button type="button" class="clean-btn tocCollapsibleButton_TO0P">On this page</button></div>'
)
_REAL_HEADING = (
    '<h2 class="anchor anchorTargetStickyNavbar_Vzrq" id="at-a-glance">At a glance'
    '<a href="#at-a-glance" class="hash-link" aria-label="Direct link to At a glance"'
    ' title="Direct link to At a glance" translate="no">\u200b</a></h2>'
)
_REAL_CODE_BLOCK = (
    '<pre tabindex="0" class="prism-code language-text codeBlock_bY9V thin-scrollbar">'
    '<code class="codeBlockLines_e6Vv">'
    '<div class="token-line"><span class="token plain">erigon [options]</span><br></div>'
    '<div class="token-line"><span class="token plain">  --datadir path</span><br></div>'
    "</code></pre>"
)


class RealMarkupTests(unittest.TestCase):
    """Fixtures captured from an actual build, not hand-written approximations."""

    def render(self, html):
        p = g._ArticleText()
        p.feed(html)
        p.close()
        return p.text()

    def test_real_breadcrumb_nav_is_stripped(self):
        out = self.render(_REAL_BREADCRUMBS + "<p>Body.</p>")
        self.assertEqual("Body.", out)

    def test_real_mobile_toc_is_stripped(self):
        out = self.render(_REAL_MOBILE_TOC + "<p>Body.</p>")
        self.assertNotIn("On this page", out)
        self.assertEqual("Body.", out)

    def test_real_heading_drops_its_anchor(self):
        self.assertEqual("## At a glance", self.render(_REAL_HEADING))

    def test_real_code_block_is_not_double_spaced(self):
        self.assertEqual(
            "```text\nerigon [options]\n  --datadir path\n```",
            self.render(_REAL_CODE_BLOCK),
        )

    def test_every_chrome_class_matches_a_real_token(self):
        # A chrome entry that matches nothing is dead config and silently stops
        # stripping what it was added for. Comparing the tuple against a literal
        # copy of itself cannot detect that, so the tokens are looked for in the
        # built site — which is what a Docusaurus rename actually changes.
        if not g.BUILD_DIR.is_dir():
            self.fail("setup: docs/site/build is absent. Run `npm run build` in docs/site before these tests; CI builds first for this reason.")
        html = "".join(p.read_text(encoding="utf-8", errors="replace")
                       for p in list(g.BUILD_DIR.rglob("*.html"))[:40])
        tokens = set(re.findall(r'class="([^"]*)"', html))
        seen = {t for group in tokens for t in group.split()}
        for entry in g._CHROME_CLASSES:
            self.assertIn(entry, seen, f"chrome class {entry!r} matches nothing built")
        self.assertTrue(any(t.startswith(g._ADM_HEADING_PREFIX) for t in seen),
                        f"{g._ADM_HEADING_PREFIX!r} matches nothing built")


class BuiltSiteIntegrationTests(unittest.TestCase):
    """Runs against docs/site/build, which is a prerequisite rather than an option.

    Skipping when the build is missing let the whole class disappear from a green
    run, so a regression in built-site handling could not be caught by CI ordering
    alone. Absence is a setup error.
    """

    @classmethod
    def setUpClass(cls):
        if not g.BUILD_DIR.is_dir():
            raise RuntimeError("setup: docs/site/build is absent. Run `npm run build` in docs/site before these tests; CI builds first for this reason.")

    def test_no_chrome_leaks_into_any_page(self):
        leaked = []
        for name in ("get-started/why-using-erigon", "fundamentals/architecture",
                     "help-center/best-practices"):
            text = g.page_text_from_build(g.BASE_URL + "/" + name)
            if text is None:
                # A page that did not build is a missing assertion, not a pass.
                leaked.append(f"{name}: page not built")
                continue
            for junk in ("On this page", "Edit this page", "breadcrumbs__"):
                if junk in text:
                    leaked.append(f"{name}: {junk}")
        self.assertEqual([], leaked)

    def test_code_blocks_are_not_double_spaced(self):
        # The <br> ending every token line must not contribute a newline of its
        # own, which would leave a blank after *every* code line. That is the
        # pattern under test; a lone blank line is valid source formatting and
        # several fences in these docs rely on it.
        text = g.page_text_from_build(g.BASE_URL + "/fundamentals/basic-usage")
        self.assertIsNotNone(
            text, "setup: fundamentals/basic-usage is not in the build")
        doubled, fence, body = [], None, []
        for line in text.split("\n") + ["```"]:
            if line.lstrip().startswith("```"):
                if fence is not None:
                    blanks = len([x for x in body if not x.strip()])
                    # Doubling inserts exactly one blank after every line, so
                    # the body alternates strictly and never has two blanks in a
                    # row. That is what separates it from real code, where two
                    # consecutive blanks are ordinary formatting.
                    pairs = list(zip(body, body[1:]))
                    twice = any(not a.strip() and not b.strip()
                                for a, b in pairs)
                    alternating = all(
                        bool(a.strip()) != bool(b.strip()) for a, b in pairs)
                    if blanks >= 2 and alternating and not twice:
                        doubled.append(fence)
                    fence, body = None, []
                else:
                    fence, body = line.strip(), []
                continue
            if fence is not None:
                body.append(line)
        self.assertEqual([], doubled, "a blank line after every code line")


class CardAndCellShapeTests(unittest.TestCase):
    def render(self, html):
        p = g._ArticleText()
        p.feed(html)
        p.close()
        return p.text()

    def test_icon_is_not_used_as_the_card_label(self):
        self.assertEqual(
            "- [Ethereum Node](https://docs.erigon.tech/x/): Run a full node.",
            self.render(
                '<a href="/x/"><img alt="Ethereum" src="/img/eth.svg">'
                "<div>Ethereum Node</div><div>Run a full node.</div></a>"
            ),
        )

    def test_image_only_link_still_keeps_the_image(self):
        self.assertEqual(
            "[![Only](https://docs.erigon.tech/i.svg)](https://docs.erigon.tech/x/)",
            self.render('<a href="/x/"><img alt="Only" src="/i.svg"></a>'),
        )

    def test_br_in_a_table_cell_does_not_split_the_row(self):
        self.assertEqual(
            "| x y | z |",
            self.render("<table><tbody><tr><td>x<br>y</td><td>z</td></tr></tbody></table>"),
        )

    def test_br_in_prose_still_breaks_the_line(self):
        self.assertEqual("a\nb", self.render("<p>a<br>b</p>"))

    def test_heading_inside_a_link_emits_no_stray_marker(self):
        self.assertEqual(
            "- [T](https://docs.erigon.tech/x): D",
            self.render('<a href="/x"><h2>T</h2><p>D</p></a>'),
        )

    def test_pre_inside_a_link_emits_no_stray_fence(self):
        self.assertEqual(
            "[cmd](https://docs.erigon.tech/x)",
            self.render('<a href="/x"><pre><code>cmd</code></pre></a>'),
        )


class FenceTrackingTests(unittest.TestCase):
    """A longer fence keeps its columns even when its body holds a shorter one."""

    def render(self, html):
        p = g._ArticleText()
        p.feed(html)
        p.close()
        return p.text()

    def test_inner_fence_does_not_end_column_preservation(self):
        out = self.render("<pre><code>col1    col2\n```\nA       B\n</code></pre>")
        self.assertIn("A       B", out)
        self.assertTrue(out.startswith("````"))

    def test_pre_after_inline_text_is_separated(self):
        out = self.render("<div>text<pre><code>x\n</code></pre></div>")
        self.assertEqual("text\n\n```\nx\n```", out)

    def test_no_placeholder_leaks(self):
        for html in (
            "<p>a<br><br></p><pre><code>x\n</code></pre>",
            "<div></div><div></div><pre><code>x\n</code></pre>",
            "<pre><code>x\n</code></pre>",
        ):
            self.assertNotIn("\x00", self.render(html))


class ChromeAndStructureTests(unittest.TestCase):
    def render(self, html):
        p = g._ArticleText()
        p.feed(html)
        p.close()
        return p.text()

    def test_self_closing_void_inside_chrome_does_not_end_the_skip(self):
        # <br/> arrives through handle_startendtag; closing nothing, it must not
        # decrement the skip depth and leak the nav text after it.
        self.assertEqual(
            "Body.",
            self.render(
                '<nav class="theme-doc-breadcrumbs">nav<br/>more nav</nav><p>Body.</p>'
            ),
        )

    def test_list_inside_a_link_emits_no_stray_marker(self):
        self.assertEqual(
            "- [one](https://docs.erigon.tech/x): D",
            self.render('<a href="/x"><ul><li>one</li></ul><p>D</p></a>'),
        )

    def test_table_inside_a_link_emits_no_row_pipes(self):
        self.assertEqual(
            "- [c](https://docs.erigon.tech/x): D",
            self.render('<a href="/x"><table><tr><td>c</td></tr></table><p>D</p></a>'),
        )


class ArticleBodyTests(unittest.TestCase):
    def test_outermost_article_is_taken(self):
        self.assertEqual(
            "<p>a</p><article><p>b</p></article><p>c</p>",
            g._article_body("<article><p>a</p><article><p>b</p></article><p>c</p></article>"),
        )

    def test_unbalanced_article_is_refused_not_truncated(self):
        # Returning a partial body would look like a successful read.
        self.assertIsNone(g._article_body("<article><p>a</p>"))

    def test_no_article_returns_none(self):
        self.assertIsNone(g._article_body("<div><p>a</p></div>"))


class MermaidLivenessTests(unittest.TestCase):
    def test_an_indented_fence_is_still_a_diagram(self):
        # MDX has no indented code blocks, so indentation does not demote a
        # fence; this site's build renders such a block as a diagram.
        self.assertEqual([("", 0, "```mermaid\ngraph TD\n```")],
                         g.mermaid_blocks("    ```mermaid\n    graph TD\n    ```\n"))

    def test_delimiters_in_code_do_not_comment_out_a_diagram(self):
        # A page documenting `{/*` and `*/}` writes them in code spans. Taking a
        # region between them removes whatever renders in between, so the search
        # must not see delimiters that are themselves code.
        src = ("# P\n\nWrite `{/*` to open.\n\n## Live\n\n"
               "```mermaid\ngraph TD; A-->B;\n```\n\nclose with `*/}`.\n")
        self.assertEqual([("Live", 0, "```mermaid\ngraph TD; A-->B;\n```")],
                         g.mermaid_blocks(src))

    def test_delimiters_inside_a_fence_do_not_comment_out_a_diagram(self):
        src = ("# P\n\n```js\nconst c = {/*\n```\n\n## Live\n\n"
               "```mermaid\ngraph TD; A-->B;\n```\n\n```js\n*/}\n```\n")
        self.assertEqual(1, len(g.mermaid_blocks(src)))

    def test_commented_out_diagram_is_not_content(self):
        self.assertEqual([], g.mermaid_blocks("{/*\n```mermaid\ngraph TD\n```\n*/}\n"))

    def test_live_diagram_is_still_found(self):
        self.assertEqual(
            [("", 0, "```mermaid\ngraph TD\n```")],
            g.mermaid_blocks("```mermaid\ngraph TD\n```\n")
        )


class TableCellTests(unittest.TestCase):
    """A cell is one Markdown line, and its text cannot contain a bare pipe."""

    def render(self, html):
        p = g._ArticleText()
        p.feed(html)
        p.close()
        return p.text()

    def test_pipe_in_cell_text_is_escaped(self):
        self.assertEqual(
            "| A | B |\n| --- | --- |\n| QUANTITY\\|TAG | x |",
            self.render(
                "<table><thead><tr><th>A</th><th>B</th></tr></thead>"
                "<tbody><tr><td>QUANTITY|TAG</td><td>x</td></tr></tbody></table>"
            ),
        )

    def test_block_children_do_not_split_a_row(self):
        self.assertEqual(
            "| one two | z |",
            self.render(
                "<table><tbody><tr><td><p>one</p><p>two</p></td>"
                "<td>z</td></tr></tbody></table>"
            ),
        )

    def test_list_in_a_cell_stays_on_one_row(self):
        self.assertEqual(
            "| - a - b | z |",
            self.render(
                "<table><tbody><tr><td><ul><li>a</li><li>b</li></ul></td>"
                "<td>z</td></tr></tbody></table>"
            ),
        )


class MalformedAttributeTests(unittest.TestCase):
    """HTMLParser reports a valueless attribute as None."""

    def render(self, html):
        p = g._ArticleText()
        p.feed(html)
        p.close()
        return p.text()

    def test_valueless_class_does_not_crash(self):
        self.assertEqual("x", self.render("<div class><p>x</p></div>"))

    def test_valueless_start_does_not_crash(self):
        self.assertEqual("1. a", self.render("<ol start><li>a</li></ol>"))

    def test_blank_run_inside_a_fence_is_preserved(self):
        self.assertEqual(
            "```\na\n\n\n\nb\n```",
            self.render("<pre><code>a\n\n\n\nb\n</code></pre>"),
        )


class RenderingFidelityTests(unittest.TestCase):
    """Structure the built HTML carries and the Markdown output must keep.

    Fence languages, admonition type and title, blockquote markers, tab labels,
    link and code-span delimiters, and the boundaries the whitespace passes must
    not cross. `--check` compares this code's output against its own earlier
    output, so it cannot see any of these going wrong.
    """

    def render(self, html, page_url=""):
        p = g._ArticleText(page_url=page_url)
        p.feed(html)
        p.close()
        return p.text()

    # -- fence info string --------------------------------------------------
    def test_fence_keeps_its_language(self):
        # The info string is the one hint that says whether a block is shell,
        # JSON or YAML; the built HTML carries it as `language-*`.
        self.assertEqual(
            "```bash\nls -la\n```",
            self.render('<pre class="prism-code language-bash"><code>ls -la</code></pre>'),
        )

    def test_fence_without_a_language_stays_bare(self):
        self.assertEqual("```\nls\n```",
                         self.render("<pre><code>ls</code></pre>"))

    # -- admonitions --------------------------------------------------------
    def test_admonition_keeps_its_type_and_extent(self):
        html = ('<div class="theme-admonition theme-admonition-warning admonition_xJq3">'
                '<div class="admonitionHeading_Gvgb">'
                '<span class="admonitionIcon_a"><svg><path/></svg></span>warning</div>'
                '<div class="admonitionContent_BuS1"><p>Do not do this.</p></div></div>')
        self.assertEqual(":::warning\nDo not do this.\n:::", self.render(html))

    def test_admonition_label_is_not_left_as_bare_prose(self):
        html = ('<div class="theme-admonition theme-admonition-tip">'
                '<div class="admonitionHeading_Gvgb">tip</div>'
                '<div class="admonitionContent_BuS1"><p>Body.</p></div></div>')
        out = self.render(html)
        self.assertNotIn("\ntip\n", out)
        self.assertIn(":::tip", out)

    # -- blockquotes --------------------------------------------------------
    def test_blockquote_keeps_its_marker(self):
        self.assertEqual("> Quoted line.",
                         self.render("<blockquote><p>Quoted line.</p></blockquote>"))

    def test_a_quote_does_not_stack_empty_marker_lines(self):
        # A `>` line is not blank, so the collapse pass in text() cannot see a
        # run of them.
        out = self.render("<blockquote><p>N:</p>"
                          '<pre class="language-sh"><code>ls</code></pre></blockquote>')
        self.assertEqual("> N:\n>\n> ```sh\n> ls\n> ```", out)

    def test_nested_quotes_nest_their_markers(self):
        self.assertEqual(
            "> a\n>\n> > b",
            self.render("<blockquote><p>a</p><blockquote><p>b</p>"
                        "</blockquote></blockquote>"))

    def test_blockquote_marks_every_line(self):
        out = self.render("<blockquote><p>One.</p><p>Two.</p></blockquote>")
        self.assertTrue(all(ln.startswith(">") for ln in out.split("\n") if ln))

    # -- link buffer routing ------------------------------------------------
    def test_space_between_inline_elements_in_a_link_survives(self):
        # The whitespace guard has to test the buffer being written to; testing
        # the page buffer while emitting into the link buffer fuses the words.
        self.assertEqual(
            "See [**Alpha** *beta*](https://docs.erigon.tech/x) now.",
            self.render('<p>See <a href="/x"><strong>Alpha</strong> '
                        '<em>beta</em></a> now.</p>'),
        )

    def test_card_link_inside_a_table_cell_segments(self):
        out = self.render('<table><tbody><tr><td><a href="/y"><h2>T</h2>'
                          '<p>blurb</p></a></td><td>b</td></tr></tbody></table>')
        self.assertNotIn("Tblurb", out)
        self.assertIn("T", out)
        self.assertIn("blurb", out)

    # -- inline code delimiters ---------------------------------------------
    def test_inline_code_holding_a_backtick_stays_a_valid_span(self):
        self.assertEqual("``a ` b``", self.render("<p><code>a ` b</code></p>"))

    def test_inline_code_touching_a_backtick_is_padded(self):
        # CommonMark 6.1 strips one leading and trailing space back out.
        self.assertEqual("`` `x ``", self.render("<p><code>`x</code></p>"))

    # -- same-page anchors --------------------------------------------------
    def test_same_page_anchor_is_resolved_against_its_page(self):
        # A bare `#frag` means nothing once every page is pooled into one file.
        self.assertEqual(
            "[Option 1](https://docs.erigon.tech/get-started/migrating#option-1)",
            self.render('<p><a href="#option-1">Option 1</a></p>',
                        page_url="https://docs.erigon.tech/get-started/migrating"),
        )

    # -- <article> boundary -------------------------------------------------
    def test_article_end_inside_a_comment_does_not_truncate(self):
        body = g._article_body(
            "<article><p>real</p><!-- stray </article> --><p>more</p></article>")
        self.assertIn("more", body)

    # -- mermaid ------------------------------------------------------------
    def test_diagram_records_the_heading_above_it(self):
        src = "## At a glance\n\n```mermaid\ngraph TD\n```\n\nEvery box above...\n"
        self.assertEqual([("At a glance", 0, "```mermaid\ngraph TD\n```")],
                         g.mermaid_blocks(src))

    def test_diagram_is_spliced_under_its_heading_not_appended(self):
        body = "## At a glance\n\nEvery box above is a Go package.\n\n## Next\n\nx"
        out = g.splice_diagram(body, "At a glance", "```mermaid\ngraph TD\n```")
        self.assertLess(out.index("graph TD"), out.index("Every box above"))

    def test_a_hash_comment_inside_a_fence_is_not_a_heading(self):
        # A `#` line inside a fence is code: without fence tracking a bash
        # `# Usage` comment matches the "Usage" heading and the diagram lands
        # inside the code block.
        body = ("## Intro\n\n```bash\n# Usage\nerigon --help\n```\n\n"
                "## Usage\n\nReal section.\n")
        out = g.splice_diagram(body, "Usage", "```mermaid\nD\n```")
        self.assertIn("## Usage\n\n```mermaid\nD\n```", out)
        self.assertGreater(out.index("mermaid"), out.index("erigon --help"))

    def test_two_diagrams_under_one_heading_keep_source_order(self):
        body = g.splice_diagram("## H\n\ntext\n", "H", "```mermaid\nFIRST\n```")
        body = g.splice_diagram(body, "H", "```mermaid\nSECOND\n```")
        self.assertLess(body.index("FIRST"), body.index("SECOND"))

    def test_diagram_falls_back_to_appending_when_the_heading_is_gone(self):
        out = g.splice_diagram("no headings here", "Missing", "```mermaid\nx\n```")
        self.assertTrue(out.endswith("```mermaid\nx\n```"))

    def test_a_top_level_diagram_keeps_its_own_angle_brackets(self):
        # `>` is legal inside a diagram, so peeling container markers from a
        # fence that was never quoted would edit the diagram's source.
        out = g.mermaid_blocks("```mermaid\ngraph TD\n> a label\n```\n")
        self.assertIn("> a label", out[0][2])

    def test_diagram_inside_a_blockquote_is_still_found(self):
        out = g.mermaid_blocks("> ```mermaid\n> graph TD\n>   A-->B\n> ```\n")
        self.assertEqual(1, len(out))
        self.assertIn("graph TD", out[0][2])

    # -- first_description fence tracking -----------------------------------
    def test_four_backtick_fence_does_not_desync_the_description(self):
        # The generator itself emits a 4-backtick fence whenever a code block
        # contains a 3-backtick run, so this is one docs edit away from live.
        desc = ("The real description, long enough to clear the minimum length "
                "this function requires of a paragraph.")
        body = f"````\n```\ncode\n```\n````\n\n{desc}\n"
        self.assertEqual(desc, g.first_description(body))

    # -- frontmatter terminator ---------------------------------------------
    def test_frontmatter_terminator_must_be_a_whole_line(self):
        meta, rest = g.parse_frontmatter(
            '---\ntitle: "A ---- b"\ndescription: kept\n---\nBody.\n')
        self.assertEqual("kept", meta.get("description"))
        self.assertEqual("Body.\n", rest)

    # -- blank-line collapse ------------------------------------------------
    def test_a_paragraph_meeting_a_fence_leaves_one_blank_line(self):
        # Blank runs collapse in one pass over the whole text: collapsing per
        # chunk and re-joining puts the separator back at every fence boundary.
        out = self.render('<p>Run:</p><pre class="language-sh"><code>ls</code></pre>')
        self.assertEqual("Run:\n\n```sh\nls\n```", out)

    def test_blank_lines_inside_a_fence_are_code(self):
        out = self.render("<pre><code>a\n\n\nb</code></pre>")
        self.assertEqual("```\na\n\n\nb\n```", out)

    # -- table rows ---------------------------------------------------------
    def test_an_admonition_in_a_table_cell_does_not_split_the_row(self):
        out = self.render(
            '<table><tbody><tr><td><div class="theme-admonition theme-admonition-note">'
            '<div class="admonitionContent_x"><p>Careful.</p></div></div></td>'
            '<td>b</td></tr></tbody></table>')
        self.assertEqual("| Careful. | b |", out)

    # -- admonition titles --------------------------------------------------
    def test_a_custom_admonition_title_survives(self):
        # `:::tip Some title` puts the title in the heading div, where the type
        # word otherwise sits, so the div cannot simply be skipped.
        html = ('<div class="theme-admonition theme-admonition-tip">'
                '<div class="admonitionHeading_Rf37">'
                '<span class="admonitionIcon_a"><svg><path/></svg></span>'
                'For validators running behind NAT</div>'
                '<div class="admonitionContent_b"><p>Body.</p></div></div>')
        self.assertEqual(
            ":::tip For validators running behind NAT\nBody.\n:::", self.render(html))

    # -- quoted code --------------------------------------------------------
    def test_a_fence_inside_a_quote_keeps_its_columns(self):
        # By the time the whitespace passes run the lines already carry `> `, so
        # a fence scanner that does not peel the marker reflows quoted CLI output.
        out = self.render("<blockquote><p>N:</p>"
                          "<pre><code>col1    col2\nA       B</code></pre></blockquote>")
        self.assertIn("> col1    col2", out)
        self.assertIn("> A       B", out)

    def test_blank_lines_inside_a_quoted_fence_are_code(self):
        out = self.render("<blockquote><pre><code>a\n\n\nb</code></pre></blockquote>")
        self.assertEqual("> ```\n> a\n>\n>\n> b\n> ```", out)

    # -- leading H1 ---------------------------------------------------------
    def test_an_empty_h1_does_not_eat_the_first_prose_line(self):
        # `#\s+` would span the newline and reach the first prose line.
        self.assertEqual("First prose line.\n\nSecond.",
                         g.strip_leading_h1("# \n\nFirst prose line.\n\nSecond."))

    def test_a_real_leading_h1_is_still_stripped(self):
        self.assertEqual("Body.", g.strip_leading_h1("# Title\n\nBody."))

    # -- tabs ---------------------------------------------------------------
    def test_tab_labels_attach_to_their_panels(self):
        # Docusaurus links a label to its panel by order only, so the binding
        # has to be reconstructed or the panels read as an unlabelled run.
        html = ('<ul role="tablist" class="tabs">'
                '<li role="tab" class="tabs__item tabs__item--active">Ethereum mainnet</li>'
                '<li role="tab" class="tabs__item">Gnosis Chain</li></ul>'
                '<div class="margin-top--md">'
                '<div role="tabpanel"><p>Mainnet body.</p></div>'
                '<div role="tabpanel" hidden=""><p>Gnosis body.</p></div></div>')
        out = self.render(html)
        self.assertEqual(
            "**Ethereum mainnet**\n\nMainnet body.\n\n**Gnosis Chain**\n\nGnosis body.", out)
        self.assertNotIn("- Ethereum mainnet", out)

    def test_a_hidden_tab_panel_is_still_content(self):
        # Only the first panel is visible; the rest carry `hidden` but are real
        # page content and the corpus needs them.
        out = self.render('<ul role="tablist" class="tabs">'
                          '<li role="tab">A</li><li role="tab">B</li></ul>'
                          '<div role="tabpanel"><p>one</p></div>'
                          '<div role="tabpanel" hidden=""><p>two</p></div>')
        self.assertIn("two", out)

    # -- details ------------------------------------------------------------
    def test_a_summary_reads_as_a_label(self):
        self.assertEqual(
            "**Reset database**\n\nDeletes chain data.",
            self.render("<details><summary>Reset database</summary>"
                        "<p>Deletes chain data.</p></details>"))

    # -- version coupling ---------------------------------------------------
    def test_a_new_release_is_drift_not_a_failure(self):
        # The build resolves {ERIGON_VERSION} from the GitHub Releases API, so
        # committed bytes change with no commit in this repo.
        a = "download erigon_v3.6.0_linux_amd64.tar.gz now"
        b = "download erigon_v3.6.1_linux_amd64.tar.gz now"
        self.assertIn("<release>", g._mask_versions(a))   # not vacuously equal
        self.assertEqual(g._mask_versions(a), g._mask_versions(b))

    def test_a_real_change_is_still_a_failure(self):
        self.assertNotEqual(g._mask_versions("erigon_v3.6.0 now"),
                            g._mask_versions("erigon_v3.6.0 later"))

    def test_only_the_erigon_release_is_masked(self):
        # Masking every N.N.N would also mask a Go or library version, so a real
        # docs edit to one of those would be downgraded to warning-only drift.
        a = "erigon_v3.6.0_linux needs Go 1.24.0"
        b = "erigon_v3.6.0_linux needs Go 1.25.0"
        self.assertIn("<release>", g._mask_versions(a))
        self.assertIn("Go 1.24.0", g._mask_versions(a))
        self.assertNotEqual(g._mask_versions(a), g._mask_versions(b))

    def test_text_with_no_release_anchor_is_untouched(self):
        self.assertEqual("needs Go 1.24.0", g._mask_versions("needs Go 1.24.0"))


class BuiltCorpusShapeTests(unittest.TestCase):
    """Assertions against the corpus this build actually produces."""

    @classmethod
    def setUpClass(cls):
        if not g.BUILD_DIR.is_dir():
            raise RuntimeError("setup: docs/site/build is absent. Run `npm run build` in docs/site before these tests; CI builds first for this reason.")
        _, cls.full, _ = g.build()

    def test_code_fences_carry_languages(self):
        self.assertGreater(len(re.findall(r"^```[a-z]", self.full, re.M)), 100)

    def test_admonitions_survive_as_directives(self):
        self.assertGreater(self.full.count(":::"), 50)

    def test_no_admonition_label_leaks_as_bare_prose(self):
        for label in ("warning", "tip", "info", "danger", "note", "caution"):
            self.assertNotIn(f"\n{label}\n", self.full,
                             f"bare {label!r} line leaked from an admonition heading")

    def test_no_link_target_is_meaningless_in_a_flat_file(self):
        self.assertEqual([], re.findall(r"\]\((?:/|#)[^)]*\)", self.full))


class ListContinuationTests(unittest.TestCase):
    """A nested block belongs to its list item, at that item's content column."""

    def render(self, html):
        p = g._ArticleText()
        p.feed(html)
        p.close()
        return p.text()

    def test_nested_bullet_takes_the_ordered_markers_width(self):
        # "16. " is four columns wide, so two spaces leaves the sub-list outside
        # the item and CommonMark reads it as a sibling list at top level.
        out = self.render(
            '<ol start="16"><li>Parent<ul><li>Child</li></ul></li></ol>')
        self.assertIn("\n    - Child", out)

    def test_a_nested_paragraph_keeps_the_items_indent(self):
        out = self.render("<ul><li><p>First</p><p>Second</p></li></ul>")
        self.assertIn("\n  Second", out)

    def test_a_blank_line_in_an_item_carries_no_trailing_space(self):
        out = self.render("<ul><li><p>First</p><p>Second</p></li></ul>")
        for line in out.split("\n"):
            self.assertEqual(line, line.rstrip(),
                             f"trailing whitespace: {line!r}")

    def test_a_two_space_marker_still_indents_by_two(self):
        self.assertEqual(
            "- Top\n  - Inner\n\n- Next",
            self.render("<ul><li>Top<ul><li>Inner</li></ul></li><li>Next</li></ul>"),
        )


class ContainerRelativeFenceTests(unittest.TestCase):
    """CommonMark's three-column fence slack is measured from the container."""

    def test_a_mermaid_fence_inside_a_list_item_is_found(self):
        src = "## H\n\n1. Step\n\n    ```mermaid\n    graph TD; A-->B;\n    ```\n"
        self.assertEqual([("H", 0, "```mermaid\ngraph TD; A-->B;\n```")],
                         g.mermaid_blocks(src))

    def test_a_four_space_fence_at_top_level_is_a_diagram(self):
        # CommonMark would call four columns an indented code block, but MDX has
        # no indented code blocks: this site's production build renders such a
        # fence as a diagram, so rejecting it dropped the graph from the corpus.
        src = "## H\n\n    ```mermaid\n    graph TD; A-->B;\n    ```\n"
        self.assertEqual([("H", 0, "```mermaid\ngraph TD; A-->B;\n```")],
                         g.mermaid_blocks(src))


class DiagramSpliceTests(unittest.TestCase):
    """Every diagram in the source reaches the corpus, under its own heading."""

    def test_two_sections_sharing_a_title_keep_their_own_diagram(self):
        # Matching heading text alone put every diagram under the first section
        # with that title; 24 headings repeat across these docs.
        a = "```mermaid\ngraph TD; A-->B;\n```"
        b = "```mermaid\ngraph TD; C-->D;\n```"
        src = f"## Setup\n\n{a}\n\n## Other\n\nx\n\n## Setup\n\n{b}\n"
        found = g.mermaid_blocks(src)
        self.assertEqual([("Setup", 0), ("Setup", 1)],
                         [(h, o) for h, o, _ in found])
        body = "# T\n\n## Setup\n\nfirst\n\n## Other\n\nx\n\n## Setup\n\nsecond\n"
        for heading, occurrence, diagram in found:
            body = g.splice_diagram(body, heading, diagram, occurrence)
        sections = body.split("## ")
        self.assertIn("A-->B", sections[1], "first diagram left its section")
        self.assertIn("C-->D", sections[3], "second diagram went to the wrong one")
        self.assertNotIn("C-->D", sections[1])

    def test_a_diagram_under_a_later_heading_is_not_moved_to_the_first(self):
        # The occurrence has to count headings, not diagrams. Counting diagrams
        # passed the test above — both sections had one, so the two numbers
        # agreed — while a diagram under the second `## Setup` alone was still
        # placed under the first.
        d = "```mermaid\ngraph TD; X-->Y;\n```"
        src = f"## Setup\n\nprose\n\n## Other\n\nx\n\n## Setup\n\n{d}\n"
        found = g.mermaid_blocks(src)
        self.assertEqual([("Setup", 1)], [(h, o) for h, o, _ in found])
        body = "# T\n\n## Setup\n\nFIRST\n\n## Other\n\nx\n\n## Setup\n\nSECOND\n"
        for heading, occurrence, diagram in found:
            body = g.splice_diagram(body, heading, diagram, occurrence)
        sections = body.split("## ")
        self.assertNotIn("X-->Y", sections[1], "diagram moved to the first section")
        self.assertIn("X-->Y", sections[3])

    def test_identical_diagrams_under_two_headings_both_survive(self):
        block = "```mermaid\ngraph TD; A-->B;\n```"
        src = f"## One\n\n{block}\n\n## Two\n\n{block}\n"
        found = g.mermaid_blocks(src)
        self.assertEqual([("One", 0, block), ("Two", 0, block)], found)
        body = "# T\n\n## One\n\nprose\n\n## Two\n\nprose\n"
        for heading, occurrence, diagram in found:
            body = g.splice_diagram(body, heading, diagram, occurrence)
        self.assertEqual(2, body.count("graph TD; A-->B;"))


class EmptyAfterHeadingRemovalTests(unittest.TestCase):
    """A page holding only its own H1 is empty once the duplicate comes off."""

    def test_a_heading_only_body_is_empty(self):
        self.assertEqual("", g.strip_leading_h1("# Only a heading\n").strip())

    def test_a_heading_plus_prose_is_not_empty(self):
        self.assertTrue(g.strip_leading_h1("# Title\n\nprose\n").strip())


class IndentMarkerTests(unittest.TestCase):
    """The indent markers are control bytes, so content must not be able to forge them."""

    def render(self, html):
        p = g._ArticleText()
        p.feed(html)
        p.close()
        return p.text()

    def test_a_control_byte_in_prose_does_not_reach_the_output(self):
        out = self.render("<p>weird \x02 and \x03 bytes</p>")
        self.assertNotIn("\x02", out)
        self.assertNotIn("\x03", out)
        self.assertIn("bytes", out)

    def test_a_control_byte_in_code_keeps_the_surrounding_text(self):
        out = self.render('<pre><code><div class="token-line">'
                          'a\x0212\x02b\x03c<br></div></code></pre>')
        self.assertIn("a12bc", out)

    def test_a_forged_pop_does_not_steal_an_enclosing_indent(self):
        # A stray pop would close the item early and put the child at column 0.
        out = self.render("<ul><li>Item \x03 text<ul><li>Child</li></ul></li></ul>")
        self.assertIn("\n  - Child", out)


class SourceFenceContainmentTests(unittest.TestCase):
    """Ties a fence in the source back to an indented fence in the corpus.

    Neither corpus-only check can catch a return to column-zero fences: a code
    block at column zero between two lists is legal Markdown and the real corpus
    has six of them. Only the source says a fence was written inside a list
    item, so the count of those is compared against the corpus.
    """

    def test_a_fence_written_inside_a_container_stays_indented(self):
        # MDX has no indented code blocks, so a fence written with any indent in
        # the source is inside a container — a list item in practice. If those
        # came back at column zero the item would have been broken open, and no
        # check over the corpus alone can see that: a fence at column zero
        # between two lists is ordinary Markdown and this corpus has six.
        # Openers are the unit; closers move independently of their container.
        def openers(lines):
            count, inside = 0, False
            for line in lines:
                fence = g._fence_at(line)
                if not fence:
                    continue
                if inside:
                    inside = False
                    continue
                inside = True
                if fence[2] > 0:
                    count += 1
            return count

        in_container = sum(
            openers(path.read_text(encoding="utf-8", errors="replace").split("\n"))
            for _, directory, _ in g.SECTIONS
            for path in sorted(directory.rglob("*.md*")))
        self.assertGreater(in_container, 0, "no indented fences found; check the scan")

        _, full, _ = g.build()
        self.assertEqual(
            in_container, openers(full.split("\n")),
            "an indented fence opener in the sources is not indented in the "
            "corpus: a fence has escaped its container")


class ReleaseDriftTests(unittest.TestCase):
    """A new Erigon release is drift; anything else is staleness."""

    def test_a_release_bump_compares_equal(self):
        a = g._mask_versions("erigon:v3.6.0 image erigon-v3.6.0 (e.g., v3.6.0)")
        b = g._mask_versions("erigon:v3.6.1 image erigon-v3.6.1 (e.g., v3.6.1)")
        self.assertEqual(a, b, "an unanchored v-prefixed mention broke drift")

    def test_a_bump_of_the_real_corpus_is_drift_not_staleness(self):
        # The token also expands where nothing marks the version as Erigon's —
        # `(e.g., v3.6.0)` and `git checkout v3.6.0` on the installation page —
        # so a mask anchored to `erigon[:_/-]` leaves those behind and the whole
        # corpus reads as stale on the next release.
        _, full, _ = g.build()
        anchor = g._RELEASE_ANCHOR_RE.findall(full)
        self.assertTrue(anchor, "no release anchor in the corpus")
        release = Counter(anchor).most_common(1)[0][0]
        major, minor, patch = release.split(".")
        bumped = full.replace(release, f"{major}.{minor}.{int(patch) + 1}")
        self.assertEqual(g._mask_versions(full), g._mask_versions(bumped))

    def test_a_longer_version_ending_in_the_release_is_not_masked(self):
        # Without a leading boundary the release matched the tail of `13.6.0`,
        # masking it to `1<release>`, which would let an edit to that unrelated
        # pin read as release drift. The source scan does not cover it either,
        # because it is not a literal use of the release.
        a = g._mask_versions("erigon_v3.6.0 and dependency 13.6.0")
        b = g._mask_versions("erigon_v3.6.1 and dependency 13.6.1")
        self.assertNotEqual(a, b, "an unrelated 13.6.x edit read as release drift")
        self.assertIn("13.6.0", a)

    def test_nothing_in_the_sources_hardcodes_the_release(self):
        # The mask is global, so it is only safe while nothing unrelated is
        # pinned at the same version — an edit to such a pin would compare equal
        # and pass as release drift. The output cannot separate the two, since
        # `dependency v3.6.0` and `git checkout v3.6.0` are the same string, but
        # the sources can: every legitimate site is written {ERIGON_VERSION}.
        _, full, _ = g.build()
        anchor = g._RELEASE_ANCHOR_RE.findall(full)
        self.assertTrue(anchor, "no release anchor in the corpus")
        release = Counter(anchor).most_common(1)[0][0]
        hits = g._literal_release_uses(release)
        self.assertEqual(
            [], [f"{p.name}:{n}: {t}" for p, n, t in hits],
            f"a source hardcodes {release} instead of the version token; "
            f"release-drift tolerance would hide an edit to it")

    def test_the_hazard_check_reports_a_hardcoded_pin(self):
        # A check that cannot fail is worse than no check, so plant one.
        import pathlib
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            src = pathlib.Path(d) / "page.mdx"
            src.write_text("dependency v9.9.9 is pinned\n", encoding="utf-8")
            real, g.SECTIONS = g.SECTIONS, [("t", pathlib.Path(d), "t")]
            try:
                hits = g._literal_release_uses("9.9.9")
            finally:
                g.SECTIONS = real
        self.assertEqual(1, len(hits), "planted pin was not reported")


if __name__ == "__main__":
    unittest.main()
