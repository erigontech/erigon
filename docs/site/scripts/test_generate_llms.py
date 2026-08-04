"""Unit tests for generate-llms.py.

Run from repo root:
  python3 -m unittest discover docs/site/scripts -v
  python3 docs/site/scripts/test_generate_llms.py    # direct invocation also works
"""

import importlib.util
import unittest
from pathlib import Path

_HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("generate_llms", _HERE / "generate-llms.py")
g = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(g)


class StripMdxTests(unittest.TestCase):
    def test_preserves_inline_uppercase_placeholder(self):
        out = g.strip_mdx("Use `erigontech/erigon:v{ERIGON_VERSION}` to pull.")
        self.assertIn("{ERIGON_VERSION}", out)

    def test_preserves_bare_uppercase_placeholder_in_prose(self):
        out = g.strip_mdx("Download erigon_v{ERIGON_VERSION}_amd64.deb from releases.")
        self.assertIn("{ERIGON_VERSION}", out)

    def test_preserves_uppercase_placeholder_in_table_cell(self):
        out = g.strip_mdx("| binary | erigon_v{ERIGON_VERSION}_amd64.deb |")
        self.assertIn("{ERIGON_VERSION}", out)

    def test_strips_jsx_expression(self):
        out = g.strip_mdx("Hello {props.name} world.")
        self.assertNotIn("{props.name}", out)
        self.assertIn("Hello", out)
        self.assertIn("world", out)

    def test_strips_jsx_expression_with_arrow_fn(self):
        out = g.strip_mdx("Render {items.map(i => i.name)} list.")
        self.assertNotIn("items.map", out)

    def test_preserves_export_var_in_bash_fence(self):
        text = "```bash\nexport ERIGON_DATA=/var/erigon\n```"
        out = g.strip_mdx(text)
        self.assertIn("export ERIGON_DATA=/var/erigon", out)

    def test_strips_mdx_export_const(self):
        out = g.strip_mdx("export const VERSION = '1.0';\n\nHello world.")
        self.assertNotIn("export const", out)
        self.assertIn("Hello world", out)

    def test_strips_jsx_component_tag(self):
        out = g.strip_mdx("<Tabs>\n  Hello\n</Tabs>")
        self.assertNotIn("<Tabs>", out)
        self.assertNotIn("</Tabs>", out)
        self.assertIn("Hello", out)

    def test_preserves_lowercase_placeholder_in_backticks(self):
        out = g.strip_mdx("Set `<YOUR_ADDRESS>` to your wallet.")
        self.assertIn("<YOUR_ADDRESS>", out)

    def test_preserves_fence_content_unchanged(self):
        text = '```json\n{"jsonrpc":"2.0","method":"eth_call","params":[]}\n```'
        out = g.strip_mdx(text)
        self.assertIn('"jsonrpc":"2.0"', out)
        self.assertIn('"params":[]', out)

    def test_strips_multiline_jsx_expression_block(self):
        text = "Before.\n\n{[\n  {a:1},\n  {b:2},\n].map(x => x)}\n\nAfter."
        out = g.strip_mdx(text)
        self.assertNotIn("{a:1}", out)
        self.assertNotIn(".map(", out)
        self.assertIn("Before.", out)
        self.assertIn("After.", out)

    def test_strips_mdx_imports_outside_fence(self):
        out = g.strip_mdx("import Link from '@docusaurus/Link';\n\nReal content.")
        self.assertNotIn("import Link", out)
        self.assertIn("Real content", out)

    def test_preserves_imports_inside_fence(self):
        text = "```python\nimport os\nimport sys\n```"
        out = g.strip_mdx(text)
        self.assertIn("import os", out)
        self.assertIn("import sys", out)


class TabItemLabelTests(unittest.TestCase):
    """Tab labels must survive as headings — without them, sibling tables and
    code blocks are indistinguishable in the text output.
    """

    def test_label_becomes_heading_below_enclosing_section(self):
        text = (
            "## Disk Size\n\n"
            "<Tabs>\n"
            '<TabItem value="ethereum-mainnet" label="Ethereum mainnet">\n'
            "| Mode | Usage |\n"
            "</TabItem>\n"
            "</Tabs>"
        )
        out = g.strip_mdx(text)
        self.assertIn("### Ethereum mainnet", out)
        self.assertNotIn("TabItem", out)

    def test_sibling_tabs_are_each_labelled(self):
        text = (
            "## Disk Size\n\n"
            "<Tabs>\n"
            '<TabItem value="sepolia" label="Sepolia">\n'
            "| Archive | 1.10 TB |\n"
            "</TabItem>\n"
            '<TabItem value="hoodi" label="Hoodi">\n'
            "| Archive | 133.73 GB |\n"
            "</TabItem>\n"
            "</Tabs>"
        )
        out = g.strip_mdx(text)
        self.assertIn("### Sepolia", out)
        self.assertIn("### Hoodi", out)
        self.assertLess(out.index("### Sepolia"), out.index("1.10 TB"))
        self.assertLess(out.index("1.10 TB"), out.index("### Hoodi"))

    def test_depth_follows_nearest_preceding_heading(self):
        text = "### Install\n\n<TabItem value=\"linux\" label=\"Linux\">\napt install\n"
        out = g.strip_mdx(text)
        self.assertIn("#### Linux", out)

    def test_following_heading_stays_a_sibling(self):
        """A heading after the tab set must not become a subsection of the last
        tab — the installation page has exactly this shape (h1, OS tabs, then
        '### All Operating Systems').
        """
        text = (
            "# Installation\n\n"
            "<Tabs>\n"
            '<TabItem value="linux" label="Linux">\n'
            "apt install\n"
            "</TabItem>\n"
            '<TabItem value="windows" label="Windows">\n'
            "choco install\n"
            "</TabItem>\n"
            "</Tabs>\n\n"
            "### All Operating Systems\n\nShared steps.\n"
        )
        out = g.strip_mdx(text)
        self.assertIn("### Linux", out)
        self.assertIn("### Windows", out)
        self.assertNotIn("## Linux", out.replace("### Linux", ""))
        # Same level as the heading that follows, so it is not nested under it.
        self.assertIn("### All Operating Systems", out)

    def test_depth_defaults_when_no_preceding_heading(self):
        out = g.strip_mdx('<TabItem value="linux" label="Linux">\napt install\n')
        self.assertIn("### Linux", out)

    def test_depth_capped_at_h6(self):
        text = "###### Deep\n\n<TabItem value=\"linux\" label=\"Linux\">\napt install\n"
        out = g.strip_mdx(text)
        self.assertIn("###### Linux", out)
        self.assertNotIn("#######", out)

    def test_single_quoted_and_braced_labels(self):
        out = g.strip_mdx("## S\n\n<TabItem value='a' label='macOS'>\nbrew\n")
        self.assertIn("### macOS", out)
        out = g.strip_mdx("## S\n\n<TabItem value=\"a\" label={'Windows'}>\nchoco\n")
        self.assertIn("### Windows", out)

    def test_tabitem_inside_fence_untouched(self):
        text = '```mdx\n<TabItem value="a" label="Linux">\n```'
        out = g.strip_mdx(text)
        self.assertIn('<TabItem value="a" label="Linux">', out)

    def test_unlabelled_tabitem_falls_back_to_value(self):
        """Docusaurus renders `value` when `label` is absent, so a tab without a
        label must still get a heading — otherwise it silently reintroduces the
        unlabelled-tables problem.
        """
        out = g.strip_mdx('## S\n\n<TabItem value="linux">\nbody\n')
        self.assertIn("### linux", out)
        self.assertNotIn("TabItem", out)
        self.assertIn("body", out)

    def test_label_wins_over_value(self):
        out = g.strip_mdx('## S\n\n<TabItem value="macos" label="macOS">\nbrew\n')
        self.assertIn("### macOS", out)
        self.assertNotIn("### macos", out)

    def test_attributeless_tabitem_emits_no_heading(self):
        out = g.strip_mdx("## S\n\n<TabItem>\nbody\n")
        self.assertNotIn("TabItem", out)
        self.assertIn("body", out)
        self.assertEqual(out.count("###"), 0)

    def test_heading_inside_tab_body_does_not_split_the_set(self):
        """A heading inside the first tab's body must not demote the tabs after
        it: the set is delimited by <Tabs>…</Tabs>, not by heading positions.
        """
        text = (
            "## Setup\n\n"
            "<Tabs>\n"
            '<TabItem value="linux" label="Linux">\n'
            "#### Extra step\n"
            "apt install\n"
            "</TabItem>\n"
            '<TabItem value="windows" label="Windows">\n'
            "choco install\n"
            "</TabItem>\n"
            "</Tabs>\n"
        )
        out = g.strip_mdx(text)
        self.assertIn("### Linux", out)
        self.assertIn("### Windows", out)
        # Both tabs sit at the same level despite the h4 inside the first body.
        self.assertNotIn("##### Windows", out)


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


class LandingSynthesisTests(unittest.TestCase):
    def test_card_grid_synthesized(self):
        body = """
<div className="lp-grid">
<Link className="lp-card" to="/get-started/">
  <div className="lp-card-title">Get Started</div>
  <div className="lp-card-desc">Hardware requirements and installation.</div>
</Link>
<Link className="lp-card" to="/staking/">
  <div className="lp-card-title">Staking</div>
  <div className="lp-card-desc">Run a validator.</div>
</Link>
</div>
"""
        out = g.synthesize_landing(body)
        self.assertIsNotNone(out)
        self.assertIn("[Get Started](https://docs.erigon.tech/get-started)", out)
        self.assertIn("[Staking](https://docs.erigon.tech/staking)", out)
        self.assertIn("Hardware requirements", out)

    def test_hero_paragraph_emitted(self):
        body = """
<div className="lp-hero">
  <h1>Get Started</h1>
  <p>Everything you need to go from zero to a running node.</p>
</div>

<div className="lp-grid">
<Link className="lp-card" to="/install/">
  <div className="lp-card-title">Install</div>
  <div className="lp-card-desc">Install instructions.</div>
</Link>
</div>
"""
        out = g.synthesize_landing(body)
        self.assertIsNotNone(out)
        self.assertIn("Everything you need", out)

    def test_no_cards_returns_none(self):
        out = g.synthesize_landing("# Hello\n\nProse only, no cards.")
        self.assertIsNone(out)


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


if __name__ == "__main__":
    unittest.main()
