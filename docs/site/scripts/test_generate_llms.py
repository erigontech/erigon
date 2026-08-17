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

    def test_desc_with_inline_markup_is_extracted(self):
        """Inline markup in a description must not defeat the match."""
        body = """
<div className="lp-grid">
<Link className="lp-card" to="/pruning/">
  <div className="lp-card-title">Flexible Pruning</div>
  <div className="lp-card-desc"><code>--prune.mode=minimal</code> gives a <strong>smaller</strong> footprint.</div>
</Link>
</div>
"""
        out = g.synthesize_landing(body)
        self.assertIsNotNone(out)
        self.assertIn("--prune.mode=minimal gives a smaller footprint.", out)
        self.assertNotIn("<code>", out)
        self.assertNotIn("<strong>", out)

    def test_markup_card_does_not_swallow_the_next_card(self):
        """A card whose desc has markup must not steal the following card's desc."""
        body = """
<div className="lp-grid">
<Link className="lp-card" to="/immutable/">
  <div className="lp-card-title">Immutable Data</div>
  <div className="lp-card-desc">Historical data lives in <strong>immutable</strong> files.</div>
</Link>
<Link className="lp-card" to="/staged-sync/">
  <div className="lp-card-title">Staged Sync</div>
  <div className="lp-card-desc">Splits processing into specialised stages.</div>
</Link>
</div>
"""
        out = g.synthesize_landing(body)
        self.assertIsNotNone(out)
        bullets = [ln for ln in out.split("\n") if ln.startswith("- [")]
        self.assertEqual(len(bullets), 2)
        self.assertIn(
            "- [Immutable Data](https://docs.erigon.tech/immutable): "
            "Historical data lives in immutable files.",
            bullets,
        )
        self.assertIn(
            "- [Staged Sync](https://docs.erigon.tech/staged-sync): "
            "Splits processing into specialised stages.",
            bullets,
        )

    def test_dropped_card_raises_instead_of_silently_shrinking(self):
        """A card the parser cannot read must fail loudly — `--check` cannot see it."""
        body = """
<div className="lp-grid">
<Link className="lp-card" to="/ok/">
  <div className="lp-card-title">Fine</div>
  <div className="lp-card-desc">Parses correctly.</div>
</Link>
<Link className="lp-card" to="/broken/">
  <div className="lp-card-title">Missing Its Description</div>
</Link>
</div>
"""
        with self.assertRaises(SystemExit) as ctx:
            g.synthesize_landing(body)
        self.assertIn("dropped cards", str(ctx.exception))

    def test_all_cards_malformed_raises_rather_than_falling_back(self):
        """Total parser failure must not look like an ordinary prose page.

        Returning None here would send the caller to strip_mdx, degrading the
        page silently — the exact outcome the guard exists to prevent.
        """
        body = """
<div className="lp-grid">
<Link className="lp-card" to="/a/">
  <div className="lp-card-title">A</div>
</Link>
<Link className="lp-card" to="/b/">
  <div className="lp-card-title">B</div>
</Link>
</div>
"""
        with self.assertRaises(SystemExit) as ctx:
            g.synthesize_landing(body)
        self.assertIn("parsed 0 of 2", str(ctx.exception))


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
