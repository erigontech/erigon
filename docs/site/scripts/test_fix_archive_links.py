"""Unit tests for fix-archive-links.py — the archive link fixer/guard.

Covers each link syntax and edge case so the CI guard genuinely prevents
recurrence (erigontech/erigon#22416), not just the two forms seen today.

Run from repo root:
  python3 -m unittest discover docs/site/scripts -v
  python3 docs/site/scripts/test_fix_archive_links.py
"""
import importlib.util
import os
import tempfile
import unittest
from pathlib import Path

_HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("fix_archive_links", _HERE / "fix-archive-links.py")
fal = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fal)


def _fix(body, relpath="fundamentals/page.md", prefix="/v3.4"):
    """Write body as a v3.4 archive page, run the fixer, return new text + violations."""
    with tempfile.TemporaryDirectory() as d:
        vroot = os.path.join(d, "versioned_docs", "version-v3.4")
        fp = os.path.join(vroot, relpath)
        os.makedirs(os.path.dirname(fp), exist_ok=True)
        open(fp, "w").write(body)
        found = fal.scan(os.path.join(d, "versioned_docs"), check=False)
        return open(fp).read(), sum(found.values(), [])


class SyntaxCoverage(unittest.TestCase):
    def test_markdown_inline(self):
        out, v = _fix("See [x](/get-started/foo).")
        self.assertIn("(/v3.4/get-started/foo)", out)
        self.assertTrue(v)

    def test_markdown_inline_with_title(self):
        out, _ = _fix('[x](/get-started/foo "Title").')
        self.assertIn('(/v3.4/get-started/foo "Title")', out)

    def test_markdown_reference_definition(self):
        out, v = _fix("[x]\n\n[x]: /get-started/foo")
        self.assertIn("[x]: /v3.4/get-started/foo", out)
        self.assertTrue(v)

    def test_jsx_link_double_quote(self):
        out, _ = _fix('<Link to="/get-started/">go</Link>')
        self.assertIn('to="/v3.4/get-started/"', out)

    def test_jsx_link_single_quote(self):
        out, _ = _fix("<Link to='/get-started/'>go</Link>")
        self.assertIn("to='/v3.4/get-started/'", out)

    def test_jsx_expression_braces(self):
        out, _ = _fix('<Link to={"/get-started/"}>go</Link>')
        self.assertIn('to={"/v3.4/get-started/"}', out)

    def test_html_anchor_href(self):
        out, v = _fix('<a href="/get-started/foo">go</a>')
        self.assertIn('href="/v3.4/get-started/foo"', out)
        self.assertTrue(v)

    def test_spaced_attr(self):
        out, _ = _fix('<Link to = "/get-started/">go</Link>')
        self.assertIn('/v3.4/get-started/', out)


class Exclusions(unittest.TestCase):
    def _unchanged(self, body):
        out, v = _fix(body)
        self.assertEqual(out, body)
        self.assertEqual(v, [])

    def test_leaves_images(self):
        self._unchanged("![i](/img/logo.svg)")

    def test_leaves_help_center(self):
        self._unchanged("[h](/help-center/faq)")

    def test_leaves_site_root(self):
        self._unchanged('<Link to="/">home</Link>')

    def test_leaves_already_versioned(self):
        self._unchanged("[x](/v3.4/get-started/foo)")

    def test_leaves_external(self):
        self._unchanged("[x](https://erigon.tech/get-started/)")

    def test_leaves_relative(self):
        self._unchanged("[x](../get-started/foo) and [y](eth.md)")


class EdgeCases(unittest.TestCase):
    def test_word_boundary_not_photo_attr(self):
        # `to="` must not match inside `photo="`, `auto="`, `data-goto="`
        self.assertEqual(*[_fix('<img photo="/get-started/pic.png"/>')[0],
                           '<img photo="/get-started/pic.png"/>'])

    def test_skips_fenced_code(self):
        body = "prose [x](/get-started/foo)\n```\ncode [y](/get-started/bar)\n```\n"
        out, _ = _fix(body)
        self.assertIn("[x](/v3.4/get-started/foo)", out)   # prose fixed
        self.assertIn("[y](/get-started/bar)", out)         # code sample untouched

    def test_same_page_anchor_localized(self):
        out, _ = _fix("[s](/fundamentals/page#sec)", relpath="fundamentals/page.md")
        self.assertIn("[s](#sec)", out)

    def test_root_index_home_anchor_localized(self):
        out, _ = _fix("[top](/#intro)", relpath="index.mdx")
        self.assertIn("[top](#intro)", out)

    def test_renamed_section_still_flagged(self):
        # a link into a section not present in the archive must still be caught
        _, v = _fix("[x](/removed-section/page)")
        self.assertTrue(v)

    def test_idempotent(self):
        once, _ = _fix("[x](/get-started/foo)")
        twice, v2 = _fix(once)
        self.assertEqual(once, twice)
        self.assertEqual(v2, [])


if __name__ == "__main__":
    unittest.main()
