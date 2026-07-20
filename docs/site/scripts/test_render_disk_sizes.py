"""Unit tests for render-disk-sizes.py.

Run from repo root:
  python3 -m unittest discover docs/site/scripts -v
  python3 docs/site/scripts/test_render_disk_sizes.py    # direct invocation also works
"""

import importlib.util
import unittest
from pathlib import Path

_HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("render_disk_sizes", _HERE / "render-disk-sizes.py")
r = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(r)


DATA = {
    "networks": {
        "mainnet": {
            "archive": {"display": "2.03 TB", "measured_at": "2026-06-02"},
            "full": {"display": "1.2 TB", "measured_at": "2026-06-02"},
            "minimal": {"display": "360 GB", "measured_at": "2026-05-01"},
        },
        "gnosis": {
            "archive": {"display": "605 GB", "measured_at": "2026-06-02"},
        },
    }
}

PAGE = (
    "| Archive | {/* ds:mainnet:archive */}1.77 TB{/* ds:end */} | 4 TB |\n"
    "| Full | {/* ds:mainnet:full */}920 GB{/* ds:end */} | 2 TB |\n"
    "| Minimal | {/* ds:mainnet:minimal */}350 GB{/* ds:end */} | 1 TB |\n"
    "\n_measured as of {/* ds-date:mainnet */}2025-09-01{/* ds:end */}._\n"
    "| Archive | {/* ds:gnosis:archive */}539 GB{/* ds:end */} | 1 TB |\n"
)


class RenderTextTests(unittest.TestCase):
    def test_updates_values(self):
        out = r.render_text(PAGE, DATA)
        self.assertIn("{/* ds:mainnet:archive */}2.03 TB{/* ds:end */}", out)
        self.assertIn("{/* ds:mainnet:full */}1.2 TB{/* ds:end */}", out)
        self.assertIn("{/* ds:mainnet:minimal */}360 GB{/* ds:end */}", out)
        self.assertIn("{/* ds:gnosis:archive */}605 GB{/* ds:end */}", out)

    def test_date_uses_latest_measured_at(self):
        out = r.render_text(PAGE, DATA)
        # max of 2026-06-02 / 2026-06-02 / 2026-05-01 = 2026-06-02
        self.assertIn("{/* ds-date:mainnet */}2026-06-02{/* ds:end */}", out)

    def test_preserves_surrounding_table_text(self):
        out = r.render_text(PAGE, DATA)
        self.assertIn("| Archive |", out)
        self.assertIn("| 4 TB |", out)
        self.assertEqual(out.count("{/* ds:end */}"), PAGE.count("{/* ds:end */}"))

    def test_idempotent(self):
        once = r.render_text(PAGE, DATA)
        twice = r.render_text(once, DATA)
        self.assertEqual(once, twice)

    def test_markers_with_no_data_change_is_noop_on_matching_page(self):
        rendered = r.render_text(PAGE, DATA)
        # re-rendering the already-correct page yields no further change
        self.assertEqual(rendered, r.render_text(rendered, DATA))

    def test_missing_value_fails_closed(self):
        page = "| X | {/* ds:mainnet:nope */}?{/* ds:end */} |\n"
        with self.assertRaises(SystemExit):
            r.render_text(page, DATA)

    def test_missing_network_date_fails_closed(self):
        page = "_as of {/* ds-date:polygon */}?{/* ds:end */}._\n"
        with self.assertRaises(SystemExit):
            r.render_text(page, DATA)


if __name__ == "__main__":
    unittest.main()
