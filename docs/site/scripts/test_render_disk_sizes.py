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

    def test_date_uses_oldest_measured_at(self):
        out = r.render_text(PAGE, DATA)
        # min of 2026-06-02 / 2026-06-02 / 2026-05-01 = 2026-05-01 (honest "as of")
        self.assertIn("{/* ds-date:mainnet */}2026-05-01{/* ds:end */}", out)

    def test_preserves_surrounding_table_text(self):
        out = r.render_text(PAGE, DATA)
        self.assertIn("| Archive |", out)
        self.assertIn("| 4 TB |", out)
        self.assertEqual(out.count("{/* ds:end */}"), PAGE.count("{/* ds:end */}"))

    def test_idempotent(self):
        once = r.render_text(PAGE, DATA)
        twice = r.render_text(once, DATA)
        self.assertEqual(once, twice)

    # --- fail-closed: missing data ---

    def test_missing_value_fails_closed(self):
        page = "| X | {/* ds:mainnet:nope */}?{/* ds:end */} |\n"
        with self.assertRaises(SystemExit):
            r.render_text(page, DATA)

    def test_no_value_markers_fails_closed(self):
        page = "_as of {/* ds-date:mainnet */}?{/* ds:end */}._\n"
        with self.assertRaises(SystemExit):
            r.render_text(page, DATA)

    def test_empty_networks_fails_closed(self):
        with self.assertRaises(SystemExit):
            r.render_text(PAGE, {"networks": {}})

    # --- fail-closed: malformed markers (the silent-stale hazard) ---

    def test_spacing_typo_marker_fails_closed(self):
        # missing spaces around ds: — must NOT be silently skipped
        page = "| X | {/*ds:mainnet:archive*/}1.77 TB{/*ds:end*/} |\n"
        with self.assertRaises(SystemExit):
            r.render_text(page, DATA)

    def test_newline_wrapped_marker_fails_closed(self):
        page = "| X | {/* ds:mainnet:archive */}\n1.77 TB\n{/* ds:end */} |\n"
        with self.assertRaises(SystemExit):
            r.render_text(page, DATA)

    def test_dangling_closer_fails_closed(self):
        page = (
            "| A | {/* ds:mainnet:archive */}1.77 TB{/* ds:end */} |\n"
            "| stray {/* ds:end */} |\n"
        )
        with self.assertRaises(SystemExit):
            r.render_text(page, DATA)

    def test_nested_marker_fails_closed(self):
        page = (
            "{/* ds:mainnet:archive */}{/* ds:mainnet:full */}x{/* ds:end */}\n"
        )
        with self.assertRaises(SystemExit):
            r.render_text(page, DATA)

    # --- fail-closed: bad data values ---

    def test_non_iso_date_fails_closed(self):
        data = {"networks": {"mainnet": {
            "archive": {"display": "2.03 TB", "measured_at": "2026-6-2"},
        }}}
        page = "{/* ds:mainnet:archive */}x{/* ds:end */} {/* ds-date:mainnet */}y{/* ds:end */}\n"
        with self.assertRaises(SystemExit):
            r.render_text(page, data)

    def test_display_with_braces_fails_closed(self):
        data = {"networks": {"mainnet": {"archive": {"display": "1 {x} TB"}}}}
        page = "{/* ds:mainnet:archive */}x{/* ds:end */}\n"
        with self.assertRaises(SystemExit):
            r.render_text(page, data)

    def test_empty_display_fails_closed(self):
        data = {"networks": {"mainnet": {"archive": {"display": ""}}}}
        page = "{/* ds:mainnet:archive */}x{/* ds:end */}\n"
        with self.assertRaises(SystemExit):
            r.render_text(page, data)

    def test_non_dict_mode_with_date_marker_fails_cleanly(self):
        data = {"networks": {"mainnet": {"archive": "1.77 TB"}}}
        page = "{/* ds:mainnet:archive */}x{/* ds:end */} {/* ds-date:mainnet */}y{/* ds:end */}\n"
        with self.assertRaises(SystemExit):
            r.render_text(page, data)


if __name__ == "__main__":
    unittest.main()
