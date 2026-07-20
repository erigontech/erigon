"""Guard: archived doc versions must be self-contained.

Fails if any `versioned_docs/version-vX.Y/` page links to another docs page via
an unversioned absolute route (e.g. `/get-started/`), which would bounce a
reader from the archive into the current version. See erigontech/erigon#22416.

Run from repo root:
  python3 -m unittest discover docs/site/scripts -v
  python3 docs/site/scripts/test_archive_links.py     # direct invocation also works

Fix violations with:
  python3 docs/site/scripts/fix-archive-links.py
"""
import importlib.util
import unittest
from pathlib import Path

_HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location(
    "fix_archive_links", _HERE / "fix-archive-links.py"
)
fal = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fal)

_VERSIONED = _HERE.parent / "versioned_docs"


class ArchiveLinksSelfContained(unittest.TestCase):
    def test_no_unversioned_absolute_links_in_archives(self):
        found = fal.scan(str(_VERSIONED), check=True)
        if found:
            lines = [
                f"  {f}: {link}"
                for f, links in sorted(found.items())
                for link in links
            ]
            self.fail(
                "Archived doc versions must be self-contained, but found "
                f"{sum(len(v) for v in found.values())} unversioned absolute "
                "link(s) that would leak to the current version:\n"
                + "\n".join(lines)
                + "\n\nFix with: python3 docs/site/scripts/fix-archive-links.py"
            )


if __name__ == "__main__":
    unittest.main()
