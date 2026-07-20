#!/usr/bin/env python3
"""Render disk-size values from disk-sizes.json into the hardware-requirements page.

Design (see README "Disk size data flow"): the hardware-requirements page holds
STATIC values between MDX-comment markers instead of importing disk-sizes.json at
runtime. Static content means `docusaurus docs:version` snapshots freeze correctly
with no per-version step, and there is no silent `?? '—'` fallback in production.

`src/data/disk-sizes.json` is the machine-readable source of record
(`update-disk-sizes.py` writes it from CI sync artifacts; archive rows are manual).
This script projects that JSON onto the page's markers:

    | Archive | {/* ds:mainnet:archive */}1.77 TB{/* ds:end */} | 4 TB | ... |
    _... measured as of {/* ds-date:mainnet */}2025-09-01{/* ds:end */}; ..._

Run it after editing the JSON; run with --check in CI to fail if they drift.

Usage:
    python3 render-disk-sizes.py            # rewrite the page in place
    python3 render-disk-sizes.py --check    # exit 1 if the page is out of sync
"""
import argparse
import json
import re
from pathlib import Path

VALUE_RE = re.compile(
    r"\{/\* ds:(?P<net>[a-z0-9]+):(?P<mode>[a-z]+) \*/\}(?P<val>.*?)\{/\* ds:end \*/\}"
)
DATE_RE = re.compile(
    r"\{/\* ds-date:(?P<net>[a-z0-9]+) \*/\}(?P<val>.*?)\{/\* ds:end \*/\}"
)


def render_text(text: str, data: dict) -> str:
    """Return `text` with every disk-size marker set from `data`.

    Raises SystemExit if a marker references data missing from the JSON —
    fail-closed rather than silently leaving a stale value."""
    networks = data.get("networks", {})

    def value_repl(m: re.Match) -> str:
        net, mode = m.group("net"), m.group("mode")
        try:
            display = networks[net][mode]["display"]
        except (KeyError, TypeError):
            raise SystemExit(
                f"Error: disk-sizes.json has no display value for "
                f"networks.{net}.{mode} (referenced by a marker in the page)"
            )
        return "{/* ds:" + net + ":" + mode + " */}" + display + "{/* ds:end */}"

    def date_repl(m: re.Match) -> str:
        net = m.group("net")
        modes = networks.get(net)
        if not modes:
            raise SystemExit(
                f"Error: disk-sizes.json has no network '{net}' "
                f"(referenced by a date marker in the page)"
            )
        dates = [v.get("measured_at") for v in modes.values() if v.get("measured_at")]
        if not dates:
            raise SystemExit(f"Error: no measured_at for any '{net}' mode")
        return "{/* ds-date:" + net + " */}" + max(dates) + "{/* ds:end */}"

    text = VALUE_RE.sub(value_repl, text)
    text = DATE_RE.sub(date_repl, text)
    return text


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit 1 if the page is out of sync with disk-sizes.json (no writes)",
    )
    args = parser.parse_args()

    site_root = Path(__file__).resolve().parent.parent  # docs/site
    json_path = site_root / "src" / "data" / "disk-sizes.json"
    page_path = site_root / "docs" / "get-started" / "hardware-requirements.mdx"

    if not json_path.exists():
        raise SystemExit(f"Error: {json_path} not found")
    if not page_path.exists():
        raise SystemExit(f"Error: {page_path} not found")

    data = json.loads(json_path.read_text(encoding="utf-8"))
    original = page_path.read_text(encoding="utf-8")
    rendered = render_text(original, data)

    if args.check:
        if rendered != original:
            raise SystemExit(
                "Error: hardware-requirements.mdx is out of sync with "
                "disk-sizes.json.\nRun: python3 docs/site/scripts/render-disk-sizes.py"
            )
        print("OK: hardware-requirements.mdx matches disk-sizes.json")
        return

    if rendered != original:
        page_path.write_text(rendered, encoding="utf-8")
        print(f"Updated {page_path}")
    else:
        print("No changes — page already matches disk-sizes.json")


if __name__ == "__main__":
    main()
