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

The renderer is deliberately fail-closed: a marker that does not match the exact
grammar (spacing typo, wrapped across a newline, nested, unpaired) is a hard
error rather than a silent skip — otherwise `--check` could stay green while a
cell quietly went stale, defeating the whole point of the static-marker scheme.

Usage:
    python3 render-disk-sizes.py            # rewrite the page in place
    python3 render-disk-sizes.py --check    # exit 1 if the page is out of sync
"""
import argparse
import json
import re
from datetime import date
from pathlib import Path

# Exact, single-line marker grammar. `val` cannot span newlines so a wrapped
# marker fails the completeness check below instead of being silently skipped.
VALUE_RE = re.compile(
    r"\{/\* ds:(?P<net>[a-z0-9]+):(?P<mode>[a-z]+) \*/\}(?P<val>[^\n\r]*?)\{/\* ds:end \*/\}"
)
DATE_RE = re.compile(
    r"\{/\* ds-date:(?P<net>[a-z0-9]+) \*/\}(?P<val>[^\n\r]*?)\{/\* ds:end \*/\}"
)
# Any MDX comment, used to find *all* disk-size marker tokens (even malformed
# ones) so unpaired/typo'd markers can be detected rather than ignored.
COMMENT_RE = re.compile(r"\{/\*(?P<body>.*?)\*/\}", re.DOTALL)


def _is_ds_token(body: str) -> bool:
    b = body.strip()
    return b.startswith("ds:") or b.startswith("ds-date:")


def _validate_display(net: str, mode: str, display) -> str:
    if not isinstance(display, str) or not display.strip():
        raise SystemExit(
            f"Error: networks.{net}.{mode}.display must be a non-empty string"
        )
    if any(c in display for c in "{}\n\r"):
        raise SystemExit(
            f"Error: networks.{net}.{mode}.display contains disallowed "
            f"characters (no braces or newlines): {display!r}"
        )
    return display


def _validate_date(net: str, mode: str, value) -> str:
    if not isinstance(value, str):
        raise SystemExit(f"Error: networks.{net}.{mode}.measured_at must be a string")
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise SystemExit(
            f"Error: networks.{net}.{mode}.measured_at is not valid "
            f"YYYY-MM-DD: {value!r}"
        )
    if value != parsed.isoformat():
        raise SystemExit(
            f"Error: networks.{net}.{mode}.measured_at must be zero-padded "
            f"YYYY-MM-DD: {value!r}"
        )
    return value


def render_text(text: str, data: dict) -> str:
    """Return `text` with every disk-size marker set from `data`.

    Fail-closed: raises SystemExit on missing data, malformed/unpaired markers,
    or unsafe values, so a broken page can never silently pass `--check`."""
    networks = data.get("networks")
    if not isinstance(networks, dict) or not networks:
        raise SystemExit("Error: disk-sizes.json has no non-empty 'networks' object")

    value_spans = list(VALUE_RE.finditer(text))
    date_spans = list(DATE_RE.finditer(text))

    # Completeness guard: every disk-size marker comment must belong to a
    # well-formed value/date pair. Each pair uses exactly two comment tokens
    # (opener + `ds:end` closer), so any surplus token means a malformed or
    # unpaired marker (spacing typo, newline-wrapped, nested, dangling closer).
    ds_tokens = [m for m in COMMENT_RE.finditer(text) if _is_ds_token(m.group("body"))]
    if not value_spans:
        raise SystemExit("Error: no disk-size value markers found in the page")
    expected_tokens = 2 * (len(value_spans) + len(date_spans))
    if len(ds_tokens) != expected_tokens:
        raise SystemExit(
            "Error: malformed or unpaired disk-size marker(s) in the page — "
            f"found {len(ds_tokens)} marker comment(s) but only "
            f"{len(value_spans) + len(date_spans)} well-formed pair(s). Check for "
            "spacing typos, markers wrapped across a newline, or a stray "
            "{/* ds:end */}."
        )
    for m in value_spans + date_spans:
        if "{/*" in m.group("val"):
            raise SystemExit("Error: a nested disk-size marker was detected")

    def value_repl(m: re.Match) -> str:
        net, mode = m.group("net"), m.group("mode")
        try:
            display = networks[net][mode]["display"]
        except (KeyError, TypeError):
            raise SystemExit(
                f"Error: disk-sizes.json has no display value for "
                f"networks.{net}.{mode} (referenced by a marker in the page)"
            )
        display = _validate_display(net, mode, display)
        return "{/* ds:" + net + ":" + mode + " */}" + display + "{/* ds:end */}"

    def date_repl(m: re.Match) -> str:
        net = m.group("net")
        modes = networks.get(net)
        if not isinstance(modes, dict) or not modes:
            raise SystemExit(
                f"Error: disk-sizes.json has no network '{net}' "
                f"(referenced by a date marker in the page)"
            )
        dates = [
            _validate_date(net, mode, v["measured_at"])
            for mode, v in modes.items()
            if isinstance(v, dict) and v.get("measured_at")
        ]
        if not dates:
            raise SystemExit(f"Error: no measured_at for any '{net}' mode")
        # Use the OLDEST date: the caption covers every mode in the table, so
        # "measured as of <oldest>" is honest — max() would overstate freshness.
        return "{/* ds-date:" + net + " */}" + min(dates) + "{/* ds:end */}"

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

    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise SystemExit(f"Error: malformed {json_path}: {e}") from None
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
