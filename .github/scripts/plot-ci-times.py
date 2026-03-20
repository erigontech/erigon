#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib", "numpy"]
# ///
"""
Plot GitHub CI job durations for the erigontech/erigon repository.

Usage:
    uv run .github/scripts/plot-ci-times.py [options]

Requires the `gh` CLI to be authenticated.

Data is cached in ~/.cache/erigon-ci-times/ to avoid re-fetching on repeat runs.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

REPO = "erigontech/erigon"

CACHE_DIR = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "erigon-ci-times"

# GitHub only returns runs from the past 90 days
MAX_DAYS = 90

SKIP_JOBS = {"source of changes", "source-of-changes", "load-matrix"}

_OS_PREFIXES = ("ubuntu", "macos", "windows")

_OS_NAMES = {
    "ubuntu": "linux",
    "windows": "windows",
    "macos": "macos",
}


def normalize_job_name(name: str) -> str:
    # Old Test Hive runs had bare matrix names like "(engine, cancun)" with no prefix.
    if name.startswith("("):
        name = "test-hive " + name

    # Strip trailing numeric shard index: "test-hive (engine, cancun, 2)" -> "test-hive (engine, cancun)"
    name = re.sub(r",\s*\d+\)", ")", name)

    # If the first matrix param is an OS, keep only the normalized OS name.
    # Otherwise strip all matrix params.
    m = re.match(r"^(.*?)\(([^)]+)\)\s*$", name)
    if m:
        prefix, params_str = m.group(1), m.group(2)
        params = [p.strip() for p in params_str.split(",")]
        if params and any(params[0].startswith(os) for os in _OS_PREFIXES):
            os_name = next((v for k, v in _OS_NAMES.items() if params[0].startswith(k)), params[0])
            name = prefix + "(" + os_name + ")"
        else:
            name = prefix.rstrip()

    return name


def parse_dt(s: str | None) -> datetime | None:
    if s is None:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def gh_api(path: str, timeout: int = 30, retries: int = 5) -> dict:
    """Call gh api and return parsed JSON. Retries on rate-limit (HTTP 403/429)."""
    delay = 5
    for attempt in range(retries):
        result = subprocess.run(
            ["gh", "api", path],
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=timeout,
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
        stderr = result.stderr.strip()
        if ("rate limit" in stderr.lower() or "HTTP 429" in stderr or "HTTP 403" in stderr) and attempt < retries - 1:
            wait = delay * (2 ** attempt)
            print(f"\n  [rate limit] waiting {wait}s before retry {attempt+1}/{retries-1}...", end="", flush=True)
            time.sleep(wait)
            continue
        raise RuntimeError(f"gh api {path!r} failed: {stderr}")
    raise RuntimeError(f"gh api {path!r} failed after {retries} retries")


def _load_runs_cache(workflow_id: int) -> dict[int, dict]:
    cache_file = CACHE_DIR / f"runs_{workflow_id}.json"
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if not cache_file.exists():
        return {}
    with open(cache_file) as f:
        cached = {r["id"]: r for r in json.load(f)}
    # Detect stale cache missing head_branch; drop and re-fetch.
    if cached and "head_branch" not in next(iter(cached.values())):
        print(f"  runs cache for {workflow_id} is stale (missing head_branch); dropping...")
        cache_file.unlink()
        return {}
    return cached


def _save_runs_cache(workflow_id: int, cached_by_id: dict[int, dict]) -> None:
    cache_file = CACHE_DIR / f"runs_{workflow_id}.json"
    with open(cache_file, "w") as f:
        json.dump(list(cached_by_id.values()), f)


def _fetch_runs_page(page: int, per_page: int = 100) -> list[dict]:
    data = gh_api(f"repos/{REPO}/actions/runs?per_page={per_page}&page={page}")
    return data.get("workflow_runs", [])


def _runs_in_window(cached_by_id: dict[int, dict], cutoff: datetime) -> list[dict]:
    return [r for r in cached_by_id.values() if parse_dt(r["created_at"]) >= cutoff]


def fetch_jobs_for_run(run_id: int, cache: bool) -> list[dict]:
    """Fetch jobs for a single run, using cache if available."""
    cache_file = CACHE_DIR / f"jobs_{run_id}.json"
    if cache and cache_file.exists():
        with open(cache_file) as f:
            return json.load(f)

    data = gh_api(f"repos/{REPO}/actions/runs/{run_id}/jobs?per_page=100")
    records = []
    for job in data.get("jobs", []):
        s = parse_dt(job.get("started_at"))
        c = parse_dt(job.get("completed_at"))
        if s is None or c is None:
            continue
        records.append({
            "name": job["name"],
            "conclusion": job.get("conclusion"),
            "duration_sec": (c - s).total_seconds(),
        })
    if cache:
        with open(cache_file, "w") as f:
            json.dump(records, f)
    return records


def cmd_fetch(args):
    """Fetch workflow runs and jobs into the cache."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    cache = not args.no_cache
    if cache:
        print(f"Cache dir: {CACHE_DIR}")

    PER_PAGE = 100
    wf_cache: dict[int, dict[int, dict]] = {}   # workflow_id -> {run_id -> run}
    wf_names: dict[int, str] = {}
    wf_added: dict[int, int] = defaultdict(int)

    job_futures: dict = {}
    submitted_run_ids: set = set()

    def _is_fetchable(run: dict) -> bool:
        return (
            run["conclusion"] is not None
            and run["run_started_at"]
            and run["updated_at"]
            and parse_dt(run["created_at"]) >= cutoff
            and (parse_dt(run["updated_at"]) - parse_dt(run["run_started_at"])).total_seconds() > 30
            and (args.branch is None or run.get("head_branch") == args.branch)
        )

    with ThreadPoolExecutor(max_workers=args.parallelism) as executor:

        def maybe_submit(run: dict) -> None:
            if run["id"] not in submitted_run_ids and _is_fetchable(run):
                submitted_run_ids.add(run["id"])
                job_futures[executor.submit(fetch_jobs_for_run, run["id"], cache)] = run

        # Load existing caches and immediately submit job fetches for them.
        for path in CACHE_DIR.glob("runs_*.json"):
            wf_id = int(path.stem.split("_")[1])
            wf_cache[wf_id] = _load_runs_cache(wf_id) if cache else {}
            for run in wf_cache[wf_id].values():
                maybe_submit(run)

        page = 1
        while True:
            runs_page = _fetch_runs_page(page, PER_PAGE)
            if not runs_page:
                break

            stop = False
            page_added = 0
            page_dates = []
            page_dirty_wfs: set[int] = set()
            for run in runs_page:
                dt = parse_dt(run["created_at"])
                if dt < cutoff:
                    stop = True
                    break
                page_dates.append(dt)
                wf_id = run["workflow_id"]
                wf_names[wf_id] = run.get("name", str(wf_id))
                if wf_id not in wf_cache:
                    wf_cache[wf_id] = {}
                if run["id"] not in wf_cache[wf_id]:
                    wf_cache[wf_id][run["id"]] = {
                        "id": run["id"],
                        "workflow_id": wf_id,
                        "head_sha": run["head_sha"],
                        "head_branch": run.get("head_branch"),
                        "created_at": run["created_at"],
                        "run_started_at": run["run_started_at"],
                        "updated_at": run["updated_at"],
                        "conclusion": run["conclusion"],
                        "status": run["status"],
                    }
                    wf_added[wf_id] += 1
                    page_added += 1
                    page_dirty_wfs.add(wf_id)
                maybe_submit(run)

            if page_dates:
                newest = max(page_dates).strftime("%Y-%m-%d %H:%M")
                oldest = min(page_dates).strftime("%Y-%m-%d %H:%M")
                print(f"page {page} | {page_added} new runs | {oldest} – {newest}")
            else:
                print(f"page {page} | {page_added} new runs")

            if cache:
                for wf_id in page_dirty_wfs:
                    _save_runs_cache(wf_id, wf_cache[wf_id])

            if stop or len(runs_page) < PER_PAGE:
                break
            page += 1

        total_runs = len(job_futures)
        print(f"\nFetching jobs for {total_runs} runs (parallelism={args.parallelism})...")
        done = 0
        for future in as_completed(job_futures):
            run = job_futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"\n  Warning: failed jobs for run {run['id']}: {e}")
            done += 1
            if done % 10 == 0 or done == total_runs:
                print(f"\r  {done}/{total_runs} jobs fetched  ", end="", flush=True)
    print()


def _load_run_dates() -> dict[int, object]:
    """Return run_id -> date for all cached runs."""
    run_date = {}
    for path in CACHE_DIR.glob("runs_*.json"):
        with open(path) as f:
            for run in json.load(f):
                dt = parse_dt(run.get("run_started_at"))
                if dt:
                    run_date[run["id"]] = dt.date()
    return run_date


def _load_jobs_by_day(run_date: dict) -> dict:
    """
    Returns dict: date -> list of (name, conclusion, duration_sec).
    Only jobs whose run_id is in run_date are included.
    """
    by_day: dict = defaultdict(list)
    for path in CACHE_DIR.glob("jobs_*.json"):
        run_id = int(path.stem.split("_")[1])
        date = run_date.get(run_id)
        if date is None:
            continue
        with open(path) as f:
            for job in json.load(f):
                name = normalize_job_name(job["name"])
                if name in SKIP_JOBS:
                    continue
                by_day[date].append((name, job.get("conclusion", ""), job["duration_sec"]))
    return by_day


def cmd_plot(args):
    """Plot per-day job counts (success/failure/cancelled) and average success duration."""
    run_date = _load_run_dates()
    by_day = _load_jobs_by_day(run_date)

    days = sorted(by_day)
    if not days:
        print("No cached data found.")
        return

    success_counts, failure_counts, cancel_counts = [], [], []
    avg_durations = []

    for day in days:
        jobs = by_day[day]
        ok = [j for j in jobs if j[1] == "success"]
        fail = [j for j in jobs if j[1] in ("failure", "timed_out")]
        cancel = [j for j in jobs if j[1] in ("cancelled", "skipped")]
        success_counts.append(len(ok))
        failure_counts.append(len(fail))
        cancel_counts.append(len(cancel))
        avg_durations.append(sum(j[2] for j in ok) / len(ok) / 60 if ok else 0)

    # EMA with span ~7 days: alpha = 2/(7+1)
    alpha = 2 / (7 + 1)
    ema = [avg_durations[0]]
    for v in avg_durations[1:]:
        ema.append(alpha * v + (1 - alpha) * ema[-1])

    x = np.arange(len(days))

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 9), sharex=False)
    fig.suptitle("Erigon CI Job Summary", fontsize=14, fontweight="bold")

    ax1.bar(x, success_counts, color="steelblue", alpha=0.85, label="success")
    ax1.bar(x, failure_counts, bottom=success_counts, color="tomato", alpha=0.85, label="failure/timeout")
    bottoms = [s + f for s, f in zip(success_counts, failure_counts)]
    ax1.bar(x, cancel_counts, bottom=bottoms, color="gold", alpha=0.85, label="cancelled/skipped")
    ax1.set_ylabel("Job count")
    ax1.legend(fontsize=9)
    ax1.grid(axis="y", alpha=0.3)
    ax1.set_xticks(x)
    ax1.set_xticklabels([d.strftime("%m/%d") for d in days], rotation=45, ha="right", fontsize=7)

    ax2.plot(x, avg_durations, color="steelblue", linewidth=1, alpha=0.4, marker="o", markersize=2)
    ax2.plot(x, ema, color="steelblue", linewidth=2, label="EMA (7d)")
    ax2.set_ylabel("Avg success duration (min)")
    ax2.set_ylim(bottom=0)
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(x)
    ax2.set_xticklabels([d.strftime("%m/%d") for d in days], rotation=45, ha="right", fontsize=7)
    ax2.legend(fontsize=9)
    ax2.set_xlabel("Date")

    plt.tight_layout()
    plt.savefig(args.output, dpi=130, bbox_inches="tight")
    print(f"Saved to {args.output}")


def cmd_job_names(_args):
    """Print all unique raw job names from the cache grouped by normalized name."""
    by_norm: dict[str, list[str]] = defaultdict(list)
    for path in CACHE_DIR.glob("jobs_*.json"):
        with open(path) as f:
            for job in json.load(f):
                name = job["name"]
                by_norm[normalize_job_name(name)].append(name)
    for norm, originals in sorted(by_norm.items()):
        print(f"{norm}:")
        for orig in sorted(set(originals)):
            print(f"  - {orig}")


def cmd_dump_jobs(_args):
    """Print every cached job: name, conclusion, duration_sec (tab-separated)."""
    print("name\tconclusion\tduration_sec")
    for path in CACHE_DIR.glob("jobs_*.json"):
        with open(path) as f:
            for job in json.load(f):
                name = normalize_job_name(job["name"])
                if name in SKIP_JOBS:
                    continue
                print(f"{name}\t{job.get('conclusion', '')}\t{job['duration_sec']:.0f}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command")

    plot_parser = subparsers.add_parser("plot", help="Plot per-day job counts and average success duration")
    plot_parser.add_argument("--output", "-o", default="ci_job_times.png", help="Output image path (default: ci_job_times.png)")

    fetch_parser = subparsers.add_parser("fetch", help="Fetch workflow runs and jobs into the cache")
    fetch_parser.add_argument("--days", type=int, default=MAX_DAYS, help=f"How many days back to fetch (default: {MAX_DAYS})")
    fetch_parser.add_argument("--no-cache", action="store_true", help="Ignore and overwrite cached data")
    fetch_parser.add_argument("--branch", default=None, help="Filter to runs on a specific branch")
    fetch_parser.add_argument("--parallelism", type=int, default=2, help="Concurrent job-fetch requests (default: 2)")

    subparsers.add_parser("job-names", help="List unique job names from cache with their normalized form")
    subparsers.add_parser("dump-jobs", help="Dump all cached jobs: name, conclusion, duration_sec")

    args = parser.parse_args()

    if args.command == "plot":
        cmd_plot(args)
    elif args.command == "fetch":
        cmd_fetch(args)
    elif args.command == "job-names":
        cmd_job_names(args)
    elif args.command == "dump-jobs":
        cmd_dump_jobs(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
