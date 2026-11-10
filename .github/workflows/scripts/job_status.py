#!/usr/bin/env python3
"""Print the status and the conclusion of a job of the current workflow run attempt."""

import json
import os
import sys
import urllib.request


def main():
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} <job name suffix>")
    name = sys.argv[1]
    url = (f"https://api.github.com/repos/{os.environ['GITHUB_REPOSITORY']}/actions/runs/"
           f"{os.environ['GITHUB_RUN_ID']}/attempts/{os.environ['GITHUB_RUN_ATTEMPT']}/jobs?per_page=100")
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {os.environ['GH_TOKEN']}",
                                               "Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=30) as rsp:
        jobs = json.load(rsp)["jobs"]
    # A job of a called workflow is named "<caller job> / <job>"
    job = next((j for j in jobs if j["name"].endswith(name)), None)
    if job is None:
        sys.exit(f"job '{name}' not found")
    print(job["status"], job["conclusion"] or "")


if __name__ == "__main__":
    main()
