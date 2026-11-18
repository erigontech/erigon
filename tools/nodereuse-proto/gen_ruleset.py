#!/usr/bin/env python3
"""Emit {fork: {env_key: value}} ruleset tables for every fork named in the manifest.

Must run inside the execution-specs consumer environment (uv sync'd) so
execution_testing.forks is importable.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from execution_testing.cli.pytest_commands.plugins.consume.simulators.helpers.ruleset import (  # noqa: E501
    ruleset_format,
)
from execution_testing.forks import ALL_FORKS_WITH_TRANSITIONS

manifest = json.load(open(sys.argv[1]))
names = {t["fork"] for t in manifest}
table = {}
for fork in ALL_FORKS_WITH_TRANSITIONS:
    name = fork.name() if hasattr(fork, "name") else type(fork).__name__
    if name in names and name not in table:
        table[name] = ruleset_format(fork)
missing = names - set(table)
if missing:
    print(f"WARNING: no ruleset for forks {sorted(missing)}", file=sys.stderr)
print(json.dumps(table, indent=1))
