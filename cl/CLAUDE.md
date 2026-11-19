# Caplin Consensus Layer

This directory contains consensus-critical Caplin code. Review all changes
against the upstream Ethereum consensus specifications, not only local tests.

Primary spec reference: https://github.com/ethereum/consensus-specs

## Per-Directory Spec Maps

Each consensus-critical subdirectory has its own `CLAUDE.md` with function-level
spec mappings and a domain-specific review checklist:

| Directory | Scope |
| --- | --- |
| [`phase1/forkchoice/CLAUDE.md`](phase1/forkchoice/CLAUDE.md) | Fork choice: `on_block`, `on_attestation`, `get_head`, payload votes, timing, data availability |
| [`transition/CLAUDE.md`](transition/CLAUDE.md) | State transition: block operations, epoch processing, slot processing |
| [`phase1/core/state/CLAUDE.md`](phase1/core/state/CLAUDE.md) | Beacon state helpers: accessors, mutators, builder/deposit routing |

## Cross-Cutting Principles

- Before modifying consensus code, locate the spec section the function
  implements. The spec maps above provide the mapping; if a function is missing
  from a map, find the spec section yourself before changing behavior.
- Partial lists vs full lists: when a loop processes a prefix of a queue (e.g.,
  pending deposits, pending consolidations), the spec often intentionally uses
  the partial accumulator — not the full list. Do not "fix" a partial list to
  the full list without checking the spec.
- Validate before mutating: spec handlers that fail assertions must not have
  already changed state. In Go, check all reject/ignore conditions before writes.
- Fork gates: every fork (Phase0 through Gloas) may change the same function's
  behavior. Preserve pre-fork semantics when the state version is earlier than
  the fork that introduced the change.
