---
name: autoresearch
description: Autonomous Goal-directed Iteration. Apply Karpathy's autoresearch principles to ANY task. Loops autonomously — modify, verify, keep/discard, repeat. Supports optional loop count via Claude Code's /loop command.
version: 1.0.1
---

# Claude Autoresearch — Autonomous Goal-directed Iteration

Inspired by [Karpathy's autoresearch](https://github.com/karpathy/autoresearch). Applies constraint-driven autonomous iteration to ANY work — not just ML research.

**Core idea:** You are an autonomous agent. Modify → Verify → Keep/Discard → Repeat.

## When to Activate

- User invokes `/autoresearch` or `/ug:autoresearch`
- User says "work autonomously", "iterate until done", "keep improving", "run overnight"
- Any task requiring repeated iteration cycles with measurable outcomes

## Optional: Controlled Loop Count

By default, autoresearch loops **forever** until manually interrupted. However, users can optionally specify a **loop count** to limit iterations using Claude Code's built-in `/loop` command.

> **Requires:** Claude Code v1.0.32+ (the `/loop` command was introduced in this version)

### Usage

**Unlimited (default):**
```
/autoresearch
Goal: Increase test coverage to 90%
```

**Bounded (N iterations):**
```
/loop 25 /autoresearch
Goal: Increase test coverage to 90%
```

This chains `/autoresearch` with `/loop 25`, running exactly 25 iteration cycles. After 25 iterations, Claude stops and prints a final summary.

### When to Use Bounded Loops

| Scenario | Recommendation |
|----------|---------------|
| Run overnight, review in morning | Unlimited (default) |
| Quick 30-min improvement session | `/loop 10 /autoresearch` |
| Targeted fix with known scope | `/loop 5 /autoresearch` |
| Exploratory — see if approach works | `/loop 15 /autoresearch` |
| CI/CD pipeline integration | `/loop N /autoresearch` (set N based on time budget) |

### Behavior with Loop Count

When a loop count is specified:
- Claude runs exactly N iterations through the autoresearch loop
- After iteration N, Claude prints a **final summary** with baseline → current best, keeps/discards/crashes
- If the goal is achieved before N iterations, Claude prints early completion and stops
- All other rules (atomic changes, mechanical verification, auto-rollback) still apply

## Setup Phase (Do Once)

1. **Read all in-scope files** for full context before any modification
2. **Define the goal** — What does "better" mean? Extract or ask for a mechanical metric:
   - Code: tests pass, build succeeds, performance benchmark improves
   - Content: word count target hit, SEO score improves, readability score
   - Design: lighthouse score, accessibility audit passes
   - If no metric exists → define one with user, or use simplest proxy (e.g. "compiles without errors")
3. **Define scope constraints** — Which files can you modify? Which are read-only?
4. **Create a results log** — Track every iteration (see `references/results-logging.md`)
5. **Establish baseline** — Run verification on current state. Record as iteration #0
6. **Confirm and go** — Show user the setup, get confirmation, then BEGIN THE LOOP

## The Loop

Read `references/autonomous-loop-protocol.md` for full protocol details.

```
LOOP (FOREVER or N times):
  1. Review: Read current state + git history + results log
  2. Ideate: Pick next change based on goal, past results, what hasn't been tried
  3. Modify: Make ONE focused change to in-scope files
  4. Commit: Git commit the change (before verification)
  5. Verify: Run the mechanical metric (tests, build, benchmark, etc.)
  6. Decide:
     - IMPROVED → Keep commit, log "keep", advance
     - SAME/WORSE → Git revert, log "discard"
     - CRASHED → Try to fix (max 3 attempts), else log "crash" and move on
  7. Log: Record result in results log
  8. Repeat: Go to step 1.
     - If unbounded: NEVER STOP. NEVER ASK "should I continue?"
     - If bounded (N): Stop after N iterations, print final summary
```

## Critical Rules

1. **Loop until done** — Unbounded: loop until interrupted. Bounded: loop N times then summarize.
2. **Read before write** — Always understand full context before modifying
3. **One change per iteration** — Atomic changes. If it breaks, you know exactly why
4. **Mechanical verification only** — No subjective "looks good". Use metrics
5. **Automatic rollback** — Failed changes revert instantly. No debates
6. **Simplicity wins** — Equal results + less code = KEEP. Tiny improvement + ugly complexity = DISCARD
7. **Git is memory** — Every kept change committed. Agent reads history to learn patterns
8. **When stuck, think harder** — Re-read files, re-read goal, combine near-misses, try radical changes. Don't ask for help unless truly blocked by missing access/permissions

## Principles Reference

See `references/core-principles.md` for the 7 generalizable principles from autoresearch.

## Adapting to Different Domains

| Domain | Metric | Scope | Verify Command |
|--------|--------|-------|----------------|
| Backend code | Tests pass + coverage % | `src/**/*.ts` | `npm test` |
| Frontend UI | Lighthouse score | `src/components/**` | `npx lighthouse` |
| ML training | val_bpb / loss | `train.py` | `uv run train.py` |
| Blog/content | Word count + readability | `content/*.md` | Custom script |
| Performance | Benchmark time (ms) | Target files | `npm run bench` |
| Refactoring | Tests pass + LOC reduced | Target module | `npm test && wc -l` |

Adapt the loop to your domain. The PRINCIPLES are universal; the METRICS are domain-specific.
