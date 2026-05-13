# Core Principles — From Karpathy's Autoresearch

7 universal principles extracted from autoresearch, applicable to ANY autonomous work.

## 1. Constraint = Enabler

Autonomy succeeds through intentional constraint, not despite it.

| Autoresearch | Generalized |
|--------------|-------------|
| 630-line codebase | Bounded scope that fits agent context |
| 5-minute time budget | Fixed iteration cost |
| One metric (val_bpb) | Single mechanical success criterion |

**Why:** Constraints enable agent confidence (full context understood), verification simplicity (no ambiguity), iteration velocity (low cost = rapid feedback loops).

**Apply:** Before starting, define: what files are in-scope? What's the ONE metric? What's the time budget per iteration?

## 2. Separate Strategy from Tactics

Humans set direction. Agents execute iterations.

| Strategic (Human) | Tactical (Agent) |
|-------------------|------------------|
| "Improve page load speed" | "Lazy-load images, code-split routes" |
| "Increase test coverage" | "Add tests for uncovered edge cases" |
| "Refactor auth module" | "Extract middleware, simplify handlers" |

**Why:** Humans understand WHY. Agents handle HOW. Mixing these roles wastes both human creativity and agent iteration speed.

**Apply:** Get clear direction from user (or program.md). Then iterate autonomously on implementation.

## 3. Metrics Must Be Mechanical

If you can't verify with a command, you can't iterate autonomously.

- Tests pass/fail (exit code 0)
- Benchmark time in milliseconds
- Coverage percentage
- Lighthouse score
- File size in bytes
- Lines of code count

**Anti-pattern:** "Looks better", "probably improved", "seems cleaner" → these KILL autonomous loops because there's no decision function.

**Apply:** Define the `grep` command (or equivalent) that extracts your metric BEFORE starting.

## 4. Verification Must Be Fast

If verification takes longer than the work itself, incentives misalign.

| Fast (enables iteration) | Slow (kills iteration) |
|-------------------------|----------------------|
| Unit tests (seconds) | Full E2E suite (minutes) |
| Type check (seconds) | Manual QA (hours) |
| Lint check (instant) | Code review (async) |

**Apply:** Use the FASTEST verification that still catches real problems. Save slow verification for after the loop.

## 5. Iteration Cost Shapes Behavior

- Cheap iteration → bold exploration, many experiments
- Expensive iteration → conservative, few experiments

Autoresearch: 5-minute cost → 100 experiments/night.
Software: 10-second test → 360 experiments/hour.

**Apply:** Minimize iteration cost. Use fast tests, incremental builds, targeted verification. Every minute saved = more experiments run.

## 6. Git as Memory and Audit Trail

Every successful change is committed. This enables:
- **Causality tracking** — which change drove improvement?
- **Stacking wins** — each commit builds on prior successes
- **Pattern learning** — agent sees what worked in THIS codebase
- **Human review** — researcher inspects agent's decision sequence

**Apply:** Commit before verify. Revert on failure. Agent reads its own git history to inform next experiment.

## 7. Honest Limitations

State what the system can and cannot do. Don't oversell.

Autoresearch CANNOT: change tokenizer, replace human direction, guarantee meaningful improvements.

**Apply:** At setup, explicitly state constraints. If agent hits a wall it can't solve (missing permissions, external dependency, needs human judgment), say so clearly instead of guessing.

## The Meta-Principle

> Autonomy scales when you constrain scope, clarify success, mechanize verification, and let agents optimize tactics while humans optimize strategy.

This isn't "removing humans." It's reassigning human effort from execution to direction. Humans become MORE valuable by focusing on irreducibly creative/strategic work.
