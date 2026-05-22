# Autonomous Loop Protocol

Detailed protocol for the autoresearch iteration loop. SKILL.md has the summary; this file has the full rules.

## Loop Modes

Autoresearch supports two loop modes:

- **Unbounded (default):** Loop forever until manually interrupted (`Ctrl+C`)
- **Bounded:** Loop exactly N times when chained with `/loop N` (requires Claude Code v1.0.32+)

When bounded, track `current_iteration` against `max_iterations`. After the final iteration, print a summary and stop.

## Phase 1: Review (30 seconds)

Before each iteration, build situational awareness:

```
1. Read current state of in-scope files (full context)
2. Read last 10-20 entries from results log
3. Read git log --oneline -20 to see recent changes
4. Identify: what worked, what failed, what's untried
5. If bounded: check current_iteration vs max_iterations
```

**Why read every time?** After rollbacks, state may differ from what you expect. Never assume — always verify.

## Phase 2: Ideate (Strategic)

Pick the NEXT change. Priority order:

1. **Fix crashes/failures** from previous iteration first
2. **Exploit successes** — if last change improved metric, try variants in same direction
3. **Explore new approaches** — try something the results log shows hasn't been attempted
4. **Combine near-misses** — two changes that individually didn't help might work together
5. **Simplify** — remove code while maintaining metric. Simpler = better
6. **Radical experiments** — when incremental changes stall, try something dramatically different

**Anti-patterns:**
- Don't repeat exact same change that was already discarded
- Don't make multiple unrelated changes at once (can't attribute improvement)
- Don't chase marginal gains with ugly complexity

**Bounded mode consideration:** If remaining iterations are limited (<3 left), prioritize exploiting successes over exploration.

## Phase 3: Modify (One Atomic Change)

- Make ONE focused change to in-scope files
- The change should be explainable in one sentence
- Write the description BEFORE making the change (forces clarity)

## Phase 4: Commit (Before Verification)

```bash
git add <changed-files>
git commit -m "experiment: <one-sentence description>"
```

Commit BEFORE running verification so rollback is clean: `git reset --hard HEAD~1`

## Phase 5: Verify (Mechanical Only)

Run the agreed-upon verification command. Capture output.

**Timeout rule:** If verification exceeds 2x normal time, kill and treat as crash.

**Extract metric:** Parse the verification output for the specific metric number.

## Phase 6: Decide (No Ambiguity)

```
IF metric_improved:
    STATUS = "keep"
    # Do nothing — commit stays
ELIF metric_same_or_worse:
    STATUS = "discard"
    git reset --hard HEAD~1
ELIF crashed:
    # Attempt fix (max 3 tries)
    IF fixable:
        Fix → re-commit → re-verify
    ELSE:
        STATUS = "crash"
        git reset --hard HEAD~1
```

**Simplicity override:** If metric barely improved (+<0.1%) but change adds significant complexity, treat as "discard". If metric unchanged but code is simpler, treat as "keep".

## Phase 7: Log Results

Append to results log (TSV format):

```
iteration  commit   metric   status   description
42         a1b2c3d  0.9821   keep     increase attention heads from 8 to 12
43         -        0.9845   discard  switch optimizer to SGD
44         -        0.0000   crash    double batch size (OOM)
```

## Phase 8: Repeat

### Unbounded Mode (default)

Go to Phase 1. **NEVER STOP. NEVER ASK IF YOU SHOULD CONTINUE.**

### Bounded Mode (with /loop N)

```
IF current_iteration < max_iterations:
    Go to Phase 1
ELIF goal_achieved:
    Print: "Goal achieved at iteration {N}! Final metric: {value}"
    Print final summary
    STOP
ELSE:
    Print final summary
    STOP
```

**Final summary format:**
```
=== Autoresearch Complete (N/N iterations) ===
Baseline: {baseline} → Final: {current} ({delta})
Keeps: X | Discards: Y | Crashes: Z
Best iteration: #{n} — {description}
```

### When Stuck (>5 consecutive discards)

Applies to both modes:
1. Re-read ALL in-scope files from scratch
2. Re-read the original goal/direction
3. Review entire results log for patterns
4. Try combining 2-3 previously successful changes
5. Try the OPPOSITE of what hasn't been working
6. Try a radical architectural change

## Crash Recovery

- Syntax error → fix immediately, don't count as separate iteration
- Runtime error → attempt fix (max 3 tries), then move on
- Resource exhaustion (OOM) → revert, try smaller variant
- Infinite loop/hang → kill after timeout, revert, avoid that approach
- External dependency failure → skip, log, try different approach

## Communication

- **DO NOT** ask "should I keep going?" — in unbounded mode, YES. ALWAYS. In bounded mode, continue until N is reached.
- **DO NOT** summarize after each iteration — just log and continue
- **DO** print a brief one-line status every ~5 iterations (e.g., "Iteration 25: metric at 0.95, 8 keeps / 17 discards")
- **DO** alert if you discover something surprising or game-changing
- **DO** print a final summary when bounded loop completes
