# ePBS reveal retry backoff

Status: local follow-up; deferred from the payload-optimizer spec refresh.

## Problem

When the canonical head contains this builder's winning bid, a permanently
failing reveal is reconciled every 100 ms. The queue and worker limits bound
concurrency, but a frozen head can sustain CPU, disk, forkchoice, network, and
log work indefinitely.

This does not block transaction simulation or bid construction. The current
branch already preserves eventual reveal, bounds each attempt, retains the
current-slot payload across restart, and cleans partial broadcast progress when
the payload is pruned.

## Proposed policy

- Stop only for deterministic identity or validation failures.
- Retry storage, forkchoice, and gossip dependency failures.
- Use per-root exponential backoff: 100 ms, 200 ms, 400 ms, 800 ms, then a
  1 second cap.
- Reset state after success, head replacement, or payload pruning.
- Start from 100 ms after restart; do not persist backoff state.
- Preserve fairness so a failing root cannot starve another winning root.

## Implementation prerequisites

- Define typed permanent and transient reveal errors instead of classifying
  every failure as `ErrRevealExpired`.
- Add an injectable clock or timer seam for deterministic scheduler tests.

## Test matrix

- A permanent identity mismatch stops without further external work.
- A transient failure follows the bounded backoff sequence and later succeeds.
- A frozen head has bounded attempt cadence over multiple seconds.
- A second root progresses while the first root backs off.
- Success, head replacement, and pruning clear retry state.
- Restart resets in-memory backoff without losing the durable pending payload.
- Shutdown cancels timers and waits for scheduler ownership to drain.

## Acceptance criteria

- No 10 Hz retry loop remains after the first transient failures.
- Valid current-head winners retain eventual reveal attempts.
- Focused scheduler tests and race tests pass without real-time sleeps.
- The full `cl/builder/epbs` test suite remains green.
