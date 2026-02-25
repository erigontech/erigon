# seboost: objectives and completion criteria

## Problem

stage_exec is slow. It does sequential transaction execution today. Parallel execution (BlockSTM-based) exists but inter-tx dependencies limit actual parallelism. BALs (Block Access Lists) will enable full parallelism natively, but they're not in the protocol yet — meaning 100% of existing blocks lack them.

## Goal

Make block execution faster for pre-BAL era blocks by providing auxiliary sidecar data ("seboost") that hints the parallel executor toward better scheduling.

**Scope**: experimental, on Sepolia, first 6M blocks.

## Phases

### Phase 1: Tx dependency hints (txdeps)

Code up txdeps generation and consumption. Run on Sepolia blocks 0–6M:
1. Generate txdeps seboost data + collect size stats.
2. Run stage_exec + parallel_exec baseline (no seboost).
3. Run stage_exec + txdeps seboost, compare.

### Phase 2: Block Access Lists (BAL)

After phase 1 results are in. Generate BAL data for the same Sepolia 0–6M range, collect size stats, run stage_exec with BAL seboost, compare against the same baseline and against txdeps results.

## Metrics

- Wall-clock time for stage_exec completion from block 0 to 6M.
- Total seboost data size.
- Graph: average per-block seboost size vs tx count in that block.

## Artifacts (all in `~/seboost-stats/`)

- `txdeps-sizes.csv` — per-block size stats from generation pass: `blockNum,txCount,bitmapBytes,sparseBytes,chosenBytes`.
- `report.md` — formal report with:
  - Summary: wall-clock times (baseline vs seboost), speedup ratio, total seboost file size.
  - Links to detailed breakdowns: size analysis plots, per-range stats, encoding distribution, etc.
- `plots/` — generated images (size vs tx count, encoding choice distribution, etc.).
- Analysis scripts.
- Seboost data files in `<datadir>/seboost_txdeps/`.

## Phase 1 completion criteria

Phase 1 is done when all of the following are true:

1. **Code complete**: txdeps generation and consumption are implemented and compile cleanly (`make lint && make erigon integration`).
2. **Pass 1 (generate)** completed: seboost files written to `<datadir>/seboost_txdeps/` for Sepolia blocks 0–6M. `txdeps-sizes.csv` written.
3. **Pass 2 (baseline)** completed: stage_exec + parallel_exec ran on Sepolia 0–6M with no seboost. Wall-clock time recorded.
4. **Pass 3 (seboost)** completed: stage_exec + parallel_exec + txdeps seboost ran on Sepolia 0–6M. Wall-clock time recorded. State root checks passed (correctness verified).
5. **Report generated**: `~/seboost-stats/report.md` exists with:
   - Baseline wall-clock time vs seboost wall-clock time.
   - Speedup ratio.
   - Total seboost file size (all `.bin` files combined).
   - Size analysis: graph of per-block seboost size vs tx count.
   - Encoding distribution: how often bitmap vs sparse was chosen.
   - Per-file-range breakdown (each 500k range).
   - Links to all plots in `plots/`.

## Phase 2 completion criteria

Phase 2 is done when:

1. BAL generation and consumption are implemented.
2. All 3 passes completed for BAL on same Sepolia 0–6M range.
3. Report updated with BAL results alongside txdeps results:
   - BAL wall-clock time vs baseline vs txdeps.
   - BAL file size vs txdeps file size.
   - Recommendation on which approach (or combination) to pursue.

## Design constraints

- **Simple**: generate once, minimal chance of bugs.
- **Small**: adding to snapshots or distributing should be cheap.
- **Universal format**: avoid coupling to erigon's internal file formats which evolve over time.
- **Optional per-block**: blocks with <3 txs are skipped. Fallback to normal parallel execution if missing.
- **Correctness is automatic**: state root verification catches bad hints.
