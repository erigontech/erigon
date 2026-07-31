// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package commands

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/node/debug"
)

var cmdRegimeDepths = &cobra.Command{
	Use:   "regime-depths",
	Short: "Compute mode-B unwind DEPTHS that verifiably hit each of the 4 runtime regimes.",
	Long: `Emits one DEPTHS line hitting each of the 4 mode-B unwind regimes:
  1 = in changeset   (target above CanUnwindToBlockNum, mode-A window)
  2 = in mdbx        (target below changeset floor, target step has no .kv file)
  3 = per-step file  (target step is in a commitment .kv file with width=1)
  4 = multi-step     (target step is in a commitment .kv file with width>1)

File layout at soak-driver call time governs which target blocks satisfy
each regime — the subcommand computes them fresh per invocation. Fails
loudly (exit 1) if any regime is unreachable, so the driver never runs
with silent regime-coverage loss.`,
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		ctx := cmd.Context()

		dirs := datadir.New(datadirCli)
		db, err := openDB(ctx, dbCfg(dbcfg.ChainDB, dirs.Chaindata), true, chain, logger)
		if err != nil {
			logger.Error("open DB", "err", err)
			os.Exit(2)
		}
		defer db.Close()

		tx, err := db.BeginTemporalRo(ctx)
		if err != nil {
			logger.Error("begin temporal ro", "err", err)
			os.Exit(2)
		}
		defer tx.Rollback()

		stepSize := tx.Debug().StepSize()
		if stepSize == 0 {
			logger.Error("aggregator StepSize == 0")
			os.Exit(2)
		}

		headPtr := rawdb.ReadCurrentBlockNumber(tx)
		if headPtr == nil {
			logger.Error("ReadCurrentBlockNumber returned nil")
			os.Exit(2)
		}
		headBlock := *headPtr

		changesetFloor, err := rawtemporaldb.CanUnwindToBlockNum(tx)
		if err != nil {
			logger.Error("CanUnwindToBlockNum", "err", err)
			os.Exit(2)
		}

		commitmentFiles, err := scanCommitmentKVFiles(dirs.SnapDomain, stepSize)
		if err != nil {
			logger.Error("scan commitment .kv files", "err", err)
			os.Exit(2)
		}
		if len(commitmentFiles) == 0 {
			logger.Error("no commitment .kv files found")
			os.Exit(2)
		}

		maxEndStep := uint64(0)
		for _, f := range commitmentFiles {
			if f.toStep > maxEndStep {
				maxEndStep = f.toStep
			}
		}

		blockAtStepEnd := func(step uint64) (uint64, error) {
			if step == 0 {
				return 0, nil
			}
			bn, ok, ferr := rawdbv3.TxNums.FindBlockNum(ctx, tx, step*stepSize-1)
			if ferr != nil {
				return 0, ferr
			}
			if !ok {
				return 0, fmt.Errorf("FindBlockNum(step=%d, txN=%d): not found", step, step*stepSize-1)
			}
			return bn, nil
		}

		// Regime 1: in changeset — target strictly between changesetFloor
		// and head. Pick middle of the window; fall back to floor+1 for
		// very narrow windows.
		if headBlock <= changesetFloor+1 {
			logger.Error("regime 1 unreachable: no room between changesetFloor and head",
				"head", headBlock, "changesetFloor", changesetFloor)
			os.Exit(1)
		}
		r1Target := (changesetFloor + headBlock) / 2
		if r1Target <= changesetFloor {
			r1Target = changesetFloor + 1
		}

		// Regime 2: in MDBX — target step > maxEndStep (no file yet), below changesetFloor.
		blockMaxStep, err := blockAtStepEnd(maxEndStep)
		if err != nil {
			logger.Error("blockAtStepEnd(maxEndStep)", "err", err, "maxEndStep", maxEndStep)
			os.Exit(2)
		}
		if changesetFloor <= blockMaxStep {
			logger.Error("regime 2 unreachable: changesetFloor is inside the in-MDBX region",
				"blockMaxStep", blockMaxStep, "changesetFloor", changesetFloor)
			os.Exit(1)
		}
		r2Target := (blockMaxStep + changesetFloor) / 2

		// Regime 3: in per-step file — first commitment .kv with width == 1.
		var perStepFile *commitmentKV
		for i := range commitmentFiles {
			if commitmentFiles[i].width() == 1 {
				perStepFile = &commitmentFiles[i]
				break
			}
		}
		if perStepFile == nil {
			logger.Error("regime 3 unreachable: no commitment .kv file with width==1")
			os.Exit(1)
		}
		r3Lo, err := blockAtStepEnd(perStepFile.fromStep)
		if err != nil {
			logger.Error("blockAtStepEnd(per-step from)", "err", err)
			os.Exit(2)
		}
		r3Hi, err := blockAtStepEnd(perStepFile.toStep)
		if err != nil {
			logger.Error("blockAtStepEnd(per-step to)", "err", err)
			os.Exit(2)
		}
		r3Target := (r3Lo + r3Hi) / 2

		// Regime 4: in multi-step file — widest MERGED file (fromStep>0)
		// whose txN range maps to distinct blocks in this datadir.
		// Unwind into the initial frozen `.0-N.kv` IS supported, but the
		// resulting depth is huge for a soak iter; the shallower
		// non-frozen merged files exercise the same regime with a
		// tractable iter runtime. AND a multi-step file whose txN range
		// predates the datadir's block-level index (fresh sync under
		// --prune.mode=minimal where preverified blocks/txns cover only
		// a subset of the historical state files) is also unusable —
		// blockAtStepEnd collapses both fromStep and toStep to the
		// same fallback block, and the emitted lo/hi range degenerates.
		// Iterate by width, largest first, keeping the first file whose
		// range maps to distinct blocks.
		candidates := make([]int, 0, len(commitmentFiles))
		for i := range commitmentFiles {
			if commitmentFiles[i].width() > 1 && commitmentFiles[i].fromStep > 0 {
				candidates = append(candidates, i)
			}
		}
		sort.Slice(candidates, func(a, b int) bool {
			return commitmentFiles[candidates[a]].width() > commitmentFiles[candidates[b]].width()
		})
		var multiStepFile *commitmentKV
		var r4Lo, r4Hi uint64
		for _, i := range candidates {
			lo, err := blockAtStepEnd(commitmentFiles[i].fromStep)
			if err != nil {
				logger.Error("blockAtStepEnd(multi-step from)", "err", err, "file", commitmentFiles[i].name)
				continue
			}
			hi, err := blockAtStepEnd(commitmentFiles[i].toStep)
			if err != nil {
				logger.Error("blockAtStepEnd(multi-step to)", "err", err, "file", commitmentFiles[i].name)
				continue
			}
			if lo >= hi {
				// Degenerate: fromStep and toStep both map to the same
				// fallback block. File's txN range predates the datadir's
				// block-level index. Try the next candidate.
				continue
			}
			multiStepFile = &commitmentFiles[i]
			r4Lo = lo
			r4Hi = hi
			break
		}
		if multiStepFile == nil {
			logger.Error("regime 4 unreachable: no multi-step commitment .kv file (fromStep>0, width>1) whose txN range maps to distinct blocks in the datadir; every candidate collapses to a single block via blockAtStepEnd")
			os.Exit(1)
		}
		r4Target := (r4Lo + r4Hi) / 2

		// TxNums-index corruption guard: shift back up to 100 blocks if the
		// picked target's TxNums.Max returns a value > head's txN (the
		// anomaly we saw at cleanrun6 block 3,007,257).
		headTxN, err := rawdbv3.TxNums.Max(ctx, tx, headBlock)
		if err != nil {
			logger.Error("TxNums.Max(head)", "err", err)
			os.Exit(2)
		}
		targets := []*uint64{&r1Target, &r2Target, &r3Target, &r4Target}
		for _, t := range targets {
			for shift := uint64(0); shift < 100 && *t > shift; shift++ {
				candTx, terr := rawdbv3.TxNums.Max(ctx, tx, *t-shift)
				if terr == nil && candTx <= headTxN {
					*t -= shift
					break
				}
			}
		}

		emit := func(regime int, target, lo, hi uint64, in string) {
			fmt.Printf("regime=%d depth=%d target=%d in=%s lo=%d hi=%d\n",
				regime, headBlock-target, target, in, lo, hi)
		}
		emit(1, r1Target, changesetFloor+1, headBlock-1, "changeset")
		emit(2, r2Target, blockMaxStep+1, changesetFloor-1, "mdbx")
		emit(3, r3Target, r3Lo+1, r3Hi, "per-step:"+perStepFile.name)
		emit(4, r4Target, r4Lo+1, r4Hi, "multi-step:"+multiStepFile.name)

		fmt.Printf("DEPTHS=%d,%d,%d,%d\n",
			headBlock-r1Target,
			headBlock-r2Target,
			headBlock-r3Target,
			headBlock-r4Target,
		)
	},
}

type commitmentKV struct {
	name     string
	fromStep uint64
	toStep   uint64
}

func (c commitmentKV) width() uint64 { return c.toStep - c.fromStep }

func scanCommitmentKVFiles(domainDir string, stepSize uint64) ([]commitmentKV, error) {
	entries, err := os.ReadDir(domainDir)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", domainDir, err)
	}
	var out []commitmentKV
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".kv") {
			continue
		}
		if !strings.Contains(name, "-commitment.") {
			continue
		}
		info, _, parsed := snaptype.ParseFileName("", name)
		if !parsed || info.From >= info.To {
			continue
		}
		var fromStep, toStep uint64
		if info.Version.Cmp(version.TxNumNamingPivot) < 0 {
			fromStep, toStep = info.From, info.To
		} else {
			fromStep, toStep = info.From/stepSize, info.To/stepSize
		}
		out = append(out, commitmentKV{name: name, fromStep: fromStep, toStep: toStep})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].fromStep < out[j].fromStep })
	return out, nil
}

func init() {
	withDataDir(cmdRegimeDepths)
	withChain(cmdRegimeDepths)
	rootCmd.AddCommand(cmdRegimeDepths)
}
