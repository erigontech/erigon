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

package storage

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/validation"
)

// ReceiptRootValidator verifies DeriveSha(receipts) == header.ReceiptHash
// for each block in a retired receipt-domain step. Delegates per-block
// work to integrity.ReceiptRootIntegrityRange so this validator and
// `erigon seg integrity` share the same logic. Pre-Byzantium blocks
// are skipped (PostState encoding doesn't reproduce).
//
// Body / txnum dependency: receipt-root verification calls into
// CheckRCacheRootAtBlkRange which itself uses
// TxnumReader.Min/Max(blockNum). Under minimal mode the body for the
// previous block (needed to compute the current block's minTxNum) can
// be absent below the prune horizon — Min then falls back to the
// chain-tip txnum and the verification compares against a bogus range.
//
// PruneMode lets the validator detect this case explicitly: if the
// step's block range is entirely below the prune horizon under a
// pruning prune.Mode, Phase C (per-block receipt-root cross-check) is
// skipped with a logged warning rather than producing 5 spurious
// failures and quarantining the file. Mirrors
// CommitmentDomainValidator.pruneModeExpectsBodyAbsent — same shape,
// same justification, same architectural rule: validators wait for
// their dependencies and tell the operator clearly when those
// dependencies are intentionally absent.
//
// Initialised=false (zero value) → behaviour unchanged (validator runs
// regardless of prune mode). Tools / tests that don't set PruneMode
// keep the old contract.
type ReceiptRootValidator struct {
	DB          kv.TemporalRoDB
	BlockReader services.FullBlockReader
	ChainConfig *chain.Config
	Logger      log.Logger
	PruneMode   prune.Mode

	// StateReady reports whether the state/receipt domains have finished
	// loading into the queryable DB (aggregator OpenFolder done,
	// InitialStateReady fired). Headers load before the receipt domain,
	// so the per-block FindBlockNum guards below pass while receipts are
	// still pending — CheckRCacheRootAtBlkRange then computes DeriveSha
	// over an empty receipt set, yielding the empty-trie root and a
	// spurious mismatch. While !StateReady() the validator returns
	// ErrPause (transient, no quarantine tick).
	//
	// nil → gate disabled (tools / tests that load everything up front).
	StateReady func() bool
}

// Name implements validation.StepValidator.
func (ReceiptRootValidator) Name() string { return "receipts_root_per_block" }

// pruneModeExpectsBodyAbsentForBlock reports whether the configured
// prune mode would have INTENTIONALLY pruned the body for blockNum.
// Mirrors CommitmentDomainValidator.pruneModeExpectsBodyAbsent. See
// that method's doc for the architectural rule.
func (v ReceiptRootValidator) pruneModeExpectsBodyAbsentForBlock(blockNum uint64) bool {
	if !v.PruneMode.Initialised || !v.PruneMode.Blocks.Enabled() {
		return false
	}
	if v.BlockReader == nil {
		return false
	}
	tip := v.BlockReader.FrozenBlocks()
	if tip == 0 {
		return false
	}
	horizon := v.PruneMode.Blocks.PruneTo(tip)
	return blockNum < horizon
}

// ValidateStep implements validation.StepValidator.
func (v ReceiptRootValidator) ValidateStep(ctx context.Context, files []*snapshot.FileEntry) error {
	if len(files) == 0 || files[0].Domain != snapshot.DomainReceipt {
		return nil
	}
	if v.DB == nil {
		return fmt.Errorf("ReceiptRootValidator: nil DB")
	}
	if v.BlockReader == nil {
		return fmt.Errorf("ReceiptRootValidator: nil BlockReader")
	}
	if v.ChainConfig == nil {
		return fmt.Errorf("ReceiptRootValidator: nil ChainConfig")
	}

	// Receipt domain still loading — CheckRCacheRootAtBlkRange would see
	// an empty receipt set and compute the empty-trie root. Pause rather
	// than fail; a quarantine tick here would strand a good file.
	if v.StateReady != nil && !v.StateReady() {
		return fmt.Errorf("receipt domain not loaded yet (InitialStateReady not fired): %w", validation.ErrPause)
	}

	fromStep := files[0].FromStep
	toStep := files[0].ToStep
	if toStep == 0 {
		return fmt.Errorf("receipt step has zero ToStep")
	}

	var fromBlock, toBlock uint64
	var rangeEmpty bool
	if err := v.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		stepSize := tx.Debug().StepSize()
		startTxNum := fromStep * stepSize
		endTxNum := toStep * stepSize
		txNumReader := v.BlockReader.TxnumReader()

		fb, ok, err := txNumReader.FindBlockNum(ctx, tx, startTxNum)
		if err != nil {
			return fmt.Errorf("FindBlockNum(startTxNum=%d): %w", startTxNum, err)
		}
		if !ok {
			// TxNum not yet covered by an imported block. Transient
			// at fresh-bootstrap (state files on disk, EL hasn't
			// OpenSegments'd yet). ErrPause makes the lifecycle
			// retry next sweep instead of accumulating toward
			// quarantine.
			return fmt.Errorf("FindBlockNum(startTxNum=%d) returned ok=false: %w", startTxNum, validation.ErrPause)
		}
		tb, ok, err := txNumReader.FindBlockNum(ctx, tx, endTxNum-1)
		if err != nil {
			return fmt.Errorf("FindBlockNum(endTxNum-1=%d): %w", endTxNum-1, err)
		}
		if !ok {
			return fmt.Errorf("FindBlockNum(endTxNum-1=%d) returned ok=false: %w", endTxNum-1, validation.ErrPause)
		}

		// Boundary trim — skip blocks that straddle a step boundary.
		// A receipt step covers a fixed txnum range; blocks aren't
		// guaranteed to align to step boundaries, so at retire time a
		// block can be split: its prefix txnums end up in the previous
		// step file, the suffix in this one (or vice versa at the
		// trailing edge). Computing DeriveSha(receipts) on a partial
		// receipt set produces a different root than header.ReceiptHash
		// (which covers the whole block) → spurious mismatch like
		// "computed=0x24b33d... header=0x4f33..." at block 25069471
		// observed live on the 2026-05-19 run.
		//
		// The fix: trim fromBlock forward if its Min(...) is below
		// startTxNum (block prefix is in the prior step), and trim
		// toBlock backward if its Max(...) extends to/past endTxNum
		// (block suffix continues into the next step). The receipt
		// step's coverage of boundary blocks is the prior/next step's
		// concern; each step validates only the blocks FULLY contained
		// in its txnum range. A future refinement could combine
		// multiple step files to validate the boundary blocks, but
		// blocks split exactly on a step boundary are common (~2 per
		// step pair) and skipping them is honest about what each
		// per-step validator can see.
		fbMin, err := txNumReader.Min(ctx, tx, fb)
		if err != nil {
			return fmt.Errorf("Min(fromBlock=%d): %w", fb, err)
		}
		if fbMin < startTxNum {
			fb++
		}
		tbMax, err := txNumReader.Max(ctx, tx, tb)
		if err != nil {
			return fmt.Errorf("Max(toBlock=%d): %w", tb, err)
		}
		if tbMax > endTxNum {
			if tb == 0 {
				rangeEmpty = true
				return nil
			}
			tb--
		}
		if fb > tb {
			rangeEmpty = true
			return nil
		}
		fromBlock, toBlock = fb, tb
		return nil
	}); err != nil {
		return err
	}
	if rangeEmpty {
		// Both boundaries were partial; nothing left to validate in
		// this step alone. Not an error — the validator's per-step
		// coverage is fundamentally limited to full blocks within the
		// step.
		if v.Logger != nil {
			v.Logger.Info("[storage] receipt step has only partial-boundary blocks; skipping per-block check",
				"step", fmt.Sprintf("[%d, %d)", fromStep, toStep))
		}
		return nil
	}

	// Skip pre-Byzantium blocks (the integrity helper does too).
	if v.ChainConfig.ByzantiumBlock != nil && *v.ChainConfig.ByzantiumBlock > fromBlock {
		fromBlock = *v.ChainConfig.ByzantiumBlock
	}
	if fromBlock > toBlock {
		return nil
	}

	// Prune-horizon gate: CheckRCacheRootAtBlkRange uses
	// TxnumReader.Min/Max per block, which falls back to the chain-tip
	// txnum when the previous block's body is pruned (returning a
	// bogus range that produces a spurious receipt-root mismatch).
	// If toBlock is below the prune horizon, the bodies needed to
	// resolve every per-block txnum range are intentionally absent
	// under this prune mode — skip Phase C with a logged warning
	// instead of producing 5 failures and quarantining.
	//
	// Step-boundary case (toBlock < horizon AND fromBlock < horizon):
	// entire range pre-pruned, skip cleanly.
	// Crossing case (fromBlock < horizon < toBlock): the boundary
	// block's body could still cause Min to fall back; we conservatively
	// skip the whole step for now. A future refinement could split the
	// range and validate only the post-horizon portion, but the
	// receipt-root invariant covers blocks individually so skipping
	// the whole step doesn't weaken anything we wouldn't lose anyway
	// — it just defers the post-horizon verification.
	if v.pruneModeExpectsBodyAbsentForBlock(toBlock) {
		if v.Logger != nil {
			v.Logger.Info("[storage] receipt Phase C skipped (anchor body pruned)",
				"step", fmt.Sprintf("[%d, %d)", fromStep, toStep),
				"blocks", fmt.Sprintf("[%d, %d]", fromBlock, toBlock))
		}
		return nil
	}

	sc, err := integrity.NewSamplerCfg(0, 1.0) // verify every block in the step's coverage
	if err != nil {
		return fmt.Errorf("sampler cfg: %w", err)
	}
	logger := v.Logger
	if logger == nil {
		logger = log.Root()
	}
	if err := integrity.CheckRCacheRootAtBlkRange(ctx, sc, v.DB, v.BlockReader, v.ChainConfig, fromBlock, toBlock, true /* failFast */, logger); err != nil {
		return fmt.Errorf("receipt step [%d, %d) blocks [%d, %d]: %w", fromStep, toStep, fromBlock, toBlock, err)
	}
	return nil
}
