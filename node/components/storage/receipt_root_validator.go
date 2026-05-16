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

	fromStep := files[0].FromStep
	toStep := files[0].ToStep
	if toStep == 0 {
		return fmt.Errorf("receipt step has zero ToStep")
	}

	var fromBlock, toBlock uint64
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
		fromBlock, toBlock = fb, tb
		return nil
	}); err != nil {
		return err
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
