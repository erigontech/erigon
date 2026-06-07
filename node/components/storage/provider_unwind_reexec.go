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
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// PROBE helpers for [PROBE-PARALLEL-READ].
func balOrNil(a *accounts.Account) string {
	if a == nil {
		return "<nil>"
	}
	return a.Balance.String()
}
func nonceOrNil(a *accounts.Account) string {
	if a == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%d", a.Nonce)
}

// fileSideTemporalGetter wraps a kv.TemporalTx + maxStep bound so that
// GetLatest reads return the value EXACTLY AT the file-side step
// boundary, ignoring any writable-shadow entries past maxStep. This is
// the foundational primitive mode-B's partial-block re-exec uses: at
// cs.txNum (= maxStep*stepSize - 1) it serves account/storage/code
// values that match the canonical state at that point, while the
// forward-exec writable shadow has unrelated values at much higher
// txNums (snapshot tip) that must not leak into the re-exec context.
//
// Per-call semantics:
//
//   - GetLatest: routes to tx.Debug().GetLatestFromFilesUpToStep(domain,
//     k, maxStep). The returned step is the file's end step (≤ maxStep);
//     callers that depend on step for incarnation reuse get a stable
//     value.
//
//   - HasPrefix: delegates to the underlying tx — files cap at maxStep
//     are not iterable through a single HasPrefix today; the only known
//     reader path that triggers this is HasStorage during contract
//     destruction. Forward-exec writes past maxStep may produce a
//     stale-true positive in that edge case; partial-block exec
//     correctness for selfdestruct in this scenario is documented in
//     [[mode-b-recompute-mid-block-step-cut-2026-06-03]] open question (2)
//     and revisited if live tests surface it.
//
//   - StepsInFiles: returns the configured maxStep; consumers use this
//     to decide whether file-side or DB-side reads are authoritative.
//
// The returned wrapper is read-only — write paths route through the
// caller-supplied StateWriter, NOT through this getter.
type fileSideTemporalGetter struct {
	tx      kv.TemporalTx
	maxStep kv.Step
}

func newFileSideTemporalGetter(tx kv.TemporalTx, maxStep kv.Step) *fileSideTemporalGetter {
	return &fileSideTemporalGetter{tx: tx, maxStep: maxStep}
}

func (g *fileSideTemporalGetter) GetLatest(name kv.Domain, k []byte) (v []byte, step kv.Step, err error) {
	v, fileEndStep, found, err := g.tx.Debug().GetLatestFromFilesUpToStep(name, k, g.maxStep)
	if err != nil {
		return nil, 0, fmt.Errorf("fileSideTemporalGetter.GetLatest(%s, maxStep=%d): %w", name, g.maxStep, err)
	}
	if !found {
		return nil, 0, nil
	}
	return v, fileEndStep, nil
}

func (g *fileSideTemporalGetter) HasPrefix(name kv.Domain, prefix []byte) ([]byte, []byte, bool, error) {
	// Delegates — see type doc-comment for the selfdestruct edge case
	// caveat. A future iteration may swap in a file-side bounded scan.
	return g.tx.HasPrefix(name, prefix)
}

func (g *fileSideTemporalGetter) StepsInFiles(entitySet ...kv.Domain) kv.Step {
	return g.maxStep
}

// newFileSideStateReader returns a state.StateReader that serves the
// canonical state at maxStep's boundary (== cs.txNum) without depending
// on per-tx history. Use as the read-side of partial-block exec.
//
// Lifetime: borrows tx; caller owns tx commit/rollback.
func newFileSideStateReader(tx kv.TemporalTx, maxStep kv.Step) state.StateReader {
	return state.NewReaderV3(newFileSideTemporalGetter(tx, maxStep))
}

// PopulateHistoryByReExec is the mode-B fallback when
// RecomputeAtTxNumWithoutSD returns commitmentdb.ErrHistoryGap — i.e.,
// the snapshot step boundary cut mid-block and per-tx history doesn't
// exist locally for the in-block tail (typical of minimal-prune
// datadirs whose exec started at the snapshot tip).
//
// The recipe (see docs in
// [[mode-b-recompute-mid-block-step-cut-2026-06-03]]):
//
//  1. Partial-block exec for `fromBlock` (the block containing the mid-
//     step cs.txNum): skip the txns whose txNums ≤ fromTxN (they
//     already ran in canonical chain history; their cumulative state is
//     captured in cs.trieState + the file-side domain values).
//     Reconstruct the EVM execution context (gas pool remaining,
//     coinbase fees) from snapshot receipts so EVM behaviour matches the
//     canonical run. Execute remaining user txns + sys-end via
//     `protocol.ApplyTransaction`. The StateWriter (an SD-backed
//     TemporalPutDel) writes per-tx history records into MDBX at the
//     true in-block txNums.
//
//  2. Full-block exec for each subsequent block in `(fromBlock, toBlock]`:
//     start from the writable shadow's view (which now contains step 1's
//     output for fromBlock), run sys-start → user txns → sys-end via the
//     same primitive. Each block's per-tx history lands.
//
//  3. Flush SD's memctx so all writes durable into MDBX before the
//     caller retries the recompute.
//
// `engine` is required (block init / finalize); `Provider.Engine` is not
// owned here, so it's passed by the caller (typically the ExecModule
// which already holds the consensus engine for setHeadModeB).
//
// `preWipeReceipts` is a per-block map of receipts the caller snapshotted
// BEFORE running the writable-shadow wipe (which whole-step clears the
// RCache domain at stepContaining, destroying receipts at the gap range's
// txnums). Required when re-execing fromBlock with startTxIdx > 0
// (mid-block resume needs prior-tx receipts to reconstruct the gas pool);
// may be nil for full-block re-exec from a clean checkpoint.
func (p *Provider) PopulateHistoryByReExec(ctx context.Context, tx kv.TemporalRwTx, engine rules.EngineReader, fromTxN, fromBlock, toBlock uint64, preWipeReceipts map[uint64]types.Receipts) error {
	if p.BlockReader == nil {
		return fmt.Errorf("PopulateHistoryByReExec: nil BlockReader")
	}
	if p.ChainConfig == nil {
		return fmt.Errorf("PopulateHistoryByReExec: nil ChainConfig")
	}
	if p.Aggregator == nil {
		return fmt.Errorf("PopulateHistoryByReExec: nil Aggregator")
	}
	if engine == nil {
		return fmt.Errorf("PopulateHistoryByReExec: nil engine")
	}
	if toBlock < fromBlock {
		return fmt.Errorf("PopulateHistoryByReExec: toBlock(%d) < fromBlock(%d)", toBlock, fromBlock)
	}

	stepSize := p.Aggregator.StepSize()
	if stepSize == 0 {
		return fmt.Errorf("PopulateHistoryByReExec: stepSize == 0")
	}
	// maxStep is the file-side step that holds cs (one-past the step boundary
	// floor of fromTxN+1). Used by the file-side StateReader for the FIRST
	// (partial) block — the writable shadow is wiped past toBlock for mode-B
	// callers, so SD's read view would otherwise serve stale forward-exec
	// values that don't match cs.txN.
	maxStep := kv.Step((fromTxN + 1) / stepSize)

	// SharedDomains gives us the TemporalPutDel write path (sd.AsPutDel) +
	// the writable-shadow read view (sd.AsGetter). NewSharedDomains seeks
	// commitment from the writable shadow's latest record. In the
	// wipe-first re-exec flow (Provider.Unwind has already wiped past
	// toBlock), TxNums.Last == toBlock but the file-side commitment domain
	// still holds a record at a later cs.blockNum (the next file's stored
	// anchor — e.g. file 264-268's cs.blockNum is the block whose lastTxN
	// hits the next step boundary, well past toBlock). SeekCommitment finds
	// that file-side record and the constructor returns
	// commitmentdb.ErrBehindCommitment — wrapped, but `sd is fully
	// initialized` (per SD docs). For our purpose (writes only, no
	// ComputeCommitment) the initialized sd is usable; tolerate the
	// expected ErrBehindCommitment and proceed.
	sd, err := execctx.NewSharedDomains(ctx, tx, p.logger)
	if err != nil && !errors.Is(err, commitmentdb.ErrBehindCommitment) {
		return fmt.Errorf("PopulateHistoryByReExec: NewSharedDomains: %w", err)
	}
	defer sd.Close()

	// engine needs the richer Engine interface for Initialize/Finalize.
	// EngineReader covers per-tx ApplyTransaction; sys-start / sys-end need
	// the full surface.
	engineFull, ok := engine.(rules.Engine)
	if !ok {
		return fmt.Errorf("PopulateHistoryByReExec: engine %T does not satisfy protocol/rules.Engine", engine)
	}

	txNumsReader := p.BlockReader.TxnumReader()
	consensusReader := consensuschain.NewReader(p.ChainConfig, tx, p.BlockReader, p.logger)

	// PROBE: total txns applied across all blocks (post-flush instrumentation
	// uses this to validate sd.Flush actually wrote the expected count of
	// history records into MDBX).
	var probeTotalTxnsApplied int

	for blockN := fromBlock; blockN <= toBlock; blockN++ {
		firstTxN, terr := txNumsReader.Min(ctx, tx, blockN)
		if terr != nil {
			return fmt.Errorf("PopulateHistoryByReExec(blockN=%d): TxNums.Min: %w", blockN, terr)
		}

		block, _, berr := p.BlockReader.BlockWithSenders(ctx, tx, common.Hash{}, blockN)
		if berr != nil {
			return fmt.Errorf("PopulateHistoryByReExec(blockN=%d): BlockWithSenders: %w", blockN, berr)
		}
		if block == nil {
			return fmt.Errorf("PopulateHistoryByReExec(blockN=%d): block not found", blockN)
		}
		header := block.HeaderNoCopy()
		txs := block.Transactions()

		// startTxIdx — user-tx index to begin EVM apply at.
		//
		// txN layout in a block:
		//   firstTxN              = sys-start txN
		//   firstTxN+1+i (i≥0)    = user-tx i's txN
		//   firstTxN+1+lastUserI+1= sys-end txN
		//
		// For blockN == fromBlock with mid-block cs.txN:
		//   already-executed txN count = fromTxN - firstTxN + 1
		//   of those, 1 is sys-start, the rest are user-txns 0..(k-1)
		//   → startTxIdx = fromTxN - firstTxN. Clamp to 0.
		startTxIdx := 0
		if blockN == fromBlock {
			delta := int64(fromTxN) - int64(firstTxN)
			if delta > 0 {
				startTxIdx = int(delta)
			}
		}

		// State reader. For the first (partial) block use the file-side
		// reader at cs's step — SD's read view would see writes from
		// forward exec past snapshot tip that aren't part of the canonical
		// post-fromTxN state. For subsequent blocks SD's view IS canonical
		// (it includes the writes this loop just landed for prior blocks).
		var reader state.StateReader
		if blockN == fromBlock {
			reader = newFileSideStateReader(tx, maxStep)
		} else {
			reader = state.NewReaderV3(sd.AsGetter(tx))
		}
		ibs := state.New(reader)
		writer := state.NewWriter(sd.AsPutDel(tx), nil, firstTxN)

		// PROBE-PARALLEL-READ: at the very start of re-exec for fromBlock,
		// compare what my file-side reader returns vs what state.NewHistoryReaderV3
		// returns for a few well-known addresses. Any divergence is a smell.
		if p.logger != nil && blockN == fromBlock {
			postSysStartTxN := firstTxN + 1
			resumeTxN := fromTxN + 1
			knownAddrs := []common.Address{
				common.HexToAddress("0x0000bbddc7ce488642fb579f8b00f3a590007251"), // Consolidation
				common.HexToAddress("0x00000961ef480eb55e80d19ad83579a64c007002"), // Withdrawal request
				common.HexToAddress("0x0000f90827f1c53a10cb7a02335b175320002935"), // History storage (EIP-2935)
				common.HexToAddress("0x000f3df6d732807ef1319fb7b8bb8522d0beac02"), // Beacon root (EIP-4788)
				header.Coinbase, // Block's coinbase
			}
			hrPost := state.NewHistoryReaderV3(tx, postSysStartTxN)
			hrResume := state.NewHistoryReaderV3(tx, resumeTxN)
			for _, a := range knownAddrs {
				fileAcc, fileErr := reader.ReadAccountData(accounts.InternAddress(a))
				hrPostAcc, hrPostErr := hrPost.ReadAccountData(accounts.InternAddress(a))
				hrResumeAcc, hrResumeErr := hrResume.ReadAccountData(accounts.InternAddress(a))
				p.logger.Info("[PROBE-PARALLEL-READ] start-of-re-exec addr read",
					"addr", a.Hex(),
					"file.balance", balOrNil(fileAcc), "file.nonce", nonceOrNil(fileAcc), "file.err", fileErr,
					"hrPost@firstTxN+1.balance", balOrNil(hrPostAcc), "hrPost.nonce", nonceOrNil(hrPostAcc), "hrPost.err", hrPostErr,
					"hrResume@fromTxN+1.balance", balOrNil(hrResumeAcc), "hrResume.nonce", nonceOrNil(hrResumeAcc), "hrResume.err", hrResumeErr,
				)
			}
		}

		// sys-start: skip for fromBlock (cs.trieState already captures it +
		// the executed user txns). For all subsequent blocks, run normally.
		if blockN > fromBlock {
			writer.SetTxNum(firstTxN)
			if err := protocol.InitializeBlockExecution(engineFull, consensusReader, header, p.ChainConfig, ibs, writer, p.logger, nil); err != nil {
				return fmt.Errorf("PopulateHistoryByReExec(blockN=%d): InitializeBlockExecution: %w", blockN, err)
			}
		}

		// gasPool reconstruction. Full block gas limit by default; when
		// resuming mid-block, subtract the canonical cumulative gas used by
		// the txns we're skipping (read from snapshot receipts).
		gasPool := new(protocol.GasPool).
			AddGas(header.GasLimit).
			AddBlobGas(p.ChainConfig.GetMaxBlobGasPerBlock(header.Time))
		if startTxIdx > 0 {
			// Subtract gas used by the txns that already ran before
			// cs.txN. We use receipts snapshotted by the caller BEFORE
			// the writable-shadow wipe; the wipe whole-step clears the
			// RCache domain at stepContaining (it spans block 2,890,316's
			// receipts), so reading via rawdb.ReadReceiptsCacheV2 here
			// would return zero entries.
			receipts, ok := preWipeReceipts[blockN]
			if !ok {
				return fmt.Errorf("PopulateHistoryByReExec(blockN=%d): preWipeReceipts missing entry — caller must snapshot receipts before wipe for partial-block resume", blockN)
			}
			if startTxIdx-1 >= len(receipts) {
				return fmt.Errorf("PopulateHistoryByReExec(blockN=%d): startTxIdx=%d but only %d receipts available in preWipeReceipts", blockN, startTxIdx, len(receipts))
			}
			gasPool.SubGas(receipts[startTxIdx-1].CumulativeGasUsed)
		}

		hashFn := protocol.GetHashFn(header, func(hash common.Hash, n uint64) (*types.Header, error) {
			return p.BlockReader.Header(ctx, tx, hash, n)
		})

		// PROBE-BAL: log a few well-known address balances pre-tx-7 (= via IBS
		// which lazily reads via file-side). Compare to canonical/snapshot-tip
		// values to spot pre-EVM divergence.
		consAddr := accounts.InternAddress(common.HexToAddress("0x0000bbddc7ce488642fb579f8b00f3a590007251"))
		withAddr := accounts.InternAddress(common.HexToAddress("0x00000961ef480eb55e80d19ad83579a64c007002"))
		if p.logger != nil && blockN == fromBlock {
			cb, _ := ibs.GetBalance(consAddr)
			wb, _ := ibs.GetBalance(withAddr)
			p.logger.Info("[PROBE-BAL] pre-tx-7 via IBS",
				"consolidation.bal", cb.String(), "withdrawal.bal", wb.String())
		}

		// Per-tx apply.
		gasUsed := &protocol.GasUsed{}
		receipts := make(types.Receipts, 0, len(txs)-startTxIdx)
		for txIdx := startTxIdx; txIdx < len(txs); txIdx++ {
			txn := txs[txIdx]
			txnNum := firstTxN + 1 + uint64(txIdx)
			writer.SetTxNum(txnNum)
			ibs.SetTxContext(blockN, txIdx)
			receipt, aerr := protocol.ApplyTransaction(p.ChainConfig, hashFn, engineFull, accounts.InternAddress(header.Coinbase), gasPool, ibs, writer, header, txn, gasUsed, vm.Config{})
			if aerr != nil {
				return fmt.Errorf("PopulateHistoryByReExec(blockN=%d, txIdx=%d): ApplyTransaction: %w", blockN, txIdx, aerr)
			}
			receipts = append(receipts, receipt)
			probeTotalTxnsApplied++
		}

		// PROBE-BAL: post-tx-43, pre-Finalize.
		if p.logger != nil && blockN == fromBlock {
			cb, _ := ibs.GetBalance(consAddr)
			wb, _ := ibs.GetBalance(withAddr)
			p.logger.Info("[PROBE-BAL] post-last-tx pre-Finalize via IBS",
				"consolidation.bal", cb.String(), "withdrawal.bal", wb.String())
		}

		// sys-end (block finalize). Writes at the sys-end txN.
		lastTxN := firstTxN + uint64(len(txs)) + 1
		writer.SetTxNum(lastTxN)
		if _, _, ferr := protocol.FinalizeBlockExecution(engineFull, reader, header, txs, block.Uncles(), writer, p.ChainConfig, ibs, receipts, block.Withdrawals(), consensusReader, false /*isMining*/, p.logger, nil); ferr != nil {
			return fmt.Errorf("PopulateHistoryByReExec(blockN=%d): FinalizeBlockExecution: %w", blockN, ferr)
		}

		// PROBE-BAL: post-Finalize, pre-Flush.
		if p.logger != nil && blockN == fromBlock {
			cb, _ := ibs.GetBalance(consAddr)
			wb, _ := ibs.GetBalance(withAddr)
			p.logger.Info("[PROBE-BAL] post-Finalize pre-Flush via IBS",
				"consolidation.bal", cb.String(), "withdrawal.bal", wb.String())
		}

		if p.logger != nil {
			p.logger.Info("[PROBE-REEXEC] block re-exec complete",
				"blockN", blockN, "startTxIdx", startTxIdx, "txsApplied", len(txs)-startTxIdx,
				"firstTxN", firstTxN, "lastTxN", lastTxN,
				"gasUsed.Block", gasUsed.BlockRegular, "headerGasUsed", header.GasUsed,
				"receiptsCount", len(receipts))
		}
	}

	// Flush SD's memctx so all per-tx writes hit MDBX. Without this the
	// retry recompute's HistoryKeyTxNumRange would still see the empty
	// range and ErrHistoryGap would fire again.
	if err := sd.Flush(ctx, tx); err != nil {
		return fmt.Errorf("PopulateHistoryByReExec: sd.Flush: %w", err)
	}

	// PROBE: post-Flush, count history records in the re-exec range across
	// all 3 history-tracked domains. Validates that sd.Flush actually wrote
	// to MDBX (HistoryKeyTxNumRange reads via cursor from the temporal db).
	if p.logger != nil {
		probeRangeFromTxN := fromTxN + 1
		probeRangeToTxNExcl, terr := txNumsReader.Max(ctx, tx, toBlock)
		if terr != nil {
			p.logger.Warn("[PROBE-REEXEC] post-flush: failed to look up toBlock's lastTxN for probe range", "err", terr)
		} else {
			probeRangeToTxNExcl++ // make exclusive
			for _, d := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
				count := 0
				it, ierr := tx.Debug().HistoryKeyTxNumRange(d, int(probeRangeFromTxN), int(probeRangeToTxNExcl), order.Asc, -1)
				if ierr != nil {
					p.logger.Warn("[PROBE-REEXEC] post-flush HistoryKeyTxNumRange err", "domain", d, "err", ierr)
					continue
				}
				for it.HasNext() {
					_, _, nerr := it.Next()
					if nerr != nil {
						break
					}
					count++
				}
				it.Close()
				p.logger.Info("[PROBE-REEXEC] post-flush history record count",
					"domain", d, "fromTxN", probeRangeFromTxN, "toTxNExcl", probeRangeToTxNExcl,
					"count", count, "txnsApplied", probeTotalTxnsApplied)
			}
		}
	}

	return nil
}
