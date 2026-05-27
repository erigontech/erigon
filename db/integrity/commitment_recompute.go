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

package integrity

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// RecomputeCommitmentAtBlock recomputes the patricia trie root + the
// encoded trie state at blockNum without re-executing transactions. It
// is the shared primitive used by:
//
//   - fork-from CLI when the cut block isn't on an existing snapshot
//     file boundary — the fork needs a commitment entry at the cut so
//     its post-cut producer has a valid anchor. The fork CLI passes
//     the parent's temporal tx (read-only) and routes the result into
//     the fork's commitment file via WriteCommitmentEntryAtBlock on a
//     fork-specific writer.
//
//   - any future SetHead variant that needs to anchor at a non-boundary
//     block. The aligned-mode SetHead path doesn't need this today
//     (its toBlock is always on a step boundary by construction); the
//     primitive exists so a non-aligned-mode SetHead could share the
//     algorithm.
//
// The algorithm:
//  1. Open a SharedDomains against the supplied read-only tx.
//  2. Position the patricia trie at the latest commitment ≤ blockNum
//     via SeekCommitment.
//  3. Replay the historical change set for blockNum (the gap between
//     the latest commitment and blockNum's end-txNum) by touching every
//     account/storage/code key the changeset records. The key set
//     comes from tx.Debug().HistoryKeyTxNumRange — no pre-built index
//     needed.
//  4. ComputeCommitment with saveStateAfter=false (read-only — the
//     supplied tx is never mutated) to get the root.
//  5. Validate the computed root against the block header's stateRoot.
//     A mismatch means the snapshot/history data the recompute reads
//     doesn't reproduce the consensus-anchored state — refuse loudly.
//  6. Extract the encoded trie state from the in-memory patricia
//     instance via EncodeCurrentState.
//
// Returns (encodedTrieState, root, error). The encoded trie state is
// the format WriteCommitmentEntryAtBlock expects as its trieState
// argument — the caller wraps it with (txNum, blockNum) metadata when
// writing.
//
// READ-ONLY against tx. The SharedDomains is closed before return.
func RecomputeCommitmentAtBlock(
	ctx context.Context,
	tx kv.TemporalTx,
	br services.FullBlockReader,
	blockNum uint64,
	logger log.Logger,
) (encodedTrieState []byte, root common.Hash, err error) {
	if tx == nil {
		return nil, common.Hash{}, fmt.Errorf("RecomputeCommitmentAtBlock: nil tx")
	}
	if br == nil {
		return nil, common.Hash{}, fmt.Errorf("RecomputeCommitmentAtBlock: nil BlockReader")
	}

	header, err := br.HeaderByNumber(ctx, tx, blockNum)
	if err != nil {
		return nil, common.Hash{}, fmt.Errorf("header at block %d: %w", blockNum, err)
	}
	if header == nil {
		return nil, common.Hash{}, fmt.Errorf("RecomputeCommitmentAtBlock: no header for block %d", blockNum)
	}

	txNumsReader := br.TxnumReader()
	minTxNum, err := txNumsReader.Min(ctx, tx, blockNum)
	if err != nil {
		return nil, common.Hash{}, fmt.Errorf("minTxNum at block %d: %w", blockNum, err)
	}
	maxTxNum, err := txNumsReader.Max(ctx, tx, blockNum)
	if err != nil {
		return nil, common.Hash{}, fmt.Errorf("maxTxNum at block %d: %w", blockNum, err)
	}
	toTxNum := maxTxNum + 1

	// Refuse if blockNum is past the state coverage in the supplied tx —
	// the recompute would read past the last available history file and
	// produce a root that doesn't match consensus.
	aggTx := state.AggTx(tx)
	if aggMax := aggTx.EndTxNumNoCommitment(); toTxNum > aggMax {
		return nil, common.Hash{}, fmt.Errorf("block %d is beyond state coverage (maxTxNum+1=%d, aggMax=%d) — recompute needs history through block end", blockNum, toTxNum, aggMax)
	}

	sd, err := execctx.NewSharedDomains(ctx, tx, logger)
	if err != nil {
		return nil, common.Hash{}, fmt.Errorf("open SharedDomains: %w", err)
	}
	defer sd.Close()
	sd.ClearRam(true)

	// commitment branch view: as-of beginning of the block (so we replay
	// THIS block's touches against the prior block's commitment state).
	// For blockNum==0 there is no prior commitment state — fall back to
	// commitmentAsOf == toTxNum so the trie restores from the end-state
	// (consistent with checkCommitmentHistAtBlkWithIdx in this package).
	commitmentAsOf := minTxNum
	if blockNum == 0 {
		commitmentAsOf = toTxNum
	}
	sd.GetCommitmentCtx().SetStateReader(commitmentdb.NewSplitHistoryReader(tx, commitmentAsOf, toTxNum, true /* withHistory */))
	sd.GetCommitmentContext().SetDeferBranchUpdates(false)

	latestTxNum, latestBlockNum, err := sd.SeekCommitment(ctx, tx)
	if err != nil {
		return nil, common.Hash{}, fmt.Errorf("SeekCommitment: %w", err)
	}
	if latestBlockNum > blockNum {
		return nil, common.Hash{}, fmt.Errorf("commitment state already past target: latest block=%d > target=%d", latestBlockNum, blockNum)
	}

	// Touch every key changed in [minTxNum, toTxNum) per the historical
	// changeset. nil idx triggers the HistoryKeyTxNumRange fallback
	// inside touchHistoricalKeys, so we don't need to pre-build a
	// ChangedKeysPerBlockIdx for a single-block recompute.
	for _, d := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
		if _, err := touchHistoricalKeys(sd, tx, d, minTxNum, toTxNum, blockNum, nil, nil); err != nil {
			return nil, common.Hash{}, fmt.Errorf("touch %s: %w", d, err)
		}
	}

	rootBytes, err := sd.ComputeCommitment(ctx, tx, false /* saveStateAfter — read-only contract */, blockNum, maxTxNum, "commitment-recompute", nil)
	if err != nil {
		return nil, common.Hash{}, fmt.Errorf("ComputeCommitment: %w", err)
	}
	computedRoot := common.BytesToHash(rootBytes)

	// Validation against consensus — the load-bearing check. The
	// recompute is only safe to use as the fork's commitment anchor if
	// it reproduces the parent's consensus-anchored state at the cut.
	if computedRoot != header.Root {
		return nil, common.Hash{}, fmt.Errorf("RecomputeCommitmentAtBlock: recomputed root %x does not match header stateRoot %x at block %d (latestTxNum=%d, maxTxNum=%d, latestBlockNum=%d)",
			computedRoot, header.Root, blockNum, latestTxNum, maxTxNum, latestBlockNum)
	}

	// Extract the encoded trie state. encodeCommitmentState (in
	// commitmentdb) is unexported; replicate its dispatch here so the
	// recompute primitive doesn't need a deeper hook into commitmentdb.
	trie := sd.GetCommitmentCtx().Trie()
	switch t := trie.(type) {
	case *commitment.HexPatriciaHashed:
		encodedTrieState, err = t.EncodeCurrentState(nil)
	case *commitment.ConcurrentPatriciaHashed:
		encodedTrieState, err = t.RootTrie().EncodeCurrentState(nil)
	default:
		return nil, common.Hash{}, fmt.Errorf("RecomputeCommitmentAtBlock: unsupported patricia trie type %T", trie)
	}
	if err != nil {
		return nil, common.Hash{}, fmt.Errorf("EncodeCurrentState: %w", err)
	}

	return encodedTrieState, computedRoot, nil
}
