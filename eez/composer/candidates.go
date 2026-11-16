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

package composer

import (
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/internal/stateless"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
)

// PairEnds returns the lengths of the prefixes of txs that end a
// cross-chain effect: after each user transaction, and after each system
// transaction that consumes its own entry, being followed by another or by
// the end of the block.
func PairEnds(txs types.Transactions) []int {
	var ends []int
	for i, tx := range txs {
		if tx.Type() != protocol.SystemTxType || i+1 == len(txs) || txs[i+1].Type() == protocol.SystemTxType {
			ends = append(ends, i+1)
		}
	}
	return ends
}

// Candidates returns the hashes of the siblings of sync, a Sync block, that
// carry its first n transactions for n 0 and each of ends: the blocks a
// batch's entries settle, from the empty Sync block to the effect of each
// prefix. They are executed on w, the witness of sync, which holds all the
// state a prefix reads.
func Candidates(config *chain.Config, engine rules.Engine, sync *types.Block, w *stateless.Witness, ends []int) ([]common.Hash, error) {
	full, err := stateless.Execute(config, engine, sync, w)
	if err != nil {
		return nil, fmt.Errorf("sync block %d: %w", sync.NumberU64(), err)
	}
	if full.StateRoot != sync.Root() {
		return nil, fmt.Errorf("%w: sync block %d", stateless.ErrStateRoot, sync.NumberU64())
	}
	// A failed effect is no effect a batch could claim.
	for i, r := range full.Receipts {
		if r.Status != types.ReceiptStatusSuccessful {
			return nil, fmt.Errorf("sync block %d transaction %d failed", sync.NumberU64(), i)
		}
	}
	txs := sync.Transactions()
	out := make([]common.Hash, 0, len(ends)+1)
	for _, n := range append([]int{0}, ends...) {
		if n > len(txs) {
			return nil, fmt.Errorf("prefix of %d of %d transactions", n, len(txs))
		}
		receipts := full.Receipts[:n]
		h := types.CopyHeader(sync.Header())
		h.TxHash = types.DeriveSha(txs[:n])
		h.ReceiptHash = types.DeriveSha(receipts)
		h.Bloom = types.CreateBloom(receipts)
		h.GasUsed = 0
		if n > 0 {
			h.GasUsed = receipts[n-1].CumulativeGasUsed
		}
		res, err := stateless.Execute(config, engine, types.NewBlockFromStorage(h.Hash(), h, txs[:n], nil, sync.Withdrawals(), nil), w)
		if err != nil {
			return nil, fmt.Errorf("prefix of %d: %w", n, err)
		}
		// A copy: Hash caches, and the root changes after the first.
		sealed := types.CopyHeader(h)
		sealed.Root = res.StateRoot
		out = append(out, sealed.Hash())
	}
	return out, nil
}
