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

// Package tools implements the eez command: batch decoding, chain diffs and
// L1 captures.
package tools

import (
	"context"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/eez/protocol"
)

// Capture reads L1 blocks from through to: their headers, the registry logs
// a follower reads and the transactions that posted batches. The result
// writes with recording.Write.
func Capture(ctx context.Context, reader l1.ChainReader, registry common.Address, from, to uint64) (*recording.MemoryChain, error) {
	if from > to {
		return nil, fmt.Errorf("empty range [%d, %d]", from, to)
	}
	id, err := reader.ChainID(ctx)
	if err != nil {
		return nil, err
	}
	blocks := make([]*recording.Block, 0, to-from+1)
	byHash := map[common.Hash]*recording.Block{}
	for n := from; n <= to; n++ {
		h, err := reader.HeaderByNumber(ctx, n)
		if err != nil {
			return nil, fmt.Errorf("header %d: %w", n, err)
		}
		if len(blocks) > 0 && h.ParentHash != blocks[len(blocks)-1].Header.Hash {
			return nil, fmt.Errorf("%w: block %d does not extend the block before it", l1.ErrSourceIncomplete, n)
		}
		b := &recording.Block{Header: *h, Recorded: true}
		blocks = append(blocks, b)
		byHash[h.Hash] = b
	}

	chunk := uint64(l1.LogChunk)
	for lo := from; lo <= to; {
		hi := min(to, lo+chunk-1)
		logs, err := reader.Logs(ctx, lo, hi, registry, l1.RegistryTopics)
		if errors.Is(err, l1.ErrRangeTooWide) && hi > lo {
			chunk = max(1, chunk/2)
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, l := range logs {
			b := byHash[l.BlockHash]
			if b == nil {
				return nil, fmt.Errorf("%w: log of block %d (%x) off the captured chain", l1.ErrSourceIncomplete, l.BlockNumber, l.BlockHash)
			}
			b.Logs = append(b.Logs, l)
			if l.Topics[0] != protocol.BatchPostedTopic {
				continue
			}
			for uint64(len(b.Txs)) <= uint64(l.TxIndex) {
				b.Txs = append(b.Txs, nil)
			}
			if b.Txs[l.TxIndex] != nil {
				continue
			}
			tx, err := reader.TransactionByBlockHashAndIndex(ctx, l.BlockHash, uint64(l.TxIndex))
			if err != nil {
				return nil, fmt.Errorf("transaction %d of block %d: %w", l.TxIndex, l.BlockNumber, err)
			}
			b.Txs[l.TxIndex] = tx
		}
		lo = hi + 1
	}

	c, err := recording.NewMemoryChain(id, blocks[0])
	if err != nil {
		return nil, err
	}
	for _, b := range blocks[1:] {
		if err := c.Append(b); err != nil {
			return nil, err
		}
	}
	f, err := reader.Finalized(ctx)
	switch {
	case errors.Is(err, l1.ErrNotFound):
	case err != nil:
		return nil, err
	case f.Number >= from:
		// Finality the capture holds, up to its last block.
		if err := c.Finalize(min(f.Number, to)); err != nil {
			return nil, err
		}
	}
	return c, nil
}
