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
package l1

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/types"
)

var (
	// ErrSourceIncomplete reports a response the source assembled from
	// different forks of one block; a retry can succeed.
	ErrSourceIncomplete = errors.New("L1 source answered from mixed forks")
	// ErrUndecodable reports a posting transaction that settled the rollup
	// but whose input is not a batch.
	ErrUndecodable = errors.New("settling transaction is not a decodable batch")
)

// LogChunk is the widest range the scanner asks for at once.
const LogChunk = 100_000

// ScannedBatch is a posted batch and what L1 applied of it for the rollup.
type ScannedBatch struct {
	L1Block     uint64
	L1BlockHash common.Hash
	TxHash      common.Hash
	TxIndex     uint64
	// Batch is the posted batch; its CallData carries the DA payload.
	Batch      *protocol.ProofSystemBatch
	Settlement protocol.Settlement
}

// Scanner reads the batches a registry settled for one rollup.
type Scanner struct {
	reader   ChainReader
	registry common.Address
	rollupID uint64
	chunk    uint64
}

// NewScanner returns a scanner of the registry's settlements of rollupID.
func NewScanner(reader ChainReader, registry common.Address, rollupID uint64) *Scanner {
	return &Scanner{reader: reader, registry: registry, rollupID: rollupID, chunk: LogChunk}
}

// SetChunk sets the widest range the scanner asks for at once.
func (s *Scanner) SetChunk(n uint64) { s.chunk = max(n, 1) }

var registryTopics = [][]common.Hash{{
	protocol.BatchPostedTopic,
	protocol.L2ExecutionPerformedTopic,
	protocol.ExecutionConsumedTopic,
	protocol.L2TxSkippedTopic,
}}

// Scan returns the batches of [from, to] that settled something for the
// rollup, in L1 order, and the last block it read. That block is below to
// when the source refused the range and the scanner narrowed it; the caller
// continues from the block after it.
func (s *Scanner) Scan(ctx context.Context, from, to uint64) ([]ScannedBatch, uint64, error) {
	hi := min(to, from+s.chunk-1)
	for {
		batches, err := s.scanRange(ctx, from, hi)
		if errors.Is(err, ErrRangeTooWide) && hi > from {
			hi = from + (hi-from)/2
			continue
		}
		return batches, hi, err
	}
}

type blockKey struct {
	number uint64
	hash   common.Hash
}

func (s *Scanner) scanRange(ctx context.Context, from, to uint64) ([]ScannedBatch, error) {
	logs, err := s.reader.Logs(ctx, from, to, s.registry, registryTopics)
	if err != nil {
		return nil, err
	}
	// One query per range: separate queries can answer from different forks.
	forks := map[uint64]common.Hash{}
	byBlock := map[blockKey][]*types.Log{}
	var blocks []blockKey
	for _, l := range logs {
		n := uint64(l.BlockNumber)
		if h, ok := forks[n]; ok && h != l.BlockHash {
			return nil, fmt.Errorf("%w: block %d as %x and %x", ErrSourceIncomplete, n, h, l.BlockHash)
		}
		forks[n] = l.BlockHash
		k := blockKey{n, l.BlockHash}
		if byBlock[k] == nil {
			blocks = append(blocks, k)
		}
		byBlock[k] = append(byBlock[k], l)
	}
	slices.SortFunc(blocks, func(a, b blockKey) int { return cmp.Compare(a.number, b.number) })

	var out []ScannedBatch
	for _, k := range blocks {
		scanned, err := s.scanBlock(ctx, k, byBlock[k])
		if err != nil {
			return nil, err
		}
		out = append(out, scanned...)
	}
	return out, nil
}

func (s *Scanner) scanBlock(ctx context.Context, k blockKey, logs []*types.Log) ([]ScannedBatch, error) {
	slices.SortFunc(logs, func(a, b *types.Log) int { return cmp.Compare(a.Index, b.Index) })
	settledBy := map[common.Hash]bool{}
	for _, l := range logs {
		if l.Topics[0] != protocol.L2ExecutionPerformedTopic {
			continue
		}
		e, err := protocol.ParseL2ExecutionPerformed(l)
		if err != nil {
			return nil, err
		}
		if e.RollupID == s.rollupID {
			settledBy[l.TxHash] = true
		}
	}

	var posted []protocol.PostedBatch
	var txHashes []common.Hash
	for _, l := range logs {
		if l.Topics[0] != protocol.BatchPostedTopic {
			continue
		}
		tx, err := s.reader.TransactionByBlockHashAndIndex(ctx, k.hash, uint64(l.TxIndex))
		if err != nil {
			return nil, err
		}
		if tx.Hash() != l.TxHash {
			return nil, fmt.Errorf("%w: transaction %d of block %d hashes to %x, its log names %x", ErrInconsistent, l.TxIndex, k.number, tx.Hash(), l.TxHash)
		}
		// A peer may post through a contract; its input is only ours to read
		// when it settled our rollup.
		batch, err := protocol.DecodePostAndVerifyBatch(tx.GetData())
		if err != nil {
			if settledBy[l.TxHash] {
				return nil, fmt.Errorf("%w: transaction %x of block %d: %w", ErrUndecodable, l.TxHash, k.number, err)
			}
			continue
		}
		posted = append(posted, protocol.PostedBatch{Batch: batch, TxIndex: uint64(l.TxIndex), LogIndex: uint64(l.Index)})
		txHashes = append(txHashes, l.TxHash)
	}

	settlements, err := protocol.AttributeBlock(posted, logs, s.rollupID)
	if err != nil {
		return nil, fmt.Errorf("block %d (%x): %w", k.number, k.hash, err)
	}
	var out []ScannedBatch
	for i, p := range posted {
		if len(settlements[i].Applied) == 0 {
			continue
		}
		out = append(out, ScannedBatch{
			L1Block:     k.number,
			L1BlockHash: k.hash,
			TxHash:      txHashes[i],
			TxIndex:     p.TxIndex,
			Batch:       p.Batch,
			Settlement:  settlements[i],
		})
	}
	return out, nil
}
