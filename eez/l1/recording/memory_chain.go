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

// Package recording holds L1 chains in memory and their recordings on disk.
package recording

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/execution/types"
)

// Block is an L1 block as the follower sees it.
type Block struct {
	Header l1.Header
	Txs    []types.Transaction
	// Logs are the block's logs in emission order, each with its TxIndex;
	// the chain fills in the other position fields.
	Logs []*types.Log
}

// MemoryChain is an l1.ChainReader over blocks held in memory. It backs recordings
// and stands in for an L1 node in tests.
type MemoryChain struct {
	mu        sync.RWMutex
	chainID   uint64
	blocks    map[common.Hash]*Block
	canonical []common.Hash
	finalized *uint64
	// maxLogRange makes Logs refuse ranges of more blocks; zero accepts any.
	maxLogRange uint64
}

var _ l1.ChainReader = (*MemoryChain)(nil)

// NewMemoryChain returns a chain holding genesis, whose number must be zero.
func NewMemoryChain(chainID uint64, genesis *Block) (*MemoryChain, error) {
	if genesis.Header.Number != 0 {
		return nil, fmt.Errorf("genesis at number %d", genesis.Header.Number)
	}
	c := &MemoryChain{chainID: chainID, blocks: map[common.Hash]*Block{}}
	c.canonical = []common.Hash{c.store(genesis)}
	return c, nil
}

// SetMaxLogRange makes Logs refuse ranges spanning more than n blocks with
// ErrRangeTooWide; zero accepts any range.
func (c *MemoryChain) SetMaxLogRange(n uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxLogRange = n
}

func (c *MemoryChain) store(b *Block) common.Hash {
	stored := &Block{Header: b.Header, Txs: b.Txs, Logs: make([]*types.Log, len(b.Logs))}
	for i, l := range b.Logs {
		cp := *l
		cp.BlockNumber = hexutil.Uint64(b.Header.Number)
		cp.BlockHash = b.Header.Hash
		cp.Index = hexutil.Uint(i)
		if int(cp.TxIndex) < len(b.Txs) {
			cp.TxHash = b.Txs[cp.TxIndex].Hash()
		}
		stored.Logs[i] = &cp
	}
	c.blocks[b.Header.Hash] = stored
	return b.Header.Hash
}

// Append extends the canonical chain by b.
func (c *MemoryChain) Append(b *Block) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.extend(b)
}

func (c *MemoryChain) extend(b *Block) error {
	head := c.blocks[c.canonical[len(c.canonical)-1]].Header
	if b.Header.Number != head.Number+1 || b.Header.ParentHash != head.Hash {
		return fmt.Errorf("block %d (%x) does not extend head %d (%x)", b.Header.Number, b.Header.Hash, head.Number, head.Hash)
	}
	c.canonical = append(c.canonical, c.store(b))
	return nil
}

// Reorg replaces the canonical blocks above ancestor with branch. The
// replaced blocks stay readable by hash.
func (c *MemoryChain) Reorg(ancestor uint64, branch ...*Block) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ancestor >= uint64(len(c.canonical)) {
		return fmt.Errorf("ancestor %d above head %d", ancestor, len(c.canonical)-1)
	}
	if c.finalized != nil && ancestor < *c.finalized {
		return fmt.Errorf("reorg below finalized block %d", *c.finalized)
	}
	c.canonical = c.canonical[:ancestor+1]
	for _, b := range branch {
		if err := c.extend(b); err != nil {
			return err
		}
	}
	return nil
}

// Finalize marks the canonical block at number finalized.
func (c *MemoryChain) Finalize(number uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if number >= uint64(len(c.canonical)) {
		return fmt.Errorf("finalized %d above head %d", number, len(c.canonical)-1)
	}
	c.finalized = &number
	return nil
}

// Canonical returns the canonical blocks from genesis to the head.
func (c *MemoryChain) Canonical() []*Block {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*Block, len(c.canonical))
	for i, h := range c.canonical {
		out[i] = c.blocks[h]
	}
	return out
}

// Orphans returns the blocks held but not canonical.
func (c *MemoryChain) Orphans() []*Block {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []*Block
	for h, b := range c.blocks {
		if b.Header.Number >= uint64(len(c.canonical)) || c.canonical[b.Header.Number] != h {
			out = append(out, b)
		}
	}
	return out
}

func (c *MemoryChain) ChainID(context.Context) (uint64, error) { return c.chainID, nil }

func (c *MemoryChain) HeaderByNumber(_ context.Context, number uint64) (*l1.Header, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if number >= uint64(len(c.canonical)) {
		return nil, fmt.Errorf("%w: block %d", l1.ErrNotFound, number)
	}
	h := c.blocks[c.canonical[number]].Header
	return &h, nil
}

func (c *MemoryChain) HeaderByHash(_ context.Context, hash common.Hash) (*l1.Header, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	b, ok := c.blocks[hash]
	if !ok {
		return nil, fmt.Errorf("%w: block %x", l1.ErrNotFound, hash)
	}
	h := b.Header
	return &h, nil
}

func (c *MemoryChain) Head(ctx context.Context) (*l1.Header, error) {
	c.mu.RLock()
	n := uint64(len(c.canonical) - 1)
	c.mu.RUnlock()
	return c.HeaderByNumber(ctx, n)
}

func (c *MemoryChain) Finalized(ctx context.Context) (*l1.Header, error) {
	c.mu.RLock()
	f := c.finalized
	c.mu.RUnlock()
	if f == nil {
		return nil, fmt.Errorf("%w: no finalized block", l1.ErrNotFound)
	}
	return c.HeaderByNumber(ctx, *f)
}

func (c *MemoryChain) Logs(_ context.Context, from, to uint64, address common.Address, topics [][]common.Hash) ([]*types.Log, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if from > to || to >= uint64(len(c.canonical)) {
		return nil, fmt.Errorf("%w: logs of [%d, %d] with head %d", l1.ErrNotFound, from, to, len(c.canonical)-1)
	}
	if c.maxLogRange > 0 && to-from+1 > c.maxLogRange {
		return nil, fmt.Errorf("%w: %d blocks, at most %d", l1.ErrRangeTooWide, to-from+1, c.maxLogRange)
	}
	var out []*types.Log
	for n := from; n <= to; n++ {
		for _, l := range c.blocks[c.canonical[n]].Logs {
			if matchLog(l, address, topics) {
				cp := *l
				out = append(out, &cp)
			}
		}
	}
	return out, nil
}

func (c *MemoryChain) TransactionByBlockHashAndIndex(_ context.Context, blockHash common.Hash, index uint64) (types.Transaction, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	b, ok := c.blocks[blockHash]
	if !ok || index >= uint64(len(b.Txs)) {
		return nil, fmt.Errorf("%w: transaction %d of block %x", l1.ErrNotFound, index, blockHash)
	}
	return b.Txs[index], nil
}

// matchLog reports whether l passes an address and topic filter.
func matchLog(l *types.Log, address common.Address, topics [][]common.Hash) bool {
	if l.Address != address || len(topics) > len(l.Topics) {
		return false
	}
	for i, accepted := range topics {
		if len(accepted) > 0 && !slices.Contains(accepted, l.Topics[i]) {
			return false
		}
	}
	return true
}
