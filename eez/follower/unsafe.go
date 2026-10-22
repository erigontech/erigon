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

package follower

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

// ErrConflictsSafe reports a sequencer chain that does not descend from the
// safe block.
var ErrConflictsSafe = errors.New("sequencer chain does not descend from the safe block")

// Sequencer serves the sequencer's chain.
type Sequencer interface {
	// Head returns the number and hash of the sequencer's latest block.
	Head(ctx context.Context) (uint64, common.Hash, error)
	// BlockByNumber returns the sequencer's block at number.
	BlockByNumber(ctx context.Context, number uint64) (*types.Block, error)
}

// RPCSequencer is a Sequencer over JSON-RPC.
type RPCSequencer struct{ client *rpc.Client }

// DialSequencer connects to the sequencer's JSON-RPC endpoint at url, which
// must serve debug_getRawBlock.
func DialSequencer(ctx context.Context, url string, logger log.Logger) (*RPCSequencer, error) {
	c, err := rpc.DialContext(ctx, url, logger)
	if err != nil {
		return nil, err
	}
	return &RPCSequencer{client: c}, nil
}

// Close releases the connection.
func (s *RPCSequencer) Close() { s.client.Close() }

func (s *RPCSequencer) Head(ctx context.Context) (uint64, common.Hash, error) {
	var h *struct {
		Number hexutil.Uint64 `json:"number"`
		Hash   common.Hash    `json:"hash"`
	}
	if err := s.client.CallContext(ctx, &h, "eth_getBlockByNumber", "latest", false); err != nil {
		return 0, common.Hash{}, err
	}
	if h == nil {
		return 0, common.Hash{}, errors.New("sequencer serves no latest block")
	}
	return uint64(h.Number), h.Hash, nil
}

func (s *RPCSequencer) BlockByNumber(ctx context.Context, number uint64) (*types.Block, error) {
	var raw hexutil.Bytes
	if err := s.client.CallContext(ctx, &raw, "debug_getRawBlock", hexutil.Uint64(number)); err != nil {
		return nil, err
	}
	b := new(types.Block)
	if err := rlp.DecodeBytes(raw, b); err != nil {
		return nil, fmt.Errorf("sequencer block %d: %w", number, err)
	}
	return b, nil
}

// overlay makes the sequencer's latest block the local head while the
// sequencer's chain descends from the safe block. Safe and finalized stay
// derivation's.
type overlay struct {
	seq     Sequencer
	chain   deriver.Chain
	deriver *deriver.Deriver
	// limit bounds the blocks one step fetches.
	limit  uint64
	last   common.Hash
	logger log.Logger
}

// step moves the head to the sequencer's latest block, or by at most limit
// blocks toward it.
func (o *overlay) step(ctx context.Context) error {
	number, hash, err := o.seq.Head(ctx)
	if err != nil || hash == o.last {
		return err
	}
	safe := o.deriver.Safe()
	head, err := o.chain.Head(ctx)
	if err != nil {
		return err
	}
	if number <= head.Number.Uint64() {
		if local, ok, err := o.chain.CanonicalHash(ctx, number); err != nil || (ok && local == hash) {
			o.last = hash
			return err
		}
	}
	target := min(number, max(head.Number.Uint64(), safe)+o.limit)
	blocks, err := o.fetch(ctx, target, safe)
	if errors.Is(err, ErrConflictsSafe) {
		o.logger.Warn("[eez] sequencer head conflicts with the safe block", "event", "eez.node.follower.head.conflicts_safe", "number", number, "hash", hash, "safe", safe)
		o.last = hash
		return nil
	}
	if err != nil || len(blocks) == 0 {
		return err
	}
	if err := o.chain.Insert(ctx, blocks); err != nil {
		return err
	}
	tip := blocks[len(blocks)-1]
	if err := o.deriver.ForkChoiceTo(ctx, tip.Hash()); err != nil {
		return fmt.Errorf("sequencer block %d (%x): %w", tip.NumberU64(), tip.Hash(), err)
	}
	if tip.Hash() == hash {
		o.last = hash
	}
	o.logger.Debug("[eez] head follows the sequencer", "event", "eez.node.follower.head.advanced", "number", tip.NumberU64(), "hash", tip.Hash())
	return nil
}

// fetch returns the sequencer's blocks up to target that the local chain
// lacks, oldest first: back to the first one whose parent is a local
// canonical block, which must be the safe block or above it.
func (o *overlay) fetch(ctx context.Context, target, safe uint64) ([]*types.Block, error) {
	if target <= safe {
		return nil, ErrConflictsSafe
	}
	var blocks []*types.Block
	var want common.Hash
	for n := target; ; n-- {
		b, err := o.seq.BlockByNumber(ctx, n)
		if err != nil {
			return nil, err
		}
		if want != (common.Hash{}) && b.Hash() != want {
			return nil, fmt.Errorf("sequencer chain moved while read at block %d", n)
		}
		blocks = append(blocks, b)
		local, ok, err := o.chain.CanonicalHash(ctx, n-1)
		if err != nil {
			return nil, err
		}
		if ok && local == b.ParentHash() {
			slices.Reverse(blocks)
			return blocks, nil
		}
		// Below here the local canonical chain is settled.
		if n-1 <= safe {
			return nil, ErrConflictsSafe
		}
		want = b.ParentHash()
	}
}
