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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/announce"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/l1"
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
	// Announcement returns the App. D.6 announcement the sequencer serves for
	// its block at number, nil when it serves none.
	Announcement(ctx context.Context, number uint64) ([]byte, error)
}

// RPCSequencer is a Sequencer over JSON-RPC.
type RPCSequencer struct{ client *rpc.Client }

// DialSequencer connects to the sequencer's JSON-RPC endpoint at url, which
// must serve debug_getRawBlock.
func DialSequencer(ctx context.Context, url string, logger log.Logger) (*RPCSequencer, error) {
	c, err := rpc.DialContext(ctx, url, logger)
	if err != nil {
		return nil, l1.RedactTransport(err)
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
		return 0, common.Hash{}, l1.RedactTransport(err)
	}
	if h == nil {
		return 0, common.Hash{}, errors.New("sequencer serves no latest block")
	}
	return uint64(h.Number), h.Hash, nil
}

func (s *RPCSequencer) BlockByNumber(ctx context.Context, number uint64) (*types.Block, error) {
	var raw hexutil.Bytes
	if err := s.client.CallContext(ctx, &raw, "debug_getRawBlock", hexutil.Uint64(number)); err != nil {
		return nil, l1.RedactTransport(err)
	}
	b := new(types.Block)
	if err := rlp.DecodeBytes(raw, b); err != nil {
		return nil, fmt.Errorf("sequencer block %d: %w", number, err)
	}
	return b, nil
}

func (s *RPCSequencer) Announcement(ctx context.Context, number uint64) ([]byte, error) {
	var env *hexutil.Bytes
	if err := s.client.CallContext(ctx, &env, "eez_getUnsafeBlockAnnouncement", hexutil.Uint64(number)); err != nil {
		return nil, l1.RedactTransport(err)
	}
	if env == nil {
		return nil, nil
	}
	return *env, nil
}

// overlay makes the sequencer's latest block the local head while the
// sequencer's chain descends from the safe block. Safe and finalized stay
// derivation's.
type overlay struct {
	seq     Sequencer
	chain   deriver.Chain
	deriver *deriver.Deriver
	// chainID is the chain announcements must name. store, when set, takes
	// only announced blocks and keeps their announcements.
	chainID *uint256.Int
	store   *announce.Store
	// limit bounds the blocks one step fetches, and those whose announcements
	// it checks again.
	limit uint64
	last  common.Hash
	// gap, zero for none, is the lowest block from which the blocks taken may
	// lack their announcement kept.
	gap uint64
	// pruned is the finalized block the store was last pruned at.
	pruned uint64
	logger log.Logger
}

// errUnannounced reports a block the sequencer serves no announcement for.
var errUnannounced = errors.New("sequencer block not announced")

// step moves the head to the sequencer's latest block, or by at most limit
// blocks toward it.
func (o *overlay) step(ctx context.Context) error {
	o.prune(ctx)
	o.reread(ctx)
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
	tip := blocks[len(blocks)-1]
	envs := make([][]byte, len(blocks))
	var producer common.Address
	for i, b := range blocks {
		if o.store == nil {
			break
		}
		if envs[i], err = o.seq.Announcement(ctx, b.NumberU64()); err != nil {
			return err
		}
		if envs[i] == nil {
			// The sequencer may announce it after it became the head, sign
			// it with the blocks that follow if it was left unsigned, or
			// keep it no longer, more than its window below its head: the
			// next step reads it again.
			o.logger.Debug("[eez] sequencer block not announced", "event", "eez.node.follower.head.unannounced", "number", b.NumberU64(), "hash", b.Hash())
			return nil
		}
		if producer, err = announce.Verify(envs[i], o.chainID, b.Hash()); err != nil {
			return o.reject(ctx, number, hash, fmt.Errorf("sequencer block %d (%x): %w", b.NumberU64(), b.Hash(), err))
		}
	}
	if err := o.chain.Insert(ctx, blocks); err != nil {
		return o.reject(ctx, number, hash, err)
	}
	if err := o.deriver.ForkChoiceTo(ctx, tip.Hash()); err != nil {
		return o.reject(ctx, number, hash, fmt.Errorf("sequencer block %d (%x): %w", tip.NumberU64(), tip.Hash(), err))
	}
	if tip.Hash() == hash {
		o.last = hash
	}
	o.logger.Debug("[eez] head follows the sequencer", "event", "eez.node.follower.head.advanced", "number", tip.NumberU64(), "hash", tip.Hash(),
		"producer", producer)
	o.keep(ctx, blocks, envs)
	return nil
}

// prune drops the announcements L1 finality covers, once per finalized
// block.
func (o *overlay) prune(ctx context.Context) {
	finalized := o.deriver.Finalized()
	if o.store == nil || finalized == o.pruned {
		return
	}
	if err := o.store.Prune(ctx, finalized); err != nil {
		if ctx.Err() == nil {
			o.logger.Warn("[eez] finalized announcements not pruned", "event", "eez.node.follower.announcement.prune_failed", "err", err)
		}
		return
	}
	o.pruned = finalized
}

// keep stores envs, the announcements of blocks, now taken, for the node to
// serve.
func (o *overlay) keep(ctx context.Context, blocks []*types.Block, envs [][]byte) {
	if o.store == nil {
		return
	}
	for i, b := range blocks {
		if err := o.store.Put(ctx, b.NumberU64(), b.Hash(), envs[i]); err != nil {
			o.logger.Warn("[eez] sequencer announcement not kept", "event", "eez.node.follower.announcement.keep_failed", "number", b.NumberU64(), "err", err)
			o.missing(b.NumberU64())
			return
		}
	}
}

// missing records block number as taken without its announcement kept.
func (o *overlay) missing(number uint64) {
	if o.gap == 0 || number < o.gap {
		o.gap = number
	}
}

// reread reads again, from gap up, the announcements of the blocks taken
// without one kept while L1 has not settled them, within the store's window
// of the head, checking limit blocks a step, those kept included. It keeps
// those served valid and stops at the first it cannot keep, one the
// sequencer no longer serves included, until L1 settles it.
func (o *overlay) reread(ctx context.Context) {
	if o.gap == 0 {
		return
	}
	head, err := o.chain.Head(ctx)
	if err != nil {
		if ctx.Err() == nil {
			o.logger.Debug("[eez] sequencer announcement still not kept", "event", "eez.node.follower.announcement.missing", "number", o.gap, "err", err)
		}
		return
	}
	// A put below the floor would drop the announcements of the latest blocks.
	n := max(o.gap, o.deriver.Safe()+1, o.store.Floor(head.Number.Uint64()))
	// L1 events wait for the step.
	for range o.limit {
		hash, ok, err := o.chain.CanonicalHash(ctx, n)
		if err == nil && !ok {
			o.gap = 0
			return
		}
		if err == nil {
			err = o.refill(ctx, n, hash)
		}
		if err != nil {
			o.gap = n
			if ctx.Err() == nil {
				o.logger.Debug("[eez] sequencer announcement still not kept", "event", "eez.node.follower.announcement.missing", "number", n, "err", err)
			}
			return
		}
		n++
	}
	o.gap = n
}

// refill keeps the announcement of block number with hash, taken without
// its announcement kept, unless kept since.
func (o *overlay) refill(ctx context.Context, number uint64, hash common.Hash) error {
	if env, err := o.store.Get(ctx, number, hash); err != nil || env != nil {
		return err
	}
	env, err := o.seq.Announcement(ctx, number)
	if err != nil {
		return err
	}
	if env == nil {
		return errUnannounced
	}
	if _, err := announce.Verify(env, o.chainID, hash); err != nil {
		return err
	}
	return o.store.Put(ctx, number, hash, env)
}

// reject keeps the overlay off the sequencer head hash until the sequencer
// moves on, instead of fetching and executing the same blocks every step.
func (o *overlay) reject(ctx context.Context, number uint64, hash common.Hash, err error) error {
	if ctx.Err() != nil {
		return err
	}
	o.last = hash
	o.logger.Warn("[eez] sequencer head rejected", "event", "eez.node.follower.head.rejected", "number", number, "hash", hash, "err", err)
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
