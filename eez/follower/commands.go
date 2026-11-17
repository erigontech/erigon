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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

var (
	// ErrStaleParent reports a command for a head that has since moved.
	ErrStaleParent = errors.New("parent is no longer the head")
	// ErrBelowSafe reports a head move below the safe block.
	ErrBelowSafe = errors.New("head would move below the safe block")
	// ErrNotCanonical reports a block that is not on the canonical chain.
	ErrNotCanonical = errors.New("not a canonical block")
)

// command is work on the local chain that Run performs between its own
// steps, so that one goroutine drives the execution module.
type command struct {
	run  func(ctx context.Context) error
	done chan error
}

// do has Run perform fn and returns its error. ctx bounds the wait while Run
// derives or boots. Once Run takes fn, do waits for it: a caller leaving
// earlier would miss a head fn moved and share fn's writes with it.
func (f *Follower) do(ctx context.Context, fn func(ctx context.Context) error) error {
	c := command{run: fn, done: make(chan error, 1)}
	select {
	case f.commands <- c:
	case <-ctx.Done():
		return ctx.Err()
	}
	return <-c.done
}

// CommitHook runs on Run's goroutine once the head is the parent of the
// block a command commits, before the block commits. The function it
// returns, when set, takes the block's header once it is the head, before
// derivation resumes.
type CommitHook func() func(*types.Header)

// perform runs c; the head moves it makes are the sequencer's.
func (f *Follower) perform(ctx context.Context, c command) {
	defer f.moving(HeadSequencer)()
	c.done <- c.run(ctx)
}

// Produce builds the block of inputs in on the head, with the transactions
// of txs that fit, and makes it the head, running hook when set. A head
// other than in.ParentHash fails it with ErrStaleParent.
func (f *Follower) Produce(ctx context.Context, in deriver.HeaderInputs, txs txnprovider.TxnProvider, hook CommitHook) (*types.BlockWithReceipts, error) {
	var built *types.BlockWithReceipts
	err := f.do(ctx, func(ctx context.Context) error {
		if err := f.requireHead(ctx, in.ParentHash); err != nil {
			return err
		}
		safe, finalized, err := f.deriver.Anchors(ctx)
		if err != nil {
			return err
		}
		var committed func(*types.Header)
		if hook != nil {
			committed = hook()
		}
		if built, err = f.chain.Build(ctx, in, txs, safe, finalized); err == nil && committed != nil {
			committed(built.Block.Header())
		}
		return err
	})
	return built, err
}

// Insert stores blocks, the first a child of the head, and makes the last
// one the head, running hook when set: on the first one's parent and with
// the last one's header. A head other than the first block's parent fails
// it with ErrStaleParent.
func (f *Follower) Insert(ctx context.Context, blocks []*types.Block, hook CommitHook) error {
	if len(blocks) == 0 {
		return nil
	}
	return f.do(ctx, func(ctx context.Context) error {
		if err := f.requireHead(ctx, blocks[0].ParentHash()); err != nil {
			return err
		}
		var committed func(*types.Header)
		if hook != nil {
			committed = hook()
		}
		if err := f.chain.Insert(ctx, blocks); err != nil {
			return err
		}
		last := blocks[len(blocks)-1]
		if err := f.deriver.ForkChoiceTo(ctx, last.Hash()); err != nil {
			return err
		}
		if committed != nil {
			committed(last.Header())
		}
		return nil
	})
}

// HeadTo moves the head back to ancestor, a canonical block at or above the
// safe block.
func (f *Follower) HeadTo(ctx context.Context, ancestor common.Hash) error {
	return f.do(ctx, func(ctx context.Context) error {
		n, ok, err := f.chain.CanonicalNumber(ctx, ancestor)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("head to %x: %w", ancestor, ErrNotCanonical)
		}
		if safe := f.deriver.Safe(); n < safe {
			return fmt.Errorf("%w: block %d, safe %d", ErrBelowSafe, n, safe)
		}
		return f.deriver.ForkChoiceTo(ctx, ancestor)
	})
}

// HeadBefore moves the head back to the parent of block and returns it.
// ErrBelowSafe reports a block at or below the safe block, ErrNotCanonical
// one another block has replaced.
func (f *Follower) HeadBefore(ctx context.Context, block *types.Header) (*types.Header, error) {
	var parent *types.Header
	err := f.do(ctx, func(ctx context.Context) error {
		n := block.Number.Uint64()
		if safe := f.deriver.Safe(); n <= safe {
			return fmt.Errorf("%w: block %d, safe %d", ErrBelowSafe, n-1, safe)
		}
		hash, ok, err := f.chain.CanonicalHash(ctx, n)
		if err != nil {
			return err
		}
		if !ok || hash != block.Hash() {
			return fmt.Errorf("head before block %d (%x): %w", n, block.Hash(), ErrNotCanonical)
		}
		// Read first: nothing fails once the head has moved.
		if parent, err = f.chain.HeaderByHash(ctx, block.ParentHash); err != nil {
			return err
		}
		if parent == nil {
			return fmt.Errorf("parent %x of block %d: %w", block.ParentHash, n, ErrNotCanonical)
		}
		return f.deriver.ForkChoiceTo(ctx, block.ParentHash)
	})
	return parent, err
}

// Head returns the header of the head block.
func (f *Follower) Head(ctx context.Context) (*types.Header, error) { return f.chain.Head(ctx) }

// CanonicalHash returns the hash of the canonical block at number.
func (f *Follower) CanonicalHash(ctx context.Context, number uint64) (common.Hash, bool, error) {
	return f.chain.CanonicalHash(ctx, number)
}

// Settled returns the block the latest derived settlement ended at, as of
// Run's latest step.
func (f *Follower) Settled() uint64 { return f.Status().Cursor }

func (f *Follower) requireHead(ctx context.Context, hash common.Hash) error {
	head, err := f.chain.Head(ctx)
	if err != nil {
		return err
	}
	if head.Hash() != hash {
		return fmt.Errorf("%w: head %d is %x, not %x", ErrStaleParent, head.Number.Uint64(), head.Hash(), hash)
	}
	return nil
}
