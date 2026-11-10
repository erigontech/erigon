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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

// headFunnel is the local chain as the follower writes it. Every write that
// can move the head goes through it, so once open it hands sink each head a
// write leaves, whichever writer made it. Only Run's goroutine writes.
type headFunnel struct {
	deriver.Chain
	sink   func(common.Hash)
	logger log.Logger
	open   bool
	// last is the head handed last.
	last common.Hash
}

func (c *headFunnel) Extend(ctx context.Context, parent *types.Header, in deriver.HeaderInputs, txs []types.Transaction, safe, finalized common.Hash) (*types.BlockWithReceipts, error) {
	b, err := c.Chain.Extend(ctx, parent, in, txs, safe, finalized)
	c.wrote(ctx)
	return b, err
}

func (c *headFunnel) Build(ctx context.Context, in deriver.HeaderInputs, txs txnprovider.TxnProvider, safe, finalized common.Hash) (*types.BlockWithReceipts, error) {
	b, err := c.Chain.Build(ctx, in, txs, safe, finalized)
	c.wrote(ctx)
	return b, err
}

func (c *headFunnel) ForkChoice(ctx context.Context, head, safe, finalized common.Hash) error {
	err := c.Chain.ForkChoice(ctx, head, safe, finalized)
	c.wrote(ctx)
	return err
}

// wrote hands sink the head a write left. It is read back rather than taken
// from the write: a failed write may have moved the head to the parent it
// builds on, and a fork choice that succeeds may leave the head where it was.
func (c *headFunnel) wrote(ctx context.Context) {
	if !c.open {
		return
	}
	h, err := c.Chain.Head(context.WithoutCancel(ctx))
	if err != nil {
		c.logger.Warn("[eez] head unreadable after a write", "err", err)
		return
	}
	c.hand(h.Hash())
}

// start opens the funnel and hands sink the head boot ended at.
func (c *headFunnel) start(ctx context.Context) {
	c.open = true
	c.wrote(ctx)
}

func (c *headFunnel) hand(head common.Hash) {
	if head == c.last {
		return
	}
	c.last = head
	c.sink(head)
}
