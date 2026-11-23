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
	"cmp"
	"context"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/post"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/types"
)

// Receipts is the L1 an observer reads.
type Receipts interface {
	Head(ctx context.Context) (*l1.Header, error)
	Receipt(ctx context.Context, hash common.Hash) (*l1.Receipt, error)
	// HasTransaction reports whether the node has the transaction, pooled
	// or in a block.
	HasTransaction(ctx context.Context, hash common.Hash) (bool, error)
}

// slowVerdict is how long a posting waits for its verdict before a warning.
const slowVerdict = 30 * time.Second

// Observer watches postings on L1 until their verdict.
type Observer struct {
	Node     Receipts
	RollupID uint64
	// EEZ is the L1 contract the batches are posted to.
	EEZ common.Address
	// Every is the polling interval.
	Every time.Duration
	// Read bounds a poll's reads, so that a node that stops answering does
	// not hold the watch; zero is DefaultL1Read.
	Read   time.Duration
	Logger log.Logger
}

// Watch returns the verdict on hash, the posting of batch sent as sent,
// whose terminal block is terminal: Settled when its receipt shows the
// rollup reaching terminal, SettledPartial when it shows the rollup moved
// elsewhere, Failed when it shows neither or the posting can no longer land.
// Failed reads give no verdict; Watch ends with ctx.
func (o *Observer) Watch(ctx context.Context, hash common.Hash, batch *protocol.ProofSystemBatch, terminal common.Hash, sent post.Sent) (Resolution, error) {
	slow := time.Now().Add(slowVerdict)
	tick := time.NewTicker(o.Every)
	defer tick.Stop()
	known := false
	for {
		if verdict, ok := o.poll(ctx, hash, batch, terminal, sent, &known); ok {
			return verdict, nil
		}
		if !slow.IsZero() && time.Now().After(slow) {
			o.Logger.Warn("[eez] posting has no verdict yet", "event", "eez.composer.observe.slow", "post", hash,
				"target", sent.Block, "pooled", sent.Pooled)
			slow = time.Time{}
		}
		select {
		case <-ctx.Done():
			return Pending, ctx.Err()
		case <-tick.C:
		}
	}
}

// poll reads once what became of hash, within Read: a node that stops
// answering must not hold the watch, and with it every later posting.
func (o *Observer) poll(ctx context.Context, hash common.Hash, batch *protocol.ProofSystemBatch, terminal common.Hash, sent post.Sent, known *bool) (Resolution, bool) {
	ctx, cancel := context.WithTimeout(ctx, cmp.Or(o.Read, DefaultL1Read))
	defer cancel()
	r, err := o.Node.Receipt(ctx, hash)
	switch {
	case err != nil:
		return Pending, false
	case r != nil:
		return o.judge(r, batch, terminal), true
	}
	if gone, err := o.gone(ctx, hash, sent, known); err != nil || !gone {
		return Pending, false
	}
	// The receipt may land between the read and the check.
	if r, err = o.Node.Receipt(ctx, hash); err != nil {
		return Pending, false
	}
	if r != nil {
		return o.judge(r, batch, terminal), true
	}
	return Failed, true
}

// gone reports whether hash, a posting without a receipt, can no longer
// land. A bundle cannot once L1 passed its block. A pooled posting may land
// later: it cannot once the node, which had it, no longer has it, and the
// next posting takes its nonce. known records that the node had it; a node
// that never showed it is taken as a bundle.
func (o *Observer) gone(ctx context.Context, hash common.Hash, sent post.Sent, known *bool) (bool, error) {
	if sent.Pooled {
		has, err := o.Node.HasTransaction(ctx, hash)
		switch {
		case err != nil:
			return false, err
		case has:
			*known = true
			return false, nil
		case *known:
			return true, nil
		}
	}
	head, err := o.Node.Head(ctx)
	if err != nil {
		return false, err
	}
	return head.Number > sent.Block, nil
}

// judge is the verdict on r, the receipt of the posting of batch.
func (o *Observer) judge(r *l1.Receipt, batch *protocol.ProofSystemBatch, terminal common.Hash) Resolution {
	if r.Status != 1 {
		return Failed
	}
	var logs []*types.Log
	posted := -1
	for _, l := range r.Logs {
		if l.Address != o.EEZ || len(l.Topics) == 0 {
			continue
		}
		if l.Topics[0] == protocol.BatchPostedTopic && posted < 0 {
			posted = int(l.Index)
		}
		logs = append(logs, l)
	}
	if posted < 0 {
		return Failed
	}
	s, err := protocol.AttributeBlock([]protocol.PostedBatch{{Batch: batch, TxIndex: r.TxIndex, LogIndex: uint64(posted)}}, logs, o.RollupID)
	if err != nil {
		o.Logger.Error("[eez] posting's logs do not attribute", "event", "eez.composer.observe.unattributed", "block", r.BlockNumber, "err", err)
		return Failed
	}
	switch {
	case len(s[0].Applied) == 0:
		return Failed
	case s[0].FinalState == terminal:
		return Settled
	}
	return SettledPartial
}
