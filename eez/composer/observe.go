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
	"context"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/types"
)

// Receipts is the L1 an observer reads.
type Receipts interface {
	Head(ctx context.Context) (*l1.Header, error)
	Receipt(ctx context.Context, hash common.Hash) (*l1.Receipt, error)
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
	Every  time.Duration
	Logger log.Logger
}

// Watch returns the verdict on post, the posting of batch for L1 block
// target, whose terminal block is terminal: Settled when its receipt shows
// the rollup reaching terminal, SettledPartial when it shows the rollup
// moved elsewhere, Failed when it shows neither or L1 passed target without
// it. Failed reads give no verdict; Watch ends with ctx.
func (o *Observer) Watch(ctx context.Context, post common.Hash, batch *protocol.ProofSystemBatch, terminal common.Hash, target uint64) (Resolution, error) {
	slow := time.Now().Add(slowVerdict)
	tick := time.NewTicker(o.Every)
	defer tick.Stop()
	for {
		if r, err := o.Node.Receipt(ctx, post); err == nil && r != nil {
			return o.judge(r, batch, terminal), nil
		} else if err == nil {
			// The receipt may land between the read and a head past target.
			if head, err := o.Node.Head(ctx); err == nil && head.Number > target {
				if r, err := o.Node.Receipt(ctx, post); err == nil {
					if r != nil {
						return o.judge(r, batch, terminal), nil
					}
					return Failed, nil
				}
			}
		}
		if !slow.IsZero() && time.Now().After(slow) {
			o.Logger.Warn("[eez] posting has no verdict yet", "event", "eez.composer.observe.slow", "post", post, "target", target)
			slow = time.Time{}
		}
		select {
		case <-ctx.Done():
			return Pending, ctx.Err()
		case <-tick.C:
		}
	}
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
