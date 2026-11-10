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
	"context"

	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/node/shards"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
)

// Local is a Native reader of an L1 chain running in the same process.
type Local struct {
	*Native
	events *shards.Events
}

var (
	_ ChainReader  = (*Local)(nil)
	_ HeadNotifier = (*Local)(nil)
)

// NewLocal returns the reader of the L1 chain config, reading its blocks
// through the block reader blocks from db and its receipts from receipts; the
// chain's execution announces its heads on events.
func NewLocal(db kv.TemporalRoDB, blocks dbservices.FullBlockReader, receipts *receipts.Generator, config *chain.Config, events *shards.Events) *Local {
	return &Local{Native: NewNative(db, blocks, receipts, config), events: events}
}

// Heads delivers a value for each header event of the chain, coalescing
// those not yet received; the channel closes when ctx ends.
func (r *Local) Heads(ctx context.Context) (<-chan struct{}, error) {
	sub, unsubscribe := r.events.AddHeaderSubscription()
	heads := make(chan struct{}, 1)
	go func() {
		defer close(heads)
		defer unsubscribe()
		for {
			select {
			case <-ctx.Done():
				return
			case <-sub:
			}
			select {
			case heads <- struct{}{}:
			default:
			}
		}
	}()
	return heads, nil
}
