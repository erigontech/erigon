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
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
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
	_ StateReader  = (*Local)(nil)
	_ HeadNotifier = (*Local)(nil)
)

// NewLocal returns the reader of the L1 chain config, reading its blocks
// through the block reader blocks from db and its receipts from receipts, and
// executing calls under the rules of engine; the chain's execution announces
// its heads on events.
func NewLocal(db kv.TemporalRoDB, blocks dbservices.FullBlockReader, receipts *receipts.Generator, engine rules.EngineReader, config *chain.Config, events *shards.Events) *Local {
	return &Local{Native: NewNative(db, blocks, receipts, engine, config), events: events}
}

// commitWait bounds how long Heads holds a header event until the reader
// reads the announced head: the chain announces a head before it commits its
// block.
const commitWait = 2 * time.Second

// Heads delivers a value for each header event of the chain once the reader
// reads the announced head, or after commitWait, coalescing those not yet
// received; the channel closes when ctx ends.
func (r *Local) Heads(ctx context.Context) (<-chan struct{}, error) {
	sub, unsubscribe := r.events.AddHeaderSubscription()
	heads := make(chan struct{}, 1)
	go func() {
		defer close(heads)
		defer unsubscribe()
		for {
			var headers [][]byte
			select {
			case <-ctx.Done():
				return
			case headers = <-sub:
			}
			r.awaitHead(ctx, sub, headers)
			select {
			case heads <- struct{}{}:
			default:
			}
		}
	}()
	return heads, nil
}

// awaitHead waits, up to commitWait, until the reader's head is the head
// announced last: the last of headers, or of a later event on sub.
func (r *Local) awaitHead(ctx context.Context, sub <-chan [][]byte, headers [][]byte) {
	want, ok := announced(headers)
	ctx, cancel := context.WithTimeout(ctx, commitWait)
	defer cancel()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		if head, err := r.Head(ctx); ok && err == nil && head.Hash == want {
			return
		}
		select {
		case <-ctx.Done():
			return
		case later := <-sub:
			if h, laterOK := announced(later); laterOK {
				want, ok = h, true
			}
		case <-poll.C:
		}
	}
}

// announced returns the hash of the last of headers, the head a header event
// announces.
func announced(headers [][]byte) (common.Hash, bool) {
	if len(headers) == 0 {
		return common.Hash{}, false
	}
	var h types.Header
	if err := rlp.DecodeBytes(headers[len(headers)-1], &h); err != nil {
		return common.Hash{}, false
	}
	return h.Hash(), true
}
