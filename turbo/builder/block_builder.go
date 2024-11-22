// Copyright 2024 The Erigon Authors
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

package builder

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/types"
)

type BlockBuilderFunc func(param *core.BlockBuilderParameters, interrupt *int32) (*types.BlockWithReceipts, error)

// BlockBuilder wraps a goroutine that builds Proof-of-Stake payloads (PoS "mining")
type BlockBuilder struct {
	interrupt int32
	syncCond  *sync.Cond
	result    *types.BlockWithReceipts
	err       error
}

func NewBlockBuilder(build BlockBuilderFunc, param *core.BlockBuilderParameters) *BlockBuilder {
	builder := new(BlockBuilder)
	builder.syncCond = sync.NewCond(new(sync.Mutex))

	go func() {
		log.Info("Building block...")
		t := time.Now()
		result, err := build(param, &builder.interrupt)
		if err != nil {
			log.Warn("Failed to build a block", "err", err)
		} else {
			block := result.Block
			reqLenStr := "nil"
			if len(result.Requests) == 3 {
				reqLenStr = fmt.Sprint("Deposit Requests", len(result.Requests[0].RequestData), "Withdrawal Requests", len(result.Requests[1].RequestData), "Consolidation Requests", len(result.Requests[2].RequestData))
			}
			log.Info("Built block", "hash", block.Hash(), "height", block.NumberU64(), "txs", len(block.Transactions()), "executionRequests", len(result.Requests), "Requests", reqLenStr, "gas used %", 100*float64(block.GasUsed())/float64(block.GasLimit()), "time", time.Since(t))
		}

		builder.syncCond.L.Lock()
		defer builder.syncCond.L.Unlock()
		builder.result = result
		builder.err = err
		builder.syncCond.Broadcast()
	}()

	return builder
}

func (b *BlockBuilder) Stop() (*types.BlockWithReceipts, error) {
	atomic.StoreInt32(&b.interrupt, 1)

	b.syncCond.L.Lock()
	defer b.syncCond.L.Unlock()
	for b.result == nil && b.err == nil {
		b.syncCond.Wait()
	}

	return b.result, b.err
}

func (b *BlockBuilder) Block() *types.Block {
	b.syncCond.L.Lock()
	defer b.syncCond.L.Unlock()

	if b.result == nil {
		return nil
	}
	return b.result.Block
}
