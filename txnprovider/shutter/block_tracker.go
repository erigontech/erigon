// Copyright 2025 The Erigon Authors
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

package shutter

import (
	"context"
	"errors"
	"sync"

	"github.com/erigontech/erigon-lib/log/v3"
)

type BlockTracker struct {
	logger          log.Logger
	blockListener   BlockListener
	blockChangeMu   *sync.Mutex
	blockChangeCond *sync.Cond
	currentBlockNum uint64
	stopped         bool
}

func NewBlockTracker(logger log.Logger, blockListener BlockListener) BlockTracker {
	blockChangeMu := sync.Mutex{}
	return BlockTracker{
		logger:          logger,
		blockListener:   blockListener,
		blockChangeMu:   &blockChangeMu,
		blockChangeCond: sync.NewCond(&blockChangeMu),
	}
}

func (bt BlockTracker) Run(ctx context.Context) error {
	defer bt.logger.Info("block tracker stopped")
	bt.logger.Info("running block tracker")

	defer func() {
		// make sure we wake up all waiters upon getting stopped
		bt.blockChangeMu.Lock()
		bt.stopped = true
		bt.blockChangeCond.Broadcast()
		bt.blockChangeMu.Unlock()
	}()

	blockEventC := make(chan BlockEvent)
	unregisterBlockEventObserver := bt.blockListener.RegisterObserver(func(blockEvent BlockEvent) {
		select {
		case <-ctx.Done(): // no-op
		case blockEventC <- blockEvent:
		}
	})

	defer unregisterBlockEventObserver()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case blockEvent := <-blockEventC:
			bt.blockChangeMu.Lock()
			bt.currentBlockNum = blockEvent.LatestBlockNum
			bt.blockChangeCond.Broadcast()
			bt.blockChangeMu.Unlock()
		}
	}
}

func (bt BlockTracker) Wait(ctx context.Context, blockNum uint64) error {
	done := make(chan struct{})
	go func() {
		defer close(done)

		bt.blockChangeMu.Lock()
		defer bt.blockChangeMu.Unlock()

		for bt.currentBlockNum < blockNum && !bt.stopped && ctx.Err() == nil {
			bt.blockChangeCond.Wait()
		}
	}()

	select {
	case <-ctx.Done():
		// note the below will wake up all waiters prematurely, but thanks to the for loop condition
		// in the waiting goroutine the ones that still need to wait will go back to sleep
		bt.blockChangeCond.Broadcast()
		return ctx.Err()
	case <-done:
		bt.blockChangeMu.Lock()
		defer bt.blockChangeMu.Unlock()

		if bt.currentBlockNum < blockNum && bt.stopped {
			return errors.New("block tracker stopped")
		}

		return nil
	}
}
