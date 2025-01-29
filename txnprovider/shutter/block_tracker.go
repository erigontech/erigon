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
	terminated      bool
}

func NewBlockTracker(logger log.Logger, blockListener BlockListener) *BlockTracker {
	blockChangeMu := sync.Mutex{}
	return &BlockTracker{
		logger:          logger,
		blockListener:   blockListener,
		blockChangeMu:   &blockChangeMu,
		blockChangeCond: sync.NewCond(&blockChangeMu),
	}
}

func (bt BlockTracker) Run(ctx context.Context) error {
	bt.logger.Info("running block tracker")

	defer func() {
		// in case of errs make sure we wake up all waiters
		bt.blockChangeMu.Lock()
		bt.terminated = true
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
			bt.currentBlockNum = blockEvent.BlockNum
			bt.blockChangeCond.Broadcast()
			bt.blockChangeMu.Unlock()
		}
	}
}

func (bt BlockTracker) WaitUntil(blockNum uint64) error {
	bt.blockChangeMu.Lock()
	defer bt.blockChangeMu.Unlock()

	for bt.currentBlockNum < blockNum {
		if bt.terminated {
			return errors.New("block observer terminated")
		}

		bt.blockChangeCond.Wait()
	}

	return nil
}
