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

	"github.com/erigontech/erigon/common/log/v3"
)

// BlockNum and StateVersionID must come from the same execution view.
type BlockState struct {
	BlockNum       uint64
	StateVersionID uint64
}

type currentBlockReader func(ctx context.Context) (BlockState, error)

type BlockTracker struct {
	logger             log.Logger
	blockListener      *BlockListener
	blockChangeCond    *sync.Cond
	currentBlockNum    uint64
	stopped            bool
	currentBlockReader currentBlockReader
}

func NewBlockTracker(logger log.Logger, blockListener *BlockListener, blockReader currentBlockReader) *BlockTracker {
	var blockChangeMu sync.Mutex
	return &BlockTracker{
		logger:             logger,
		blockListener:      blockListener,
		blockChangeCond:    sync.NewCond(&blockChangeMu),
		currentBlockReader: blockReader,
	}
}

func (bt *BlockTracker) Run(ctx context.Context) error {
	defer bt.logger.Info("block tracker stopped")
	bt.logger.Info("running block tracker")

	defer func() {
		// make sure we wake up all waiters upon getting stopped
		bt.blockChangeCond.L.Lock()
		bt.stopped = true
		bt.blockChangeCond.Broadcast()
		bt.blockChangeCond.L.Unlock()
	}()

	ctx, cancel := context.WithCancel(ctx)
	blockEventC := make(chan BlockEvent, 1)
	var initialState BlockState
	unregisterBlockEventObserver := bt.blockListener.registerObserver(func(blockEvent BlockEvent) {
		select {
		case <-ctx.Done(): // no-op
		case blockEventC <- blockEvent:
		}
	}, func(replay BlockEvent) {
		initialState = BlockState{BlockNum: replay.LatestBlockNum, StateVersionID: replay.StateVersionID}
	})
	defer unregisterBlockEventObserver()
	defer cancel() // make sure we release the observer before unregistering to avoid leaks/deadlocks

	committedState, err := bt.currentBlockReader(ctx)
	if err != nil {
		return err
	}
	// State versions order execution views across unwinds; block heights do not.
	// The replay may also be newer than committed state because publication can precede commit.
	if committedState.StateVersionID >= initialState.StateVersionID {
		initialState = committedState
	}
	bt.logger.Debug("block tracker setting initial block num", "blockNum", initialState.BlockNum)
	bt.blockChangeCond.L.Lock()
	bt.currentBlockNum = initialState.BlockNum
	bt.blockChangeCond.Broadcast()
	bt.blockChangeCond.L.Unlock()

	stateVersionID := initialState.StateVersionID
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case blockEvent := <-blockEventC:
			if blockEvent.StateVersionID < stateVersionID {
				continue
			}
			stateVersionID = blockEvent.StateVersionID
			bt.logger.Debug("block tracker got block event", "blockNum", blockEvent.LatestBlockNum)
			bt.blockChangeCond.L.Lock()
			bt.currentBlockNum = blockEvent.LatestBlockNum
			bt.blockChangeCond.Broadcast()
			bt.blockChangeCond.L.Unlock()
		}
	}
}

func (bt *BlockTracker) Wait(ctx context.Context, blockNum uint64) error {
	done := make(chan struct{})
	go func() {
		defer close(done)

		bt.blockChangeCond.L.Lock()
		defer bt.blockChangeCond.L.Unlock()

		for bt.currentBlockNum < blockNum && !bt.stopped && ctx.Err() == nil {
			bt.blockChangeCond.Wait()
		}
	}()

	select {
	case <-ctx.Done():
		// note the below will wake up all waiters prematurely, but thanks to the for loop condition
		// in the waiting goroutine the ones that still need to wait will go back to sleep. Holding the
		// lock keeps the broadcast out of the gap between that condition and the wait.
		bt.blockChangeCond.L.Lock()
		bt.blockChangeCond.Broadcast()
		bt.blockChangeCond.L.Unlock()
		<-done
	case <-done:
	}

	bt.blockChangeCond.L.Lock()
	defer bt.blockChangeCond.L.Unlock()
	switch {
	case bt.currentBlockNum >= blockNum:
		return nil
	case ctx.Err() != nil:
		return ctx.Err()
	default:
		return errors.New("block tracker stopped")
	}
}
