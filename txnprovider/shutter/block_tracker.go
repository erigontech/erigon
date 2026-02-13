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

type currentBlockNumReader func(ctx context.Context) (*uint64, error)

type BlockTracker struct {
	logger                log.Logger
	blockListener         *BlockListener
	mu                    sync.Mutex
	currentBlockNum       uint64
	stopped               bool
	currentBlockNumReader currentBlockNumReader
	// blockChange is closed and replaced each time the block number changes or the tracker stops.
	// Using a channel instead of sync.Cond for compatibility with testing/synctest.
	blockChange chan struct{}
}

func NewBlockTracker(logger log.Logger, blockListener *BlockListener, bnReader currentBlockNumReader) *BlockTracker {
	return &BlockTracker{
		logger:                logger,
		blockListener:         blockListener,
		currentBlockNumReader: bnReader,
		blockChange:           make(chan struct{}),
	}
}

// broadcast closes the current blockChange channel and replaces it with a new one,
// waking up all goroutines waiting on it.
func (bt *BlockTracker) broadcast() {
	close(bt.blockChange)
	bt.blockChange = make(chan struct{})
}

func (bt *BlockTracker) Run(ctx context.Context) error {
	defer bt.logger.Info("block tracker stopped")
	bt.logger.Info("running block tracker")

	defer func() {
		// make sure we wake up all waiters upon getting stopped
		bt.mu.Lock()
		bt.stopped = true
		bt.broadcast()
		bt.mu.Unlock()
	}()

	ctx, cancel := context.WithCancel(ctx)
	blockEventC := make(chan BlockEvent)
	unregisterBlockEventObserver := bt.blockListener.RegisterObserver(func(blockEvent BlockEvent) {
		select {
		case <-ctx.Done(): // no-op
		case blockEventC <- blockEvent:
		}
	})
	defer unregisterBlockEventObserver()
	defer cancel() // make sure we release the observer before unregistering to avoid leaks/deadlocks

	bn, err := bt.currentBlockNumReader(ctx)
	if err != nil {
		return err
	}
	if bn != nil {
		bt.logger.Debug("block tracker setting initial block num", "blockNum", *bn)
		bt.mu.Lock()
		bt.currentBlockNum = *bn
		bt.broadcast()
		bt.mu.Unlock()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case blockEvent := <-blockEventC:
			bt.logger.Debug("block tracker got block event", "blockNum", blockEvent.LatestBlockNum)
			bt.mu.Lock()
			bt.currentBlockNum = blockEvent.LatestBlockNum
			bt.broadcast()
			bt.mu.Unlock()
		}
	}
}

func (bt *BlockTracker) Wait(ctx context.Context, blockNum uint64) error {
	for {
		bt.mu.Lock()
		curBlock := bt.currentBlockNum
		stopped := bt.stopped
		ch := bt.blockChange
		bt.mu.Unlock()

		if curBlock >= blockNum {
			return nil
		}
		if stopped {
			return errors.New("block tracker stopped")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}
