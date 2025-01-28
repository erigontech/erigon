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

	"google.golang.org/grpc"

	"github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
)

type BlockObserver struct {
	stateChangesClient StateChangesClient
	blockChangeMu      *sync.Mutex
	blockChangeCond    *sync.Cond
	currentBlockNum    uint64
	terminated         bool
}

func NewBlockObserver(stateChangesClient StateChangesClient) *BlockObserver {
	blockChangeMu := sync.Mutex{}
	return &BlockObserver{
		stateChangesClient: stateChangesClient,
		blockChangeMu:      &blockChangeMu,
		blockChangeCond:    sync.NewCond(&blockChangeMu),
	}
}

func (bo BlockObserver) Run(ctx context.Context) error {
	defer func() {
		// in case of errs make sure we wake up all waiters
		bo.blockChangeMu.Lock()
		bo.terminated = true
		bo.blockChangeCond.Broadcast()
		bo.blockChangeMu.Unlock()
	}()

	changes, err := bo.stateChangesClient.StateChanges(ctx, &remoteproto.StateChangeRequest{})
	if err != nil {
		return err
	}

	// note the changes stream is ctx-aware so changes.Recv() should terminate with err if ctx gets done
	var batch *remoteproto.StateChangeBatch
	for batch, err = changes.Recv(); err != nil; batch, err = changes.Recv() {
		if batch == nil || len(batch.ChangeBatch) == 0 {
			continue
		}

		bo.blockChangeMu.Lock()
		bo.currentBlockNum = batch.ChangeBatch[len(batch.ChangeBatch)-1].BlockHeight
		bo.blockChangeCond.Broadcast()
		bo.blockChangeMu.Unlock()
	}

	return err
}

func (bo BlockObserver) WaitUntil(blockNum uint64) error {
	bo.blockChangeMu.Lock()
	defer bo.blockChangeMu.Unlock()

	for bo.currentBlockNum < blockNum {
		if bo.terminated {
			return errors.New("block observer terminated")
		}

		bo.blockChangeCond.Wait()
	}

	return nil
}

type StateChangesClient interface {
	StateChanges(ctx context.Context, in *remoteproto.StateChangeRequest, opts ...grpc.CallOption) (remoteproto.KV_StateChangesClient, error)
}
