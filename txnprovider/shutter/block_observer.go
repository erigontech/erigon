package shutter

import (
	"context"
	"errors"
	"sync"

	"google.golang.org/grpc"

	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
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

	changes, err := bo.stateChangesClient.StateChanges(ctx, &remote.StateChangeRequest{})
	if err != nil {
		return err
	}

	// note the changes stream is ctx-aware so changes.Recv() should terminate with err if ctx gets done
	var batch *remote.StateChangeBatch
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
	StateChanges(ctx context.Context, in *remote.StateChangeRequest, opts ...grpc.CallOption) (remote.KV_StateChangesClient, error)
}
