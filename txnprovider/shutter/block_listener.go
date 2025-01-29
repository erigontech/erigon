package shutter

import (
	"context"

	"github.com/erigontech/erigon-lib/event"
	"github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/log/v3"
)

type BlockListener struct {
	logger             log.Logger
	stateChangesClient stateChangesClient
	events             *event.Observers[BlockEvent]
}

func NewBlockListener(logger log.Logger, stateChangesClient stateChangesClient) BlockListener {
	return BlockListener{
		logger:             logger,
		stateChangesClient: stateChangesClient,
		events:             event.NewObservers[BlockEvent](),
	}
}

func (bl BlockListener) RegisterObserver(o event.Observer[BlockEvent]) event.UnregisterFunc {
	return bl.events.Register(o)
}

func (bl BlockListener) Run(ctx context.Context) error {
	bl.logger.Info("running block listener")

	sub, err := bl.stateChangesClient.StateChanges(ctx, &remoteproto.StateChangeRequest{})
	if err != nil {
		return err
	}

	// note the changes stream is ctx-aware so Recv should terminate with err if ctx gets done
	var batch *remoteproto.StateChangeBatch
	for batch, err = sub.Recv(); err != nil; batch, err = sub.Recv() {
		if batch == nil || len(batch.ChangeBatch) == 0 {
			continue
		}

		latestChange := batch.ChangeBatch[len(batch.ChangeBatch)-1]
		blockEvent := BlockEvent{
			BlockNum: latestChange.BlockHeight,
			Unwind:   latestChange.Direction == remoteproto.Direction_UNWIND,
		}

		bl.events.NotifySync(blockEvent)
	}

	return err
}

type BlockEvent struct {
	BlockNum uint64
	Unwind   bool
}
