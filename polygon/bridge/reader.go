package bridge

import (
	"context"
	"fmt"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/polygon/polygoncommon"
)

type Reader struct {
	store Store
	log   log.Logger
}

func AssembleReader(ctx context.Context, dataDir string, logger log.Logger) (*Reader, error) {
	bridgeDB := polygoncommon.NewDatabase(dataDir, kv.PolygonBridgeDB, databaseTablesCfg, logger)
	bridgeStore := NewStore(bridgeDB)

	err := bridgeStore.Prepare(ctx)
	if err != nil {
		return nil, err
	}

	return NewReader(bridgeStore, logger), nil
}

func NewReader(store Store, log log.Logger) *Reader {
	return &Reader{
		store: store,
		log:   log,
	}
}

// Events returns all sync events at blockNum
func (r *Reader) Events(ctx context.Context, blockNum uint64) ([]*types.Message, error) {
	start, end, err := r.store.EventIDRange(ctx, blockNum)
	if err != nil {
		return nil, err
	}

	if end == 0 { // exception for tip processing
		end, err = r.store.LastProcessedEventID(ctx)
		if err != nil {
			return nil, err
		}
	}

	eventsRaw := make([]*types.Message, 0, end-start+1)

	// get events from DB
	events, err := r.store.Events(ctx, start+1, end+1)
	if err != nil {
		return nil, err
	}

	r.log.Debug(bridgeLogPrefix(fmt.Sprintf("got %v events for block %v", len(events), blockNum)))

	address := libcommon.HexToAddress("0x0000000000000000000000000000000000001001")

	// convert to message
	for _, event := range events {
		msg := types.NewMessage(
			state.SystemAddress,
			&address, // TODO: use variable from struct
			0, u256.Num0,
			core.SysCallGasLimit,
			u256.Num0,
			nil, nil,
			event, nil, false,
			true,
			nil,
		)

		eventsRaw = append(eventsRaw, &msg)
	}

	return eventsRaw, nil
}

func (r *Reader) EventTxnLookup(ctx context.Context, borTxHash libcommon.Hash) (uint64, bool, error) {
	return r.store.EventTxnToBlockNum(ctx, borTxHash)
}

func (r *Reader) Close() {
	r.store.Close()
}
