package bridge

import (
	"context"
	"errors"
	"fmt"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/polygon/polygoncommon"
)

type Reader struct {
	store              Store
	logger             log.Logger
	stateClientAddress libcommon.Address
}

func AssembleReader(ctx context.Context, dataDir string, logger log.Logger, borConfig *borcfg.BorConfig) (*Reader, error) {
	bridgeDB := polygoncommon.NewDatabase(dataDir, kv.PolygonBridgeDB, databaseTablesCfg, logger)
	bridgeStore := NewStore(bridgeDB)

	err := bridgeStore.Prepare(ctx)
	if err != nil {
		return nil, err
	}

	return NewReader(bridgeStore, logger, borConfig.StateReceiverContract), nil
}

func NewReader(store Store, logger log.Logger, stateReceiverContractAddress string) *Reader {
	return &Reader{
		store:              store,
		logger:             logger,
		stateClientAddress: libcommon.HexToAddress(stateReceiverContractAddress),
	}
}

// Events returns all sync events at blockNum
func (r *Reader) Events(ctx context.Context, blockNum uint64) ([]*types.Message, error) {
	start, end, err := r.store.BlockEventIDsRange(ctx, blockNum)
	if err != nil {
		if errors.Is(err, ErrEventIDRangeNotFound) {
			return nil, nil
		}

		return nil, err
	}

	eventsRaw := make([]*types.Message, 0, end-start+1)

	// get events from DB
	events, err := r.store.Events(ctx, start, end+1)
	if err != nil {
		return nil, err
	}

	r.logger.Debug(bridgeLogPrefix(fmt.Sprintf("got %v events for block %v", len(events), blockNum)))

	// convert to message
	for _, event := range events {
		msg := types.NewMessage(
			state.SystemAddress,
			&r.stateClientAddress,
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
