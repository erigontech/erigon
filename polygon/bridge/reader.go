package bridge

import (
	"context"
	"errors"
	"fmt"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
)

type Reader struct {
	store              Store
	logger             log.Logger
	stateClientAddress libcommon.Address
}

func AssembleReader(ctx context.Context, dataDir string, logger log.Logger, stateReceiverContractAddress string) (*Reader, error) {
	bridgeStore := NewMdbxStore(dataDir, logger, true)

	err := bridgeStore.Prepare(ctx)
	if err != nil {
		return nil, err
	}

	return NewReader(bridgeStore, logger, stateReceiverContractAddress), nil
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
	start, end, err := r.store.BlockEventIdsRange(ctx, blockNum)
	if err != nil {
		if errors.Is(err, ErrEventIdRangeNotFound) {
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
	return r.store.EventLookup(ctx, borTxHash)
}

func (r *Reader) Close() {
	r.store.Close()
}

type RemoteReader struct {
	client                       remote.ETHBACKENDClient
	stateReceiverContractAddress libcommon.Address
}

func NewRemoteReader(client remote.ETHBACKENDClient, stateReceiverContractAddress string) *RemoteReader {
	return &RemoteReader{
		client:                       client,
		stateReceiverContractAddress: libcommon.HexToAddress(stateReceiverContractAddress),
	}
}

func (r *RemoteReader) Events(ctx context.Context, blockNum uint64) ([]*types.Message, error) {
	reply, err := r.client.BorEvents(ctx, &remote.BorEventsRequest{BlockNum: blockNum})
	if err != nil {
		return nil, err
	}
	if reply == nil {
		return nil, nil
	}

	result := make([]*types.Message, len(reply.EventRlps))
	for i, event := range reply.EventRlps {
		result[i] = messageFromData(r.stateReceiverContractAddress, event)
	}

	return result, nil
}

func (r *RemoteReader) EventTxnLookup(ctx context.Context, borTxHash libcommon.Hash) (uint64, bool, error) {
	reply, err := r.client.BorTxnLookup(ctx, &remote.BorTxnLookupRequest{BorTxHash: gointerfaces.ConvertHashToH256(borTxHash)})
	if err != nil {
		return 0, false, err
	}
	if reply == nil {
		return 0, false, nil
	}

	return reply.BlockNumber, reply.Present, nil
}

// Close implements bridge.ReaderService. It's a noop as there is no attached store.
func (r *RemoteReader) Close() {
}

func messageFromData(to libcommon.Address, data []byte) *types.Message {
	msg := types.NewMessage(
		state.SystemAddress,
		&to,
		0, u256.Num0,
		core.SysCallGasLimit,
		u256.Num0,
		nil, nil,
		data, nil, false,
		true,
		nil,
	)

	return &msg
}
