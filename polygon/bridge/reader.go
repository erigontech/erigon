package bridge

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/polygoncommon"
)

type Reader struct {
	store              Store
	logger             log.Logger
	stateClientAddress libcommon.Address
}

func AssembleReader(ctx context.Context, dataDir string, logger log.Logger, stateReceiverContractAddress string, rwTxLimit int64) (*Reader, error) {
	bridgeDB := polygoncommon.NewDatabase(dataDir, kv.PolygonBridgeDB, databaseTablesCfg, logger, true /* accede */, rwTxLimit)
	bridgeStore := NewStore(bridgeDB)

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

type RemoteReader struct {
	client  remote.BridgeBackendClient
	logger  log.Logger
	version gointerfaces.Version
}

func NewRemoteReader(client remote.BridgeBackendClient) (*RemoteReader, error) {
	return &RemoteReader{
		client:  client,
		logger:  log.New("remote_service", "bridge"),
		version: gointerfaces.VersionFromProto(APIVersion),
	}, nil
}

func (r *RemoteReader) Events(ctx context.Context, blockNum uint64) ([]*types.Message, error) {
	reply, err := r.client.BorEvents(ctx, &remote.BorEventsRequest{BlockNum: blockNum})
	if err != nil {
		return nil, err
	}
	if reply == nil {
		return nil, nil
	}

	stateReceiverContractAddress := libcommon.HexToAddress(reply.StateReceiverContractAddress)
	result := make([]*types.Message, len(reply.EventRlps))
	for i, event := range reply.EventRlps {
		result[i] = messageFromData(stateReceiverContractAddress, event)
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
	return
}

func (r *RemoteReader) EnsureVersionCompatibility() bool {
	versionReply, err := r.client.Version(context.Background(), &emptypb.Empty{}, grpc.WaitForReady(true))
	if err != nil {
		r.logger.Error("getting Version", "err", err)
		return false
	}
	if !gointerfaces.EnsureVersion(r.version, versionReply) {
		r.logger.Error("incompatible interface versions", "client", r.version.String(),
			"server", fmt.Sprintf("%d.%d.%d", versionReply.Major, versionReply.Minor, versionReply.Patch))
		return false
	}
	r.logger.Info("interfaces compatible", "client", r.version.String(),
		"server", fmt.Sprintf("%d.%d.%d", versionReply.Major, versionReply.Minor, versionReply.Patch))
	return true
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
