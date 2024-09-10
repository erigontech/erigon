package bridge

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/turbo/services"
)

type bridgeReader interface {
	Events(ctx context.Context, blockNum uint64) ([]*types.Message, error)
	EventTxnLookup(ctx context.Context, borTxHash libcommon.Hash) (uint64, bool, error)
}

var APIVersion = &typesproto.VersionReply{Major: 1, Minor: 0, Patch: 0}

type BackendServer struct {
	remoteproto.UnimplementedBridgeBackendServer // must be embedded to have forward compatible implementations.

	ctx          context.Context
	db           kv.RoDB
	bridgeReader bridgeReader
}

func NewBackendServer(ctx context.Context, db kv.RoDB, blockReader services.FullBlockReader, bridgeReader bridgeReader) *BackendServer {
	return &BackendServer{
		ctx:          ctx,
		db:           db,
		bridgeReader: bridgeReader,
	}
}

func (b *BackendServer) Version(ctx context.Context, in *emptypb.Empty) (*typesproto.VersionReply, error) {
	return APIVersion, nil
}

func (b *BackendServer) BorTxnLookup(ctx context.Context, in *typesproto.BorTxnLookupRequest) (*typesproto.BorTxnLookupReply, error) {
	blockNum, ok, err := b.bridgeReader.EventTxnLookup(ctx, gointerfaces.ConvertH256ToHash(in.BorTxHash))
	if err != nil {
		return nil, err
	}

	return &typesproto.BorTxnLookupReply{
		Present:     ok,
		BlockNumber: blockNum,
	}, nil
}

func (b *BackendServer) BorEvents(ctx context.Context, in *typesproto.BorEventsRequest) (*typesproto.BorEventsReply, error) {
	events, err := b.bridgeReader.Events(ctx, in.BlockNum)
	if err != nil {
		return nil, err
	}

	eventsRaw := make([][]byte, len(events))
	for i, event := range events {
		eventsRaw[i] = event.Data()
	}

	return &typesproto.BorEventsReply{
		EventRlps: eventsRaw,
	}, nil
}
